package shardkv

import (
	"sync"
	"time"
)

func (kv *ShardKV) applyTask() {
	for !kv.killed() {
		select {
		case message := <-kv.applyCh:
			if message.CommandValid {
				kv.mu.Lock()
				// 如果消息已经处理过则直接忽略
				if message.CommandIndex <= kv.lastApplied {
					kv.mu.Unlock()
					continue
				}
				kv.lastApplied = message.CommandIndex

				raftCommand := message.Command.(RaftCommand)
				var opReply *OpReply

				if raftCommand.CmdType == ClientOperation {
					// 取出用户的操作
					op := raftCommand.Data.(Op)
					// 将操作应用到状态机里
					if op.OpType != OpGet && kv.requestDuplicated(op.ClientId, op.SeqId) {
						// 重复的请求
						opReply = kv.duplicateTable[op.ClientId].Reply
					} else {
						shardId := key2shard(op.Key)
						opReply = kv.applyToStateMachine(op, shardId)
						if op.OpType != OpGet {
							kv.duplicateTable[op.ClientId] = LastOperationInfo{
								SeqId: op.SeqId,
								Reply: opReply,
							}
						}
					}
				} else {
					// 关于配置变更的任务
					opReply = kv.handleConfigChangeMessage(raftCommand)
				}

				// 将结果发送出去，只需要 Leader 进行返回结果
				if _, isLeader := kv.rf.GetState(); isLeader {
					notifyCh := kv.getNotifyChannel(message.CommandIndex)
					notifyCh <- opReply
				}

				// 判断是否需要 Snapshot
				if kv.maxraftstate != -1 && kv.rf.GetRaftStateSize() >= kv.maxraftstate {
					kv.makeSnapshot(message.CommandIndex)
				}

				kv.mu.Unlock()
			} else if message.SnapshotValid {
				// 收到了 snapshot 的消息，去恢复状态机状态
				kv.mu.Lock()
				kv.restoreFromSnapshot(message.Snapshot)
				kv.lastApplied = message.SnapshotIndex
				kv.mu.Unlock()
			}
		}
	}
}

// 获取当前配置
func (kv *ShardKV) fetchConfigTask() {
	for !kv.killed() {
		if _, isLeader := kv.rf.GetState(); isLeader {
			kv.mu.Lock()
			// 通过 mck 客户端查找最新的配置
			newConfig := kv.mck.Query(kv.currentConfig.Num + 1)
			// kv.currentConfig = newConfig
			kv.mu.Unlock()

			// 传入 raft 模块进行同步
			kv.ConfigCommand(RaftCommand{
				ConfigChange,
				newConfig,
			}, &OpReply{})
			time.Sleep(FetchConfigInterval)
		}
	}
}

func (kv *ShardKV) shardMigrationTask() {
	for !kv.killed() {
		if _, isLeader := kv.rf.GetState(); isLeader {
			kv.mu.Lock()
			// 找到需要迁移进来的 shard
			gidToShards := kv.getShardByStatus(MoveIn)
			var wg sync.WaitGroup
			for gid, shardIds := range gidToShards {
				go func(servers []string, configNum int, shardIds []int) {
					defer wg.Done()
					// 向每个节点所属的 Group，从 Leader 读取对应的数据
					args := ShardOperationArgs{
						ConfigNum: configNum,
						ShardIds:  shardIds,
					}
					// 不知道 group 哪个 server 是 Leader，所以遍历每一个 Server
					for _, server := range servers {
						var shardReply ShardOperationReply
						clientEnd := kv.make_end(server)
						ok := clientEnd.Call("ShardKV.GetShardsData", &args, &shardReply)
						// 获取到了 shard 数据，执行 shard 迁移
						if ok && shardReply.Err == OK {
							kv.ConfigCommand(RaftCommand{
								CmdType: ShardMigration,
								Data:    shardReply,
							}, &OpReply{})
						}
					}
				}(kv.prevConfig.Groups[gid], kv.currentConfig.Num, shardIds)
			}
			kv.mu.Unlock()
			wg.Wait()
		}
		time.Sleep(ShardMigrationInterval)
	}
}

// 根据状态查找 shard
func (kv *ShardKV) getShardByStatus(status ShardStatus) map[int][]int {
	gidToShards := make(map[int][]int)
	for i, shard := range kv.shards {
		if shard.Status == status {
			// 原来所属的 Group
			gid := kv.prevConfig.Shards[i]
			if gid != 0 {
				if _, ok := gidToShards[gid]; !ok {
					gidToShards[gid] = make([]int, 0)
				}
				gidToShards[gid] = append(gidToShards[gid], i)
			}
		}
	}
	return gidToShards
}

func (kv *ShardKV) GetShardsData(args *ShardOperationArgs, reply *ShardOperationReply) {
	//只要从 Leader 获取
	if _, isLeader := kv.rf.GetState(); !isLeader {
		reply.Err = ErrWrongLeader
		return
	}
	kv.mu.Lock()
	defer kv.mu.Unlock()

	if kv.currentConfig.Num < args.ConfigNum {
		// 当前 group 的配置不是期望的配置
		reply.Err = ErrNotReady
		return
	}

	// 拷贝 shard 数据
	reply.ShardData = make(map[int]map[string]string)
	for _, shardId := range args.ShardIds {
		reply.ShardData[shardId] = kv.shards[shardId].copyData()
	}

	// 拷贝去重表
	reply.DuplicateTable = make(map[int64]LastOperationInfo)
	for clientId, op := range kv.duplicateTable {
		reply.DuplicateTable[clientId] = op.copyData()
	}
	reply.ConfigNum = args.ConfigNum
	reply.Err = OK
}
