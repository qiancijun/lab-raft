package shardkv

import (
	"course/shardctrler"
	"time"
)

func (kv *ShardKV) ConfigCommand(command RaftCommand, reply *OpReply) {
	index, _, isLeader := kv.rf.Start(command)

	// 如果不是 Leader，让客户端去重试
	if !isLeader {
		reply.Err = ErrWrongLeader
		return
	}

	// 等待结果
	kv.mu.Lock()
	notifyCh := kv.getNotifyChannel(index)
	kv.mu.Unlock()

	select {
	case result := <-notifyCh:
		reply.Value = result.Value
		reply.Err = result.Err
	case <-time.After(ClientRequestTimeout):
		reply.Err = ErrTimeout
	}

	// 处理完 channel 就没有用了，可以异步的去删除
	go func() {
		kv.mu.Lock()
		kv.removeNotifyChannel(index)
		kv.mu.Unlock()
	}()
}

func (kv *ShardKV) handleConfigChangeMessage(command RaftCommand) *OpReply {
	switch command.CmdType {
	case ConfigChange:
		newConfig := command.Data.(shardctrler.Config)
		return kv.applyNewConfig(newConfig)
	case ShardMigration:
		shardData := command.Data.(ShardOperationReply)
		return kv.applyShardMigration(&shardData)
	default:
		panic("unknown config change type")
	}
}

func (kv *ShardKV) applyNewConfig(newConfig shardctrler.Config) *OpReply {
	if kv.currentConfig.Num+1 == newConfig.Num {
		// 判断配置是不是匹配
		for i := 0; i < shardctrler.NShards; i++ {
			if kv.currentConfig.Shards[i] == kv.gid && newConfig.Shards[i] != kv.gid {
				// 当前的 shard 属于当前的 group，但是在下一个 config 中不属于
				// 该 shard 需要迁移出去
				gid := kv.currentConfig.Shards[i]
				if gid != 0 {
					kv.shards[i].Status = MoveOut
				}
			}

			if kv.currentConfig.Shards[i] != kv.gid && newConfig.Shards[i] == kv.gid {
				// 当前的 shard 不属于当前的 group，但是在下一个 config 中属于
				// 该 shard 需要迁移进来
				gid := newConfig.Shards[i]
				if gid != 0 {
					kv.shards[i].Status = MoveIn
				}
			}
		}
		kv.prevConfig = kv.currentConfig
		kv.currentConfig = newConfig
		return &OpReply{Err: OK}
	}
	return &OpReply{Err: ErrWrongConfig}
}

func (kv *ShardKV) applyShardMigration(shardDataReply *ShardOperationReply) *OpReply {
	if shardDataReply.ConfigNum == kv.currentConfig.Num {
		for shardId, shardData := range shardDataReply.ShardData {
			shard := kv.shards[shardId]
			// 将数据存储到当前 Group 对应的 shard 中
			if shard.Status == MoveIn {
				for k, v := range shardData {
					shard.KV[k] = v
				}
				// 状态置为 GC，等待清理
				shard.Status = GC
			} else {
				break
			}
		}

		// 拷贝去重表
		for clientId, dupTable := range shardDataReply.DuplicateTable {
			table, ok := kv.duplicateTable[clientId]
			if !ok || table.SeqId < dupTable.SeqId {
				kv.duplicateTable[clientId] = dupTable
			}
		}
	}
	return &OpReply{
		Err: ErrWrongConfig,
	}
}