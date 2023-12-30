// // Author: Hongyao Zhao
// // Copyright (c) 2023

// #include "batch/local_batch.h"
// #include "util/latency.h"

// bool LocalBatch::IssueReadRO(std::vector<DirectRead>& pending_direct_ro, std::vector<HashRead>& pending_hash_ro) {
//   for (auto& dtx : txn_list) {
//     dtx->dtx->IssueReadRO(pending_direct_ro,pending_hash_ro);
//   }
//   return true;
// }

// bool LocalBatch::IssueReadLock(std::vector<CasRead>& pending_cas_rw,
//                         std::vector<HashRead>& pending_hash_rw,
//                         std::vector<InsertOffRead>& pending_insert_off_rw) {
//   // For read-write set, we need to read and lock them
//   for (auto& dtx : txn_list) {
//     dtx->IssueReadLock(pending_cas_rw,pending_hash_rw,pending_insert_off_rw);
//   }
//   return true;
// }

// bool LocalBatch::IssueReadRW(std::vector<DirectRead>& pending_direct_rw,
//                       std::vector<HashRead>& pending_hash_rw,
//                       std::vector<InsertOffRead>& pending_insert_off_rw) {
//   for (auto& dtx : txn_list) {
//     dtx->IssueReadRW(pending_direct_rw,pending_hash_rw,pending_insert_off_rw);
//   }
//   return true;
// }

// ValStatus LocalBatch::IssueLocalValidate(std::vector<ValidateRead>& pending_validate) {
//   bool need_val_rw_set = false;
//   if (!not_eager_locked_rw_set.empty()) {
//     // For those are not locked during exe phase, we lock and read their versions in a batch
//     // They cannot use local validation because they must be locked
//     for (auto& index : not_eager_locked_rw_set) {
//       locked_rw_set.emplace_back(index);
//       char* cas_buf = thread_rdma_buffer_alloc->Alloc(sizeof(lock_t));
//       *(lock_t*)cas_buf = 0xdeadbeaf;
//       char* version_buf = thread_rdma_buffer_alloc->Alloc(sizeof(version_t));
//       auto& it = read_write_set[index].item_ptr;
//       // Must be the primary
//       RCQP* qp = thread_qp_man->GetRemoteDataQPWithNodeID(read_write_set[index].read_which_node);
//       pending_validate.push_back(ValidateRead{.qp = qp, .item = &read_write_set[index], .cas_buf = cas_buf, .version_buf = version_buf, .has_lock_in_validate = true});

//       std::shared_ptr<LockReadBatch> doorbell = std::make_shared<LockReadBatch>();
//       doorbell->SetLockReq(cas_buf, it->GetRemoteLockAddr(), STATE_CLEAN, STATE_LOCKED);
//       doorbell->SetReadReq(version_buf, it->GetRemoteVersionAddr(), sizeof(version_t));  // Read a version
//       if (!doorbell->SendReqs(coro_sched, qp, coro_id)) {
//         return ValStatus::RDMA_ERROR;
//       }
//     }
//     need_val_rw_set = true;
//   }

//   if (!read_only_set.empty()) {
//     auto find_res = global_vcache->CheckVersion(read_only_set, tx_id);
//     if (find_res == VersionStatus::NO_VERSION_CHANGED) {
//       // There is no version changed, so no validation needed
//       return need_val_rw_set ? ValStatus::NEED_VAL : ValStatus::NO_NEED_VAL;
//     } else if (find_res == VersionStatus::VERSION_CHANGED) {
//       return ValStatus::MUST_ABORT;
//     } else {
//       // Performance penalty: if version is evicted, then we do useless local version check.
//       // But we can adjust the version table to avoid this penalty
//       // Nevertheless, if the miss occurs, we need to fill the key and its version into Vcache.
//       for (auto& set_it : read_only_set) {
//         auto it = set_it.item_ptr;
//         // If reading from backup, using backup's qp to validate the version on backup.
//         // Otherwise, the qp mismatches the remote version addr
//         RCQP* qp = thread_qp_man->GetRemoteDataQPWithNodeID(set_it.read_which_node);
//         char* version_buf = thread_rdma_buffer_alloc->Alloc(sizeof(version_t));
//         pending_validate.push_back(ValidateRead{.qp = qp, .item = &set_it, .cas_buf = nullptr, .version_buf = version_buf, .has_lock_in_validate = false});
//         if (!coro_sched->RDMARead(coro_id, qp, version_buf, it->GetRemoteVersionAddr(), sizeof(version_t))) {
//           return ValStatus::RDMA_ERROR;
//         }
//       }
//     }
//   }

//   return ValStatus::NEED_VAL;
// }

// bool LocalBatch::IssueRemoteValidate(std::vector<ValidateRead>& pending_validate) {
//   // For those are not locked during exe phase, we lock and read their versions in a batch
//   for (auto& index : not_eager_locked_rw_set) {
//     locked_rw_set.emplace_back(index);
//     char* cas_buf = thread_rdma_buffer_alloc->Alloc(sizeof(lock_t));
//     *(lock_t*)cas_buf = 0xdeadbeaf;
//     char* version_buf = thread_rdma_buffer_alloc->Alloc(sizeof(version_t));
//     auto& it = read_write_set[index].item_ptr;
//     RCQP* qp = thread_qp_man->GetRemoteDataQPWithNodeID(read_write_set[index].read_which_node);
//     pending_validate.push_back(ValidateRead{.qp = qp, .item = &read_write_set[index], .cas_buf = cas_buf, .version_buf = version_buf, .has_lock_in_validate = true});

//     std::shared_ptr<LockReadBatch> doorbell = std::make_shared<LockReadBatch>();
//     doorbell->SetLockReq(cas_buf, it->GetRemoteLockAddr(), STATE_CLEAN, STATE_LOCKED);
//     doorbell->SetReadReq(version_buf, it->GetRemoteVersionAddr(), sizeof(version_t));  // Read a version
//     if (!doorbell->SendReqs(coro_sched, qp, coro_id)) {
//       return false;
//     }
//   }
//   // For read-only items, we only need to read their versions
//   for (auto& set_it : read_only_set) {
//     auto it = set_it.item_ptr;
//     // If reading from backup, using backup's qp to validate the version on backup.
//     // Otherwise, the qp mismatches the remote version addr
//     RCQP* qp = thread_qp_man->GetRemoteDataQPWithNodeID(set_it.read_which_node);
//     char* version_buf = thread_rdma_buffer_alloc->Alloc(sizeof(version_t));
//     pending_validate.push_back(ValidateRead{.qp = qp, .item = &set_it, .cas_buf = nullptr, .version_buf = version_buf, .has_lock_in_validate = false});
//     if (!coro_sched->RDMARead(coro_id, qp, version_buf, it->GetRemoteVersionAddr(), sizeof(version_t))) {
//       return false;
//     }
//   }
//   return true;
// }

// bool LocalBatch::IssueCommitAll(std::vector<CommitWrite>& pending_commit_write, char* cas_buf) {
//   for (auto& set_it : read_write_set) {
//     // We cannot use a shared data_buf for all the written data, although it seems good
//     // to save buffers thanks to the sequential data sending. But it is totally wrong. The reason
//     // is that `ibv_post_send' does not guarantee that the RDMA NIC will actually send the data packets
//     // when `ibv_post_send' returns. In fact, the RDMA device sends the packets later in an **asynchronous** way.
//     // As a result, using a shared data_buf will render a bug: The latter data item will be written to the previous target machine, instead of the latter target machine.
//     // Here is the description of `ibv_post_send':
//     // ibv_post_send() posts a linked list of Work Requests (WRs) to the Send Queue of a Queue Pair (QP). ibv_post_send() go over all of the entries in the linked list, one by one, check that it is valid, generate a HW-specific Send Request out of it and add it to the tail of the QP's Send Queue without performing any context switch. The RDMA device will handle it (later) in **asynchronous** way. If there is a failure in one of the WRs because the Send Queue is full or one of the attributes in the WR is bad, it stops immediately and return the pointer to that WR.

//     char* data_buf = thread_rdma_buffer_alloc->Alloc(DataItemSize);

//     auto it = set_it.item_ptr;
//     // Maintain the version that user specified
//     if (!it->user_insert) {
//       it->version = tx_id;
//     }
//     it->lock = STATE_LOCKED | STATE_INVISIBLE;
//     memcpy(data_buf, (char*)it.get(), DataItemSize);

//     // Commit primary
//     node_id_t node_id = global_meta_man->GetPrimaryNodeID(it->table_id);  // Read-write data can only be read from primary
//     RCQP* qp = thread_qp_man->GetRemoteDataQPWithNodeID(node_id);
//     pending_commit_write.push_back(CommitWrite{.node_id = node_id, .lock_off = it->GetRemoteLockAddr()});
//     std::shared_ptr<InvisibleWriteBatch> doorbell = std::make_shared<InvisibleWriteBatch>();
//     doorbell->SetInvisibleReq(cas_buf, it->GetRemoteLockAddr());
//     doorbell->SetWriteRemoteReq(data_buf, it->remote_offset, DataItemSize);
//     if (!doorbell->SendReqs(coro_sched, qp, coro_id, 0)) {
//       return false;
//     }

//     // Commit backup
//     // Get the offset (item's addr relative to table's addr) in backup
//     // The offset is the same with that in primary
//     const HashMeta& primary_hash_meta = global_meta_man->GetPrimaryHashMetaWithTableID(it->table_id);
//     auto offset_in_backup_hash_store = it->remote_offset - primary_hash_meta.base_off;

//     // Get all the backup queue pairs and hash metas for this table
//     auto* backup_node_ids = global_meta_man->GetBackupNodeID(it->table_id);
//     if (!backup_node_ids) continue;  // There are no backups in the PM pool
//     const std::vector<HashMeta>* backup_hash_metas = global_meta_man->GetBackupHashMetasWithTableID(it->table_id);
//     // backup_node_ids guarantees that the order of remote machine is the same in backup_hash_metas and backup_qps

//     for (size_t i = 0; i < backup_node_ids->size(); i++) {
//       auto remote_item_off = offset_in_backup_hash_store + (*backup_hash_metas)[i].base_off;
//       auto remote_lock_off = it->GetRemoteLockAddr(remote_item_off);
//       pending_commit_write.push_back(CommitWrite{.node_id = backup_node_ids->at(i), .lock_off = remote_lock_off});

//       // Reason as the above. ibv_post_send is asynchronous. We cannot use the same data buf because we need to modify the data which is sent to the backup

//       // TEMP comment
//       char* data_buf = thread_rdma_buffer_alloc->Alloc(DataItemSize);
//       it->lock = STATE_INVISIBLE;
//       it->remote_offset = remote_item_off;
//       memcpy(data_buf, (char*)it.get(), DataItemSize);

//       doorbell->SetInvisibleReq(cas_buf, remote_lock_off);
//       doorbell->SetWriteRemoteReq(data_buf, remote_item_off, DataItemSize);
//       RCQP* backup_qp = thread_qp_man->GetRemoteDataQPWithNodeID(backup_node_ids->at(i));
//       if (!doorbell->SendReqs(coro_sched, backup_qp, coro_id, 0)) {
//         return false;
//       }
//     }
//   }
//   return true;
// }

// bool LocalBatch::IssueCommitAllFullFlush(std::vector<CommitWrite>& pending_commit_write, char* cas_buf) {
//   for (auto& set_it : read_write_set) {
//     char* data_buf = thread_rdma_buffer_alloc->Alloc(DataItemSize);

//     auto it = set_it.item_ptr;
//     // Maintain the version that user specified
//     if (!it->user_insert) {
//       it->version = tx_id;
//     }
//     it->lock = STATE_LOCKED | STATE_INVISIBLE;
//     memcpy(data_buf, (char*)it.get(), DataItemSize);

//     // Commit primary
//     node_id_t node_id = global_meta_man->GetPrimaryNodeID(it->table_id);  // Read-write data can only be read from primary
//     RCQP* qp = thread_qp_man->GetRemoteDataQPWithNodeID(node_id);
//     pending_commit_write.push_back(CommitWrite{.node_id = node_id, .lock_off = it->GetRemoteLockAddr()});
//     std::shared_ptr<InvisibleWriteBatch> doorbell = std::make_shared<InvisibleWriteBatch>();
//     doorbell->SetInvisibleReq(cas_buf, it->GetRemoteLockAddr());
//     doorbell->SetWriteRemoteReq(data_buf, it->remote_offset, DataItemSize);

//     // RDMA FLUSH
//     char* flush_buf = thread_rdma_buffer_alloc->Alloc(RFlushReadSize);
// #if 0
//     // Open this choice when testing remote flush in MICRO benchmark
//     if (!doorbell->SendReqsSync(coro_sched, qp, coro_id, 0)) {
//       return false;
//     }
//     if (!coro_sched->RDMAReadSync(coro_id, qp, flush_buf, it->remote_offset, RFlushReadSize)) {
//       return false;
//     }
// #else
//     if (!doorbell->SendReqs(coro_sched, qp, coro_id, 0)) {
//       return false;
//     }
//     if (!coro_sched->RDMARead(coro_id, qp, flush_buf, it->remote_offset, RFlushReadSize)) {
//       return false;
//     }
// #endif

//     // Commit backup
//     // Get the offset (item's addr relative to table's addr) in backup
//     // The offset is the same with that in primary
//     const HashMeta& primary_hash_meta = global_meta_man->GetPrimaryHashMetaWithTableID(it->table_id);
//     auto offset_in_backup_hash_store = it->remote_offset - primary_hash_meta.base_off;

//     // Get all the backup queue pairs and hash metas for this table
//     auto* backup_node_ids = global_meta_man->GetBackupNodeID(it->table_id);
//     if (!backup_node_ids) continue;  // There are no backups in the PM pool
//     const std::vector<HashMeta>* backup_hash_metas = global_meta_man->GetBackupHashMetasWithTableID(it->table_id);
//     // backup_node_ids guarantees that the order of remote machine is the same in backup_hash_metas and backup_qps

//     for (size_t i = 0; i < backup_node_ids->size(); i++) {
//       auto remote_item_off = offset_in_backup_hash_store + (*backup_hash_metas)[i].base_off;
//       auto remote_lock_off = it->GetRemoteLockAddr(remote_item_off);
//       pending_commit_write.push_back(CommitWrite{.node_id = backup_node_ids->at(i), .lock_off = remote_lock_off});

//       char* data_buf = thread_rdma_buffer_alloc->Alloc(DataItemSize);
//       it->lock = STATE_INVISIBLE;
//       it->remote_offset = remote_item_off;
//       memcpy(data_buf, (char*)it.get(), DataItemSize);

//       doorbell->SetInvisibleReq(cas_buf, remote_lock_off);
//       doorbell->SetWriteRemoteReq(data_buf, remote_item_off, DataItemSize);
//       RCQP* backup_qp = thread_qp_man->GetRemoteDataQPWithNodeID(backup_node_ids->at(i));
// #if 0
//       // Open this choice when testing remote flush in MICRO benchmark
//       if (!doorbell->SendReqsSync(coro_sched, backup_qp, coro_id, 0)) {
//         return false;
//       }
//       if (!coro_sched->RDMAReadSync(coro_id, backup_qp, flush_buf, it->remote_offset, RFlushReadSize)) {
//         return false;
//       }
// #else
//       if (!doorbell->SendReqs(coro_sched, backup_qp, coro_id, 0)) {
//         return false;
//       }
//       // RDMA FLUSH
//       if (!coro_sched->RDMARead(coro_id, backup_qp, flush_buf, it->remote_offset, RFlushReadSize)) {
//         return false;
//       }
// #endif
//     }
//   }
//   return true;
// }

// bool LocalBatch::IssueCommitAllSelectFlush(std::vector<CommitWrite>& pending_commit_write, char* cas_buf) {
//   size_t current_i = 0;

// #if LOCAL_VALIDATION
//   global_vcache->SetVersion(read_write_set, tx_id);
// #endif

//   for (auto& set_it : read_write_set) {
//     char* data_buf = thread_rdma_buffer_alloc->Alloc(DataItemSize);

//     auto it = set_it.item_ptr;
//     // Maintain the version that user specified
//     if (!it->user_insert) {
//       it->version = tx_id;
//     }

// #if LOCAL_LOCK
//     it->lock = STATE_INVISIBLE;
// #else
//     it->lock = STATE_LOCKED | STATE_INVISIBLE;
// #endif
//     memcpy(data_buf, (char*)it.get(), DataItemSize);

//     // Commit primary
//     node_id_t node_id = global_meta_man->GetPrimaryNodeID(it->table_id);  // Read-write data can only be read from primary
//     RCQP* qp = thread_qp_man->GetRemoteDataQPWithNodeID(node_id);
//     pending_commit_write.push_back(CommitWrite{.node_id = node_id, .lock_off = it->GetRemoteLockAddr()});

//     // if (!coro_sched->RDMAWrite(coro_id, qp, data_buf, it->remote_offset, DataItemSize)) {
//     //   return false;
//     // }

//     std::shared_ptr<InvisibleWriteBatch> doorbell = std::make_shared<InvisibleWriteBatch>();
//     doorbell->SetInvisibleReq(cas_buf, it->GetRemoteLockAddr());
//     doorbell->SetWriteRemoteReq(data_buf, it->remote_offset, DataItemSize);
//     if (!doorbell->SendReqs(coro_sched, qp, coro_id, 0)) {
//       return false;
//     }
//     // Commit backup
//     // Get the offset (item's addr relative to table's addr) in backup
//     // The offset is the same with that in primary
//     const HashMeta& primary_hash_meta = global_meta_man->GetPrimaryHashMetaWithTableID(it->table_id);
//     auto offset_in_backup_hash_store = it->remote_offset - primary_hash_meta.base_off;

//     // Get all the backup queue pairs and hash metas for this table
//     auto* backup_node_ids = global_meta_man->GetBackupNodeID(it->table_id);
//     if (!backup_node_ids) continue;  // There are no backups in the PM pool
//     const std::vector<HashMeta>* backup_hash_metas = global_meta_man->GetBackupHashMetasWithTableID(it->table_id);
//     // backup_node_ids guarantees that the order of remote machine is the same in backup_hash_metas and backup_qps

//     for (size_t i = 0; i < backup_node_ids->size(); i++) {
//       auto remote_item_off = offset_in_backup_hash_store + (*backup_hash_metas)[i].base_off;
//       auto remote_lock_off = it->GetRemoteLockAddr(remote_item_off);
//       pending_commit_write.push_back(CommitWrite{.node_id = backup_node_ids->at(i), .lock_off = remote_lock_off});

//       char* data_buf = thread_rdma_buffer_alloc->Alloc(DataItemSize);
//       it->lock = STATE_INVISIBLE;
//       it->remote_offset = remote_item_off;
//       memcpy(data_buf, (char*)it.get(), DataItemSize);
//       RCQP* backup_qp = thread_qp_man->GetRemoteDataQPWithNodeID(backup_node_ids->at(i));

//       // if (!coro_sched->RDMAWrite(coro_id, backup_qp, data_buf, remote_item_off, DataItemSize)) {
//       //   return false;
//       // }

//       doorbell->SetInvisibleReq(cas_buf, remote_lock_off);
//       doorbell->SetWriteRemoteReq(data_buf, remote_item_off, DataItemSize);
//       if (!doorbell->SendReqs(coro_sched, backup_qp, coro_id, 0)) {
//         return false;
//       }

//       // Selective Remote FLUSH: Only flush the last data that is written to backup
//       if (current_i == read_write_set.size() - 1) {
//         char* flush_buf = thread_rdma_buffer_alloc->Alloc(RFlushReadSize);
//         if (!coro_sched->RDMARead(coro_id, backup_qp, flush_buf, it->remote_offset, RFlushReadSize)) {
//           return false;
//         }
//       }
//     }
//     current_i++;
//   }
//   return true;
// }

// bool LocalBatch::IssueCommitAllBatchSelectFlush(std::vector<CommitWrite>& pending_commit_write, char* cas_buf) {
//   // Obsolete

//   size_t current_i = 0;
//   for (auto& set_it : read_write_set) {
//     char* data_buf = thread_rdma_buffer_alloc->Alloc(DataItemSize);

//     auto it = set_it.item_ptr;
//     // Maintain the version that user specified
//     if (!it->user_insert) {
//       it->version = tx_id;
//     }
//     it->lock = STATE_LOCKED | STATE_INVISIBLE;
//     memcpy(data_buf, (char*)it.get(), DataItemSize);

//     // Commit primary
//     node_id_t node_id = set_it.read_which_node;  // Read-write data can only be read from primary
//     RCQP* qp = thread_qp_man->GetRemoteDataQPWithNodeID(node_id);
//     pending_commit_write.push_back(CommitWrite{.node_id = node_id, .lock_off = it->GetRemoteLockAddr()});
//     std::shared_ptr<InvisibleWriteBatch> doorbell = std::make_shared<InvisibleWriteBatch>();
//     doorbell->SetInvisibleReq(cas_buf, it->GetRemoteLockAddr());
//     doorbell->SetWriteRemoteReq(data_buf, it->remote_offset, DataItemSize);
//     if (!doorbell->SendReqs(coro_sched, qp, coro_id, 0)) {
//       return false;
//     }

//     // Commit backup
//     // Get the offset (item's addr relative to table's addr) in backup
//     // The offset is the same with that in primary
//     const HashMeta& primary_hash_meta = global_meta_man->GetPrimaryHashMetaWithTableID(it->table_id);
//     auto offset_in_backup_hash_store = it->remote_offset - primary_hash_meta.base_off;

//     // Get all the backup queue pairs and hash metas for this table
//     auto* backup_node_ids = global_meta_man->GetBackupNodeID(it->table_id);
//     if (!backup_node_ids) continue;  // There are no backups in the PM pool
//     const std::vector<HashMeta>* backup_hash_metas = global_meta_man->GetBackupHashMetasWithTableID(it->table_id);
//     // backup_node_ids guarantees that the order of remote machine is the same in backup_hash_metas and backup_qps

//     for (size_t i = 0; i < backup_node_ids->size(); i++) {
//       auto remote_item_off = offset_in_backup_hash_store + (*backup_hash_metas)[i].base_off;
//       auto remote_lock_off = it->GetRemoteLockAddr(remote_item_off);

//       char* data_buf = thread_rdma_buffer_alloc->Alloc(DataItemSize);
//       it->lock = STATE_INVISIBLE;
//       it->remote_offset = remote_item_off;
//       memcpy(data_buf, (char*)it.get(), DataItemSize);

//       pending_commit_write.push_back(CommitWrite{.node_id = backup_node_ids->at(i), .lock_off = remote_lock_off});
//       RCQP* backup_qp = thread_qp_man->GetRemoteDataQPWithNodeID(backup_node_ids->at(i));

//       // Selective Remote FLUSH: Only flush the last data that is written to backup
//       if (current_i == read_write_set.size() - 1) {
//         char* flush_buf = thread_rdma_buffer_alloc->Alloc(RFlushReadSize);
//         std::shared_ptr<InvisibleWriteFlushBatch> flush_doorbell = std::make_shared<InvisibleWriteFlushBatch>();
//         flush_doorbell->SetInvisibleReq(cas_buf, remote_lock_off);
//         flush_doorbell->SetWriteRemoteReq(data_buf, remote_item_off, DataItemSize);
//         flush_doorbell->SetReadRemoteReq(flush_buf, remote_item_off, RFlushReadSize);
//         if (!flush_doorbell->SendReqs(coro_sched, backup_qp, coro_id, 0)) return false;
//       } else {
//         doorbell->SetInvisibleReq(cas_buf, remote_lock_off);
//         doorbell->SetWriteRemoteReq(data_buf, remote_item_off, DataItemSize);
//         if (!doorbell->SendReqs(coro_sched, backup_qp, coro_id, 0)) return false;
//       }
//     }

//     current_i++;
//   }
//   return true;
// }