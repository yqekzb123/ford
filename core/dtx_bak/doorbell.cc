// Author: Ming Zhang
// Copyright (c) 2022

#include "dtx/doorbell.h"

// 共享锁上锁实现doorbell
void SharedLock_SharedMutex_Batch::SetFAAReq(char* local_addr, uint64_t remote_off) {
  sr[0].opcode = IBV_WR_ATOMIC_FETCH_AND_ADD;
  sr[0].wr.atomic.remote_addr = remote_off;
  sr[0].wr.atomic.compare_add = 1;
  sge[0].length = sizeof(uint64_t);
  sge[0].addr = (uint64_t)local_addr;
}

void SharedLock_SharedMutex_Batch::SetReadReq(char* local_addr, uint64_t remote_off, size_t size) {
  sr[1].opcode = IBV_WR_RDMA_READ;
  sr[1].wr.rdma.remote_addr = remote_off;
  sge[1].length = size;
  sge[1].addr = (uint64_t)local_addr;
}

bool SharedLock_SharedMutex_Batch::SendReqs(CoroutineScheduler* coro_sched, RCQP* qp, coro_id_t coro_id) {
  // sr[0] must be an atomic operation
  sr[0].wr.atomic.remote_addr += qp->remote_mr_.buf;
  sr[0].wr.atomic.rkey = qp->remote_mr_.key;
  sge[0].lkey = qp->local_mr_.key;

  sr[1].wr.rdma.remote_addr += qp->remote_mr_.buf;
  sr[1].wr.rdma.rkey = qp->remote_mr_.key;
  sge[1].lkey = qp->local_mr_.key;

  if (!coro_sched->RDMABatch(coro_id, qp, &(sr[0]), &bad_sr, 1)) return false;
  return true;
}

// 排他锁上锁实现doorbell
void ExclusiveLock_SharedMutex_Batch::SetLockReq(char* local_addr, uint64_t remote_off) {
  sr[0].opcode = IBV_WR_ATOMIC_CMP_AND_SWP;
  sr[0].wr.atomic.remote_addr = remote_off ;
  // TODO: 这里实现的是读者优先
  // 可能出现写锁饿死的情况，在SIGMOD RDMA_Guidelines中原文使用的这种方法
  // 未来可以考虑如何实现写者优先
  sr[0].wr.atomic.compare_add = UNLOCKED;
  sr[0].wr.atomic.swap = EXCLUSIVE_LOCKED;
  sge[0].length = sizeof(uint64_t);
  sge[0].addr = (uint64_t)local_addr;
}

void ExclusiveLock_SharedMutex_Batch::SetReadReq(char* local_addr, uint64_t remote_off, size_t size) {
  sr[1].opcode = IBV_WR_RDMA_READ;
  sr[1].wr.rdma.remote_addr = remote_off;
  sge[1].length = size;
  sge[1].addr = (uint64_t)local_addr;
}

bool ExclusiveLock_SharedMutex_Batch::SendReqs(CoroutineScheduler* coro_sched, RCQP* qp, coro_id_t coro_id) {
  // sr[0] must be an atomic operation
  sr[0].wr.atomic.remote_addr += qp->remote_mr_.buf;
  sr[0].wr.atomic.rkey = qp->remote_mr_.key;
  sge[0].lkey = qp->local_mr_.key;

  sr[1].wr.rdma.remote_addr += qp->remote_mr_.buf;
  sr[1].wr.rdma.rkey = qp->remote_mr_.key;
  sge[1].lkey = qp->local_mr_.key;

  if (!coro_sched->RDMABatch(coro_id, qp, &(sr[0]), &bad_sr, 1)) return false;
  return true;
}

// ----------------------------------------------------------------
// 解锁排他锁batch doorbell
void ExclusiveUnlock_SharedMutex_Batch::SetWriteReq(char* local_addr, uint64_t remote_off, size_t size) {
  sr[0].opcode = IBV_WR_RDMA_WRITE;
  sr[0].wr.rdma.remote_addr = remote_off;
  sge[0].addr = (uint64_t)local_addr;
  sge[0].length = size;
  if (size < 64) {
    sr[0].send_flags |= IBV_SEND_INLINE;
  }
}

void ExclusiveUnlock_SharedMutex_Batch::SetUnLockReq(char* local_addr, uint64_t remote_off) {
  sr[1].opcode = IBV_WR_ATOMIC_FETCH_AND_ADD;
  sr[1].wr.atomic.remote_addr = remote_off;
  sr[1].wr.atomic.compare_add = EXCLUSIVE_UNLOCK_TO_BE_ADDED;
  sge[1].length = sizeof(uint64_t);
  sge[1].addr = (uint64_t)local_addr;
}

bool ExclusiveUnlock_SharedMutex_Batch::SendReqs(CoroutineScheduler* coro_sched, RCQP* qp, coro_id_t coro_id) {
  // sr[1] must be an atomic operation
  sr[0].wr.rdma.remote_addr += qp->remote_mr_.buf;
  sr[0].wr.rdma.rkey = qp->remote_mr_.key;
  sge[0].lkey = qp->local_mr_.key;

  sr[1].wr.atomic.remote_addr += qp->remote_mr_.buf;
  sr[1].wr.atomic.rkey = qp->remote_mr_.key;
  sge[1].lkey = qp->local_mr_.key;

  if (!coro_sched->RDMABatch(coro_id, qp, &(sr[0]), &bad_sr, 1)) return false;
  return true;
}

// --------------------------------------------------------------

void LockReadBatch::SetLockReq(char* local_addr, uint64_t remote_off, uint64_t compare, uint64_t swap) {
  sr[0].opcode = IBV_WR_ATOMIC_CMP_AND_SWP;
  sr[0].wr.atomic.remote_addr = remote_off;
  sr[0].wr.atomic.compare_add = compare;
  sr[0].wr.atomic.swap = swap;
  sge[0].length = sizeof(uint64_t);
  sge[0].addr = (uint64_t)local_addr;
}

void LockReadBatch::SetReadReq(char* local_addr, uint64_t remote_off, size_t size) {
  sr[1].opcode = IBV_WR_RDMA_READ;
  sr[1].wr.rdma.remote_addr = remote_off;
  sge[1].addr = (uint64_t)local_addr;
  sge[1].length = size;
}

bool LockReadBatch::SendReqs(CoroutineScheduler* coro_sched, RCQP* qp, coro_id_t coro_id) {
  // sr[0] must be an atomic operation
  sr[0].wr.atomic.remote_addr += qp->remote_mr_.buf;
  sr[0].wr.atomic.rkey = qp->remote_mr_.key;
  sge[0].lkey = qp->local_mr_.key;

  sr[1].wr.rdma.remote_addr += qp->remote_mr_.buf;
  sr[1].wr.rdma.rkey = qp->remote_mr_.key;
  sge[1].lkey = qp->local_mr_.key;

  if (!coro_sched->RDMABatch(coro_id, qp, &(sr[0]), &bad_sr, 1)) return false;
  return true;
}

bool LockReadBatch::FillParams(RCQP* qp) {
  // sr[0] must be an atomic operation
  sr[0].wr.atomic.remote_addr += qp->remote_mr_.buf;
  sr[0].wr.atomic.rkey = qp->remote_mr_.key;
  sge[0].lkey = qp->local_mr_.key;

  sr[1].wr.rdma.remote_addr += qp->remote_mr_.buf;
  sr[1].wr.rdma.rkey = qp->remote_mr_.key;
  sge[1].lkey = qp->local_mr_.key;
}

void WriteUnlockBatch::SetWritePrimaryReq(char* local_addr, uint64_t remote_off, size_t size) {
  sr[0].opcode = IBV_WR_RDMA_WRITE;
  sr[0].wr.rdma.remote_addr = remote_off;
  sge[0].addr = (uint64_t)local_addr;
  sge[0].length = size;
  if (size < 64) {
    sr[0].send_flags |= IBV_SEND_INLINE;
  }
}

void WriteUnlockBatch::SetUnLockReq(char* local_addr, uint64_t remote_off) {
  sr[1].opcode = IBV_WR_RDMA_WRITE;
  sr[1].send_flags |= IBV_SEND_INLINE;
  sr[1].wr.rdma.remote_addr = remote_off;
  sge[1].addr = (uint64_t)local_addr;
  sge[1].length = sizeof(uint64_t);
}

void WriteUnlockBatch::SetUnLockReq(char* local_addr, uint64_t remote_off, uint64_t compare, uint64_t swap) {
  sr[1].opcode = IBV_WR_ATOMIC_CMP_AND_SWP;
  sr[1].wr.atomic.remote_addr = remote_off;
  sr[1].wr.atomic.compare_add = compare;
  sr[1].wr.atomic.swap = swap;
  sge[1].length = sizeof(uint64_t);
  sge[1].addr = (uint64_t)local_addr;
}

bool WriteUnlockBatch::SendReqs(CoroutineScheduler* coro_sched, RCQP* qp, coro_id_t coro_id, int use_cas) {
  sr[0].wr.rdma.remote_addr += qp->remote_mr_.buf;
  sr[0].wr.rdma.rkey = qp->remote_mr_.key;
  sge[0].lkey = qp->local_mr_.key;

  if (use_cas) {
    sr[1].wr.atomic.remote_addr += qp->remote_mr_.buf;
    sr[1].wr.atomic.rkey = qp->remote_mr_.key;
  } else {
    sr[1].wr.rdma.remote_addr += qp->remote_mr_.buf;
    sr[1].wr.rdma.rkey = qp->remote_mr_.key;
  }

  sge[1].lkey = qp->local_mr_.key;

  if (!coro_sched->RDMABatch(coro_id, qp, &(sr[0]), &bad_sr, 1)) return false;
  return true;
}

void InvisibleWriteBatch::SetInvisibleReq(char* local_addr, uint64_t remote_off, uint64_t compare, uint64_t swap) {
  sr[0].opcode = IBV_WR_ATOMIC_CMP_AND_SWP;
  sr[0].wr.atomic.remote_addr = remote_off;
  sr[0].wr.atomic.compare_add = compare;
  sr[0].wr.atomic.swap = swap;
  sge[0].length = sizeof(uint64_t);
  sge[0].addr = (uint64_t)local_addr;
}

void InvisibleWriteBatch::SetInvisibleReq(char* local_addr, uint64_t remote_off) {
  sr[0].opcode = IBV_WR_RDMA_WRITE;
  sr[0].send_flags |= IBV_SEND_INLINE;
  sr[0].wr.rdma.remote_addr = remote_off;
  sge[0].addr = (uint64_t)local_addr;
  sge[0].length = sizeof(uint64_t);
}

void InvisibleWriteBatch::SetWriteRemoteReq(char* local_addr, uint64_t remote_off, size_t size) {
  sr[1].opcode = IBV_WR_RDMA_WRITE;
  sr[1].wr.rdma.remote_addr = remote_off;
  sge[1].addr = (uint64_t)local_addr;
  sge[1].length = size;
  if (size < 64) {
    sr[1].send_flags |= IBV_SEND_INLINE;
  }
}

bool InvisibleWriteBatch::SendReqs(CoroutineScheduler* coro_sched, RCQP* qp, coro_id_t coro_id, int use_cas) {
  if (use_cas) {
    // sr[0] must be an atomic operation
    sr[0].wr.atomic.remote_addr += qp->remote_mr_.buf;
    sr[0].wr.atomic.rkey = qp->remote_mr_.key;
  } else {
    sr[0].wr.rdma.remote_addr += qp->remote_mr_.buf;
    sr[0].wr.rdma.rkey = qp->remote_mr_.key;
  }
  sge[0].lkey = qp->local_mr_.key;

  sr[1].wr.rdma.remote_addr += qp->remote_mr_.buf;
  sr[1].wr.rdma.rkey = qp->remote_mr_.key;
  sge[1].lkey = qp->local_mr_.key;

  if (!coro_sched->RDMABatch(coro_id, qp, &(sr[0]), &bad_sr, 1)) return false;
  return true;
}

bool InvisibleWriteBatch::SendReqsSync(CoroutineScheduler* coro_sched, RCQP* qp, coro_id_t coro_id, int use_cas) {
  if (use_cas) {
    // sr[0] must be an atomic operation
    sr[0].wr.atomic.remote_addr += qp->remote_mr_.buf;
    sr[0].wr.atomic.rkey = qp->remote_mr_.key;
  } else {
    sr[0].wr.rdma.remote_addr += qp->remote_mr_.buf;
    sr[0].wr.rdma.rkey = qp->remote_mr_.key;
  }
  sge[0].lkey = qp->local_mr_.key;

  sr[1].wr.rdma.remote_addr += qp->remote_mr_.buf;
  sr[1].wr.rdma.rkey = qp->remote_mr_.key;
  sge[1].lkey = qp->local_mr_.key;

  if (!coro_sched->RDMABatchSync(coro_id, qp, &(sr[0]), &bad_sr, 1)) return false;
  return true;
}

void WriteFlushBatch::SetWriteRemoteReq(char* local_addr, uint64_t remote_off, size_t size) {
  sr[0].opcode = IBV_WR_RDMA_WRITE;
  sr[0].wr.rdma.remote_addr = remote_off;
  sge[0].addr = (uint64_t)local_addr;
  sge[0].length = size;
  if (size < 64) {
    sr[0].send_flags |= IBV_SEND_INLINE;
  }
}

void WriteFlushBatch::SetReadRemoteReq(char* local_addr, uint64_t remote_off, size_t size) {
  sr[1].opcode = IBV_WR_RDMA_READ;
  sr[1].wr.rdma.remote_addr = remote_off;
  sge[1].addr = (uint64_t)local_addr;
  sge[1].length = size;
}

bool WriteFlushBatch::SendReqs(CoroutineScheduler* coro_sched, RCQP* qp, coro_id_t coro_id, MemoryAttr& remote_mr) {
  sr[0].wr.rdma.remote_addr += remote_mr.buf;
  sr[0].wr.rdma.rkey = remote_mr.key;
  sge[0].lkey = qp->local_mr_.key;

  sr[1].wr.rdma.remote_addr += remote_mr.buf;
  sr[1].wr.rdma.rkey = remote_mr.key;
  sge[1].lkey = qp->local_mr_.key;

  if (!coro_sched->RDMABatch(coro_id, qp, &(sr[0]), &bad_sr, 1)) return false;
  return true;
}

void InvisibleWriteFlushBatch::SetInvisibleReq(char* local_addr, uint64_t remote_off) {
  // Set invisible in a write way
  sr[0].opcode = IBV_WR_RDMA_WRITE;
  sr[0].send_flags |= IBV_SEND_INLINE;
  sr[0].wr.rdma.remote_addr = remote_off;
  sge[0].addr = (uint64_t)local_addr;
  sge[0].length = sizeof(uint64_t);
}

void InvisibleWriteFlushBatch::SetWriteRemoteReq(char* local_addr, uint64_t remote_off, size_t size) {
  sr[1].opcode = IBV_WR_RDMA_WRITE;
  sr[1].wr.rdma.remote_addr = remote_off;
  sge[1].addr = (uint64_t)local_addr;
  sge[1].length = size;
  if (size < 64) {
    sr[1].send_flags |= IBV_SEND_INLINE;
  }
}

void InvisibleWriteFlushBatch::SetReadRemoteReq(char* local_addr, uint64_t remote_off, size_t size) {
  sr[2].opcode = IBV_WR_RDMA_READ;
  sr[2].wr.rdma.remote_addr = remote_off;
  sge[2].addr = (uint64_t)local_addr;
  sge[2].length = size;
}

bool InvisibleWriteFlushBatch::SendReqs(CoroutineScheduler* coro_sched, RCQP* qp, coro_id_t coro_id, int use_cas) {
  if (use_cas) {
    // sr[0] must be an atomic operation
    sr[0].wr.atomic.remote_addr += qp->remote_mr_.buf;
    sr[0].wr.atomic.rkey = qp->remote_mr_.key;
  } else {
    sr[0].wr.rdma.remote_addr += qp->remote_mr_.buf;
    sr[0].wr.rdma.rkey = qp->remote_mr_.key;
  }
  sge[0].lkey = qp->local_mr_.key;

  sr[1].wr.rdma.remote_addr += qp->remote_mr_.buf;
  sr[1].wr.rdma.rkey = qp->remote_mr_.key;
  sge[1].lkey = qp->local_mr_.key;

  sr[2].wr.rdma.remote_addr += qp->remote_mr_.buf;
  sr[2].wr.rdma.rkey = qp->remote_mr_.key;
  sge[2].lkey = qp->local_mr_.key;

  if (!coro_sched->RDMABatch(coro_id, qp, &(sr[0]), &bad_sr, 2)) return false;
  return true;
}

void ComparatorUpdateRemote::SetInvisibleReq(char* local_addr, uint64_t remote_off) {
  // Set invisible in a write way
  sr[0].opcode = IBV_WR_RDMA_WRITE;
  sr[0].send_flags |= IBV_SEND_INLINE;
  sr[0].wr.rdma.remote_addr = remote_off;
  sge[0].addr = (uint64_t)local_addr;
  sge[0].length = sizeof(uint64_t);
}

void ComparatorUpdateRemote::SetWriteRemoteReq(char* local_addr, uint64_t remote_off, size_t size) {
  sr[1].opcode = IBV_WR_RDMA_WRITE;
  sr[1].wr.rdma.remote_addr = remote_off;
  sge[1].addr = (uint64_t)local_addr;
  sge[1].length = size;
  if (size < 64) {
    sr[1].send_flags |= IBV_SEND_INLINE;
  }
}

void ComparatorUpdateRemote::SetReleaseReq(char* local_addr, uint64_t remote_off) {
  sr[2].opcode = IBV_WR_RDMA_WRITE;
  sr[2].send_flags |= IBV_SEND_INLINE;
  sr[2].wr.rdma.remote_addr = remote_off;
  sge[2].addr = (uint64_t)local_addr;
  sge[2].length = sizeof(uint64_t);
}

bool ComparatorUpdateRemote::SendReqs(CoroutineScheduler* coro_sched, RCQP* qp, coro_id_t coro_id, int use_cas) {
  if (use_cas) {
    // sr[0] must be an atomic operation
    sr[0].wr.atomic.remote_addr += qp->remote_mr_.buf;
    sr[0].wr.atomic.rkey = qp->remote_mr_.key;
  } else {
    sr[0].wr.rdma.remote_addr += qp->remote_mr_.buf;
    sr[0].wr.rdma.rkey = qp->remote_mr_.key;
  }
  sge[0].lkey = qp->local_mr_.key;

  sr[1].wr.rdma.remote_addr += qp->remote_mr_.buf;
  sr[1].wr.rdma.rkey = qp->remote_mr_.key;
  sge[1].lkey = qp->local_mr_.key;

  sr[2].wr.rdma.remote_addr += qp->remote_mr_.buf;
  sr[2].wr.rdma.rkey = qp->remote_mr_.key;
  sge[2].lkey = qp->local_mr_.key;

  if (!coro_sched->RDMABatch(coro_id, qp, &(sr[0]), &bad_sr, 2)) return false;
  return true;
}