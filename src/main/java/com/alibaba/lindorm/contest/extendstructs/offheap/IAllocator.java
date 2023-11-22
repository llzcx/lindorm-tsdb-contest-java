package com.alibaba.lindorm.contest.extendstructs.offheap;

/**
 * @author 陈翔
 */
public interface IAllocator {
  long allocate(long size);

  void free(long peer);

  long getTotalAllocated();
}
