package com.alibaba.lindorm.contest.extendstructs.offheap;

import jdk.internal.misc.Unsafe;

import java.lang.reflect.Field;

/**
 * @author 陈翔
 */
public final class UnsafeAllocator implements IAllocator {

  static final Unsafe unsafe;

  static {
    try {
      Field field = Unsafe.class.getDeclaredField("theUnsafe");
      field.setAccessible(true);
      unsafe = (Unsafe) field.get(null);
    } catch (Exception e) {
      throw new AssertionError(e);
    }
  }

  @Override
  public long allocate(long size) {
    try {
      return unsafe.allocateMemory(size);
    } catch (OutOfMemoryError oom) {
      return 0L;
    }
  }

  @Override
  public void free(long peer) {
    unsafe.freeMemory(peer);
  }

  @Override
  public long getTotalAllocated() {
    return -1L;
  }
}
