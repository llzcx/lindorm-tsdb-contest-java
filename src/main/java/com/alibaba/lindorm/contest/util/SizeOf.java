package com.alibaba.lindorm.contest.util;


import static jdk.internal.misc.Unsafe.*;

/**
 * 各种Size的定义。
 *
 * @author xu.zx
 */
public final class SizeOf {

  public static long R1k = 1024;
  public static long R1MB = 1024*R1k;
  public static long R1GB = 1024*R1MB;

  public static long MIN = 60;
  public static long HOUR = 3600;

  public static final byte SIZE_OF_BYTE = 1;
  public static final byte SIZE_OF_SHORT = 2;
  public static final byte SIZE_OF_INT = 4;
  public static final byte SIZE_OF_LONG = 8;
  public static final byte SIZE_OF_FLOAT = 4;
  public static final byte SIZE_OF_DOUBLE = 8;




  public static long sizeOf(boolean[] array) {
    return (array == null) ? 0 : sizeOfBooleanArray(array.length);
  }

  public static long sizeOf(byte[] array) {
    return (array == null) ? 0 : sizeOfByteArray(array.length);
  }

  public static long sizeOf(short[] array) {
    return (array == null) ? 0 : sizeOfShortArray(array.length);
  }

  public static long sizeOf(char[] array) {
    return (array == null) ? 0 : sizeOfCharArray(array.length);
  }

  public static long sizeOf(int[] array) {
    return (array == null) ? 0 : sizeOfIntArray(array.length);
  }

  public static long sizeOf(long[] array) {
    return (array == null) ? 0 : sizeOfLongArray(array.length);
  }

  public static long sizeOf(float[] array) {
    return (array == null) ? 0 : sizeOfFloatArray(array.length);
  }

  public static long sizeOf(double[] array) {
    return (array == null) ? 0 : sizeOfDoubleArray(array.length);
  }

  public static long sizeOf(Object[] array) {
    return (array == null) ? 0 : sizeOfObjectArray(array.length);
  }

  public static long sizeOfBooleanArray(int length) {
    return ARRAY_BOOLEAN_BASE_OFFSET + (((long) ARRAY_BOOLEAN_INDEX_SCALE) * length);
  }

  public static long sizeOfByteArray(int length) {
    return ARRAY_BYTE_BASE_OFFSET + (((long) ARRAY_BYTE_INDEX_SCALE) * length);
  }

  public static long sizeOfShortArray(int length) {
    return ARRAY_SHORT_BASE_OFFSET + (((long) ARRAY_SHORT_INDEX_SCALE) * length);
  }

  public static long sizeOfCharArray(int length) {
    return ARRAY_CHAR_BASE_OFFSET + (((long) ARRAY_CHAR_INDEX_SCALE) * length);
  }

  public static long sizeOfIntArray(int length) {
    return ARRAY_INT_BASE_OFFSET + (((long) ARRAY_INT_INDEX_SCALE) * length);
  }

  public static long sizeOfLongArray(int length) {
    return ARRAY_LONG_BASE_OFFSET + (((long) ARRAY_LONG_INDEX_SCALE) * length);
  }

  public static long sizeOfFloatArray(int length) {
    return ARRAY_FLOAT_BASE_OFFSET + (((long) ARRAY_FLOAT_INDEX_SCALE) * length);
  }

  public static long sizeOfDoubleArray(int length) {
    return ARRAY_DOUBLE_BASE_OFFSET + (((long) ARRAY_DOUBLE_INDEX_SCALE) * length);
  }

  public static long sizeOfObjectArray(int length) {
    return ARRAY_OBJECT_BASE_OFFSET + (((long) ARRAY_OBJECT_INDEX_SCALE) * length);
  }

  private SizeOf() {
  }

}
