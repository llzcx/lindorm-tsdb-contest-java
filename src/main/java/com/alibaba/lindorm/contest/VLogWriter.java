package com.alibaba.lindorm.contest;


import com.alibaba.lindorm.contest.structs.ColumnValue;

import java.io.File;
import java.io.IOException;

/**
 * vlog writer。
 *
 * @author 陈翔
 */
public interface VLogWriter {

  /**
   * 获取log file句柄
   *
   * @return file
   */
  File getFile();

  /**
   * 获取file size，可能会进行系统调用，少用
   *
   * @return file size
   * @throws IOException 抛出异常
   */
  long getFileSize() throws IOException;

  /**
   * 追加一条记录
   * @throws IOException 抛出异常
   */
  boolean append() throws IOException;


  /**
   * 关闭
   *
   * @throws IOException 抛出异常
   */
  void close() throws IOException;


  void flush() throws IOException;


  public boolean canRead(short compressIndex) throws IOException;


  public ColumnValue getVlog(short internalIndex, int colIndex) throws IOException;

}
