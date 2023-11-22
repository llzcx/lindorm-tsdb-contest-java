package com.alibaba.lindorm.contest;

import com.alibaba.lindorm.contest.extendstructs.wal.ValEntity;
import com.alibaba.lindorm.contest.extendstructs.wal.WalEntry;

import java.io.IOException;
import java.util.List;

/**
 * read使用的索引
 *
 * @author xu.zx
 */
public interface ImmutableIndex {

  /**
   * 线性查找 logn
   * @param tp 目标时间戳 [已经被压缩]
   * @return
   */
  ValEntity linearSearch(int tp) throws IOException;

  /**
   * 获取当前容量
   * @return
   */
  int getSize();

  /**
   * 添加
   */
  void add();

  /**
   * 范围查询
   * @param leftLimit 时间戳左边界 [已经被压缩]
   * @param rightLimit 时间戳右边界 [已经被压缩]
   * @return
   */
  List<WalEntry> rangeList(int leftLimit, int rightLimit);
}
