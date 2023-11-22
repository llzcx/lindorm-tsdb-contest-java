package com.alibaba.lindorm.contest.extendstructs;

/**
 * @author 陈翔
 */
public interface FileName {
    /**
     * maxRow
     */
    String MAX_ROW_F_NAME = "max.bin";
    /**
     *
     */
    String STR_COLS_F_NAME = "str.bin";
    String INT_COLS_F_NAME = "int.bin";
    String DOUBLE_COLS_F_NAME = "dou.bin";

    /**
     * wal
     */
    String WAL_F_NAME = "wal.bin";

    /**
     *
     */
    String META_F_NAME = "meta.bin";



    /**
     * 聚合平均值
     */
    String AGG = "agg_avg.bin";
}
