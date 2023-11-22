package com.alibaba.lindorm.contest;

import com.alibaba.lindorm.contest.extendstructs.DBConstant;
import com.alibaba.lindorm.contest.extendstructs.meta.MmapMetaImpl;
import org.junit.Test;

import static com.alibaba.lindorm.contest.extendstructs.vlog.MmapVLogWriter.*;
import static com.alibaba.lindorm.contest.util.SizeOf.R1GB;

/**
 * 常量计算
 * @author 陈翔
 */
public class ConstantCalculate implements DBConstant {

    public static long totalSize = (long) (79.2077793 * R1GB);


    /**
     * 计算堆外内存占用
     */
    @Test
    public void test() {
        System.out.println(TSDBEngineImpl.offheapSize());
    }

}
