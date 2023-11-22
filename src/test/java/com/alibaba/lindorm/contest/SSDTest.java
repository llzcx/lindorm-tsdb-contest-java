package com.alibaba.lindorm.contest;

import com.alibaba.lindorm.contest.extendstructs.DBConstant;
import org.junit.Test;

public class SSDTest implements DBConstant {

    /**
     * 假设其他都不是瓶颈，求峰值QPS
     */
    @Test
    public void test(){
        //单个vin一次请求磁盘请求数量
        int rangeBlockNum = DBConstant.RANGE_TIME_LENGTH / COMPRESS_LIMIT * 3;
        System.out.println("rangeBlockNum:"+rangeBlockNum);
        System.out.println("IOPS:"+IOPS);
        //单线程峰值qps
        System.out.println("maxQPS:"+IOPS/rangeBlockNum);
        //多线程
        System.out.println("maxQPS:"+IOPS/rangeBlockNum*READ_COUNT);
    }


}
