package com.alibaba.lindorm.contest.extendstructs.compression.tp;

import static com.alibaba.lindorm.contest.extendstructs.DBConstant.BASE_TP;

/**
 * long压缩器
 * @author 陈翔
 */
public class TPCompressor {

    public static int compress(long data){
        return Math.toIntExact(data - BASE_TP);
    }

}
