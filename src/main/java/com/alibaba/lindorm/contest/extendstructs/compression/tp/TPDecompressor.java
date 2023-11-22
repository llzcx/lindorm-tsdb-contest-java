package com.alibaba.lindorm.contest.extendstructs.compression.tp;

import static com.alibaba.lindorm.contest.extendstructs.DBConstant.BASE_TP;

/**
 * @author 陈翔
 */
public class TPDecompressor {
    public static long decompress(int data){
        return data+BASE_TP;
    }
}
