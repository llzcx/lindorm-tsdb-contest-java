package com.alibaba.lindorm.contest;

import java.io.IOException;
import java.nio.ByteBuffer;


/**
 * @author 陈翔
 */
public interface Compressor {

    /**
     * 预期最长
     * @param var1
     * @return
     */
    int expectedMaximumLength(int var1);

    /**
     * 将数据进行压缩
     * @param des 待写入磁盘的des,将 [src.position , src.limit()) 写入区间 [des.position , des.limit)
     * @return 压缩以后的长度 压缩以后des.pos会定位到解压缩以后的位置（pos + len） ,src的指针不会动
     * @throws IOException
     */
    int compress(ByteBuffer des) throws IOException;
}
