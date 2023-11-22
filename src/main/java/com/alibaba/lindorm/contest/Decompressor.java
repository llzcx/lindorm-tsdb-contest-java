package com.alibaba.lindorm.contest;

import java.io.IOException;
import java.nio.ByteBuffer;

/**
 * @author 陈翔
 */
public interface Decompressor {
    /**
     *
     * @param src 从磁盘中读入的数据（待解压）将 [src.position , src.limit()) 写入区间 [des.position , des.limit)
     * @return 解压以后的长度,解压以后des.pos会定位到解压缩以后的位置（pos + len）,src的指针不会动
     * @throws IOException
     */
    int decompress(ByteBuffer des,ByteBuffer src) throws IOException;


}
