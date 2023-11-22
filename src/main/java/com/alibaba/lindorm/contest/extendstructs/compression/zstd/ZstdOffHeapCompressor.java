package com.alibaba.lindorm.contest.extendstructs.compression.zstd;

import com.alibaba.lindorm.contest.Compressor;
import com.github.luben.zstd.Zstd;

import java.io.IOException;
import java.nio.ByteBuffer;

/**
 * @author 陈翔
 */
public class ZstdOffHeapCompressor implements Compressor {

    private final ByteBuffer src;
    private final ByteBuffer dict;
    private final Integer compressLevel;

    public ZstdOffHeapCompressor(ByteBuffer src,ByteBuffer dict, Integer compressLevel) {
        this.src = src;
        this.dict = dict;
        this.compressLevel = compressLevel;
    }

    @Override
    public int expectedMaximumLength(int var1) {
        return Math.toIntExact(Zstd.compressBound(var1));
    }

    @Override
    public int compress(ByteBuffer des) throws IOException {
        int len;
        if (dict != null) {
            //字典压缩
            len = Math.toIntExact(Zstd.compressDirectByteBufferUsingDict(des, des.position(), des.remaining(), src, src.position(), src.remaining(), dict.array(), compressLevel));
        } else {
            //非字典压缩
            len = Math.toIntExact(Zstd.compressDirectByteBuffer(des,  des.position(), des.remaining(), src, src.position(), src.remaining(), compressLevel));
        }
        des.position(des.position()+len);
        return len;
    }
}
