package com.alibaba.lindorm.contest.extendstructs.compression.zstd;

import com.alibaba.lindorm.contest.Decompressor;
import com.github.luben.zstd.Zstd;

import java.io.IOException;
import java.nio.ByteBuffer;

/**
 * @author 陈翔
 */
public class ZstdOffHeapDecompressor implements Decompressor {
    private ByteBuffer dict;

    public ZstdOffHeapDecompressor(ByteBuffer dict) {
        this.dict = dict;
    }


    @Override
    public int decompress(ByteBuffer des,ByteBuffer src) throws IOException {
        int len;
        if(dict!=null){
            len = Math.toIntExact(Zstd.decompressDirectByteBufferUsingDict(des, des.position(), des.remaining(), src, src.position(), src.remaining(), dict.array()));
        }else{
            len = Math.toIntExact(Zstd.decompressDirectByteBuffer(des, des.position(), des.remaining(), src, src.position(), src.remaining()));
        }
        des.position(des.position()+len);
        if(len==0){
            throw new IOException("Error decompressed size:"+len);
        }
        return len;
    }

}
