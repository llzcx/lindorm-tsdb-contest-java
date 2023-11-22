package com.alibaba.lindorm.contest;

import com.alibaba.lindorm.contest.extendstructs.vlog.ByteBufferVo;
import com.alibaba.lindorm.contest.structs.ColumnValue;

import java.io.IOException;

/**
 * @author 陈翔
 */
public interface VlogRead {

    void recycle(ByteBufferVo bufferVo);

    ByteBufferVo getDecompressed(short compressIndex) throws IOException;

    void close();

    ColumnValue read(ByteBufferVo bufferVo, short internalIndex, short colIndex) throws IOException;

}
