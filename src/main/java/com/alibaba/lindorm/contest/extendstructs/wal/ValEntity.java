package com.alibaba.lindorm.contest.extendstructs.wal;


import java.nio.ByteBuffer;

/**
 * @author 陈翔
 */
public class ValEntity {
    /**
     * 压缩块下标
     */
    private short compressIndex;

    @Override
    public String toString() {
        return "ValEntity{" +
                "compressIndex=" + compressIndex +
                ", internalIndex=" + internalIndex +
                '}';
    }

    /**
     *  压缩块内部下标
     */
    private short internalIndex;

    public ValEntity(short compressIndex, short internalIndex) {
        this.compressIndex = compressIndex;
        this.internalIndex = internalIndex;
    }

    public void saveToBuffer(ByteBuffer buffer){
        buffer.putShort(compressIndex);
        buffer.putShort(internalIndex);
    }

    public short getCompressIndex() {
        return compressIndex;
    }

    public void setCompressIndex(short compressIndex) {
        this.compressIndex = compressIndex;
    }

    public short getInternalIndex() {
        return internalIndex;
    }

    public void setInternalIndex(short internalIndex) {
        this.internalIndex = internalIndex;
    }
}
