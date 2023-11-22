package com.alibaba.lindorm.contest.extendstructs.wal;

/**
 * @author 陈翔
 */
public class CmpIndexValue {

    private Integer compressTp;
    private Short internalIndex;

    public CmpIndexValue(Integer compressTp, Short internalIndex) {
        this.compressTp = compressTp;
        this.internalIndex = internalIndex;
    }

    public Integer getCompressTp() {
        return compressTp;
    }

    public void setCompressTp(Integer compressTp) {
        this.compressTp = compressTp;
    }

    public Short getInternalIndex() {
        return internalIndex;
    }

    public void setInternalIndex(Short internalIndex) {
        this.internalIndex = internalIndex;
    }
}
