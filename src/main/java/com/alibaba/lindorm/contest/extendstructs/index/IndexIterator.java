package com.alibaba.lindorm.contest.extendstructs.index;

import com.alibaba.lindorm.contest.extendstructs.offheap.Uns;
import com.alibaba.lindorm.contest.extendstructs.wal.ValEntity;
import com.alibaba.lindorm.contest.extendstructs.wal.WalEntry;
import com.alibaba.lindorm.contest.util.SizeOf;

import static com.alibaba.lindorm.contest.extendstructs.DBConstant.SIZE_OF_KEY;

/**
 * 索引迭代器对象
 *
 * @author 陈翔
 */
public class IndexIterator<T>{

    long baseAdd;
    int l;
    int r;

    public IndexIterator(long baseAdd, int l, int r) {
        this.baseAdd = baseAdd;
        this.l = l;
        this.r = r;
    }

    public int getActualRecordSize() {
        return l+1;
    }

    public boolean hasNext() {
        return l < r;
    }

    public WalEntry next() {
        final int k = Uns.getInt(baseAdd, l);
        final short s1 = Uns.getShort(baseAdd, l + SIZE_OF_KEY);
        final short s2 = Uns.getShort(baseAdd, l + SIZE_OF_KEY + SizeOf.SIZE_OF_SHORT);
        l+=1;
        return new WalEntry(k, new ValEntity(s1, s2));
    }

    public long getBaseAdd() {
        return baseAdd;
    }

    public void setBaseAdd(long baseAdd) {
        this.baseAdd = baseAdd;
    }

    public int getL() {
        return l;
    }

    public void setL(int l) {
        this.l = l;
    }

    public int getR() {
        return r;
    }

    public void setR(int r) {
        this.r = r;
    }
}
