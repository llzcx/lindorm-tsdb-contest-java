package com.alibaba.lindorm.contest.extendstructs.wal;


import java.util.Comparator;

import static com.alibaba.lindorm.contest.extendstructs.DBConstant.SIZE_OF_KEY;

/**
 * key字典序比较器
 *
 * @author 陈翔
 */
public final class WalKeyComparator implements Comparator<byte[]> {

    private WalKeyComparator(){

    }

    public static final WalKeyComparator COMPARATOR = new WalKeyComparator();

    @Override
    public int compare(byte[] a, byte[] b) {
        for (int j = SIZE_OF_KEY - 1; j >= 0; --j) {
            int thisByte = 0xFF & a[j];
            int thatByte = 0xFF & b[j];
            if (thisByte != thatByte) {
                return (thisByte) - (thatByte);
            }
        }
        return 0;

    }

}
