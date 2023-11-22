package com.alibaba.lindorm.contest.extendstructs.agg;

import com.alibaba.lindorm.contest.extendstructs.DBConstant;
import com.alibaba.lindorm.contest.extendstructs.log.TSDBLog;
import com.alibaba.lindorm.contest.util.Closeables;
import com.alibaba.lindorm.contest.util.MMapUtil;
import com.alibaba.lindorm.contest.util.SizeOf;
import org.apache.log4j.Logger;

import java.io.File;
import java.io.IOException;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.StandardOpenOption;

/**
 * mmap 存储聚合结果
 *
 * @author 陈翔
 */
public class MmapAggImpl implements DBConstant {
    public static final Logger logger = TSDBLog.getLogger();
    /**
     * 压缩块的数量
     */
    public static final int PRESS_BLOCK_NUM = TIME_LENGTH / COMPRESS_LIMIT;

    /**
     * 一个int row的max占用大小 40个int
     */
    private static final int SIZE_OF_A_ROW_INT_MAX = INT_NUM_OF_A_ROW * SizeOf.SIZE_OF_INT;
    /**
     * 一个int row的total占用大小 40个double
     */
    private static final int SIZE_OF_A_ROW_INT_TOTAL = INT_NUM_OF_A_ROW * SizeOf.SIZE_OF_DOUBLE;
    /**
     * 一个double row的max、total占用大小 10个double
     */
    public static final int SIZE_OF_A_ROW_DOUBLE_MAX_OR_TOTAL = DOUBLE_NUM_OF_A_ROW * SizeOf.SIZE_OF_DOUBLE;

    /**
     * int max需要占用的大小
     */
    public static final int SIZE_OF_BLOCK_INT_MAX = PRESS_BLOCK_NUM * SIZE_OF_A_ROW_INT_MAX;

    /**
     * int total需要占用的大小
     */
    public static final int SIZE_OF_BLOCK_INT_TOTAL = PRESS_BLOCK_NUM * SIZE_OF_A_ROW_INT_TOTAL;

    /**
     * duble max and total需要占用的大小
     */
    public static int SIZE_OF_BLOCK_DOU_MAX_OR_TOTAL = PRESS_BLOCK_NUM * SIZE_OF_A_ROW_DOUBLE_MAX_OR_TOTAL;

    /**
     * 前4个字节 从左到右每个short存数量int double
     */
    public static int FRONT_SIZE = SizeOf.SIZE_OF_SHORT * 2;
    //第一个存放int max
    private static final int INT_MAX_OFFSET = FRONT_SIZE;
    //第二个存放int total
    private static final int INT_TOTAL_OFFSET = INT_MAX_OFFSET + SIZE_OF_BLOCK_INT_MAX;
    //第三个存放double max
    private static final int DOU_MAX_OFFSET = INT_TOTAL_OFFSET + SIZE_OF_BLOCK_INT_TOTAL;
    //第三个存放double total
    private static final int DOU_TOTAL_OFFSET = DOU_MAX_OFFSET + SIZE_OF_BLOCK_DOU_MAX_OR_TOTAL;
    /**
     * 最少需要的空间
     */
    public static final int PAGE_SIZE = DOU_TOTAL_OFFSET + SIZE_OF_BLOCK_DOU_MAX_OR_TOTAL;


    static {
        logger.info("Agg value occupies the least space:" + PAGE_SIZE + "B.");
    }


    private FileChannel pipe;

    private MappedByteBuffer mapped;


    private short intCompressIndex = 0;
    private short intColumnIndex = 0;
    private short douCompressIndex = 0;
    private short douColumnIndex = 0;

    public MmapAggImpl(File file) throws IOException {
        boolean flag = file.exists();
        if (!flag) {
            file.createNewFile();
        }
        this.pipe = FileChannel.open(file.toPath(), StandardOpenOption.WRITE, StandardOpenOption.READ);
        //TODO append position should be file size
        mapped = pipe.map(FileChannel.MapMode.READ_WRITE, 0, PAGE_SIZE);
        if (flag) {
            intCompressIndex = mapped.getShort();
            douCompressIndex = mapped.getShort();
        }
    }


    public void flush() {
        if (mapped != null) {
            mapped.position(0);
            mapped.putShort(intCompressIndex);
            mapped.putShort(douCompressIndex);
            mapped.position(PAGE_SIZE);
            MMapUtil.unmap(mapped);
            mapped = null;
        }
    }

    public void close() throws IOException {
        Closeables.closeQuietly(pipe);
        pipe = null;
    }

    public void appendInt(int maxValue, double totalValue) {
        int maxOff = intCompressIndex * SIZE_OF_A_ROW_INT_MAX + intColumnIndex * SizeOf.SIZE_OF_INT;
        int totalOff = intCompressIndex * SIZE_OF_A_ROW_INT_TOTAL + intColumnIndex * SizeOf.SIZE_OF_DOUBLE;
        mapped.putInt(INT_MAX_OFFSET + maxOff, maxValue);
        mapped.putDouble(INT_TOTAL_OFFSET + totalOff, totalValue);
        intColumnIndex++;
        if (intColumnIndex == INT_NUM_OF_A_ROW) {
            intCompressIndex += 1;
            intColumnIndex = 0;
        }
    }

    public void appendDouble(double maxValue, double totalValue) {
        int off = douCompressIndex * SIZE_OF_A_ROW_DOUBLE_MAX_OR_TOTAL + douColumnIndex * SizeOf.SIZE_OF_DOUBLE;
        mapped.putDouble(DOU_MAX_OFFSET + off, maxValue);
        mapped.putDouble(DOU_TOTAL_OFFSET + off, totalValue);
        douColumnIndex++;
        if (douColumnIndex == DOUBLE_NUM_OF_A_ROW) {
            douCompressIndex += 1;
            douColumnIndex = 0;
        }
    }


    public int readIntMax(short compressIndex, short colIndex) {
        int off = compressIndex * SIZE_OF_A_ROW_INT_MAX + colIndex * SizeOf.SIZE_OF_INT;
        return mapped.getInt(INT_MAX_OFFSET + off);
    }

    public double readIntTotal(short compressIndex, short colIndex) {
        int off = compressIndex * SIZE_OF_A_ROW_INT_TOTAL + colIndex * SizeOf.SIZE_OF_DOUBLE;
        return mapped.getDouble(INT_TOTAL_OFFSET + off);
    }

    public double readDoubleMax(short compressIndex, short colIndex) {
        int off = compressIndex * SIZE_OF_A_ROW_DOUBLE_MAX_OR_TOTAL + colIndex * SizeOf.SIZE_OF_DOUBLE;
        return mapped.getDouble(DOU_MAX_OFFSET + off);
    }

    public double readDoubleTotal(short compressIndex, short colIndex) {
        int off = compressIndex * SIZE_OF_A_ROW_DOUBLE_MAX_OR_TOTAL + colIndex * SizeOf.SIZE_OF_DOUBLE;
        return mapped.getDouble(DOU_TOTAL_OFFSET + off);
    }

}
