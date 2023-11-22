package com.alibaba.lindorm.contest.extendstructs.meta;

import com.alibaba.lindorm.contest.extendstructs.DBConstant;
import com.alibaba.lindorm.contest.extendstructs.log.TSDBLog;
import com.alibaba.lindorm.contest.structs.ColumnValue;
import com.alibaba.lindorm.contest.util.Closeables;
import com.alibaba.lindorm.contest.util.MMapUtil;
import com.alibaba.lindorm.contest.util.SizeOf;
import org.apache.log4j.Logger;

import java.io.File;
import java.io.IOException;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.StandardOpenOption;
import java.util.concurrent.locks.ReentrantLock;

/**
 * mmap 元数据管理
 *
 * @author 陈翔
 */
public class MmapMetaImpl implements DBConstant {
    public static Logger logger = TSDBLog.getLogger();
    /**
     * 压缩块的数量
     */
    public static int PRESS_BLOCK_NUM = TIME_LENGTH / COMPRESS_LIMIT;

    /**
     * 每种类型需要占用的大小
     */
    public static int SIZE_OF_PER_TYPE = PRESS_BLOCK_NUM * SIZE_OF_META_BLOCK;

    /**
     * 前6个字节 从左到右每个short存数量：str int double
     */
    public static int FRONT_SIZE = SizeOf.SIZE_OF_SHORT*3;
    private static final int STR_OFFSET = FRONT_SIZE;
    private static final int INT_OFFSET = STR_OFFSET + SIZE_OF_PER_TYPE;
    private static int DOU_OFFSET = INT_OFFSET + SIZE_OF_PER_TYPE;

    /**
     * 最少需要的空间
     */
    public static int PAGE_SIZE = SIZE_OF_PER_TYPE * 3 + FRONT_SIZE;

    public static int OUT_OF_BOUND = -1;


    static {
        logger.info("Meta occupies the least space:" + PAGE_SIZE + "B.");
        if(PAGE_SIZE > 4*SizeOf.R1k){
            logger.warn("[warning] We suggest that the maximum metadata usage should not exceed 4k("+4*SizeOf.R1k+"B)");
        }
    }




    private FileChannel pipe;

    private MappedByteBuffer mapped;

    private short strCurPos = 0;
    private short intCurPos = 0;
    private short douCurPos = 0;

    public MmapMetaImpl(File file) throws IOException {
        boolean flag = file.exists();
        if(!flag){
            file.createNewFile();
        }
        this.pipe = FileChannel.open(file.toPath(), StandardOpenOption.WRITE,StandardOpenOption.READ);
        //TODO append position should be file size
        mapped = pipe.map(FileChannel.MapMode.READ_WRITE, 0, PAGE_SIZE);
        if(flag){
            strCurPos = mapped.getShort();
            intCurPos = mapped.getShort();
            douCurPos = mapped.getShort();
        }else{
            append(ColumnValue.ColumnType.COLUMN_TYPE_STRING,0);
            append(ColumnValue.ColumnType.COLUMN_TYPE_DOUBLE_FLOAT,0);
            append(ColumnValue.ColumnType.COLUMN_TYPE_INTEGER,0);
        }
    }

    public void flush(){
        if(mapped!=null){
            mapped.position(0);
            mapped.putShort(strCurPos);
            mapped.putShort(intCurPos);
            mapped.putShort(douCurPos);
            mapped.position(PAGE_SIZE);
//            System.out.println("Meta刷盘时每个类型的数量："+strCurPos/SIZE_OF_META_BLOCK+","+intCurPos/SIZE_OF_META_BLOCK+","+douCurPos/SIZE_OF_META_BLOCK);
            MMapUtil.unmap(mapped);
            mapped = null;
        }
    }

    public void close() throws IOException {
        Closeables.closeQuietly(pipe);
        pipe = null;
    }

    /**
     * @param type  写入的类型
     * @param index 第n个压缩块的偏移量
     * @throws IOException
     */
    public void append(ColumnValue.ColumnType type, int index) throws IOException {
//        System.out.println("Metaappend:"+type+","+index);
        try {
            switch (type) {
                case COLUMN_TYPE_STRING:
                    mapped.putInt(STR_OFFSET + strCurPos,index);
                    strCurPos += 4;
                    break;
                case COLUMN_TYPE_INTEGER:
                    mapped.putInt(INT_OFFSET + intCurPos,index);
                    intCurPos += 4;
                    break;
                case COLUMN_TYPE_DOUBLE_FLOAT:
                    mapped.putInt(DOU_OFFSET + douCurPos,index);
                    douCurPos += 4;
                    break;
                default:
                    break;
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /**
     * @param type 类型
     * @param compressIndex  第几个压缩块(从0开始)
     * @return 偏移量（该压缩块的偏移量）
     */
    public int read(ColumnValue.ColumnType type, short compressIndex) throws IOException{
        try {
            int offset = compressIndex*4;
            switch (type) {
                case COLUMN_TYPE_STRING:
                    if(offset < strCurPos){
                        return mapped.getInt(STR_OFFSET + offset);
                    }
                    break;
                case COLUMN_TYPE_INTEGER:
                    if(offset < intCurPos){
                        return mapped.getInt(INT_OFFSET + offset);
                    }
                    break;
                case COLUMN_TYPE_DOUBLE_FLOAT:
                    if(offset < douCurPos){
                        return mapped.getInt(DOU_OFFSET + offset);
                    }
                    break;
                default:
                    break;
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        return OUT_OF_BOUND;
    }



}
