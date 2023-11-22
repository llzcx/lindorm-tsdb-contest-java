package com.alibaba.lindorm.contest.extendstructs.index;


import com.alibaba.lindorm.contest.ImmutableIndex;
import com.alibaba.lindorm.contest.MetaIterator;
import com.alibaba.lindorm.contest.extendstructs.DBConstant;
import com.alibaba.lindorm.contest.extendstructs.log.TSDBLog;
import com.alibaba.lindorm.contest.extendstructs.offheap.Uns;
import com.alibaba.lindorm.contest.extendstructs.wal.*;
import com.alibaba.lindorm.contest.util.BytesUtils;
import com.alibaba.lindorm.contest.util.Closeables;
import com.alibaba.lindorm.contest.util.MMapUtil;
import com.alibaba.lindorm.contest.util.SizeOf;
import org.apache.log4j.Logger;
import sun.nio.ch.DirectBuffer;

import java.io.File;
import java.io.IOException;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;


/**
 * 堆外二分查找的索引，用于read
 *
 * @author 陈翔
 */
public class OffHeapImmutableIndex implements ImmutableIndex, DBConstant {
    public static Logger logger = TSDBLog.getLogger();

    private FileChannel pipe;

    private MappedByteBuffer buffer;

    public long baseAddress;

    private int offset = 0;

    private int index = -1;

    private int min;

    private int max;

    public ThreadLocal<byte[]> target1 = new ThreadLocal<>();

    public ThreadLocal<byte[]> target2 = new ThreadLocal<>();

    public ThreadLocal<byte[]> keyAndVlogSeq = new ThreadLocal<>();



    public OffHeapImmutableIndex(File file,int maxRecordSize) throws IOException {
        if(file.exists()){
            this.baseAddress = Uns.allocate((long) maxRecordSize * SIZE_OF_WAL_RECORD);
            //读取wal并排序,准备索引
            MetaIterator<byte[]> metaIterator = new MmapWalMetaIterator(file);
            sort(metaIterator);
            metaIterator.close();
            metaIterator.freeUp();
        }else{
            file.createNewFile();
            pipe = FileChannel.open(file.toPath(), StandardOpenOption.READ,StandardOpenOption.WRITE);
            buffer = pipe.map(FileChannel.MapMode.READ_WRITE,0,(long) maxRecordSize *SIZE_OF_WAL_RECORD);
            buffer.load();
            this.baseAddress = ((DirectBuffer) buffer).address();
        }
    }

    public void threadLocalInitWrite(){
        keyAndVlogSeq.set(new byte[SIZE_OF_WAL_RECORD]);
    }

    public void threadLocalInitRead(){
        target1.set(new byte[SIZE_OF_KEY]);
        target2.set(new byte[SIZE_OF_KEY]);
    }

    public void close() throws IOException{
        if(pipe!=null){
            MMapUtil.unmap(buffer);
            pipe.truncate(offset);
            Closeables.closeQuietly(pipe);
            pipe = null;
        }else{
            Uns.free(baseAddress);
        }
        buffer = null;
        target1.remove();
        target2.remove();
        keyAndVlogSeq.remove();
    }


    /**
     * 查找第一个大于等于targetKey [targetKey必须是小端]
     *
     * @param targetKey
     * @return
     */
    public int binarySearchLeft(byte[] targetKey) {
        int left = 0;
        int right = index;
        while (left <= right) {
            int mid = (left + right) >> 1;
            int compareResult = 0;
            for (int j = SIZE_OF_KEY - 1; j >= 0; --j) {
                int thisByte = 0xFF & Uns.getByte(baseAddress, SIZE_OF_WAL_RECORD * mid + j);
                int thatByte = 0xFF & targetKey[j];
                if (thisByte != thatByte) {
                    compareResult = thisByte - thatByte;
                    break;
                }
            }
            if (compareResult >= 0) {
                right = mid - 1;
            } else {
                left = mid + 1;
            }
        }
        return left > index ? index+1 : left;
    }

    /**
     * 从右往左，查找第一个小于targetKey [targetKey必须是小端]
     *
     * @param targetKey
     * @return
     */
    public int binarySearchRight(byte[] targetKey) {
        int left = 0;
        int right = index;
        int index = -1;
        while (left <= right) {
            int mid = (left + right) >> 1;
            int compareResult = 0;
            for (int j = SIZE_OF_KEY - 1; j >= 0; --j) {
                int thisByte = 0xFF & Uns.getByte(baseAddress, SIZE_OF_WAL_RECORD * mid + j);
                int thatByte = 0xFF & targetKey[j];
                if (thisByte != thatByte) {
                    compareResult = thisByte - thatByte;
                    break;
                }
            }
            if (compareResult < 0) {
                index = mid;
                left = mid + 1;
            } else {
                right = mid - 1;
            }
        }
        return index;
    }

    /**
     * 线性查询，反向查询加快效率
     *
     * @param cmp
     * @return
     */
    @Override
    public ValEntity linearSearch(int cmp) throws IOException {
        byte[] targetKey = BytesUtils.int2BytesLittle(cmp);
        for (int i = index ; i >= 0; --i) {
            boolean flag = true;
            int offset = SIZE_OF_WAL_RECORD * i;
            for (int j = SIZE_OF_KEY - 1; j >= 0; --j) {
                int thisByte = 0xFF & Uns.getByte(baseAddress, offset + j);
                int thatByte = 0xFF & targetKey[j];
                if (thisByte != thatByte) {
                    flag = false;
                    break;
                }
            }
            if (flag) {
                short s1 = Uns.getShort(baseAddress, offset + SIZE_OF_KEY);
                short s2 = Uns.getShort(baseAddress, offset + SIZE_OF_KEY + SizeOf.SIZE_OF_SHORT);
                return new ValEntity(s1, s2);
            }
        }
        throw new IOException("Check query not find.");
    }


    /**
     * 返回list
     * @return
     */
    @Override
    public List<WalEntry> rangeList(int leftLimit,int rightLimit) {
        if(target1.get()==null){
            threadLocalInitRead();
        }
        BytesUtils.int2BytesLittle(leftLimit, target1.get());
        int l = binarySearchLeft(target1.get());
        BytesUtils.int2BytesLittle(rightLimit, target2.get());
        int r = binarySearchRight(target2.get());

        if (l <= r) {
            List<WalEntry> list = new ArrayList<>(r-l);
            for (int i = l; i <= r ; i++) {
                int offset = i*SIZE_OF_WAL_RECORD;
                final int key = Uns.getInt(baseAddress, offset);
                final short compressIndex = Uns.getShort(baseAddress, offset + SIZE_OF_KEY);
                final short internalIndex = Uns.getShort(baseAddress, offset + SIZE_OF_KEY + SizeOf.SIZE_OF_SHORT);
                list.add(new WalEntry(key,new ValEntity(compressIndex,internalIndex)));
            }
            return list;
        }
        return new ArrayList<>();
    }


    /**
     *
     * @param leftLimit
     * @param rightLimit
     * @return compressIndex -> list(rowIndex)
     */
    public HashMap<Short, ArrayList<CmpIndexValue>> rangeMapToKV(int leftLimit, int rightLimit) {
        if(target1.get()==null){
            threadLocalInitRead();
        }
        BytesUtils.int2BytesLittle(leftLimit, target1.get());
        int l = binarySearchLeft(target1.get());
        BytesUtils.int2BytesLittle(rightLimit, target2.get());
        int r = binarySearchRight(target2.get());
        HashMap<Short, ArrayList<CmpIndexValue>> mp = new HashMap<>();
        if (l <= r) {
            for (int i = l; i <= r ; i++) {
                int offset = i*SIZE_OF_WAL_RECORD;
                final int key = Uns.getInt(baseAddress, offset);
                final short compressIndex = Uns.getShort(baseAddress, offset + SIZE_OF_KEY);
                final short internalIndex = Uns.getShort(baseAddress, offset + SIZE_OF_KEY + SizeOf.SIZE_OF_SHORT);
                final ArrayList<CmpIndexValue> list = mp.computeIfAbsent(compressIndex, k -> new ArrayList<>());
                list.add(new CmpIndexValue(key,internalIndex));
            }
        }
        return mp;
    }


    public HashMap<Short, ArrayList<Short>> rangeMapToRowIndex(int leftLimit, int rightLimit) {
        if(target1.get()==null){
            threadLocalInitRead();
        }
        BytesUtils.int2BytesLittle(leftLimit, target1.get());
        int l = binarySearchLeft(target1.get());
        BytesUtils.int2BytesLittle(rightLimit, target2.get());
        int r = binarySearchRight(target2.get());
        HashMap<Short, ArrayList<Short>> mp = new HashMap<>();
        if (l <= r) {
            for (int i = l; i <= r ; i++) {
                int offset = i*SIZE_OF_WAL_RECORD;
                //final int key = Uns.getInt(baseAddress, offset);
                final short compressIndex = Uns.getShort(baseAddress, offset + SIZE_OF_KEY);
                final short internalIndex = Uns.getShort(baseAddress, offset + SIZE_OF_KEY + SizeOf.SIZE_OF_SHORT);
                final ArrayList<Short> list = mp.computeIfAbsent(compressIndex, k -> new ArrayList<>());
                list.add(internalIndex);
            }
        }
        return mp;
    }
    @Override
    public int getSize() {
        return index + 1;
    }


    @Override
    public void add() {
        Uns.copyMemory(keyAndVlogSeq.get(), 0, baseAddress, offset, SIZE_OF_WAL_RECORD);
        offset += SIZE_OF_WAL_RECORD;
        index++;
    }


    private void add(byte[] bytes) {
        Uns.copyMemory(bytes, 0, baseAddress, offset, SIZE_OF_WAL_RECORD);
        offset += SIZE_OF_WAL_RECORD;
        index++;
    }



    /**
     * 从磁盘当中读取数据排序，逐个拷贝到堆外
     *
     * @param iterator
     */
    private void sort(MetaIterator<byte[]> iterator) {
        byte[][] records = new byte[iterator.getRecordSize()][];
        int pos = 0;
        while (iterator.hasNext()) {
            records[pos++] = iterator.next();
        }
        final int actualRecordSize = iterator.getActualRecordSize();
        Arrays.sort(records, 0, actualRecordSize, WalKeyComparator.COMPARATOR);
        for (int i = 0; i < actualRecordSize; i++) {
            add(records[i]);
        }
        min = BytesUtils.bytes2IntLittle(records[0]);
        max = BytesUtils.bytes2IntLittle(records[actualRecordSize-1]);
    }

}
