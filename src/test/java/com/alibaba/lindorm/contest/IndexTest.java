package com.alibaba.lindorm.contest;

import com.alibaba.lindorm.contest.extendstructs.FileName;
import com.alibaba.lindorm.contest.extendstructs.wal.ValEntity;
import com.alibaba.lindorm.contest.extendstructs.wal.WalEntry;
import com.alibaba.lindorm.contest.extendstructs.index.OffHeapImmutableIndex;
import com.alibaba.lindorm.contest.extendstructs.offheap.Uns;
import com.alibaba.lindorm.contest.util.BytesUtils;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;

public class IndexTest {

    File file = new File(FileName.WAL_F_NAME);

    /**
     * 线性查询正确性校验
     * @throws IOException
     */
    @Test
    public void lineCorrectnessTest() throws IOException{
        file.delete();
        int len = 20000;
        OffHeapImmutableIndex immutableIndex = new OffHeapImmutableIndex(file,len);
        immutableIndex.threadLocalInitWrite();
        for (int i = 0; i < 10; i++) {
            new WalEntry(i, new ValEntity((short) i, (short)i)).toBytes(immutableIndex.keyAndVlogSeq.get());
            immutableIndex.add();
        }
        System.out.println("---------------start query-----------------");
        final ValEntity valEntity = immutableIndex.linearSearch(9);
        System.out.println("valEntity:"+valEntity);
        immutableIndex.close();
    }

    /**
     * 范围（二分）查询正确性校验
     * @throws IOException
     */
    @Test
    public void binaryCorrectnessTest() throws IOException{
        file.delete();
        int len = 20000;
        OffHeapImmutableIndex immutableIndex = new OffHeapImmutableIndex(file,len);
        immutableIndex.threadLocalInitWrite();
        for (int i = 100; i >= 50; i--) {
            new WalEntry(i*1000, new ValEntity((short) i, (short)i)).toBytes(immutableIndex.keyAndVlogSeq.get());
            immutableIndex.add();
        }
        immutableIndex.close();
        immutableIndex = new OffHeapImmutableIndex(file, len);
        System.out.println("---------------start query-----------------");
        final List<WalEntry> list = immutableIndex.rangeList(170*1000,181*1000);
        System.out.println(list);
        immutableIndex.close();
    }

    /**
     * 二分、线性 查询性能对比
     */
    @Test
    public void Test() throws IOException{
        int len = 20000;
        OffHeapImmutableIndex immutableIndex = new OffHeapImmutableIndex(file,len);
        for (int i = 1; i <= len; i++) {
            new WalEntry((i)*1000,new ValEntity((short) i, (short) i)).toBytes(immutableIndex.keyAndVlogSeq.get());
            immutableIndex.add();
        }
        int element = 10010*1000;
        long t1 = System.nanoTime();
        System.out.println("wal:"+immutableIndex.linearSearch(element));
        long t2 = System.nanoTime();
        long time1 = (t2-t1);
        System.out.println("time:"+time1+"ns");
        System.out.println("--------------------");
        t1 = System.nanoTime();
        System.out.println("index:"+immutableIndex.binarySearchLeft(BytesUtils.int2BytesLittle(element)));
        t2 = System.nanoTime();
        System.out.println("time:"+(t2-t1)+"ns");
        System.out.println("rate:"+time1*1.0/(t2-t1));
    }


    /**
     * Unsafe GET大端小端测试，实测小端
     */
    @Test
    public void testUnsafe()throws IOException{
        final long allocate = Uns.allocate(4);
        Uns.putInt(allocate, 0, 123);
        Uns.putInt(allocate, 4, 456);
        System.out.println(Uns.getByte(allocate, 4));
        System.out.println(Uns.getByte(allocate, 5));
        System.out.println(Uns.getByte(allocate, 6));
        System.out.println(Uns.getByte(allocate, 7));
        System.out.println(Arrays.toString(BytesUtils.int2BytesLittle(456)));
        final int anInt = Uns.getInt(allocate, 4);
        System.out.println(anInt);
    }


    @Test
    public void range()throws IOException{
        int len = 36000;
        OffHeapImmutableIndex immutableIndex = new OffHeapImmutableIndex(file,len);
        long start = Runtime.getRuntime().freeMemory();
        for (int i = 1; i <= len; i++) {
            new WalEntry(i*1000,new ValEntity((short) i, (short) i)).toBytes(immutableIndex.keyAndVlogSeq.get());
            immutableIndex.add();
        }
        long end = Runtime.getRuntime().freeMemory();
        System.out.println("插入占内存："  + (float) (start - end)*1.0/1024/1024 + " Mb");
        long t1 = System.nanoTime();
        start = Runtime.getRuntime().freeMemory();
        final List<WalEntry> list = immutableIndex.rangeList(10000*1000,22000*1000);
        System.out.println(list.size());
        end = Runtime.getRuntime().freeMemory();
        System.out.println("查询占内存："  + (float) (start - end)/1024/1024 + " Mb");
        long t2 = System.nanoTime();
        System.out.println("time:"+(t2-t1)+"ns");
        System.out.println("QPS:"+(1000*1000000)/(t2-t1));
        immutableIndex.close();
    }

    @Test
    public void skiplist(){
        ConcurrentSkipListMap<Long,WalEntry> map = new ConcurrentSkipListMap<>();
        int len = 30000;
        long start = Runtime.getRuntime().freeMemory();
        for (long i = 1; i <= len; i++) {
            map.put(i*1000, new WalEntry(Math.toIntExact(i), new ValEntity((short) i,(short)i)));
        }
        long end = Runtime.getRuntime().freeMemory();
        System.out.println("占内存："  + (float) (start - end)*1.0/1024/1024 + " Mb");
        long t1 = System.nanoTime();
        start = Runtime.getRuntime().freeMemory();
        final ConcurrentNavigableMap<Long, WalEntry> longLongConcurrentNavigableMap = map.subMap(10000 * 1000L, 22000 * 1000L);
        end = Runtime.getRuntime().freeMemory();
        System.out.println("占内存："  + (float) (start - end)/1024/1024 + " Mb");
        long t2 = System.nanoTime();
        System.out.println("time:"+(t2-t1)+"ns");
        System.out.println("QPS:"+(1000*1000000)/(t2-t1));
    }


    @Test
    public void TreeMap(){
        TreeMap<Long,WalEntry> map = new TreeMap<>();
        int len = 30000;
        long start = Runtime.getRuntime().freeMemory();
        for (long i = 1; i <= len; i++) {
            map.put(i*1000, new WalEntry(Math.toIntExact(i), new ValEntity((short) i,(short)i)));
        }
        long end = Runtime.getRuntime().freeMemory();
        System.out.println("占内存："  + (float) (start - end)*1.0/1024/1024 + " Mb");
        long t1 = System.nanoTime();
        final SortedMap<Long, WalEntry> longLongSortedMap = map.subMap(10000 * 1000L, 22000 * 1000L);
        long t2 = System.nanoTime();
        System.out.println("time:"+(t2-t1)+"ns");
        System.out.println("QPS:"+(1000*1000000)/(t2-t1));
    }


    @Test
    public void save() throws IOException {
        int len = 36000;
        OffHeapImmutableIndex immutableIndex = new OffHeapImmutableIndex(file,len);
        for (int i = len; i >= 1; --i) {
            new WalEntry((i)*1000,new ValEntity((short) i, (short) i)).toBytes(immutableIndex.keyAndVlogSeq.get());
            immutableIndex.add();
        }
        //保存
        final File file = new File(FileName.WAL_F_NAME);
        immutableIndex.close();
    }
}
