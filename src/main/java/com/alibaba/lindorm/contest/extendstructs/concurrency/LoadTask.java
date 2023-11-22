package com.alibaba.lindorm.contest.extendstructs.concurrency;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * 磁盘加载任务,加载压缩块
 * @author 陈翔
 */
public class LoadTask implements Runnable{

    private FileChannel pipe;
    private int off;
    private int size;
    private short compressIndex;
    private Map<Short, ByteBuffer> mp;


    public LoadTask(FileChannel pipe, int off, int size, short compressIndex, ConcurrentHashMap<Short, ByteBuffer> mp) {
        this.pipe = pipe;
        this.off = off;
        this.size = size;
        this.compressIndex = compressIndex;
        this.mp = mp;
    }

    @Override
    public void run() {
        try {
            final MappedByteBuffer map = pipe.map(FileChannel.MapMode.READ_ONLY, off, size);
            map.load();
            mp.put(compressIndex,map);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
