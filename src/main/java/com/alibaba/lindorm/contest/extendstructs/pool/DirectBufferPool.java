package com.alibaba.lindorm.contest.extendstructs.pool;

import com.alibaba.lindorm.contest.extendstructs.directio.DirectIO;
import com.alibaba.lindorm.contest.extendstructs.directio.DirectIOUtils;
import jdk.internal.ref.Cleaner;
import sun.nio.ch.DirectBuffer;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;

/**
 * 池化堆外内存 反复擦写同一块内存区域
 * @author 陈翔
 */
public class DirectBufferPool {

    private static volatile DirectBufferPool INSTANCE = null;

    private Queue<ByteBuffer> queue;

    private volatile boolean init;


    /**
     *
     * @param poolSize
     * @param bufferSize
     * @param forDirectIo 是否需要内存对齐
     * @throws IOException
     */
    public DirectBufferPool(int poolSize, int bufferSize,boolean forDirectIo) throws IOException {
        if (init) {
            return;
        }
        queue = new ConcurrentLinkedQueue<>();
        for (int i = 0; i < poolSize; i++) {
            final ByteBuffer buffer;
            if(forDirectIo){
                buffer = DirectIOUtils.allocateForDirectIO(DirectIO.DIRECTIOLIB, bufferSize);
            }else{
                buffer = ByteBuffer.allocateDirect(bufferSize);
            }
            if (buffer == null) {
                throw new IOException("buffer allocate error.");
            }
            queue.add(buffer);
        }
        init = true;
    }

    public ByteBuffer take() {
        ByteBuffer buffer;
        while ((buffer = queue.poll()) == null) {

        }
        return buffer;
    }

    public void recycle(ByteBuffer buffer) {
        while (!queue.offer(buffer)) {

        }
    }

    public void destroy() {
        ByteBuffer buffer;
        if (queue != null) {
            while ((buffer = queue.poll()) != null) {
                final Cleaner cleaner = ((DirectBuffer) buffer).cleaner();
                if (cleaner != null) {
                    cleaner.clean();
                }
            }
        }
        queue = null;
        init = false;
    }

    public int getQueueSize() {
        return queue.size();
    }
}
