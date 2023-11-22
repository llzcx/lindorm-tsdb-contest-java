package com.alibaba.lindorm.contest.extendstructs.directio;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;

import static com.alibaba.lindorm.contest.extendstructs.directio.DirectIO.BLOCK_SIZE;
import static com.alibaba.lindorm.contest.extendstructs.directio.DirectIO.DIRECTIOLIB;

/**
 * @author daofeng.xjf
 */
public class DirectIOTestMain {

    public static void main(String[] args) throws IOException {
        write();
        read();
    }

    private static void write() throws IOException {
        if (DirectIOLib.binit) {
            ByteBuffer byteBuffer = DirectIOUtils.allocateForDirectIO(DIRECTIOLIB, 4 * BLOCK_SIZE);
            for (int i = 0; i < BLOCK_SIZE; i++) {
                byteBuffer.putInt(i);
            }
            byteBuffer.flip();
            DirectRandomAccessFile directRandomAccessFile = new DirectRandomAccessFile(new File("./database.data"), "rw");
            directRandomAccessFile.write(byteBuffer, 0);
        } else {
            throw new RuntimeException("your system do not support direct io");
        }
    }

    public static void read() throws IOException {
        if (DirectIOLib.binit) {
            ByteBuffer byteBuffer = DirectIOUtils.allocateForDirectIO(DIRECTIOLIB, 4 * BLOCK_SIZE);
            DirectRandomAccessFile directRandomAccessFile = new DirectRandomAccessFile(new File("./database.data"), "rw");
            directRandomAccessFile.read(byteBuffer, BLOCK_SIZE/2 * 4);
            byteBuffer.flip();
            for (int i = 0; i < BLOCK_SIZE/2 ; i++) {
                System.out.print(byteBuffer.getInt() + " ");
            }
        } else {
            throw new RuntimeException("your system do not support direct io");
        }
    }

    /**
     *
     * @param input
     * @return
     */
    public String test(String input){
        return null;
    }

}
