package com.alibaba.lindorm.contest.test;



import com.alibaba.lindorm.contest.extendstructs.directio.DirectIOLib;
import com.alibaba.lindorm.contest.extendstructs.directio.DirectIOUtils;
import com.alibaba.lindorm.contest.extendstructs.directio.DirectRandomAccessFile;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;

import static com.alibaba.lindorm.contest.extendstructs.directio.DirectIO.BLOCK_SIZE;
import static com.alibaba.lindorm.contest.extendstructs.directio.DirectIO.DIRECTIOLIB;


/**
 * @author 陈翔
 */
public class DirectIOTest {

    public static void main(String[] args) throws IOException{
        if (DirectIOLib.binit) {
            ByteBuffer byteBuffer = DirectIOUtils.allocateForDirectIO(DIRECTIOLIB, 4 * BLOCK_SIZE);
            for (int i = 0; i < BLOCK_SIZE; i++) {
                byteBuffer.putInt(i);
            }

            DirectRandomAccessFile directRandomAccessFile = new DirectRandomAccessFile(new File("./database.data"), "rw");
            directRandomAccessFile.read(byteBuffer, 0);
            byteBuffer.flip();
            for (int i = 0; i < BLOCK_SIZE; i++) {
                System.out.print(byteBuffer.getInt() + " ");
            }
        } else {
            throw new RuntimeException("your system do not support direct io");
        }
    }


}
