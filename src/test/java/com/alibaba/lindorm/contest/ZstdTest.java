package com.alibaba.lindorm.contest;

import com.alibaba.lindorm.contest.extendstructs.DBConstant;
import com.alibaba.lindorm.contest.extendstructs.compression.zstd.TrainData;
import com.alibaba.lindorm.contest.extendstructs.compression.zstd.ZstdDictGenerator;
import com.alibaba.lindorm.contest.extendstructs.compression.zstd.ZstdOffHeapCompressor;
import com.alibaba.lindorm.contest.util.FileUtil;
import com.github.luben.zstd.Zstd;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;

public class ZstdTest {

    private static int size =8*1024*1024;

    @Test
    public void ZSTD_INT(){
        ByteBuffer inBuffer = ByteBuffer.allocateDirect(size);
        ByteBuffer pressBuffer = ByteBuffer.allocateDirect(size);
        final String s = FileUtil.readFileToString("C:\\Users\\陈翔\\Desktop\\int样本.txt");
        final String[] split = s.split("\\[SEC\\]");
        int cnt = 0;
        for (String s1 : split) {
            final int intV = Integer.parseInt(s1);
            inBuffer.putInt(intV);
            cnt++;
        }
        System.out.println("cnt:"+cnt);
        inBuffer.flip();
        int compressionLevel = 7;
        long compressedSize = Zstd.compressDirectByteBuffer(pressBuffer, 0, pressBuffer.capacity(), inBuffer, 0, inBuffer.limit(), compressionLevel);
        pressBuffer.position((int)compressedSize);
        pressBuffer.flip();
        System.out.println("compressedSize:"+compressedSize);
        System.out.println("rate:"+(compressedSize/(4.0*cnt)));
    }

    @Test
    public void ZSTD_DOUBLE(){
        ByteBuffer inBuffer = ByteBuffer.allocateDirect(size);
        ByteBuffer pressBuffer = ByteBuffer.allocateDirect(size);
        final String s = FileUtil.readFileToString("C:\\Users\\陈翔\\Desktop\\double样本.txt");
        final String[] split = s.split("\\[SEC\\]");
        int cnt = 0;
        for (String s1 : split) {
            final double v = Double.parseDouble(s1);
            inBuffer.putDouble(v);
            cnt++;
        }
        System.out.println("cnt:"+cnt);
        inBuffer.flip();
        int compressionLevel = 13;
        long compressedSize = Zstd.compressDirectByteBuffer(pressBuffer, 0, pressBuffer.capacity(), inBuffer, 0, inBuffer.limit(), compressionLevel);
        pressBuffer.position((int)compressedSize);
        pressBuffer.flip();
        System.out.println("compressedSize:"+compressedSize);
        System.out.println("rate:"+(compressedSize/(8.0*cnt)));
    }

    @Test
    public void ZSTD_STRING() throws Exception{
        ByteBuffer inBuffer = ByteBuffer.allocateDirect(size);
        RandomAccessFile randomAccessFile = new RandomAccessFile(new File("test.bin"), "rw");
        randomAccessFile.setLength(size);
        final FileChannel channel = randomAccessFile.getChannel();
        ByteBuffer pressBuffer = channel.map(FileChannel.MapMode.READ_WRITE,0,channel.size());

        int offset = 0;
        final String s = FileUtil.readFileToString("C:\\Users\\陈翔\\Desktop\\string样本.txt");
        final String[] split = s.split("\\[SEC\\]");
        int cnt = 0;
        long size = 0;
        int compressionLevel = 13;
        for (String s1 : split) {
            final byte[] bytes = s1.replace("\n","").getBytes(StandardCharsets.UTF_8);
            inBuffer.put(bytes);
            System.out.println(s1);
            cnt++;
            size += bytes.length;
            size += 4;
            if(cnt%20==0){
                inBuffer.flip();
                long compressedSize = Zstd.compressDirectByteBuffer(pressBuffer, offset, pressBuffer.capacity()-offset, inBuffer, 0, inBuffer.limit(), compressionLevel);
                offset+=(int)compressedSize;
                pressBuffer.position(offset);
                inBuffer.clear();
            }
        }
        if(cnt%20!=0){
            inBuffer.flip();
            long compressedSize = Zstd.compressDirectByteBuffer(pressBuffer, offset, pressBuffer.remaining(), inBuffer, 0, inBuffer.limit(), compressionLevel);
            offset+=(int)compressedSize;
            pressBuffer.position(offset);
            inBuffer.clear();
        }
        System.out.println("cnt:"+cnt);
        System.out.println("compressedSize:"+offset);
        System.out.println("rate:"+(offset/(1.0*size)));
    }


    @Test
    public void bad(){

    }


    @Test
    public void testDict() throws IOException {
        int len = 1024*1024;
        ByteBuffer src = ByteBuffer.allocateDirect(len);
        String s = TrainData.STRING_DATA.replace("\n","");
        src.put(s.getBytes(StandardCharsets.UTF_8));
        double size = src.position();
        System.out.println("source:"+size);

        src.flip();
        Compressor compressor1 = new ZstdOffHeapCompressor(src, ZstdDictGenerator.dict(), DBConstant.COMPRESSION_LEVEL);
        Compressor compressor2 = new ZstdOffHeapCompressor(src, null, DBConstant.COMPRESSION_LEVEL);
        ByteBuffer des  = ByteBuffer.allocateDirect(len);
        final int compress1 = compressor1.compress(des);
        final int compress2 = compressor2.compress(des);
        System.out.println("使用dict："+compress1/size);
        System.out.println("不使用dict："+compress2/size);
    }


    @Test
    public void test03(){
        byte[] bytes = new byte[1];
        bytes[0] =  1;
        System.out.println(new String(bytes));
    }



}
