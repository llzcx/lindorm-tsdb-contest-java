package com.alibaba.lindorm.contest;

import com.alibaba.lindorm.contest.structs.ColumnValue;
import com.alibaba.lindorm.contest.extendstructs.FileName;
import com.alibaba.lindorm.contest.extendstructs.meta.MmapMetaImpl;
import com.alibaba.lindorm.contest.extendstructs.vlog.MmapVLogWriter;
import com.alibaba.lindorm.contest.util.MMapUtil;
import com.github.luben.zstd.Zstd;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.StandardOpenOption;

/**
 * vlog正确性测试
 */
public class VlogTest {

    @Test
    public void test() throws IOException {
        MmapMetaImpl mmapMeta = new MmapMetaImpl(new File(FileName.META_F_NAME));
        MmapVLogWriter writer = new MmapVLogWriter(new File(FileName.STR_COLS_F_NAME),mmapMeta , ColumnValue.ColumnType.COLUMN_TYPE_STRING);

        writer.append();
    }

    @Test
    public void mmapZstdTest() throws IOException{
        File file = new File("mmapZstdTest.bin");
        file.delete();
        file.createNewFile();
        final FileChannel pipe = FileChannel.open(file.toPath(), StandardOpenOption.WRITE, StandardOpenOption.READ);
        final MappedByteBuffer map = pipe.map(FileChannel.MapMode.READ_WRITE, 0, 100);
//        map.load();
        ByteBuffer buffer = ByteBuffer.allocateDirect(100);
        buffer.putInt(100);
        buffer.putInt(200);
        buffer.putInt(300);
        buffer.putInt(400);
        buffer.putInt(500);
        buffer.putInt(600);
        buffer.putInt(700);
        buffer.putInt(800);
        buffer.flip();
        System.out.println("buffer.limit:"+buffer.limit());
        final int size = Math.toIntExact(Zstd.compressDirectByteBuffer(map, 0, 100, buffer, 0, buffer.limit(),20));
        System.out.println("size:"+size);
        ByteBuffer res = ByteBuffer.allocateDirect(100);
        final long l = Zstd.decompressDirectByteBuffer(res, 0, 100, map, 0, 41);
        map.position(Math.toIntExact(l));
        System.out.println("decom:"+l);
        System.out.println(res.getInt());
        System.out.println(res.getInt());
        System.out.println(res.getInt());
        System.out.println(res.getInt());

        MMapUtil.unmap(map);
        pipe.truncate(l);
        pipe.close();
    }

    @Test
    public void read() throws IOException{
        File file = new File("mmapZstdTest.bin");
        final FileChannel pipe = FileChannel.open(file.toPath(), StandardOpenOption.WRITE, StandardOpenOption.READ);
        final MappedByteBuffer map = pipe.map(FileChannel.MapMode.READ_WRITE, 0, 100);
        ByteBuffer res = ByteBuffer.allocateDirect(100);
        final long l = Zstd.decompressDirectByteBuffer(res, 0, 100, map, 0, 41);
        map.position(Math.toIntExact(l));
        System.out.println("decom:"+l);
        System.out.println(res.getInt());
        System.out.println(res.getInt());
        System.out.println(res.getInt());
        System.out.println(res.getInt());
        MMapUtil.unmap(map);
        pipe.truncate(l);
        pipe.close();
    }

    @Test
    public void mmap() throws IOException{
        File file = new File("mmapZstdTest.bin");
        file.createNewFile();
        final FileChannel pipe = FileChannel.open(file.toPath(), StandardOpenOption.WRITE, StandardOpenOption.READ);
        final MappedByteBuffer map = pipe.map(FileChannel.MapMode.READ_WRITE, 0, 100);
        System.out.println(map.position());
        System.out.println(map.remaining());
        System.out.println(map.limit());
    }

}
