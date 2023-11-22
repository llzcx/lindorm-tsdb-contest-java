package com.alibaba.lindorm.contest.extendstructs.vlog;

import com.alibaba.lindorm.contest.Decompressor;
import com.alibaba.lindorm.contest.VlogRead;
import com.alibaba.lindorm.contest.extendstructs.DBConstant;
import com.alibaba.lindorm.contest.extendstructs.agg.MmapAggImpl;
import com.alibaba.lindorm.contest.extendstructs.compression.zstd.ZstdOffHeapDecompressor;
import com.alibaba.lindorm.contest.extendstructs.log.TSDBLog;
import com.alibaba.lindorm.contest.extendstructs.meta.MmapMetaImpl;
import com.alibaba.lindorm.contest.extendstructs.pool.DirectBufferPool;
import com.alibaba.lindorm.contest.structs.ColumnValue;
import com.alibaba.lindorm.contest.util.Closeables;
import com.alibaba.lindorm.contest.util.MMapUtil;
import org.apache.log4j.Logger;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.StandardOpenOption;

import static com.alibaba.lindorm.contest.extendstructs.DBConstant.COMPRESS_LIMIT;
import static com.alibaba.lindorm.contest.extendstructs.meta.MmapMetaImpl.OUT_OF_BOUND;

/**无锁vlog读
 * @author 陈翔
 */
public class MmapVlogReader implements VlogRead{
    public static Logger logger = TSDBLog.getLogger();

    private FileChannel pipe;

    private ColumnValue.ColumnType type;

    private MmapVLogWriter writer;

    private MmapMetaImpl mmapMeta;

    private DirectBufferPool pool;

    private Decompressor decompressor;

    public MmapAggImpl mmapAgg;

    public MmapVlogReader(File file, MmapMetaImpl mmapMeta, ColumnValue.ColumnType type, MmapVLogWriter writer, DirectBufferPool pool, MmapAggImpl mmapAgg) throws IOException {
        this.pipe = FileChannel.open(file.toPath(), StandardOpenOption.READ);
        this.type = type;
        this.writer = writer;
        this.mmapMeta = mmapMeta;
        this.pool = pool;
        this.mmapAgg = mmapAgg;
        switch (type) {
            case COLUMN_TYPE_STRING:
                this.decompressor = new ZstdOffHeapDecompressor(null);
                break;
            case COLUMN_TYPE_INTEGER:
                this.decompressor = new ZstdOffHeapDecompressor(null);
                break;
            case COLUMN_TYPE_DOUBLE_FLOAT:
                this.decompressor = new ZstdOffHeapDecompressor(null);
                break;
            default:
                break;
        }
    }

    @Override
    public ColumnValue read(ByteBufferVo bufferVo,short internalIndex, short colIndex) throws IOException {
        if (bufferVo.inWriter) {
            return writer.getVlog(internalIndex, colIndex);
        } else {
            return ReadUtil.getVlog(bufferVo.decompressed, COMPRESS_LIMIT, type, internalIndex, colIndex);
        }
    }


    @Override
    public void close() {
        Closeables.closeQuietly(pipe);
    }

    @Override
    public ByteBufferVo getDecompressed(short compressIndex) throws IOException {
        boolean flag = writer != null && writer.canRead(compressIndex);
        if(flag){
            return new ByteBufferVo(null,true);
        }else{
            final ByteBuffer decompressed = pool.take();
            //在磁盘中
            final int start = mmapMeta.read(type, compressIndex);
            final int next = mmapMeta.read(type, (short) (compressIndex + 1));
            final int end = Math.toIntExact(next == OUT_OF_BOUND ? pipe.size() : next);
            MappedByteBuffer map = pipe.map(FileChannel.MapMode.READ_ONLY, start, end-start);
            //清理出待写入的des
            decompressed.clear();
            //将数据解压入mmap
            decompressor.decompress(decompressed,map);
            MMapUtil.unmap(map);
            return new ByteBufferVo(decompressed,false);
        }
    }



    @Override
    public void recycle(ByteBufferVo bufferVo) {
        if(bufferVo.decompressed!=null){
            pool.recycle(bufferVo.decompressed);
        }
    }
}
