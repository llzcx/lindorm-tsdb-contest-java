package com.alibaba.lindorm.contest.extendstructs.vlog;

import com.alibaba.lindorm.contest.Compressor;
import com.alibaba.lindorm.contest.VLogWriter;
import com.alibaba.lindorm.contest.extendstructs.DBConstant;
import com.alibaba.lindorm.contest.extendstructs.compression.zstd.ZstdOffHeapCompressor;
import com.alibaba.lindorm.contest.extendstructs.log.TSDBLog;
import com.alibaba.lindorm.contest.extendstructs.meta.MmapMetaImpl;
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


/**
 * vlog mmap写入器
 *
 * @author 陈翔
 */
public class MmapVLogWriter implements DBConstant, VLogWriter {
    public static Logger logger = TSDBLog.getLogger();

    public static int STR_DES_SIZE;
    public static int INT_DES_SIZE;
    public static int DOU_DES_SIZE;
    /**
     * 文件描述符
     */
    private final File file;
    /**
     * src大小
     */
    private int srcSize;
    /**
     * 缓冲区大小 4*KB的倍数
     */
    private int desSize;
    /**
     * 文件描述符
     */
    private final FileChannel pipe;

    /**
     * 文件大小
     */
    private int fileSize;

    /**
     * 待压缩缓冲区
     */
    public ByteBuffer src;

    /**
     * 待写入磁盘的缓冲区mmap
     */
    private MappedByteBuffer des;
    /**
     * 当前src中记录条数
     */
    public short srcNum = 0;

    public double avg = DOUBLE_NaN;

    public double max = DOUBLE_NaN;

    /**
     * 当前有多少个压缩单位
     */
    public short compressNum = 0;
    /**
     * 类型
     */
    private ColumnValue.ColumnType type;
    /**
     * mmap
     */
    private MmapMetaImpl mmapMeta;

    private Compressor compressor;


    static {
        STR_DES_SIZE = getDesSize(STRING_MAX_BYTE_SIZE*COMPRESS_LIMIT/1024.0);
        INT_DES_SIZE = getDesSize(INT_MAX_BYTE_SIZE*COMPRESS_LIMIT/1024.0);
        DOU_DES_SIZE = getDesSize(DOUBLE_MAX_BYTE_SIZE*COMPRESS_LIMIT/1024.0);
        logger.info("Writer Des info:"+
        "INT_DES_SIZE:"+INT_DES_SIZE
        +"KB,DOU_DES_SIZE:"+DOU_DES_SIZE
        +"KB,STR_DES_SIZE:"+STR_DES_SIZE+"KB.");
    }

    public MmapVLogWriter(File file, MmapMetaImpl meta, ColumnValue.ColumnType type) throws IOException {
        this.file = file;
        if (!file.exists()) {
            file.createNewFile();
        }
        pipe = FileChannel.open(file.toPath(), StandardOpenOption.READ, StandardOpenOption.WRITE);
        //一开始的长度
        fileSize = 0;
        this.mmapMeta = meta;
        this.type = type;
        switch (type) {
            case COLUMN_TYPE_STRING:
                //压缩块的偏移量存储+实际字符串存储
                this.srcSize = (DBConstant.SIZE_OF_STR_ROW_OFFSET + DBConstant.STRING_MAX_BYTE_SIZE) * COMPRESS_LIMIT;
                src = ByteBuffer.allocateDirect(srcSize);
                //先偏移到指定位置
                clearSrc();
                this.desSize = STR_DES_SIZE * 1024 + 4096;
                //初始化压缩器的输入端和输出端
                compressor = new ZstdOffHeapCompressor(src, null, COMPRESSION_LEVEL);
                break;
            case COLUMN_TYPE_INTEGER:
                this.srcSize = DBConstant.INT_MAX_BYTE_SIZE * COMPRESS_LIMIT;
                src = ByteBuffer.allocateDirect(srcSize);
                this.desSize = INT_DES_SIZE * 1024  + 4096;;
                //初始化压缩器的输入端和输出端
                compressor = new ZstdOffHeapCompressor(src, null, COMPRESSION_LEVEL);
                break;
            case COLUMN_TYPE_DOUBLE_FLOAT:
                this.srcSize = DBConstant.DOUBLE_MAX_BYTE_SIZE * COMPRESS_LIMIT;
                src = ByteBuffer.allocateDirect(srcSize);
                this.desSize = DOU_DES_SIZE * 1024  + 4096;;
                //初始化压缩器的输入端和输出端
                compressor = new ZstdOffHeapCompressor(src, null, COMPRESSION_LEVEL);
                break;
            default:
                break;
        }
        des = getMMap();

    }


    /**
     * 一次append20个col
     *
     * @return
     * @throws IOException
     */
    @Override
    public boolean append() throws IOException {
        ++srcNum;
        //检查是否已经达到可压缩标准
        if (checkConditions()) {
            //切换读模式
            src.flip();
            //检查des容量是否足够
            final long need = compressor.expectedMaximumLength(src.remaining());
            if (des.remaining() < need) {
                throw new IOException("Des container capacity is too small." + "{" + "Type=" + type + ",Pending processing=" + src.remaining() + ",Need=" + need + ",Now=" + des.remaining() + "}");
            }
            fileSize += compressor.compress(des);
            //插入元数据，第n个压缩块的末尾偏移量(压缩块的偏移量是以本地文件系统的文件大小为相对位置)
            mmapMeta.append(type, fileSize);
            //刷mmap
            flush();
            //重新映射
            des = getMMap();
            compressNum++;
            srcNum = 0;
            //重置缓冲区
            clearSrc();
            return true;
        }
        return false;
    }

    private void clearSrc() {
        src.clear();
        if (type == ColumnValue.ColumnType.COLUMN_TYPE_STRING) {
            src.position(SIZE_OF_STR_ROW_OFFSET * COMPRESS_LIMIT);
        }
    }


    @Override
    public void flush() throws IOException {
        //释放
        MMapUtil.unmap(des);
        //截断，否则pipe.size()报错
        pipe.truncate(fileSize);
    }


    @Override
    public File getFile() {
        return file;
    }

    @Override
    public long getFileSize() throws IOException {
        return fileSize;
    }

    @Override
    public void close() throws IOException {
        if (pipe != null && pipe.isOpen()) {
            pipe.truncate(fileSize);
        }
        Closeables.closeQuietly(pipe);
    }

    private boolean checkConditions() {
        return srcNum >= COMPRESS_LIMIT;
    }

    private MappedByteBuffer getMMap() throws IOException {
        return pipe.map(FileChannel.MapMode.READ_WRITE, fileSize, this.desSize);
    }

    /**
     * 在缓冲区拿vlog
     *
     * @param internalIndex
     * @param colIndex
     * @return
     */
    @Override
    public ColumnValue getVlog(short internalIndex, int colIndex) throws IOException {
        return ReadUtil.getVlog(src, srcNum, type, internalIndex, colIndex);
    }

    @Override
    public boolean canRead(short compressIndex) throws IOException {
        return compressIndex == compressNum;
    }

    private static int getDesSize(double a){
        int k = (int) Math.ceil(a / 4);
        return  4 * k;
    }

}
