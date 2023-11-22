package com.alibaba.lindorm.contest.extendstructs;

import com.alibaba.lindorm.contest.TSDBEngineImpl;
import com.alibaba.lindorm.contest.VinDB;
import com.alibaba.lindorm.contest.VlogRead;
import com.alibaba.lindorm.contest.extendstructs.agg.MmapAggImpl;
import com.alibaba.lindorm.contest.extendstructs.index.OffHeapImmutableIndex;
import com.alibaba.lindorm.contest.extendstructs.log.TSDBLog;
import com.alibaba.lindorm.contest.extendstructs.meta.MmapMetaImpl;
import com.alibaba.lindorm.contest.extendstructs.pool.DirectBufferPool;
import com.alibaba.lindorm.contest.extendstructs.vlog.MmapVLogWriter;
import com.alibaba.lindorm.contest.extendstructs.vlog.MmapVlogReader;
import com.alibaba.lindorm.contest.structs.ColumnValue;
import com.alibaba.lindorm.contest.structs.Row;
import com.alibaba.lindorm.contest.structs.Vin;
import com.alibaba.lindorm.contest.util.FileUtil;
import org.apache.log4j.Logger;

import java.io.File;
import java.io.IOException;

import static com.alibaba.lindorm.contest.extendstructs.FileName.*;

/**
 * @author 陈翔
 */
public final class VinDBImpl implements VinDB {
    public static Logger logger = TSDBLog.getLogger();
    /**
     * vin
     */
    public Vin vin;
    /**
     * 文件夹
     */
    public File dir;
    /**
     * 这个vin最大的tp
     */
    private Row maxRow;

    /**
     * vlogWriter
     */
    private MmapVLogWriter strWriter;

    private MmapVLogWriter intWriter;

    private MmapVLogWriter douWriter;

    /**
     * vlogReader
     */
    private VlogRead strReader;

    private VlogRead intReader;

    private VlogRead douReader;

    /**
     * wal
     */
    public OffHeapImmutableIndex immutableIndex;


    /**
     * meta
     */
    private MmapMetaImpl mmapMeta;

    public boolean readyToRead = false;

    public boolean readyToWrite = false;

    public static DirectBufferPool strBufferPool;

    public static DirectBufferPool douBufferPool;

    public static DirectBufferPool intBufferPool;

    public int[] intAggMax = new int[40];

    public double[] intAggTotal = new double[40];

    public double[] douAggMax = new double[10];

    public double[] douAggTotal = new double[10];

    public MmapAggImpl mmapAgg;

    public void initAgg() throws IOException {
        for (int i = 0; i < 40; i++) {
            intAggTotal[i] = 0;
            intAggMax[i] = Integer.MIN_VALUE;
        }
        for (int i = 0; i < 10; i++) {
            douAggTotal[i] = 0;
            douAggMax[i] = Double.MIN_VALUE;
        }
        mmapAgg = new MmapAggImpl(new File(dir, AGG));
    }


    public static void initBufferPool() throws IOException {
        logger.info("size of pool string buffer:" + COMPRESS_LIMIT * STRING_MAX_BYTE_SIZE + "B.");
        logger.info("size of pool int buffer:" + COMPRESS_LIMIT * INT_MAX_BYTE_SIZE + "B.");
        logger.info("size of pool double buffer:" + COMPRESS_LIMIT * DOUBLE_MAX_BYTE_SIZE + "B.");
        strBufferPool = new DirectBufferPool(STR_READ_DES_BUFFER_SIZE, COMPRESS_LIMIT * STRING_MAX_BYTE_SIZE, false);
        intBufferPool = new DirectBufferPool(INT_READ_DES_BUFFER_SIZE, COMPRESS_LIMIT * INT_MAX_BYTE_SIZE, false);
        douBufferPool = new DirectBufferPool(DOU_READ_DES_BUFFER_SIZE, COMPRESS_LIMIT * DOUBLE_MAX_BYTE_SIZE, false);
    }

    public static void destroyBufferPool() {
        strBufferPool.destroy();
        ;
        intBufferPool.destroy();
        ;
        douBufferPool.destroy();
        ;
    }


    public VinDBImpl(File file, Vin vin) {
        try {
            this.vin = vin;
            //初始化指向该vin的文件夹
            dir = new File(file, new String(vin.getVin()));
            if (dir.exists() && !dir.isDirectory()) {
                throw new IOException("Folder creation failed, file already exists.");
            } else if (!dir.exists()) {
                dir.mkdirs();
            }
            //元数据
            mmapMeta = new MmapMetaImpl(new File(dir, META_F_NAME));
            //索引
            final File walF = new File(dir, WAL_F_NAME);
            immutableIndex = new OffHeapImmutableIndex(walF, TIME_LENGTH);
            //agg
            initAgg();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }


    /**
     * 获取maxRow
     *
     * @return
     */
    @Override
    public Row getMaxRow() throws IOException {

        if (maxRow != null) {
            return maxRow;
        } else {
            //指向maxRow的文件
            final File file = new File(dir, MAX_ROW_F_NAME);
            if (file.exists()) {
                maxRow = FileUtil.readRow(file);
            }
            return maxRow;
        }
    }


    /**
     * 设置MaxTP
     *
     * @param row
     */
    @Override
    public void setMaxRow(Row row) {
        if (maxRow == null || row.getTimestamp() > maxRow.getTimestamp()) {
            maxRow = row;
        }
    }


    /**
     * 准备写入
     */
    @Override
    public void prepareForWrite() throws IOException {
        //blog写入就绪
        strWriter = new MmapVLogWriter(new File(dir, STR_COLS_F_NAME), mmapMeta, ColumnValue.ColumnType.COLUMN_TYPE_STRING);
        intWriter = new MmapVLogWriter(new File(dir, INT_COLS_F_NAME), mmapMeta, ColumnValue.ColumnType.COLUMN_TYPE_INTEGER);
        douWriter = new MmapVLogWriter(new File(dir, DOUBLE_COLS_F_NAME), mmapMeta, ColumnValue.ColumnType.COLUMN_TYPE_DOUBLE_FLOAT);
        readyToWrite = true;
    }

    /**
     * 准备读取
     */
    @Override
    public void prepareForRead() {
        try {
            if (TSDBEngineImpl.FIRST) {
                strReader = new MmapVlogReader(new File(dir, STR_COLS_F_NAME), mmapMeta, ColumnValue.ColumnType.COLUMN_TYPE_STRING, strWriter, strBufferPool, mmapAgg);
                intReader = new MmapVlogReader(new File(dir, INT_COLS_F_NAME), mmapMeta, ColumnValue.ColumnType.COLUMN_TYPE_INTEGER, intWriter, intBufferPool, mmapAgg);
                douReader = new MmapVlogReader(new File(dir, DOUBLE_COLS_F_NAME), mmapMeta, ColumnValue.ColumnType.COLUMN_TYPE_DOUBLE_FLOAT, douWriter, douBufferPool, mmapAgg);
            } else {
                strReader = new MmapVlogReader(new File(dir, STR_COLS_F_NAME), mmapMeta, ColumnValue.ColumnType.COLUMN_TYPE_STRING, null, strBufferPool, mmapAgg);
                intReader = new MmapVlogReader(new File(dir, INT_COLS_F_NAME), mmapMeta, ColumnValue.ColumnType.COLUMN_TYPE_INTEGER, null, intBufferPool, mmapAgg);
                douReader = new MmapVlogReader(new File(dir, DOUBLE_COLS_F_NAME), mmapMeta, ColumnValue.ColumnType.COLUMN_TYPE_DOUBLE_FLOAT, null, douBufferPool, mmapAgg);
            }
            readyToRead = true;
        } catch (Exception e) {
            e.printStackTrace();
        }
    }


    /**
     * vlog刷盘
     */
    @Override
    public void flushVlogBuffer() throws IOException {
        if (TSDBEngineImpl.FIRST) {
            strWriter.flush();
            intWriter.flush();
            douWriter.flush();
        }
    }


    /**
     * 关闭操作
     */
    @Override
    public void close() {
        try {
            if (strWriter != null) {
                strWriter.flush();
                strWriter.close();
            }
            if (intWriter != null) {
                intWriter.flush();
                intWriter.close();
            }
            if (douWriter != null) {
                douWriter.flush();
                douWriter.close();
            }

            if (strReader != null) {
                strReader.close();
            }
            if (intReader != null) {
                intReader.close();
            }
            if (douReader != null) {
                douReader.close();
            }

            //保存wal
            immutableIndex.close();

            //刷盘元数据
            mmapMeta.flush();
            mmapMeta.close();

            //保存最大
            FileUtil.saveRow(new File(dir, MAX_ROW_F_NAME), maxRow);

            //保存agg
            mmapAgg.flush();
            mmapAgg.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Override
    public VlogRead getReader(ColumnValue.ColumnType type) {
        VlogRead reader = null;
        switch (type) {
            case COLUMN_TYPE_STRING:
                reader = getStrReader();
                break;
            case COLUMN_TYPE_INTEGER:
                reader = getIntReader();
                break;
            case COLUMN_TYPE_DOUBLE_FLOAT:
                reader = getDouReader();
                break;
            default:
                break;
        }
        return reader;
    }

    @Override
    public MmapVLogWriter getStrWriter() {
        return strWriter;
    }

    @Override
    public MmapVLogWriter getIntWriter() {
        return intWriter;
    }

    @Override
    public MmapVLogWriter getDouWriter() {
        return douWriter;
    }

    public MmapMetaImpl getMmapMeta() {
        return mmapMeta;
    }

    public void setMmapMeta(MmapMetaImpl mmapMeta) {
        this.mmapMeta = mmapMeta;
    }

    @Override
    public VlogRead getStrReader() {
        return strReader;
    }

    @Override
    public VlogRead getIntReader() {
        return intReader;
    }

    @Override
    public VlogRead getDouReader() {
        return douReader;
    }
}
