//
// You should modify this file.
//
// Refer TSDBEngineSample.java to ensure that you have understood
// the interface semantics correctly.
//

package com.alibaba.lindorm.contest;


import com.alibaba.lindorm.contest.extendstructs.DBConstant;
import com.alibaba.lindorm.contest.extendstructs.SchemaInfo;
import com.alibaba.lindorm.contest.extendstructs.VinDBImpl;
import com.alibaba.lindorm.contest.extendstructs.agg.MmapAggImpl;
import com.alibaba.lindorm.contest.extendstructs.compression.tp.TPCompressor;
import com.alibaba.lindorm.contest.extendstructs.compression.tp.TPDecompressor;
import com.alibaba.lindorm.contest.extendstructs.index.OffHeapImmutableIndex;
import com.alibaba.lindorm.contest.extendstructs.log.TSDBLog;
import com.alibaba.lindorm.contest.extendstructs.meta.MmapMetaImpl;
import com.alibaba.lindorm.contest.extendstructs.vlog.ByteBufferVo;
import com.alibaba.lindorm.contest.extendstructs.vlog.MmapVLogWriter;
import com.alibaba.lindorm.contest.extendstructs.wal.CmpIndexValue;
import com.alibaba.lindorm.contest.extendstructs.wal.ValEntity;
import com.alibaba.lindorm.contest.extendstructs.wal.WalEntry;
import com.alibaba.lindorm.contest.structs.*;
import com.alibaba.lindorm.contest.util.FileViewer;
import org.apache.log4j.Logger;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.locks.ReentrantLock;

import static com.alibaba.lindorm.contest.extendstructs.DBConstant.*;
import static com.alibaba.lindorm.contest.extendstructs.vlog.MmapVLogWriter.*;
import static com.alibaba.lindorm.contest.structs.ColumnValue.ColumnType.*;

/**
 * @author 陈翔
 */
public class TSDBEngineImpl extends TSDBEngine {
    public static Logger logger = TSDBLog.getLogger();
    /**
     * 是否已连接
     */
    public static boolean connected = false;
    /**
     * 表结构信息
     */
    public static SchemaInfo schemaInfo;

    /**
     * 是否是第一次加载
     */
    public static boolean FIRST = true;


    private static final ConcurrentMap<Vin, ReentrantLock> LOCKS = new ConcurrentHashMap<>(VIN_NUM + 10);

    private static final ConcurrentMap<Vin, VinDBImpl> VIN_INFO = new ConcurrentHashMap<>(VIN_NUM + 10);

    /**
     * This constructor's function signature should not be modified.
     * Our evaluation program will call this constructor.
     * The function's body can be modified.
     */
    public TSDBEngineImpl(File dataPath) {
        super(dataPath);
    }

    @Override
    public void connect() throws IOException {
        //判断是否已经连接
        if (connected) {
            throw new IOException("Do not repeat the connection.");
        }
        //加载表结构
        schemaInfo = new SchemaInfo(getDataPath());
        loadAllCol();
        //连接成功
        connected = true;
        //池化资源加载
        VinDBImpl.initBufferPool();
        if (!FIRST) {
            //预读
            final File[] files = getDataPath().listFiles();
            for (File file : files) {
                if (file.isDirectory()) {
                    final Vin vin = new Vin(file.getName().getBytes(StandardCharsets.UTF_8));
                    final VinDBImpl vinDBImpl = VIN_INFO.computeIfAbsent(vin, key -> new VinDBImpl(getDataPath(), vin));
                    vinDBImpl.prepareForRead();
                    vinDBImpl.getMaxRow();
                }
            }
        } else {
            //第一次打印信息
            //打印Limit
            logger.info("COMPRESS_LIMIT:" + COMPRESS_LIMIT);
            logger.info("ZSTD_COMPRESSION_LEVEL:" + COMPRESSION_LEVEL);
            //打印堆外占用
            logger.info(offheapSize());
        }
    }

    private List<Short> AllStrCol = new ArrayList<>(60);

    private List<Short> AllIntCol = new ArrayList<>(60);

    private List<Short> AllDouCol = new ArrayList<>(60);

    private boolean reQAllCol = false;

    public void loadAllCol(){
        if(!reQAllCol){
            //加载，减少gc
            for (short i = 0; i < INT_NUM_OF_A_ROW; i++) {
                AllIntCol.add(i);
            }
            for (short i = 0; i < DOUBLE_NUM_OF_A_ROW; i++) {
                AllDouCol.add(i);
            }
            for (short i = 0; i < STRING_NUM_OF_A_ROW; i++) {
                AllStrCol.add(i);
            }
            reQAllCol = true;
        }
    }

    @Override
    public void createTable(String tableName, Schema schema) throws IOException {
        schemaInfo = new SchemaInfo(tableName, schema);
        loadAllCol();
        System.out.println("Table create ok.");
    }

    @Override
    public void shutdown() {
        //1.关闭资源
        VIN_INFO.forEach((k, v) -> v.close());
        VIN_INFO.clear();
        //最后再存储表结构
        schemaInfo.save(getDataPath());
        //取消连接
        connected = false;
        //销毁池化资源
        //directIOBufferPool.destroy();
        //打印文件大小
        if (FIRST) {
            FileViewer.printSize(getDataPath());
        }
        //threadlocal资源
        downThreadLocal.remove();
    }

    @Override
    public void write(WriteRequest wReq) throws IOException {
        final Collection<Row> rows = wReq.getRows();
        for (Row row : rows) {
            final Vin vin = row.getVin();
            final ReentrantLock lock = LOCKS.computeIfAbsent(vin, key -> new ReentrantLock());
            lock.lock();
            try {
                final VinDBImpl vinDBImpl = VIN_INFO.computeIfAbsent(vin, key -> new VinDBImpl(getDataPath(), vin));
                if (!vinDBImpl.readyToWrite) {
                    vinDBImpl.prepareForWrite();
                }
                //设置最大的tp
                vinDBImpl.setMaxRow(row);

                final MmapVLogWriter douWriter = vinDBImpl.getDouWriter();
                final MmapVLogWriter intWriter = vinDBImpl.getIntWriter();
                final MmapVLogWriter strWriter = vinDBImpl.getStrWriter();

                //--------------wal--------------
                final OffHeapImmutableIndex index = vinDBImpl.immutableIndex;
                if (index.keyAndVlogSeq.get() == null) {
                    index.threadLocalInitWrite();
                }
                byte[] b = index.keyAndVlogSeq.get();
                //tp压缩
                int key = TPCompressor.compress(row.getTimestamp());
                //int short小端存储
                b[0] = (byte) (key & 0xff);
                b[1] = (byte) (key >> 8 & 0xff);
                b[2] = (byte) (key >> 16 & 0xff);
                b[3] = (byte) (key >> 24 & 0xff);
                //为了减少存储量 index三者统一
                short compressIndex = douWriter.compressNum;
                short internalIndex = douWriter.srcNum;
                b[4] = (byte) (compressIndex & 0xff);
                b[5] = (byte) (compressIndex >> 8 & 0xff);

                b[6] = (byte) (internalIndex & 0xff);
                b[7] = (byte) (internalIndex >> 8 & 0xff);
                index.add();

                //--------------写vlog和meta--------------
                //保存当前写入位置
                int rowOffset = strWriter.src.position();
                //定位到写入索的位置
                strWriter.src.position(strWriter.srcNum * SIZE_OF_STR_ROW_OFFSET);
                //将索引写入
                strWriter.src.putInt(rowOffset);
                //回到写入字符串的位置
//                strWriter.src.position(rowOffset);
                int start = rowOffset;
                //从索引后面开始写,先偏移这10个string的偏移量(short)
                strWriter.src.position(start + SIZE_OF_STR_COL_OFFSET * schemaInfo.getStrColNum());

                //保存int和double的写入位置
                int intOffset = intWriter.src.position();
                int douOffset = douWriter.src.position();
                for (Map.Entry<String, ColumnValue> entry : row.getColumns().entrySet()) {
                    //final short colIndex = schemaInfo.getIndex(entry.getKey());
                    final ColumnValue.ColumnType type = schemaInfo.getT(entry.getKey());
                    final ColumnValue value = entry.getValue();
                    if (type == COLUMN_TYPE_STRING) {
                        //---写偏移量---
                        //保存当前写入字符串的相对地址
                        final int colOffset = strWriter.src.position();
                        //定位到写入索的位置
                        strWriter.src.position(start);
                        //将索引写入
                        strWriter.src.putInt(colOffset);
                        //写入索引的位置+1
                        start += SIZE_OF_STR_COL_OFFSET;
                        //回到写入字符串的位置
                        strWriter.src.position(colOffset);
                        //---再写数据---
                        if (value.getStringValue().remaining() > 0) {
                            strWriter.src.put(value.getStringValue().array());
                        }
                    } else if (type == COLUMN_TYPE_INTEGER) {
                        int colIndex = (intWriter.src.position()-intOffset)/4;
                        vinDBImpl.intAggTotal[colIndex] += value.getIntegerValue();
                        if(value.getIntegerValue() > vinDBImpl.intAggMax[colIndex]){
                            vinDBImpl.intAggMax[colIndex] = value.getIntegerValue();
                        }
                        intWriter.src.putInt(value.getIntegerValue());
                    } else {
                        int colIndex = (douWriter.src.position()-douOffset)/8;
                        vinDBImpl.douAggTotal[colIndex] += value.getDoubleFloatValue();
                        if(value.getDoubleFloatValue() > vinDBImpl.douAggMax[colIndex]){
                            vinDBImpl.douAggMax[colIndex] = value.getDoubleFloatValue();
                        }
                        douWriter.src.putDouble(value.getDoubleFloatValue());
                    }
                }
                //---提交写入的double----
                if(douWriter.append()){
                    //将数据写入
                    for (short i = 0; i < 10 ; i++) {
                        vinDBImpl.mmapAgg.appendDouble(vinDBImpl.douAggMax[i],vinDBImpl.douAggTotal[i]);
                        vinDBImpl.douAggMax[i] = Double.MIN_VALUE;
                        vinDBImpl.douAggTotal[i] = 0;
                    }
                }
                //---提交写入的int---
                if(intWriter.append()){
                    //将数据写入
                    for (short i = 0; i < 40 ; i++) {
                        vinDBImpl.mmapAgg.appendInt(vinDBImpl.intAggMax[i],vinDBImpl.intAggTotal[i]);
                        vinDBImpl.intAggMax[i] = Integer.MIN_VALUE;
                        vinDBImpl.intAggTotal[i] = 0;
                    }
                }
                //---提交写入的str---
                strWriter.append();
            } catch (Exception e) {
                e.printStackTrace();
                System.exit(0);
            } finally {
                lock.unlock();
            }
        }
    }

    @Override
    public ArrayList<Row> executeLatestQuery(LatestQueryRequest pReadReq) throws IOException {
        ArrayList<Row> ans = new ArrayList<>();
        for (Vin vin : pReadReq.getVins()) {
            try {
                final VinDBImpl vinDBImpl = VIN_INFO.computeIfAbsent(vin, key -> new VinDBImpl(getDataPath(), vin));
                final Row maxRow = vinDBImpl.getMaxRow();
                if (maxRow == null) {
                    continue;
                }
                if (!pReadReq.getRequestedColumns().isEmpty()) {
                    Map<String, ColumnValue> filteredColumns = new HashMap<>(pReadReq.getRequestedColumns().size());
                    Map<String, ColumnValue> columns = maxRow.getColumns();
                    for (String key : pReadReq.getRequestedColumns()) {
                        filteredColumns.put(key, columns.get(key));
                    }
                    ans.add(new Row(vin, maxRow.getTimestamp(), filteredColumns));
                } else {
                    ans.add(maxRow);
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        return ans;
    }


    @Override
    public ArrayList<Row> executeTimeRangeQuery(TimeRangeQueryRequest trReadReq) throws IOException {
//        System.out.println("executeTimeRangeQuery:"+trReadReq.getTimeLowerBound()+"->"+trReadReq.getTimeUpperBound());
        final Vin vin = trReadReq.getVin();
        final ReentrantLock lock = LOCKS.computeIfAbsent(vin, key -> new ReentrantLock());
        lock.lock();
        try {
            ArrayList<Row> rows = new ArrayList<>();
            //获取该vin的数据
            final VinDBImpl vinDBImpl = VIN_INFO.computeIfAbsent(vin, key -> new VinDBImpl(getDataPath(), vin));
            final OffHeapImmutableIndex index = vinDBImpl.immutableIndex;
            //需要的string col
            List<Short> strColIndex;
            List<Short> douColIndex;
            List<Short> intColIndex;
            Map<Short, ArrayList<CmpIndexValue>> mp = new HashMap<>();
            final int reqColSize = trReadReq.getRequestedColumns().size();
            if (reqColSize == 60 || reqColSize == 0) {
                strColIndex = AllStrCol;
                douColIndex = AllDouCol;
                intColIndex = AllIntCol;
            } else {
                strColIndex = new ArrayList<>();
                douColIndex = new ArrayList<>();
                intColIndex = new ArrayList<>();
                for (String cname : trReadReq.getRequestedColumns()) {
                    final ColumnValue.ColumnType type = schemaInfo.getT(cname);
                    switch (type) {
                        case COLUMN_TYPE_STRING:
                            strColIndex.add(schemaInfo.getIndex(cname));
                            break;
                        case COLUMN_TYPE_INTEGER:
                            intColIndex.add(schemaInfo.getIndex(cname));
                            break;
                        case COLUMN_TYPE_DOUBLE_FLOAT:
                            douColIndex.add(schemaInfo.getIndex(cname));
                            break;
                        default:
                            break;
                    }
                }
            }
            //range查询结果，对于不同的场景采用不同的查询
            if (FIRST) {
                //准备读取
                if (!vinDBImpl.readyToRead) {
                    vinDBImpl.prepareForRead();
                }
                if (trReadReq.getTimeUpperBound() - trReadReq.getTimeLowerBound() == 1) {
                    final int cmp = TPCompressor.compress(trReadReq.getTimeLowerBound());
                    final ValEntity valEntity = index.linearSearch(cmp);
                    final ArrayList<CmpIndexValue> list = new ArrayList<>();
                    list.add(new CmpIndexValue(cmp, valEntity.getInternalIndex()));
                    mp.put(valEntity.getCompressIndex(), list);
                } else {
                    throw new IOException("FIRST check error:" + (trReadReq.getTimeUpperBound() - trReadReq.getTimeLowerBound() == 1) + ",size:"
                            + (trReadReq.getRequestedColumns().size()));
                }
            } else {
                //查询wal 找到需要的压缩块
                mp = index.rangeMapToKV(TPCompressor.compress(trReadReq.getTimeLowerBound()), TPCompressor.compress(trReadReq.getTimeUpperBound()));
            }
            HashMap<Integer, HashMap<String, ColumnValue>> cols = new HashMap<>();
            for (Map.Entry<Short, ArrayList<CmpIndexValue>> item : mp.entrySet()) {
                for (CmpIndexValue cmpIndexValue : item.getValue()) {
                    //最终结果tp（未解压） -> cols
                    cols.put(cmpIndexValue.getCompressTp(), new HashMap<>());
                }
            }
            //遍历该数据结构
            for (Map.Entry<Short, ArrayList<CmpIndexValue>> item : mp.entrySet()) {
                if (intColIndex.size() != 0) {
                    //先加载压缩块
                    final ByteBufferVo byteBufferVo = vinDBImpl.getIntReader().getDecompressed(item.getKey());
                    for (CmpIndexValue cmpIndexValue : item.getValue()) {
                        //从压缩块内读数据
                        final HashMap<String, ColumnValue> colmp = cols.get(cmpIndexValue.getCompressTp());
                        for (Short colIndex : intColIndex) {
                            colmp.put(SchemaInfo.intIndex[colIndex],
                                    vinDBImpl.getIntReader().read(byteBufferVo, cmpIndexValue.getInternalIndex(), colIndex));
                        }
                    }
                    //回收利用
                    vinDBImpl.getIntReader().recycle(byteBufferVo);
                }
                if (strColIndex.size() != 0) {
                    //先加载压缩块
                    final ByteBufferVo byteBufferVo = vinDBImpl.getStrReader().getDecompressed(item.getKey());
                    for (CmpIndexValue cmpIndexValue : item.getValue()) {
                        //从压缩块内读数据
                        final HashMap<String, ColumnValue> colmp = cols.get(cmpIndexValue.getCompressTp());
                        for (Short colIndex : strColIndex) {
                            colmp.put(SchemaInfo.strIndex[colIndex],
                                    vinDBImpl.getStrReader().read(byteBufferVo, cmpIndexValue.getInternalIndex(), colIndex));
                        }
                    }
                    //回收利用
                    vinDBImpl.getStrReader().recycle(byteBufferVo);
                }
                if (douColIndex.size() != 0) {
                    //先加载压缩块
                    final ByteBufferVo byteBufferVo = vinDBImpl.getDouReader().getDecompressed(item.getKey());
                    for (CmpIndexValue cmpIndexValue : item.getValue()) {
                        //从压缩块内读数据
                        final HashMap<String, ColumnValue> colmp = cols.get(cmpIndexValue.getCompressTp());
                        for (Short colIndex : douColIndex) {
                            colmp.put(SchemaInfo.douIndex[colIndex],
                                    vinDBImpl.getDouReader().read(byteBufferVo, cmpIndexValue.getInternalIndex(), colIndex));
                        }
                    }
                    //回收利用
                    vinDBImpl.getDouReader().recycle(byteBufferVo);
                }
            }
            //将tp解压缩
            for (Map.Entry<Integer, HashMap<String, ColumnValue>> item : cols.entrySet()) {
                rows.add(new Row(vin, TPDecompressor.decompress(item.getKey()), item.getValue()));
            }
            if (mp.size() != 0 && rows.size() == 0) {
                throw new IOException("Index query num is not zero,but final result is zero.");
            }
            return rows;
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            lock.unlock();
        }
        logger.error("you have a rangeQuery Exception:" + trReadReq.getTimeLowerBound() + "->" + trReadReq.getTimeUpperBound());
        throw new IOException("executeTimeRangeQuery error.");
    }
    @Override
    public ArrayList<Row> executeAggregateQuery(TimeRangeAggregationRequest aggregationReq) throws IOException {
        final Vin vin = aggregationReq.getVin();
        try {
            ArrayList<Row> ans = new ArrayList<>();
            final VinDBImpl vinDBImpl = VIN_INFO.computeIfAbsent(vin, key -> new VinDBImpl(getDataPath(), vin));
            final ColumnValue.ColumnType type = schemaInfo.getT(aggregationReq.getColumnName());
            final VlogRead reader = vinDBImpl.getReader(type);
            final HashMap<Short, ArrayList<Short>> map = vinDBImpl.immutableIndex.
                    rangeMapToRowIndex(TPCompressor.compress(aggregationReq.getTimeLowerBound()), TPCompressor.compress(aggregationReq.getTimeUpperBound()));
            if (map.size() == 0) {
                return ans;
            }
            final short colIndex = schemaInfo.getIndex(aggregationReq.getColumnName());
            HashMap<String,ColumnValue> cols = new HashMap<>();
            if(aggregationReq.getAggregator().equals(Aggregator.MAX) && type==COLUMN_TYPE_INTEGER){
                int max = Integer.MIN_VALUE;
                for (Map.Entry<Short, ArrayList<Short>> item : map.entrySet()) {
                    final Short compressIndex = item.getKey();
                    if(item.getValue().size()==COMPRESS_LIMIT){
                        max = Math.max(max, vinDBImpl.mmapAgg.readIntMax(compressIndex,colIndex));
                    }else{
                        final ByteBufferVo byteBufferVo = reader.getDecompressed(item.getKey());
                        for (Short internalIndex : item.getValue()) {
                            max = Math.max(max, reader.read(byteBufferVo, internalIndex, colIndex).getIntegerValue());
                        }
                        //回收利用
                        reader.recycle(byteBufferVo);
                    }
                }
                cols.put(aggregationReq.getColumnName(),new ColumnValue.IntegerColumn(max));
            }else if(aggregationReq.getAggregator().equals(Aggregator.MAX) && type==COLUMN_TYPE_DOUBLE_FLOAT){
                double max = Double.MIN_VALUE;
                for (Map.Entry<Short, ArrayList<Short>> item : map.entrySet()) {
                    final Short compressIndex = item.getKey();
                    if(item.getValue().size()==COMPRESS_LIMIT){
                        max = Math.max(max, vinDBImpl.mmapAgg.readDoubleMax(compressIndex,colIndex));
                    }else{
                        final ByteBufferVo byteBufferVo = reader.getDecompressed(item.getKey());
                        for (Short internalIndex : item.getValue()) {
                            max = Math.max(max, reader.read(byteBufferVo, internalIndex, colIndex).getDoubleFloatValue());
                        }
                        //回收利用
                        reader.recycle(byteBufferVo);
                    }
                }
                cols.put(aggregationReq.getColumnName(),new ColumnValue.DoubleFloatColumn(max));
            }else if(aggregationReq.getAggregator().equals(Aggregator.AVG) && type==COLUMN_TYPE_INTEGER){
                double total = 0;
                int num = 0;
                for (Map.Entry<Short, ArrayList<Short>> item : map.entrySet()) {
                    final Short compressIndex = item.getKey();
                    if(item.getValue().size()==COMPRESS_LIMIT){
                        total += vinDBImpl.mmapAgg.readIntTotal(compressIndex,colIndex);
                        num += COMPRESS_LIMIT;
                    }else{
                        num += item.getValue().size();
                        final ByteBufferVo byteBufferVo = reader.getDecompressed(item.getKey());
                        for (Short internalIndex : item.getValue()) {
                            total += reader.read(byteBufferVo, internalIndex, colIndex).getIntegerValue();
                        }
                        //回收利用
                        reader.recycle(byteBufferVo);
                    }
                }
                cols.put(aggregationReq.getColumnName(),new ColumnValue.DoubleFloatColumn(total/num));
            }else if(aggregationReq.getAggregator().equals(Aggregator.AVG) && type==COLUMN_TYPE_DOUBLE_FLOAT){
                double total = 0;
                int num = 0;
                for (Map.Entry<Short, ArrayList<Short>> item : map.entrySet()) {
                    final Short compressIndex = item.getKey();
                    if(item.getValue().size()==COMPRESS_LIMIT){
                        total += vinDBImpl.mmapAgg.readDoubleTotal(compressIndex,colIndex);
                        num += COMPRESS_LIMIT;
                    }else{
                        num += item.getValue().size();
                        final ByteBufferVo byteBufferVo = reader.getDecompressed(item.getKey());
                        for (Short internalIndex : item.getValue()) {
                            total += reader.read(byteBufferVo, internalIndex, colIndex).getDoubleFloatValue();
                        }
                        //回收利用
                        reader.recycle(byteBufferVo);
                    }
                }
                cols.put(aggregationReq.getColumnName(),new ColumnValue.DoubleFloatColumn(total/num));
            }else{
                throw new IOException("AggReader error.");
            }
            final Row row = new Row(vin, aggregationReq.getTimeLowerBound(), cols);
            ans.add(row);
            return ans;
        } catch (Exception e) {
            e.printStackTrace();
        }
        logger.error("you have a aggregateQuery Exception:" + aggregationReq.getTimeLowerBound() + "->" + aggregationReq.getTimeUpperBound());
        throw new IOException("executeAggregateQuery error.");
    }

//    @Override
//    public ArrayList<Row> executeAggregateQuery(TimeRangeAggregationRequest aggregationReq) throws IOException {
//        final Vin vin = aggregationReq.getVin();
//        try {
//            ArrayList<Row> ans = new ArrayList<>();
//            final VinDBImpl vinDBImpl = VIN_INFO.computeIfAbsent(vin, key -> new VinDBImpl(getDataPath(), vin));
//            final ColumnValue.ColumnType type = schemaInfo.getT(aggregationReq.getColumnName());
//            System.out.print("this is a agg req:");
//            System.out.println(aggregationReq.getTimeLowerBound()+"->"+aggregationReq.getTimeUpperBound());
//            System.out.println("agg:"+aggregationReq.getAggregator());
//            System.out.println("type:"+type);
//            final VlogRead reader = vinDBImpl.getReader(type);
////            long time1 = System.nanoTime();
//            final ThreadLocal<byte[]> target1 = vinDBImpl.immutableIndex.target1;
//            final ThreadLocal<byte[]> target2 = vinDBImpl.immutableIndex.target2;
//            final long baseAddress = vinDBImpl.immutableIndex.baseAddress;
//            ArrayList<ColumnValue> values = aggValues.get();
//            if (values == null) {
//                values = new ArrayList<>();
//                aggValues.set(values);
//            }
//            values.clear();
//            final short colIndex = schemaInfo.getIndex(aggregationReq.getColumnName());
////            long time2 = System.nanoTime();
//            if (target1.get() == null) {
//                vinDBImpl.immutableIndex.threadLocalInitRead();
//            }
//            BytesUtils.int2BytesLittle(TPCompressor.compress(aggregationReq.getTimeLowerBound()), target1.get());
//            int l = vinDBImpl.immutableIndex.binarySearchLeft(target1.get());
//            BytesUtils.int2BytesLittle(TPCompressor.compress(aggregationReq.getTimeUpperBound()), target2.get());
//            int r = vinDBImpl.immutableIndex.binarySearchRight(target2.get());
//            if (l <= r) {
//                HashMap<Short, ByteBufferVo> blocks = new HashMap<>();
//                short current = -1;
//                ByteBufferVo byteBufferVo = null;
//                for (int i = l; i <= r; i++) {
//                    int offset = i * SIZE_OF_WAL_RECORD;
//                    final short compressIndex = Uns.getShort(baseAddress, offset + SIZE_OF_KEY);
//                    final short internalIndex = Uns.getShort(baseAddress, offset + SIZE_OF_KEY + SizeOf.SIZE_OF_SHORT);
//                    if (current != compressIndex) {
//                        byteBufferVo = blocks.get(compressIndex);
//                        if (byteBufferVo == null) {
//                            byteBufferVo = reader.getDecompressed(compressIndex);
//                            blocks.put(compressIndex, byteBufferVo);
//                        }
//                        current = compressIndex;
//                    }
//                    values.add(reader.read(byteBufferVo, internalIndex, colIndex));
//                }
//                //回收利用
//                for (ByteBufferVo value : blocks.values()) {
//                    reader.recycle(value);
//                }
//                final Row row = aggregator(aggregationReq.getAggregator(), values,
//                        aggregationReq.getTimeLowerBound(), type, aggregationReq.getColumnName(), vin);
//                ans.add(row);
//            }
////            long time3 = System.nanoTime();
////            long time4 = System.nanoTime();
////            calculateRatio((time2-time1),(time3-time2), (time4-time3));
//            return ans;
//        } catch (Exception e) {
//            e.printStackTrace();
//        }
//        logger.error("you have a aggregateQuery Exception:" + aggregationReq.getTimeLowerBound() + "->" + aggregationReq.getTimeUpperBound());
//        throw new IOException("executeAggregateQuery error.");
//    }


    private ThreadLocal<ArrayList<ColumnValue>[]> downThreadLocal = new ThreadLocal<>();
    private int maxAreaNum = 3600 * 5;


    @Override
    public ArrayList<Row> executeDownsampleQuery(TimeRangeDownsampleRequest downsampleReq) throws IOException {
        final Vin vin = downsampleReq.getVin();
        try {
            final CompareExpression columnFilter = downsampleReq.getColumnFilter();
            ArrayList<Row> ans = new ArrayList<>();
            final VinDBImpl vinDBImpl = VIN_INFO.computeIfAbsent(vin, key -> new VinDBImpl(getDataPath(), vin));
            long low = downsampleReq.getTimeLowerBound();
            long up = downsampleReq.getTimeUpperBound();
            final int interval = Math.toIntExact(downsampleReq.getInterval());
            int areaNum = Math.toIntExact((up - low) / interval);
            ArrayList<ColumnValue>[] values = downThreadLocal.get();
            if (values == null) {
                //最多的情况:interval = 1,maxV = rangeMax
                values = new ArrayList[maxAreaNum];
                for (int i = 0; i < maxAreaNum; i++) {
                    values[i] = new ArrayList<>();
                }
            }
            for (int i = 0; i < areaNum; i++) {
                values[i].clear();
            }
            boolean[] valuesNum = new boolean[areaNum];
            final ColumnValue.ColumnType type = schemaInfo.getT(downsampleReq.getColumnName());
            final VlogRead reader = vinDBImpl.getReader(type);
            final HashMap<Short, ArrayList<CmpIndexValue>> map = vinDBImpl.immutableIndex.rangeMapToKV(TPCompressor.compress(downsampleReq.getTimeLowerBound()), TPCompressor.compress(downsampleReq.getTimeUpperBound()));
            if (map.size() == 0) {
                return ans;
            }
            final short colIndex = schemaInfo.getIndex(downsampleReq.getColumnName());
            final int compressLow = TPCompressor.compress(low);
            for (Map.Entry<Short, ArrayList<CmpIndexValue>> item : map.entrySet()) {
                //先加载
                final ByteBufferVo byteBufferVo = reader.getDecompressed(item.getKey());
                for (CmpIndexValue cmpIndexValue : item.getValue()) {
                    final ColumnValue val = reader.read(byteBufferVo, cmpIndexValue.getInternalIndex(), colIndex);
                    int area = (cmpIndexValue.getCompressTp() - compressLow) / interval;
                    if (columnFilter.doCompare(val)) {
                        values[area].add(val);
                    }
                    valuesNum[area] = true;
                }
                //回收利用
                reader.recycle(byteBufferVo);
            }
            for (int i = 0; i < areaNum; i++) {
                if (valuesNum[i]) {
                    if (values[i].size() == 0) {
                        //添加过，但是被过滤了 NaN处理
                        Map<String, ColumnValue> nanMap = new HashMap<>(1);
                        if (downsampleReq.getAggregator() == Aggregator.MAX && type == ColumnValue.ColumnType.COLUMN_TYPE_INTEGER) {
                            nanMap.put(downsampleReq.getColumnName(), new ColumnValue.DoubleFloatColumn(DBConstant.INTEGER_NaN));
                        } else {
                            nanMap.put(downsampleReq.getColumnName(), new ColumnValue.DoubleFloatColumn(DBConstant.DOUBLE_NaN));
                        }
                        ans.add(new Row(vin, low + i * interval, nanMap));
                    } else {
                        ans.add(aggregator(downsampleReq.getAggregator(), values[i],
                                low + i * interval, type, downsampleReq.getColumnName(), vin));
                    }
                }
            }
            return ans;
        } catch (Exception e) {
            e.printStackTrace();
        }
        logger.error("you have a downsampleQuery Exception:" + downsampleReq.getTimeLowerBound() + "->" + downsampleReq.getTimeUpperBound());
        throw new IOException("downsampleQuery error.");
    }

    /**
     * 聚合
     *
     * @param timeLowerBound 左区间
     * @return 返回聚合以后的结果
     * @throws IOException
     */
    private Row aggregator(Aggregator agg, List<ColumnValue> list, long timeLowerBound
            , ColumnValue.ColumnType type, String colName, Vin vin) throws IOException {
        Map<String, ColumnValue> singleColumn = new HashMap<>();
        singleColumn.clear();
        //进行聚合
        switch (agg) {
            case AVG:
                //求平均
                if (type == ColumnValue.ColumnType.COLUMN_TYPE_DOUBLE_FLOAT) {
                    double res = 0;
                    for (ColumnValue entry : list) {
                        res += entry.getDoubleFloatValue();
                    }
                    singleColumn.put(colName, new ColumnValue.DoubleFloatColumn(res / list.size()));
                } else {
                    // COLUMN_TYPE_INTEGER
                    double res = 0;
                    for (ColumnValue entry : list) {
                        res += entry.getIntegerValue();
                    }
                    singleColumn.put(colName, new ColumnValue.DoubleFloatColumn(res / list.size()));
                }
                break;
            case MAX:
                //求最大
                if (type == ColumnValue.ColumnType.COLUMN_TYPE_DOUBLE_FLOAT) {
                    ColumnValue maxV = null;
                    for (ColumnValue entry : list) {
                        if (maxV == null || entry.getDoubleFloatValue() > maxV.getDoubleFloatValue()) {
                            maxV = entry;
                        }
                    }
                    singleColumn.put(colName, maxV);
                } else {
                    // COLUMN_TYPE_INTEGER
                    ColumnValue maxV = null;
                    for (ColumnValue entry : list) {
                        if (maxV == null || entry.getIntegerValue() > maxV.getIntegerValue()) {
                            maxV = entry;
                        }
                    }
                    singleColumn.put(colName, maxV);
                }
                break;
            default:
                throw new IOException("Aggregator error.");
        }
        return new Row(vin, timeLowerBound, singleColumn);
    }

    private HashMap<Short, ArrayList<WalEntry>> resToMap(List<WalEntry> res) {
        HashMap<Short, ArrayList<WalEntry>> map = new HashMap<>(res.size() / COMPRESS_LIMIT + 2);
        for (WalEntry walEntry : res) {
            final ArrayList<WalEntry> list = map.computeIfAbsent(walEntry.getValue().getCompressIndex(), key -> new ArrayList<>());
            list.add(walEntry);
        }
        return map;
    }

    public static void calculateRatio(long... numbers) {
        if (numbers == null || numbers.length < 2) {
            throw new IllegalArgumentException("Input array should contain at least 2 integers");
        }

        // 计算总和
        int sum = 0;
        for (long num : numbers) {
            sum += num;
        }

        // 计算比值
        for (int i = 0; i < numbers.length; i++) {
            long numerator = numbers[i];
            double ratio = (double) numerator / sum;
            System.out.println(numerator + " / " + sum + " = " + ratio);
        }
    }

    /**
     * 计算堆外大小
     */
    public static String offheapSize() {
        //索引
        long indexSize = (long) TIME_LENGTH * SIZE_OF_WAL_RECORD;
        //写入vlog
        long strSrc = (DBConstant.SIZE_OF_STR_ROW_OFFSET + DBConstant.STRING_MAX_BYTE_SIZE) * COMPRESS_LIMIT;
        long strDes = STR_DES_SIZE * 1024L;
        long intSrc = DBConstant.INT_MAX_BYTE_SIZE * COMPRESS_LIMIT;
        long intDes = INT_DES_SIZE * 1024L;
        long doubleSrc = DBConstant.DOUBLE_MAX_BYTE_SIZE * COMPRESS_LIMIT;
        long doubleDes = DOU_DES_SIZE * 1024L;
        //meta
        long metaSize = MmapMetaImpl.PAGE_SIZE;
        //read
        long readStrDes = DBConstant.STRING_MAX_BYTE_SIZE * COMPRESS_LIMIT * STR_READ_DES_BUFFER_SIZE;
        long readIntDes = DBConstant.INT_MAX_BYTE_SIZE * COMPRESS_LIMIT * INT_READ_DES_BUFFER_SIZE;
        long readDoubleDes = DBConstant.DOUBLE_MAX_BYTE_SIZE * COMPRESS_LIMIT * DOU_READ_DES_BUFFER_SIZE;
        //agg
        long aggSize = (long) MmapAggImpl.PAGE_SIZE * VIN_NUM;
        return "Size of space occupied total the offHeap:" + ((indexSize
                + strSrc + strDes + intSrc + intDes + doubleSrc + doubleDes
                + metaSize) * 5000 + (readStrDes + readIntDes + readDoubleDes)+aggSize) / 1024.0 / 1024.0 + "MB";
    }
}
