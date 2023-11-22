package com.alibaba.lindorm.contest;

import com.alibaba.lindorm.contest.test.RowGenerator;
import com.alibaba.lindorm.contest.structs.*;
import com.alibaba.lindorm.contest.extendstructs.DBConstant;
import com.alibaba.lindorm.contest.util.FileUtil;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static com.alibaba.lindorm.contest.test.TestMyDb.showResult;
import static com.alibaba.lindorm.contest.extendstructs.DBConstant.BASE_TP;

/**
 * 单线程正确性测试
 */
public class SingleThreadTest {
    public static void main(String[] args) throws IOException {

        File dataDir = new File("data_dir");

        if (dataDir.isFile()) {
            throw new IllegalStateException("Clean the directory before we start the demo");
        }

        FileUtil.cleanDir(dataDir, true);

        boolean ret = dataDir.mkdirs();
        if (!ret) {
            throw new IllegalStateException("Cannot create the temp data directory: " + dataDir);
        }

        TSDBEngineImpl tsdbEngineSample = new TSDBEngineImpl(dataDir);

        // Stage1: write
        tsdbEngineSample.connect();

        //构造数据
        int vinNum = 1;
        int colNum = 60;
        int tsL = DBConstant.COMPRESS_LIMIT * 5;
        final List<Row> list = RowGenerator.generateRandomRows(vinNum, colNum, tsL);
        System.out.println("Total test data:"+ list.size());
        final Vin vin1 = RowGenerator.vins.get(0);
        Set<String> requestedColumns = new HashSet<>();
        requestedColumns.add("col11");
        String queryCol = "col11";
        long RangeL = BASE_TP+10*1000;
        long RangeR = BASE_TP+20*1000;
        long interval = 2*1000;

        //创建表
        tsdbEngineSample.createTable("test", new Schema(RowGenerator.schema));
        System.out.println("Start upsert.");

        long startTime = System.currentTimeMillis();
        for (int i = 0; i < list.size(); i++) {
            final Row row = list.get(i);
            row.getColumns().put("col0", new ColumnValue.StringColumn(ByteBuffer.wrap(new byte[0])));
            row.getColumns().put("col9", new ColumnValue.StringColumn(ByteBuffer.wrap(new byte[0])));
            //写入
            final ArrayList<Row> wr = new ArrayList<>();
            wr.add(row);
            tsdbEngineSample.write(new WriteRequest("test",wr));
            if(i==102){
                System.out.println("-------check query-------");
                final ArrayList<Row> rows = tsdbEngineSample.executeTimeRangeQuery(new TimeRangeQueryRequest("test",
                        vin1, new HashSet<>(), BASE_TP + 102 * 1000, BASE_TP + 102 * 1000 + 1));
                System.out.println(rows);
                System.out.println("ok");
            }
        }

        System.out.println("Write need time:" + (System.currentTimeMillis() - startTime) + " ms");
        //关闭
        tsdbEngineSample.shutdown();
        ArrayList<Row> resultSet = null;
        tsdbEngineSample = new TSDBEngineImpl(dataDir);
        tsdbEngineSample.connect();

        System.out.println("start TimeRangeQueryRequest");
        startTime = System.currentTimeMillis();
        resultSet = tsdbEngineSample.executeTimeRangeQuery(new TimeRangeQueryRequest("test",
                vin1, requestedColumns, RangeL, RangeR));
        System.out.println("TimeRangeQueryRequest need time:"+(System.currentTimeMillis()-startTime)+" ms");
        showResult(resultSet);


        System.out.println("start executeAggregateQuery:");
        startTime = System.currentTimeMillis();
        resultSet = tsdbEngineSample.executeAggregateQuery(new TimeRangeAggregationRequest(
                "test",vin1,queryCol,RangeL, RangeR,Aggregator.MAX
        ));
        System.out.println("executeAggregateQuery need time:"+(System.currentTimeMillis()-startTime)+" ms");


        showResult(resultSet);
        System.out.println("start TimeRangeDownsampleRequest:");
        startTime = System.currentTimeMillis();
        resultSet = tsdbEngineSample.executeDownsampleQuery(new TimeRangeDownsampleRequest(
                "test",vin1,
                queryCol,RangeL, RangeR
                ,Aggregator.AVG,interval,new CompareExpression(new ColumnValue.DoubleFloatColumn(5000), CompareExpression.CompareOp.GREATER)
        ));
        System.out.println("TimeRangeDownsampleRequest need time:"+(System.currentTimeMillis()-startTime)+" ms");
        showResult(resultSet);

        tsdbEngineSample.shutdown();
    }
}
