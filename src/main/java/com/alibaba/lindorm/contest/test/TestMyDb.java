package com.alibaba.lindorm.contest.test;//
// A simple evaluation program example helping you to understand how the
// evaluation program calls the protocols you will implement.
// Formal evaluation program is much more complex than this.
//

/*
 * Copyright Alibaba Group Holding Ltd.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import com.alibaba.lindorm.contest.TSDBEngineImpl;
import com.alibaba.lindorm.contest.extendstructs.DBConstant;
import com.alibaba.lindorm.contest.structs.*;
import com.alibaba.lindorm.contest.util.FileUtil;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static com.alibaba.lindorm.contest.extendstructs.DBConstant.BASE_TP;

/**
 * 多接口、多线程测试
 */
public class TestMyDb {
    public static void main(String[] args) {

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
        ExecutorService writeThread = Executors.newFixedThreadPool(10);

        try {
            // Stage1: write
            tsdbEngineSample.connect();

            //构造数据
            int vinNum = 1;
            int colNum = 60;
            int tsL = DBConstant.COMPRESS_LIMIT * 180;
            final List<Row> list = RowGenerator.generateRandomRows(vinNum, colNum, tsL);
            System.out.println("Total test data:" + list.size());
            final Vin vin1 = RowGenerator.vins.get(0);

            //创建表
            tsdbEngineSample.createTable("test", new Schema(RowGenerator.schema));
            System.out.println("Start upsert.");

            //写入
            long startTime = System.currentTimeMillis();
//            final List<List<Row>> lists = splitList(list, 10);
//            CountDownLatch countDownLatch = new CountDownLatch(10);
//            for (int i = 0; i < lists.size(); i++) {
//                int finalI = i;
//                TSDBEngineImpl finalTsdbEngineSample = tsdbEngineSample;
//                writeThread.submit(() -> {
//                    try {
//                        final List<Row> list1 = lists.get(finalI);
//                        for (final Row row : list1) {
//                            int num  = 121;
//                            final boolean b = row.getTimestamp() == BASE_TP + num * 1000 && row.getVin().equals(vin1);
//                            if(b){
//                                row.getColumns().put("col0", new ColumnValue.StringColumn(ByteBuffer.wrap(new byte[0])));
//                                row.getColumns().put("col9", new ColumnValue.StringColumn(ByteBuffer.wrap(new byte[0])));
//                            }
//                            final ArrayList<Row> wr = new ArrayList<>();
//                            wr.add(row);
//                            //写入
//                            finalTsdbEngineSample.write(new WriteRequest("test", wr));
//                            if (b) {
//                                System.out.println("-------check query-------");
//                                final ArrayList<Row> rows = finalTsdbEngineSample.executeTimeRangeQuery(new TimeRangeQueryRequest("test",
//                                        vin1, new HashSet<>(), BASE_TP + num * 1000, BASE_TP + num * 1000 + 1));
//                                System.out.println(rows);
//                                System.out.println("ok");
//                            }
//                        }
//                        countDownLatch.countDown();
//                    } catch (IOException e) {
//                        e.printStackTrace();
//                    }
//                });
//            }
//            try {
//                countDownLatch.await();
//            } catch (InterruptedException e) {
//                e.printStackTrace();
//            }
            tsdbEngineSample.write(new WriteRequest("test", list));
            System.out.println("Write need time:" + (System.currentTimeMillis() - startTime) + " ms");

            //关闭
            tsdbEngineSample.shutdown();

            System.out.println("start read.");
            tsdbEngineSample = new TSDBEngineImpl(dataDir);
            // Stage2: read

            long RangeL = RowGenerator.current + 100 * 1000;
            long RangeR = RowGenerator.current + 111 * 1000;
            long interval = 1000;

            String queryCol = "col19";


            tsdbEngineSample.connect();
            System.out.println("start executeLatestQuery:");
            startTime = System.currentTimeMillis();
            ArrayList<Vin> vinList = new ArrayList<>();
            vinList.add(vin1);
            Set<String> requestedColumns = new HashSet<>();
            requestedColumns.add(queryCol);
            ArrayList<Row> resultSet = tsdbEngineSample.executeLatestQuery(new LatestQueryRequest("test", vinList, requestedColumns));
            System.out.println("ExecuteLatestQuery need time:" + (System.currentTimeMillis() - startTime) + " ms");
            showResult(resultSet);


            System.out.println("start TimeRangeQueryRequest:"+RangeL+"->"+RangeR);
            System.out.println(requestedColumns);
            startTime = System.currentTimeMillis();
            resultSet = tsdbEngineSample.executeTimeRangeQuery(new TimeRangeQueryRequest("test",
                    vin1, requestedColumns, RangeL, RangeR));
            System.out.println("TimeRangeQueryRequest need time:" + (System.currentTimeMillis() - startTime) + " ms");
            showResult(resultSet);


            System.out.println("start executeAggregateQuery:"+RangeL+"->"+RangeR);
            final Aggregator aggregator = Aggregator.MAX;
            System.out.println(aggregator);
            System.out.println(queryCol);
            startTime = System.currentTimeMillis();
            resultSet = tsdbEngineSample.executeAggregateQuery(new TimeRangeAggregationRequest(
                    "test", vin1, queryCol, RangeL, RangeR, aggregator
            ));
            System.out.println("executeAggregateQuery need time:" + (System.currentTimeMillis() - startTime) + " ms");
            showResult(resultSet);

            final CompareExpression compareExpression = new CompareExpression(new ColumnValue.IntegerColumn(1), CompareExpression.CompareOp.GREATER);
            final Aggregator agg = Aggregator.MAX;
            System.out.println("start TimeRangeDownsampleRequest:"+RangeL+"->"+RangeR);
            System.out.println(agg);
            System.out.println(interval);
            System.out.println(compareExpression.getCompareOp());
            System.out.println(compareExpression.getValue());
            startTime = System.currentTimeMillis();
            resultSet = tsdbEngineSample.executeDownsampleQuery(new TimeRangeDownsampleRequest(
                    "test", vin1,
                    queryCol, RangeL, RangeR
                    , agg, interval, compareExpression
            ));
            System.out.println("TimeRangeDownsampleRequest need time:" + (System.currentTimeMillis() - startTime) + " ms");
            showResult(resultSet);
            //关闭
            tsdbEngineSample.shutdown();
            System.out.println("close.");
            writeThread.shutdown();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    static class MyComparator implements Comparator<Row> {
        @Override
        public int compare(Row o1, Row o2) {
            // 自定义比较规则，按照value字段进行降序排序
            return Math.toIntExact(o1.getTimestamp() - o2.getTimestamp());
        }
    }

    public static void showResult(ArrayList<Row> resultSet) {
        Collections.sort(resultSet, new MyComparator());
        for (Row result : resultSet) {
            System.out.println(result);
        }
        System.out.println("-------next query-------");
    }

    public static <T> List<List<T>> splitList(List<T> originalList, int parts) {
        Collections.shuffle(originalList);
        List<List<T>> splitLists = new ArrayList<>();
        int size = originalList.size();
        int chunkSize = (int) Math.ceil((double) size / parts);

        int startIndex = 0;
        int endIndex = chunkSize;

        for (int i = 0; i < parts; i++) {
            if (endIndex > size) {
                endIndex = size;
            }

            List<T> splitList = originalList.subList(startIndex, endIndex);
            splitLists.add(splitList);

            startIndex = endIndex;
            endIndex += chunkSize;
        }

        return splitLists;
    }
}