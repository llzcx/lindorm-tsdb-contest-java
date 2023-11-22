package com.alibaba.lindorm.contest.test;

import com.alibaba.lindorm.contest.structs.ColumnValue;
import com.alibaba.lindorm.contest.structs.ColumnValue.DoubleFloatColumn;
import com.alibaba.lindorm.contest.structs.ColumnValue.IntegerColumn;
import com.alibaba.lindorm.contest.structs.ColumnValue.StringColumn;
import com.alibaba.lindorm.contest.structs.Row;
import com.alibaba.lindorm.contest.structs.Vin;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.*;

import static com.alibaba.lindorm.contest.extendstructs.DBConstant.BASE_TP;

/**
 * 测试数据生成类
 * @author 陈翔
 */
public class RowGenerator {

    public static ArrayList<Vin> vins = new ArrayList<>();

    public static long current = BASE_TP;

    public static HashMap<String,ColumnValue.ColumnType> schema = new HashMap<>();

    public static List<Row> generateRandomRows(int vinNum, int columnNum, int timeLength) {
        System.out.println("Data create ok:"+"vinNum: "+ vinNum + ",colNum: "+columnNum + ",tsL: "+timeLength);
        schema.clear();
        vins.clear();
        generateSchema(columnNum);
        List<Row> rows = new ArrayList<>();
        for (int i = 0; i < vinNum; i++) {
            // Generate random data for each row
            // 车辆唯一标识，随机生成一个长度为17的字符串
            String vinStr =  UUID.randomUUID().toString().replace("-", "").substring(0, 17);
            final Vin vin = new Vin(vinStr.getBytes(StandardCharsets.UTF_8));
            vins.add(vin);
            // 时间戳
            for (int k = 1; k <= timeLength; k++) {
                Map<String, ColumnValue> columns = new HashMap<>();
                for (int j = 0; j < columnNum; j++) {
                    String columnKey = "col"+j;
                    // 随机选择指标类型
                    ColumnValue.ColumnType indicatorType = schema.get(columnKey);
                    if (indicatorType == ColumnValue.ColumnType.COLUMN_TYPE_STRING) {
                        // 不定长度的字符串类型
                        String randomString = getRandomString(15);
//                        columns.put(columnKey, new StringColumn(ByteBuffer.wrap(randomString.getBytes())));
                        columns.put(columnKey,new StringColumn(ByteBuffer.wrap(columnKey.getBytes(StandardCharsets.UTF_8))));
                    } else if (indicatorType == ColumnValue.ColumnType.COLUMN_TYPE_DOUBLE_FLOAT) {
                        // 8 字节浮点数类型
                        double randomDouble = getRandomDouble(1,10000);
                        columns.put(columnKey, new DoubleFloatColumn(randomDouble));
                    } else {
                        // 4 字节整数类型
                        int randomInt = getRandomInt(1,10000);
                        columns.put(columnKey, new IntegerColumn(randomInt));
                    }
                }
                rows.add(new Row(vin, current + k * 1000L,columns));
            }
        }
        return rows;
    }

    public static List<Row> generateCorrectnessRow(){
        generateSchema(60);
        Vin vin = new Vin("11111111111111111".getBytes(StandardCharsets.UTF_8));
        List<Row> rows = new ArrayList<>();
        Map<String, ColumnValue> columns = new HashMap<>();
        for (int i = 0; i < 60; i++) {
            if(i==20 || i==40){
                continue;
            }
            String columnKey = "col"+i;
            // 随机选择指标类型
            ColumnValue.ColumnType indicatorType = schema.get(columnKey);
            if (indicatorType == ColumnValue.ColumnType.COLUMN_TYPE_STRING) {
                // 不定长度的字符串类型
                columns.put(columnKey, new StringColumn(ByteBuffer.wrap("111".getBytes(StandardCharsets.UTF_8))));
            } else if (indicatorType == ColumnValue.ColumnType.COLUMN_TYPE_DOUBLE_FLOAT) {
                // 8 字节浮点数类型
                columns.put(columnKey, new DoubleFloatColumn(0.0));
            } else {
                // 4 字节整数类型
                columns.put(columnKey, new IntegerColumn(0));
            }
        }
        for (int i = 0; i < 10; i++) {
            HashMap<String,ColumnValue> mp = new HashMap<>();
            HashMap<String,ColumnValue> clone = (HashMap<String, ColumnValue>) mp.clone();
            clone.put("col"+20, new DoubleFloatColumn(i));
            clone.put("col"+40, new IntegerColumn(i));
            rows.add(new Row(vin, i,clone));
        }
        return rows;
    }

    /**
     * string first: 0~10
     * double first: 10~20
     * int first: 20~60
     * @param columnNum
     */
    public static void generateSchema(int columnNum){
        for (int j = 0; j < columnNum; j++) {
            String columnKey = "col"+j;
            if (j  < 10) {
                // 不定长度的字符串类型
                schema.put(columnKey, ColumnValue.ColumnType.COLUMN_TYPE_STRING);
            } else if (j < 20) {
                // 8 字节浮点数类型
                schema.put(columnKey, ColumnValue.ColumnType.COLUMN_TYPE_DOUBLE_FLOAT);
            } else {
                // 4 字节整数类型
                schema.put(columnKey, ColumnValue.ColumnType.COLUMN_TYPE_INTEGER);
            }
        }
    }

    private static Random random = new Random();

    private static String str = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789";

    public static int getRandomInt(int minValue, int maxValue) {
        return random.nextInt(maxValue - minValue + 1) + minValue;
    }

    public static double getRandomDouble(double minValue, double maxValue) {
        return random.nextDouble() * (maxValue - minValue) + minValue;
    }


    public static String getRandomString(int length) {
        StringBuffer sb = new StringBuffer();
        for (int i = 0; i < length; i++) {
            int number = random.nextInt(str.length());
            sb.append(str.charAt(number));
        }
        return sb.toString();
    }
}
