package com.alibaba.lindorm.contest.extendstructs;

import com.alibaba.lindorm.contest.extendstructs.compression.CompressionAlgorithm;
import com.alibaba.lindorm.contest.util.SizeOf;

import static com.alibaba.lindorm.contest.util.SizeOf.*;

/**
 * 常见的常量
 *
 * @author 陈翔
 */
public interface DBConstant {
    /**-------------------------------可调节程序参数-------------------------------------**/
    /**
     * 压缩等级
     */
    int COMPRESSION_LEVEL = 13;

    /**
     * 多少个存储单位为一个压缩块
     * 160*101 = 15.625K
     * 80*101 = 7.8125k
     * 234*101 = 23.73046875k
     */
    int COMPRESS_LIMIT = 100;

    /**
     * reader的各个buffer池大小
     */
    int STR_READ_DES_BUFFER_SIZE = 1000;
    int INT_READ_DES_BUFFER_SIZE = 1000;
    int DOU_READ_DES_BUFFER_SIZE = 1000;
    /**-------------------------------评测程序常量-------------------------------------**/
    /**
     * NaN
     */
    int INTEGER_NaN = Integer.MIN_VALUE;
    double DOUBLE_NaN = Double.NEGATIVE_INFINITY;

    int INT_NUM_OF_A_ROW = 40;

    int DOUBLE_NUM_OF_A_ROW = 10;



    int STRING_NUM_OF_A_ROW = 10;


    /**
     * 评测线程数量
     */
    int THREAD_COUNT = 8;
    int READ_COUNT = 16;

    /**
     * 车辆数
     */
    int VIN_NUM = 5000;

    /**
     * WAL中key大小（压缩以后的long long）
     */
    int SIZE_OF_KEY = SizeOf.SIZE_OF_INT;

    /**
     * wal的大小 int -> short + short
     */
    int SIZE_OF_WAL_RECORD = SIZE_OF_KEY + SizeOf.SIZE_OF_SHORT + SizeOf.SIZE_OF_SHORT;

    /**
     * 每个vin的数据量
     */
    int TIME_LENGTH = Math.toIntExact(10 * HOUR) + COMPRESS_LIMIT;

    /**
     * 各个查询时间长度
     */
    int RANGE_TIME_LENGTH = Math.toIntExact(MIN)*10;  // 60
    int AGGR_TIME_LENGTH = Math.toIntExact(3*HOUR); // 3600*3 = 10800
    int DOWN_TIME_LENGTH = Math.toIntExact(HOUR); //3600

    /**
     * 每一种类型转化为字节数组以后长度（存储单位）【用于确定src数组大小】
     */
    int INT_MAX_BYTE_SIZE = 4 * 40;
    int DOUBLE_MAX_BYTE_SIZE = 8 * 10;
    int SIZE_OF_STR_ROW_OFFSET = SIZE_OF_INT;
    int SIZE_OF_STR_COL_OFFSET = SIZE_OF_INT;
    int STRING_MAX_BYTE_SIZE = 203 + 10 * SIZE_OF_STR_COL_OFFSET;

    /**
     * 压缩块偏移量大小表示
     */
    int SIZE_OF_META_BLOCK = SIZE_OF_INT;

    /**
     * tp的最小值
     */
    Long BASE_TP = 1694007124000L;

    /**
     * SSD容量(G)
     */
    long SSD_SIZE = 500;

    long IOPS = Math.min(1800+12*SSD_SIZE, 10000);

    long THROUGHPUT = (long) Math.min(100+0.25*SSD_SIZE, 180);

    double INT_TOTAL = 26.82209014 * R1GB;
    double DOU_TOTAL = 13.41104507 * R1GB;
    double STR_TOTAL = 39.36011278 * R1GB;

    double ALL = 80.94684847192093 * R1GB;

    static void main(String[] args) {
        System.out.println(INT_MAX_BYTE_SIZE*COMPRESS_LIMIT/1024.0);
        System.out.println(DOUBLE_MAX_BYTE_SIZE*COMPRESS_LIMIT/1024.0);
        System.out.println(STRING_MAX_BYTE_SIZE*COMPRESS_LIMIT/1024.0);
        System.out.println(THROUGHPUT*R1GB/480/COMPRESS_LIMIT);
        //15.625
        //7.8125
        //23.73046875]
        double total_not_compress = 47.324529286/0.5846370819787513;
        System.out.println("total_not_compress:"+total_not_compress);
        double other = (1373.291015625 + 2.41036129 + 10.385513306)*R1MB + INT_TOTAL + DOU_TOTAL;
        System.out.println((ALL - other)/R1GB);

        System.out.println(9.290511385*R1GB / DOU_TOTAL);
        System.out.println(12.90891971*R1GB / INT_TOTAL);
        System.out.println(23.771497712*R1GB / STR_TOTAL);
    }




}
