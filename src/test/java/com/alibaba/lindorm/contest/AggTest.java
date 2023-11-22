package com.alibaba.lindorm.contest;

import com.alibaba.lindorm.contest.extendstructs.DBConstant;
import com.alibaba.lindorm.contest.extendstructs.FileName;
import com.alibaba.lindorm.contest.extendstructs.agg.MmapAggImpl;
import com.alibaba.lindorm.contest.structs.Row;
import com.alibaba.lindorm.contest.structs.Vin;
import org.junit.Test;

import java.io.File;
import java.io.IOException;

public class AggTest implements DBConstant{

    @Test
    public void test() throws IOException {
        final File file = new File(FileName.AGG);
        file.delete();
        MmapAggImpl mmapAgg = new MmapAggImpl(file);

        short colIndex = 3;

        int num = DBConstant.TIME_LENGTH / DBConstant.COMPRESS_LIMIT;
        for (int i = 0; i < num; i++) {
            for (short j = 0; j < INT_NUM_OF_A_ROW; j++) {
                mmapAgg.appendInt(i+j, j-1);
            }
            for (short j = 0; j < DOUBLE_NUM_OF_A_ROW; j++) {
                mmapAgg.appendDouble(i+j, j+1);
            }

        }


        for (short i = 0; i < 5; i++) {
            System.out.println(mmapAgg.readIntMax(i, colIndex) + "," + mmapAgg.readIntTotal(i, colIndex));
        }
        for (short i = 0; i < 5; i++) {
            System.out.println(mmapAgg.readDoubleMax(i, colIndex) + "," + mmapAgg.readDoubleTotal(i, colIndex));
        }

        mmapAgg.flush();
        mmapAgg.close();

    }

    @Test
    public void test1() throws IOException {
        final File file = new File(FileName.AGG);
        file.delete();
        MmapAggImpl mmapAgg = new MmapAggImpl(file);

        short colIndex = 1;

        mmapAgg.appendDouble(10.1, 10.2);

        short compressIndex = 0;

        System.out.println(mmapAgg.readDoubleMax(compressIndex, colIndex) + "," + mmapAgg.readDoubleTotal(compressIndex, colIndex));

        mmapAgg.flush();
        mmapAgg.close();
    }

    static class Person{
        private String name;
        private Integer age;
        private Boolean sex;

        public String getName() {
            return name;
        }

        public void setName(String name) {
            this.name = name;
        }

        public Integer getAge() {
            return age;
        }

        public void setAge(Integer age) {
            this.age = age;
        }

        public Boolean getSex() {
            return sex;
        }

        public void setSex(Boolean sex) {
            this.sex = sex;
        }
    }

    @Test
    public void test2(){
        Person person  = new Person();


    }
}
