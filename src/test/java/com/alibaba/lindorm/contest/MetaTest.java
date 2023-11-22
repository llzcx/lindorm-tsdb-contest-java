package com.alibaba.lindorm.contest;

import com.alibaba.lindorm.contest.structs.ColumnValue;
import com.alibaba.lindorm.contest.extendstructs.meta.MmapMetaImpl;
import org.junit.Test;

import java.io.File;
import java.io.IOException;

public class MetaTest {


    /**
     * meta正确性测试
     * @throws IOException
     */
    @Test
    public void test() throws IOException {
        final File file = new File("meta.bin");
        MmapMetaImpl mmapMetaImpl = new MmapMetaImpl(file);
        ColumnValue.ColumnType type1 = ColumnValue.ColumnType.COLUMN_TYPE_STRING;
        ColumnValue.ColumnType type2 = ColumnValue.ColumnType.COLUMN_TYPE_INTEGER;
        ColumnValue.ColumnType type3 = ColumnValue.ColumnType.COLUMN_TYPE_DOUBLE_FLOAT;
        mmapMetaImpl.append(type1,0);
        mmapMetaImpl.append(type1,20);
        mmapMetaImpl.append(type1,40);
        mmapMetaImpl.append(type1,60);

        mmapMetaImpl.append(type2,0);
        mmapMetaImpl.append(type2,4);
        mmapMetaImpl.append(type2,8);
        mmapMetaImpl.append(type2,12);

        mmapMetaImpl.append(type3,0);
        mmapMetaImpl.append(type3,8);
        mmapMetaImpl.append(type3,16);
        mmapMetaImpl.append(type3,24);
        System.out.println("----------str测试-------------");
        System.out.println("result:"+ mmapMetaImpl.read(type1, (short) 0)+","+ mmapMetaImpl.read(type1, (short) 1)
        +","+ mmapMetaImpl.read(type1, (short) 2)+","+ mmapMetaImpl.read(type1, (short) 3));
        System.out.println("----------int测试-------------");
        System.out.println("result:"+ mmapMetaImpl.read(type2, (short) 0)+","+ mmapMetaImpl.read(type2, (short) 1)
                +","+ mmapMetaImpl.read(type2, (short) 2)+","+ mmapMetaImpl.read(type2, (short) 3));
        System.out.println("----------double测试-------------");
        System.out.println("result:"+ mmapMetaImpl.read(type3,(short)0)+","+ mmapMetaImpl.read(type3,(short)1)
                +","+ mmapMetaImpl.read(type3,(short)2)+","+ mmapMetaImpl.read(type3,(short)3));
        mmapMetaImpl.flush();
        mmapMetaImpl.close();
        file.delete();
    }

}
