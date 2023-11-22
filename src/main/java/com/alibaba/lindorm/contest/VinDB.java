package com.alibaba.lindorm.contest;

import com.alibaba.lindorm.contest.extendstructs.DBConstant;
import com.alibaba.lindorm.contest.structs.ColumnValue;
import com.alibaba.lindorm.contest.structs.Row;

import java.io.IOException;

/**
 * @author 陈翔
 */
public interface VinDB extends DBConstant {

    VlogRead getDouReader();

    VlogRead getIntReader();

    VlogRead getStrReader();

    VLogWriter getDouWriter();

    VLogWriter getIntWriter();

    VLogWriter getStrWriter();

    VlogRead getReader(ColumnValue.ColumnType type);

    void close();

    void flushVlogBuffer() throws IOException;

    void prepareForRead();

    void prepareForWrite() throws IOException;

    void setMaxRow(Row row);

    Row getMaxRow() throws IOException;



}
