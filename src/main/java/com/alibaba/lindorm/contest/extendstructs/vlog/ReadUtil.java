package com.alibaba.lindorm.contest.extendstructs.vlog;

import com.alibaba.lindorm.contest.extendstructs.log.TSDBLog;
import com.alibaba.lindorm.contest.structs.ColumnValue;
import com.alibaba.lindorm.contest.util.SizeOf;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.nio.ByteBuffer;

import static com.alibaba.lindorm.contest.TSDBEngineImpl.schemaInfo;
import static com.alibaba.lindorm.contest.extendstructs.DBConstant.SIZE_OF_STR_ROW_OFFSET;
import static com.alibaba.lindorm.contest.extendstructs.DBConstant.SIZE_OF_STR_COL_OFFSET;

/**
 * @author 陈翔
 */
public class ReadUtil {
    public static Logger logger = TSDBLog.getLogger();
    /**
     * 在缓冲区拿vlog
     * @param pressBlock 压缩块 最多可读为.limit
     * @param internalIndex
     * @param colIndex
     * @return
     */
    public static ColumnValue getVlog(ByteBuffer pressBlock, int srcNum,ColumnValue.ColumnType type, short internalIndex, int colIndex) throws IOException{
        //以当前写入位置为末尾，不要切换读模式（flip）！！！
        int size = pressBlock.position();
        if (size == 0) {
            throw new IOException("PressBlock's size is zero,Don't switch read mode!!!");
        }
        ColumnValue value;
        if (type == ColumnValue.ColumnType.COLUMN_TYPE_STRING) {
            //读取行索引
            pressBlock.position(internalIndex * SIZE_OF_STR_ROW_OFFSET);
            //第一个row
            final int rowoffset1 = pressBlock.getInt();
            //第二个row
            final int rowoffset2 = pressBlock.getInt();
            //读取列索引
            pressBlock.position(rowoffset1 + colIndex * SIZE_OF_STR_COL_OFFSET);
            final int col1 = pressBlock.getInt();
            final int col2 = pressBlock.getInt();
            //回到读取值的地方
            pressBlock.position(col1);
            int length;
            if (colIndex + 1 >= schemaInfo.getStrColNum()) {
                if(internalIndex + 1 >= srcNum){
                    length = size-col1;
                    if(length < 0){
                        throw new IOException("size:"+size+",col1:"+col1);
                    }
                }else{
                    length = rowoffset2-col1;
                    if(length < 0){
                        throw new IOException("rowoffset2:"+size+",col1:"+col1);
                    }
                }
            }else{
                length = col2-col1;
                if(length < 0){
                    throw new IOException("col2:"+size+",col1:"+col1);
                }
            }
            byte[] arr = new byte[length];
            if(length!=0){
                pressBlock.get(arr);
            }
            value = new ColumnValue.StringColumn(ByteBuffer.wrap(arr));
        } else if (type == ColumnValue.ColumnType.COLUMN_TYPE_DOUBLE_FLOAT) {
            pressBlock.position(internalIndex * schemaInfo.getDouColNum() * SizeOf.SIZE_OF_DOUBLE + colIndex*SizeOf.SIZE_OF_DOUBLE);
            value = new ColumnValue.DoubleFloatColumn(pressBlock.getDouble());
        } else {
            pressBlock.position(internalIndex * schemaInfo.getIntColNum() * SizeOf.SIZE_OF_INT + colIndex*SizeOf.SIZE_OF_INT);
            value = new ColumnValue.IntegerColumn(pressBlock.getInt());
        }
        pressBlock.position(size);
        return value;
    }
}
