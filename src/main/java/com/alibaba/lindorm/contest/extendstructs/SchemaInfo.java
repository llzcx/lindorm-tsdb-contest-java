package com.alibaba.lindorm.contest.extendstructs;

import com.alibaba.lindorm.contest.TSDBEngineImpl;
import com.alibaba.lindorm.contest.extendstructs.log.TSDBLog;
import com.alibaba.lindorm.contest.structs.ColumnValue;
import com.alibaba.lindorm.contest.structs.Schema;
import com.alibaba.lindorm.contest.util.FileUtil;
import com.alibaba.lindorm.contest.util.StringUtil;
import org.apache.log4j.Logger;

import java.io.File;
import java.util.HashMap;
import java.util.Map;

/**
 * 表节点管理
 *
 * @author 陈翔
 */
public final class SchemaInfo {
    public static Logger logger = TSDBLog.getLogger();
    /**
     * 表名
     */
    private String tableName;
    /**
     * 列数量
     */
    private short strColNum = 0;
    private short intColNum = 0;
    private short douColNum = 0;

    private static Map<String, ColumnValue.ColumnType> mp;

    public static String[] strIndex = new String[60];

    public static String[] intIndex = new String[60];

    public static String[] douIndex = new String[60];

    private static HashMap<String, Short> tableIndex = new HashMap<>(60);


    /**
     * @param tableName 表名
     * @param schema    结构信息
     */
    public SchemaInfo(String tableName, Schema schema) {
        this.tableName = tableName;
        mp = schema.getColumnTypeMap();
        load(schema.getColumnTypeMap());
    }


    public void load(Map<String, ColumnValue.ColumnType> columnTypeMap){
        for (Map.Entry<String, ColumnValue.ColumnType> entry : columnTypeMap.entrySet()) {
            switch (entry.getValue()) {
                case COLUMN_TYPE_STRING:
                    strIndex[strColNum] = entry.getKey();
                    tableIndex.put(entry.getKey(), strColNum++);
                    break;
                case COLUMN_TYPE_INTEGER:
                    intIndex[intColNum] = entry.getKey();
                    tableIndex.put(entry.getKey(), intColNum++);
                    break;
                case COLUMN_TYPE_DOUBLE_FLOAT:
                    douIndex[douColNum] = entry.getKey();
                    tableIndex.put(entry.getKey(), douColNum++);
                    break;
                default:
                    break;
            }
        }
    }

    /**
     * 利用Scheme文件加载
     *
     * @param schemaFile sheme.txt
     */
    public SchemaInfo(File schemaFile) {
        final File[] files = schemaFile.listFiles();
        for (File file : files) {
            if (file.getName().endsWith(".txt")) {
                TSDBEngineImpl.FIRST = false;
                logger.info("Find schema file: " + file.getName());
                tableName = StringUtil.getPrefix(file.getName());
                mp = FileUtil.loadFromDisk(file, Map.class);
                load(mp);
                logger.info("Schema information load finish.");
            }
        }

    }

    /**
     * 保存数据到某个文件夹下
     *
     * @param file 父级文件夹
     */
    public void save(File file) {
        // Persist the schema.
        File schemaFile = new File(file, tableName + ".txt");
        FileUtil.saveToDisk(mp, schemaFile);
    }


    public short getIndex(String colName) {
        return tableIndex.get(colName);
    }

    public ColumnValue.ColumnType getT(String colName) {
        return mp.get(colName);
    }

    public Map<String, ColumnValue.ColumnType> getMp() {
        return mp;
    }

    public int getStrColNum() {
        return strColNum;
    }

    public int getIntColNum() {
        return intColNum;
    }

    public int getDouColNum() {
        return douColNum;
    }

    public int getTotal() {
        return 60;
    }
}
