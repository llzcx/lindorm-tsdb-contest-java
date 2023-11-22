package com.alibaba.lindorm.contest.util;

import com.alibaba.lindorm.contest.TSDBEngineImpl;
import com.alibaba.lindorm.contest.structs.ColumnValue;
import com.alibaba.lindorm.contest.structs.Row;
import com.alibaba.lindorm.contest.structs.Vin;

import java.io.*;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.StandardOpenOption;
import java.util.HashMap;
import java.util.Map;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;



/**
 * @author 陈翔
 */
public final class FileUtil {
    /**
     * 序列化保存
     * @param o
     * @param file
     */
    public static void saveToDisk(Object o, File file) {
        try (ObjectOutputStream oos = new ObjectOutputStream(
                new GZIPOutputStream(new FileOutputStream(file)))) {
            oos.writeObject(o);
        } catch (IOException e) {
            e.printStackTrace();
            System.out.println("保存失败：" + e.getMessage());
        }
    }

    /**
     * 从磁盘中加载磁盘数据
     * @param file
     * @param cls
     * @param <T>
     * @return
     */
    public static <T> T loadFromDisk(File file,Class<T> cls) {
        T o = null;
        try (ObjectInputStream ois = new ObjectInputStream(
                new GZIPInputStream(new FileInputStream(file)))) {
            o = (T) ois.readObject();
        } catch (IOException | ClassNotFoundException | ClassCastException e) {
            e.printStackTrace();
            System.out.println("加载失败：" + e.getMessage());
        }
        return o;
    }

    /**
     * 删除文件夹当中所有数据
     * @param dir
     * @param deleteDirItself
     * @return
     */
    public static boolean cleanDir(File dir, boolean deleteDirItself) {
        if (dir.isDirectory()) {
            String[] children = dir.list();
            assert children != null;
            for (String child : children) {
                boolean ret = cleanDir(new File(dir, child), true);
                if (!ret) {
                    return false;
                }
            }
        }
        if (deleteDirItself) {
            return dir.delete();
        }
        return true;
    }

    public static String readFileToString(String filePath) {
        StringBuilder contentBuilder = new StringBuilder();

        try (BufferedReader reader = new BufferedReader(new FileReader(filePath))) {
            String line;
            while ((line = reader.readLine()) != null) {
                contentBuilder.append(line);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }

        return contentBuilder.toString();
    }

    public static void saveRow(File file, Row row) throws IOException{
        file.delete();
        file.createNewFile();
        FileChannel channel = FileChannel.open(file.toPath(), StandardOpenOption.WRITE,StandardOpenOption.READ);
        final int size = calculate(row);
        final MappedByteBuffer map = channel.map(FileChannel.MapMode.READ_WRITE, 0, size);
        //先写ts
        CommonUtils.writeLong(map, row.getTimestamp());
        //再写vin
        CommonUtils.writeString(map,ByteBuffer.wrap(row.getVin().getVin()));
        final Map<String, ColumnValue.ColumnType> mp = TSDBEngineImpl.schemaInfo.getMp();
        for (Map.Entry<String, ColumnValue.ColumnType> entry : mp.entrySet()) {
            ColumnValue.ColumnType  type = entry.getValue();
            final ColumnValue columnValue = row.getColumns().get(entry.getKey());
            switch (type){
                case COLUMN_TYPE_STRING:
                    CommonUtils.writeString(map,columnValue.getStringValue());
                    break;
                case COLUMN_TYPE_INTEGER:
                    CommonUtils.writeInt(map,columnValue.getIntegerValue());
                    break;
                case COLUMN_TYPE_DOUBLE_FLOAT:
                    CommonUtils.writeDouble(map,columnValue.getDoubleFloatValue());
                    break;
                default:
                    break;
            }
        }
        MMapUtil.unmap(map);
    }
    public static Row readRow(File file) throws IOException{
        FileChannel channel = FileChannel.open(file.toPath(),StandardOpenOption.READ);
        final MappedByteBuffer map = channel.map(FileChannel.MapMode.READ_ONLY, 0, channel.size());
        //先读ts
        final long ts = CommonUtils.readLong(map);
        //然后读vin
        final ByteBuffer vin = CommonUtils.readString(map);
        Map<String, ColumnValue> columns = new HashMap<>();
        //最后读row
        for (Map.Entry<String, ColumnValue.ColumnType> entry : TSDBEngineImpl.schemaInfo.getMp().entrySet()) {
            ColumnValue.ColumnType  type = entry.getValue();
            ColumnValue cv = null;
            switch (type){
                case COLUMN_TYPE_STRING:
                    final ByteBuffer st = CommonUtils.readString(map);
                    cv = new ColumnValue.StringColumn(st);
                    break;
                case COLUMN_TYPE_INTEGER:
                    final int in = CommonUtils.readInt(map);
                    cv = new ColumnValue.IntegerColumn(in);
                    break;
                case COLUMN_TYPE_DOUBLE_FLOAT:
                    final double dou = CommonUtils.readDouble(map);
                    cv = new ColumnValue.DoubleFloatColumn(dou);
                    break;
                default:
                    break;
            }
            columns.put(entry.getKey(),cv);
        }
        return new Row(new Vin(vin.array()),ts,columns);
    }
    public static int calculate(Row row){
        int num = 0;
        num += 8;
        num += 4 + row.getVin().getVin().length;
        for (Map.Entry<String, ColumnValue.ColumnType> entry : TSDBEngineImpl.schemaInfo.getMp().entrySet()) {
            final ColumnValue.ColumnType type = entry.getValue();
            switch (type){
                case COLUMN_TYPE_STRING:
                    num += 4 + row.getColumns().get(entry.getKey()).getStringValue().remaining();
                    break;
                case COLUMN_TYPE_INTEGER:
                    num += 4;
                    break;
                case COLUMN_TYPE_DOUBLE_FLOAT:
                    num += 8;
                    break;
                default:
                    break;
            }
        }
        return num;
    }


    public static boolean checkBuffer(ByteBuffer buffer,long size){
        return buffer != null && buffer.remaining() >= size;
    }


    /**
     * 将buffer里的数据写入channel
     * @param channel
     * @param buffer
     * @throws IOException
     */
    public static void flush(FileChannel channel,ByteBuffer buffer) throws IOException{
        buffer.flip();
        final MappedByteBuffer map = channel.map(FileChannel.MapMode.READ_WRITE, channel.size(), buffer.remaining());
        map.put(buffer);

        buffer.clear();
    }
}
