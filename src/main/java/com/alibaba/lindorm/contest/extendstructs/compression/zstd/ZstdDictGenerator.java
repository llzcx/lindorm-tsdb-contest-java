package com.alibaba.lindorm.contest.extendstructs.compression.zstd;

import com.alibaba.lindorm.contest.extendstructs.log.TSDBLog;
import com.github.luben.zstd.Zstd;
import org.apache.log4j.Logger;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;


/**
 * 字典生成器
 * @author 陈翔
 */
public class ZstdDictGenerator {

    public static Logger logger = TSDBLog.getLogger();

    private static ByteBuffer dict;

    public static void create(){
        final String[] data = TrainData.STRING_DATA.split("\n");
        byte[][] bytes = new byte[data.length][];
        for (int i = 0; i < data.length; i++) {
            bytes[i] = data[i].getBytes(StandardCharsets.UTF_8);
        }
        byte[] pre = new byte[1024*6];
        final int len = Math.toIntExact(Zstd.trainFromBuffer(bytes, pre,false));
        dict = ByteBuffer.wrap(pre, 0, len);
    }

    static {
        create();
        logger.info("Zstd dict prepare ok."+"{dictLength="+dict.limit()+",check="+!Zstd.isError(dict.limit())+"}");
    }

    public static ByteBuffer dict(){
        return dict;
    }
}
