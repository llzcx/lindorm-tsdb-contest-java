package com.alibaba.lindorm.contest.util;

import java.lang.reflect.Method;
import java.nio.MappedByteBuffer;

/**
 * Utility class which provides a method for attempting to directly unmap a {@link MappedByteBuffer} rather than
 * waiting for the JVM & OS eventually unmap.</p>
 */
public final class MMapUtil {

    public static void unmap(final MappedByteBuffer buffer) {
        if (buffer == null || !buffer.isDirect()) {
            return;
        }
        try {
            Method cleanerMethod = buffer.getClass().getMethod("cleaner");
            cleanerMethod.setAccessible(true);
            Object cleaner = cleanerMethod.invoke(buffer);
            if(cleaner!=null){
              cleaner.getClass().getMethod("clean").invoke(cleaner);
            }
        } catch (Exception e) {
            // 异常处理
            e.printStackTrace();
        }

    }

}


