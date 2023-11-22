package com.alibaba.lindorm.contest.util;

import jdk.internal.misc.Unsafe;

import java.io.IOException;
import java.nio.ByteBuffer;

public final class CommonUtils {
    // Add "--add-opens java.base/jdk.internal.misc=ALL-UNNAMED" to your VM properties to enable unsafe.
    private static final Unsafe UNSAFE = Unsafe.getUnsafe();
    private static final long ARRAY_BASE_OFFSET = UNSAFE.arrayBaseOffset(byte[].class);

    public static void writeLong(ByteBuffer out, long v) throws IOException {
        out.putLong(v);
    }

    public static long readLong(ByteBuffer in) throws IOException {
        return in.getLong();
    }

    public static void writeInt(ByteBuffer out, int v) throws IOException {
        out.putInt(v);
    }

    public static int readInt(ByteBuffer in) throws IOException {
        return in.getInt();
    }

    public static void writeDouble(ByteBuffer out, double v) throws IOException {
        out.putDouble(v);
    }

    public static double readDouble(ByteBuffer in) throws IOException {
        return in.getDouble();
    }

    public static void writeString(ByteBuffer out, ByteBuffer v) throws IOException {
        writeInt(out, v.remaining());
        if (v.remaining() > 0) {
            byte[] array1 = null;
            if (v.hasArray()) {
                array1 = v.array();
                if (array1.length != v.remaining()) {
                    array1 = null;
                }
            }
            if (array1 == null) {
                array1 = new byte[v.remaining()];
                v.get(array1);
            }
            out.put(array1);
        }
    }

    public static ByteBuffer readString(ByteBuffer in,int strLen) throws IOException {
        byte[] b = new byte[strLen];
        in.get(b);
        return ByteBuffer.wrap(b);
    }

    public static ByteBuffer readString(ByteBuffer in) throws IOException {
        int strLen = readInt(in);
        if (strLen == 0) {
            ByteBuffer res = ByteBuffer.allocate(0);
            res.flip();
            return res;
        }
        byte[] b = new byte[strLen];
        in.get(b);
        return ByteBuffer.wrap(b);
    }

}
