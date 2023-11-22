package com.alibaba.lindorm.contest.util;

import java.io.*;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Arrays;

public class BytesUtils {

    /**
     * 将int转为高字节在前，低字节在后的byte数组（大端）
     *
     * @param n int
     * @return byte[]
     */
    public static byte[] int2BytesBig(int n) {
        byte[] b = new byte[4];
        b[3] = (byte) (n & 0xff);
        b[2] = (byte) (n >> 8 & 0xff);
        b[1] = (byte) (n >> 16 & 0xff);
        b[0] = (byte) (n >> 24 & 0xff);
        return b;
    }

    /**
     * 将int转为低字节在前，高字节在后的byte数组（小端）
     *
     * @param n int
     * @return byte[]
     */
    public static byte[] int2BytesLittle(int n) {
        byte[] b = new byte[4];
        b[0] = (byte) (n & 0xff);
        b[1] = (byte) (n >> 8 & 0xff);
        b[2] = (byte) (n >> 16 & 0xff);
        b[3] = (byte) (n >> 24 & 0xff);
        return b;
    }

    /**
     * 将int转为低字节在前，高字节在后的byte数组（小端）
     * @param n int
     * @param b  byte[]
     */
    public static void int2BytesLittle(int n,byte[] b){
        b[0] = (byte) (n & 0xff);
        b[1] = (byte) (n >> 8 & 0xff);
        b[2] = (byte) (n >> 16 & 0xff);
        b[3] = (byte) (n >> 24 & 0xff);
    }


    /**
     * byte数组到int的转换(小端)
     *
     * @param bytes
     * @return
     */
    public static int bytes2IntLittle(byte[] bytes) {
        int int1 = bytes[0] & 0xff;
        int int2 = (bytes[1] & 0xff) << 8;
        int int3 = (bytes[2] & 0xff) << 16;
        int int4 = (bytes[3] & 0xff) << 24;

        return int1 | int2 | int3 | int4;
    }

    /**
     * byte数组到int的转换(大端)
     *
     * @param bytes
     * @return
     */
    public static int bytes2IntBig(byte[] bytes) {
        int int1 = bytes[3] & 0xff;
        int int2 = (bytes[2] & 0xff) << 8;
        int int3 = (bytes[1] & 0xff) << 16;
        int int4 = (bytes[0] & 0xff) << 24;

        return int1 | int2 | int3 | int4;
    }

    /**
     * 将short转为高字节在前，低字节在后的byte数组（大端）
     *
     * @param n short
     * @return byte[]
     */
    public static byte[] short2BytesBig(short n) {
        byte[] b = new byte[2];
        b[1] = (byte) (n & 0xff);
        b[0] = (byte) (n >> 8 & 0xff);
        return b;
    }

    /**
     * 将short转为低字节在前，高字节在后的byte数组(小端)
     *
     * @param n short
     * @return byte[]
     */
    public static byte[] short2BytesLittle(short n) {
        byte[] b = new byte[2];
        b[0] = (byte) (n & 0xff);
        b[1] = (byte) (n >> 8 & 0xff);
        return b;
    }

    /**
     * 读取小端byte数组为short
     *
     * @param b
     * @return
     */
    public static short bytes2ShortLittle(byte[] b) {
        return (short) (((b[1] << 8) | b[0] & 0xff));
    }

    /**
     * 读取大端byte数组为short
     *
     * @param b
     * @return
     */
    public static short bytes2ShortBig(byte[] b) {
        return (short) (((b[0] << 8) | b[1] & 0xff));
    }

    /**
     * long类型转byte[] (大端)
     *
     * @param n
     * @return
     */
    public static byte[] long2BytesBig(long n) {
        byte[] b = new byte[8];
        b[7] = (byte) (n & 0xff);
        b[6] = (byte) (n >> 8 & 0xff);
        b[5] = (byte) (n >> 16 & 0xff);
        b[4] = (byte) (n >> 24 & 0xff);
        b[3] = (byte) (n >> 32 & 0xff);
        b[2] = (byte) (n >> 40 & 0xff);
        b[1] = (byte) (n >> 48 & 0xff);
        b[0] = (byte) (n >> 56 & 0xff);
        return b;
    }

    /**
     * long类型转byte[] (小端)
     *
     * @param n
     * @return
     */
    public static byte[] long2BytesLittle(long n) {
        byte[] b = new byte[8];
        b[0] = (byte) (n & 0xff);
        b[1] = (byte) (n >> 8 & 0xff);
        b[2] = (byte) (n >> 16 & 0xff);
        b[3] = (byte) (n >> 24 & 0xff);
        b[4] = (byte) (n >> 32 & 0xff);
        b[5] = (byte) (n >> 40 & 0xff);
        b[6] = (byte) (n >> 48 & 0xff);
        b[7] = (byte) (n >> 56 & 0xff);
        return b;
    }

    /**
     * byte[]转long类型(小端)
     *
     * @param array
     * @return
     */
    public static long bytes2LongLittle(byte[] array) {
        return ((((long) array[0] & 0xff))
                | (((long) array[1] & 0xff) << 8)
                | (((long) array[2] & 0xff) << 16)
                | (((long) array[3] & 0xff) << 24)
                | (((long) array[4] & 0xff) << 32)
                | (((long) array[5] & 0xff) << 40)
                | (((long) array[6] & 0xff) << 48)
                | (((long) array[7] & 0xff) << 56));
    }

    /**
     * byte[]转long类型(大端)
     *
     * @param array
     * @return
     */
    public static long bytes2LongBig(byte[] array) {
        return ((((long) array[0] & 0xff) << 56)
                | (((long) array[1] & 0xff) << 48)
                | (((long) array[2] & 0xff) << 40)
                | (((long) array[3] & 0xff) << 32)
                | (((long) array[4] & 0xff) << 24)
                | (((long) array[5] & 0xff) << 16)
                | (((long) array[6] & 0xff) << 8)
                | (((long) array[7] & 0xff)));
    }


    /**
     * long类型转byte[] (大端),通过ByteArrayOutputStream实现
     *
     * @param l
     * @return
     * @throws IOException
     */
    public static byte[] long2BytesBOS(long l) throws IOException {
        ByteArrayOutputStream bao = new ByteArrayOutputStream();
        DataOutputStream dos = new DataOutputStream(bao);
        dos.writeLong(l);
        byte[] buf = bao.toByteArray();
        return buf;
    }

    /**
     * byte[]转long类型(大端),通过ByteArrayOutputStream实现
     *
     * @param data
     * @return
     * @throws IOException
     */
    public long bytes2LongBOS(byte[] data) throws IOException {
        ByteArrayInputStream bai = new ByteArrayInputStream(data);
        DataInputStream dis = new DataInputStream(bai);
        return dis.readLong();
    }

    /**
     * long类型转byte[] (大端),通过ByteBuffer实现
     *
     * @param value
     * @return
     */
    public static byte[] long2BytesByteBuffer(long value) {
        return ByteBuffer.allocate(Long.SIZE / Byte.SIZE).putLong(value).array();
    }

    /**
     * byte[]转long类型(大端), 通过ByteBuffer实现
     *
     * @param bytes
     * @return
     */
    public static long bytes2LongByteBuffer(byte[] bytes) {
        ByteBuffer buffer = ByteBuffer.allocate(8);
        buffer.put(bytes, 0, bytes.length);
        buffer.flip();
        return buffer.getLong();
    }

    /**
     * 将字节数组转为long<br>
     * 如果input为null,或offset指定的剩余数组长度不足8字节则抛出异常
     *
     * @param input        byte[]
     * @param offset       起始偏移量 0
     * @param littleEndian 输入数组是否小端模式
     * @return
     */
    public static long bytes2Long(byte[] input, int offset, boolean littleEndian) {
        long value = 0;
        // 循环读取每个字节通过移位运算完成long的8个字节拼装
        for (int count = 0; count < 8; ++count) {
            int shift = (littleEndian ? count : (7 - count)) << 3;
            value |= ((long) 0xff << shift) & ((long) input[offset + count] << shift);
        }
        return value;
    }

    /**
     * 利用 {@link java.nio.ByteBuffer}实现byte[]转long
     *
     * @param input        byte[]
     * @param offset       0
     * @param littleEndian 输入数组是否小端模式
     * @return
     */
    public static long bytes2LongByteBuffer(byte[] input, int offset, boolean littleEndian) {
        // 将byte[] 封装为 ByteBuffer
        ByteBuffer buffer = ByteBuffer.wrap(input, offset, 8);
        if (littleEndian) {
            // ByteBuffer.order(ByteOrder) 方法指定字节序,即大小端模式(BIG_ENDIAN/LITTLE_ENDIAN)
            // ByteBuffer 默认为大端(BIG_ENDIAN)模式
            buffer.order(ByteOrder.LITTLE_ENDIAN);
        }
        return buffer.getLong();
    }

    /**
     * int 转 byte
     *
     * @param t
     * @return
     */
    public static byte int2Byte(int t) {
        return (byte) t;
    }

    /**
     * byte 转 int,解决java中byte输出可能为负数的问题
     *
     * @param b
     * @return
     */
    public static int byte2Int(byte b) {
        return b & 0xFF;
    }

    /**
     * 将 object --> byte 数组
     *
     * @param obj
     * @return
     */
    public static byte[] object2Bytes(Object obj) {
        byte[] bytes = null;
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        try {
            ObjectOutputStream oos = new ObjectOutputStream(bos);
            oos.writeObject(obj);
            oos.flush();
            bytes = bos.toByteArray();
            oos.close();
            bos.close();
        } catch (IOException ex) {
            ex.printStackTrace();
        }
        return bytes;
    }

    /**
     * 数组转对象
     *
     * @param bytes
     * @return
     */
    public Object bytes2Object(byte[] bytes) {
        Object obj = null;
        try {
            ByteArrayInputStream bis = new ByteArrayInputStream(bytes);
            ObjectInputStream ois = new ObjectInputStream(bis);
            obj = ois.readObject();
            ois.close();
            bis.close();
        } catch (IOException | ClassNotFoundException ex) {
            ex.printStackTrace();
        }
        return obj;
    }

    /**
     * 两个byte[]是否相同
     * @param data1
     * @param data2
     * @return
     */
    public static boolean bytesEquals(byte[] data1, byte[] data2) {
        return Arrays.equals(data1, data2);
    }

    /**
     * 截取byte[]
     * @param data
     * @param position
     * @param length
     * @return
     */
    public static byte[] subBytes(byte[] data, int position, int length) {
        byte[] temp = new byte[length];
        System.arraycopy(data, position, temp, 0, length);
        return temp;
    }
    /**
     * 拼接byte[] 和 byte[]
     *
     * @param bytes1
     * @param bytes2
     * @return
     */
    public static byte[] bytesMerger(byte[] bytes1, byte[] bytes2) {
        byte[] bytes3 = new byte[bytes1.length + bytes2.length];
        System.arraycopy(bytes1, 0, bytes3, 0, bytes1.length);
        System.arraycopy(bytes2, 0, bytes3, bytes1.length, bytes2.length);
        return bytes3;
    }
    /**
     * 拼接byte 和 byte[]
     *
     * @param byte1
     * @param bytes2
     * @return
     */
    public static byte[] bytesMerger(byte byte1, byte[] bytes2) {
        byte[] bytes3 = new byte[1 + bytes2.length];
        bytes3[0] = byte1;
        System.arraycopy(bytes2, 0, bytes3, 1, bytes2.length);
        return bytes3;
    }

    /**
     * 拼接byte[] 和 byte
     *
     * @param bytes1
     * @param byte2
     * @return
     */
    public static byte[] bytesMerger(byte[] bytes1, byte byte2) {
        byte[] bytes3 = new byte[1 + bytes1.length];
        System.arraycopy(bytes1, 0, bytes3, 0, bytes1.length);
        bytes3[bytes3.length - 1] = byte2;
        return bytes3;
    }

    /**
     * 拼接三个数组
     *
     * @param bt1
     * @param bt2
     * @param bt3
     * @return
     */
    public static byte[] bytesMerger(byte[] bt1, byte[] bt2, byte[] bt3) {
        byte[] data = new byte[bt1.length + bt2.length + bt3.length];
        System.arraycopy(bt1, 0, data, 0, bt1.length);
        System.arraycopy(bt2, 0, data, bt1.length, bt2.length);
        System.arraycopy(bt3, 0, data, bt1.length + bt2.length, bt3.length);
        return data;
    }

  /**
     * byte转Hex 16 进制字符串
     */
    public static String byte2Hex(byte b) {
        String hex = Integer.toHexString(b & 0xFF);
        if (hex.length() < 2) {
            hex = "0" + hex;
        }
        return hex;
    }

    /**
     * byte[]转Hex
     */
    public static String bytes2Hex(byte[] b) {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < b.length; i++) {
            String hex = Integer.toHexString(b[i] & 0xFF);
            if (hex.length() < 2) {
                hex = "0" + hex;
            }
            sb.append(hex.toUpperCase());
        }
        return sb.toString();
    }

    /**
     * 字节数组转为16进制字符串
     */
    public static String bytes2Hex2(byte[] bytes) {
        String strHex = "";
        StringBuilder stringBuilder = new StringBuilder();
        for (int n = 0; n < bytes.length; n++) {
            strHex = Integer.toHexString(bytes[n] & 0xFF);
            stringBuilder.append((strHex.length() == 1) ? "0" + strHex : strHex);
        }
        return stringBuilder.toString().trim();
    }

    /**
     * Hex转byte,hex只能含两个字符，如果是一个字符byte高位会是0
     */
    public static byte hex2Byte(String hex) {
        return (byte) Integer.parseInt(hex, 16);
    }

    /**
     * Hex转byte[]，两种情况，Hex长度为奇数最后一个字符会被舍去
     */
    public static byte[] hex2Bytes(String hex) {
        if (hex.length() < 1) {
            return null;
        } else {
            byte[] result = new byte[hex.length() / 2];
            int j = 0;
            for (int i = 0; i < hex.length(); i += 2) {
                result[j++] = (byte) Integer.parseInt(hex.substring(i, i + 2), 16);
            }
            return result;
        }
    }

    /**
     * 字符串转为16进制字符串，推荐
     */
    public static String str2HexStr(String str) {
        char[] chars = "0123456789ABCDEF".toCharArray();
        StringBuilder sb = new StringBuilder("");
        byte[] bs = str.getBytes();
        int bit;
        for (byte b : bs) {
            bit = (b & 0x0f0) >> 4;
            sb.append(chars[bit]);
            bit = b & 0x0f;
            sb.append(chars[bit]);
        }
        return sb.toString().trim();
    }

    /**
     * 16进制直接转换成为字符串(无需Unicode解码)，推荐
     *
     * @param hexStr
     * @return
     */
    public static String hexStr2Str(String hexStr) {
        String str = "0123456789ABCDEF";
        char[] hexs = hexStr.toCharArray();
        byte[] bytes = new byte[hexStr.length() / 2];
        int n;
        for (int i = 0; i < bytes.length; i++) {
            n = str.indexOf(hexs[2 * i]) * 16;
            n += str.indexOf(hexs[2 * i + 1]);
            bytes[i] = (byte) (n & 0xff);
        }
        return new String(bytes);
    }


}
