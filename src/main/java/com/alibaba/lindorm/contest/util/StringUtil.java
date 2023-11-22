package com.alibaba.lindorm.contest.util;

import com.alibaba.lindorm.contest.structs.Vin;

import java.io.File;
import java.util.Random;

/**
 * @author 陈翔
 */
public final class StringUtil {

    static Random random = new Random();
    /**
     * 获取最后一个英文点前的字符串
     * @param filename
     * @return
     */
    public static String getPrefix(String filename) {
        File file = new File(filename);
        String name = file.getName();
        int index = name.lastIndexOf(".");
        if (index != -1) {
            return name.substring(0, index);
        }
        return name;
    }
    public static String generateRandomString(int length) {
        String characters = "0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ";
        StringBuilder sb = new StringBuilder(length);
        for (int i = 0; i < length; i++) {
            int index = random.nextInt(characters.length());
            char randomChar = characters.charAt(index);
            sb.append(randomChar);
        }
        return sb.toString();
    }

    public static double generateRandomDouble(double min, double max) {
        double randomValue = min + (max - min) * random.nextDouble();
        return randomValue;
    }

    public static int generateRandomInt(int min, int max) {
        Random random = new Random();
        int randomValue = random.nextInt(max - min + 1) + min;
        return randomValue;
    }

    public static String vinToString(Vin vin){
        return new String(vin.getVin());
    }

}
