package com.alibaba.lindorm.contest.util;

import com.alibaba.lindorm.contest.extendstructs.DBConstant;
import com.alibaba.lindorm.contest.extendstructs.FileName;
import com.alibaba.lindorm.contest.extendstructs.log.TSDBLog;
import org.apache.log4j.Logger;

import java.io.File;
import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.List;

public class FileViewer {
    public static Logger logger = TSDBLog.getLogger();

    public static List<String> getListFiles(String path, String suffix,
                                            boolean isdepth) {
        List<String> lstFileNames = new ArrayList<String>();
        File file = new File(path);
        return FileViewer.listFile(lstFileNames, file, suffix, isdepth);
    }

    public static void printSize(File dir){
        double strf = 0,intf = 0,douf = 0,walf = 0,maxf = 0,metaf = 0;
        final File[] files = dir.listFiles();
        for (File vinf : files) {
            if(vinf.isDirectory()){
                final File[] files1 = vinf.listFiles();
                for (File file : files1) {
                    if(file.getName().equals(FileName.STR_COLS_F_NAME)){
                        strf += file.length();
                    }else if(file.getName().equals(FileName.DOUBLE_COLS_F_NAME)){
                        douf += file.length();
                    }else if(file.getName().equals(FileName.INT_COLS_F_NAME)){
                        intf += file.length();
                    }else if(file.getName().equals(FileName.WAL_F_NAME)){
                        walf += file.length();
                    }else if(file.getName().equals(FileName.MAX_ROW_F_NAME)){
                        maxf += file.length();
                    }else if(file.getName().equals(FileName.META_F_NAME)){
                        metaf += file.length();
                    }
                }
            }
        }
        DecimalFormat df = new DecimalFormat("#.#########");
        System.out.println("\n---------------------"+"\nstrf="+df.format(strf/SizeOf.R1GB)+"GB\ndouf="+df.format(douf/SizeOf.R1GB)+"GB\nintf="+df.format(intf/SizeOf.R1GB)
                +"GB\nwalf="+df.format(walf/SizeOf.R1MB)+"MB\nmaxf="+df.format(maxf/SizeOf.R1MB)+"MB\nmetaf="+df.format(metaf/SizeOf.R1MB)+"MB\n");
        System.out.println("Double Rating:"+ df.format(douf/ DBConstant.DOU_TOTAL) +"%");
        System.out.println("Int Rating:"+ df.format(intf/ DBConstant.INT_TOTAL)+"%");
        System.out.println("String Rating:"+ df.format(strf/ DBConstant.STR_TOTAL)+"%");
        double total = strf+intf+douf+metaf+walf+maxf;
        System.out.println("total:"+df.format(total/SizeOf.R1GB)+"GB");
        System.out.println("total Rating:"+ df.format(total/DBConstant.ALL)+"%");
        System.out.println("---------------------");
    }

    private static List<String> listFile(List<String> lstFileNames, File f,
                                         String suffix, boolean isdepth) {
        // 若是目录, 采用递归的方法遍历子目录
        if (f.isDirectory()) {
            File[] t = f.listFiles();

            for (int i = 0; i < t.length; i++) {
                if (isdepth || t[i].isFile()) {
                    listFile(lstFileNames, t[i], suffix, isdepth);
                }
            }
        } else {
            String filePath = f.getAbsolutePath();
            if (!"".equals(suffix)) {
                int begIndex = filePath.lastIndexOf("."); // 最后一个.(即后缀名前面的.)的索引
                String tempsuffix = "";

                if (begIndex != -1) {
                    tempsuffix = filePath.substring(begIndex + 1,
                            filePath.length());
                    if (tempsuffix.equals(suffix)) {
                        lstFileNames.add(filePath);
                    }
                }
            } else {
                lstFileNames.add(filePath);
            }
        }
        return lstFileNames;
    }

    // 递归取得文件夹（包括子目录）中所有文件的大小
    public static long getFileSize(File f)// 取得文件夹大小
    {
        long size = 0;
        File flist[] = f.listFiles();
        for (int i = 0; i < flist.length; i++) {
            if (flist[i].isDirectory()) {
                size = size + getFileSize(flist[i]);
            } else {
                size = size + flist[i].length();
            }
        }
        return size;
    }

    public static String FormetFileSize(long fileS) {// 转换文件大小
        DecimalFormat df = new DecimalFormat("#.#########");
        String fileSizeString = "";
        if (fileS < 1024) {
            fileSizeString = df.format((double) fileS) + "B";
        } else if (fileS < 1048576) {
            fileSizeString = df.format((double) fileS / 1024) + "KB";
        } else if (fileS < 1073741824) {
            fileSizeString = df.format((double) fileS / 1048576) + "MB";
        } else {
            fileSizeString = df.format((double) fileS / 1073741824) + "GB";
        }
        return fileSizeString;
    }
    
}
