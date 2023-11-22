package com.alibaba.lindorm.contest.extendstructs.directio;






/**
 * @author 陈翔
 */
public interface DirectIO {
    // file path should be specific since the different file path determine whether your system support direct io
    DirectIOLib DIRECTIOLIB = DirectIOLib.getLibForPath("/");
    // you should always write into your disk the Integer-Multiple of block size through direct io.
    // in most system, the block size is 4kb
    int BLOCK_SIZE = 4 * 1024;
}
