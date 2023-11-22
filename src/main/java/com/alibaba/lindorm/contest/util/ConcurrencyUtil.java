package com.alibaba.lindorm.contest.util;

import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

/**
 * @author 陈翔
 */
public class ConcurrencyUtil {
    public static void waitFutures(List<Future> futureList) {
        for (Future future : futureList) {
            try {
                future.get();
            } catch (InterruptedException | ExecutionException e) {
                e.printStackTrace();
            }
        }
    }
}
