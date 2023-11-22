package com.alibaba.lindorm.contest.extendstructs.concurrency;

import java.util.concurrent.BlockingQueue;

/**
 *
 * @author 陈翔
 */
public class LoadThread implements Runnable{

    public static BlockingQueue<LoadTask> queue;



    @Override
    public void run() {
        try {
            while (true){
                final LoadTask take = queue.take();

            }
        }catch (Exception e){

        }finally {

        }
    }
}
