package com.alibaba.rocketmq.example.handler;

/**
 * Created by vick on 15-12-28.
 */
public class MediumQueueHandler extends QueueHandler {


    @Override
    public void handle(Object target) {
        System.out.println("handle:" + this.getClass().getSimpleName() + " params:" + target);
        // 此处handle
    }

    @Override
    public void post(Object target) {
        System.out.println("post:" + this.getClass().getSimpleName() + " params:" + target);
    }
}
