package com.alibaba.rocketmq.example.handler;

/**
 * Created by vick on 15-12-28.
 */
public abstract class QueueHandler {

    QueueHandler nextHandler;


    public abstract void handle(Object target);

    public void invoke(Object target) {
        this.handle(target);
        if (null != nextHandler) {
            nextHandler.invoke(target);
        }
        this.post(target);
    }

    public void setNextHandler(QueueHandler nextHandler) {
        this.nextHandler = nextHandler;
    }

    public abstract void post(Object target);

}
