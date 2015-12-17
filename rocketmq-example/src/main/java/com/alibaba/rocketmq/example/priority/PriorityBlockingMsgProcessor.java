package com.alibaba.rocketmq.example.priority;

import com.alibaba.rocketmq.client.consumer.DefaultMQPullConsumer;
import com.alibaba.rocketmq.client.consumer.PullResult;
import com.alibaba.rocketmq.common.message.MessageExt;
import com.alibaba.rocketmq.common.message.MessageQueue;

import java.util.*;
import java.util.concurrent.*;

/**
 * Created by vick on 15-12-17.
 */
public class PriorityBlockingMsgProcessor<E> {

    private static final long TIMEOUT_GAP = 3; // 时间差，防止由于CPU切换的问题导致低优先级的先返回

    // 优先级
    enum Priority {
        HIGH(3), MEDIUM(2), LOW(1);
        private int val;

        Priority(int val) {
            this.val = val;
        }

        public int getValue() {
            return this.val;
        }
    }

    public static void main(String[] args) throws ExecutionException, InterruptedException {
        PriorityBlockingMsgProcessor priorityBlockingMsgProcessor = new PriorityBlockingMsgProcessor<Object>();
        priorityBlockingMsgProcessor.take(3, 10000);
    }

    public E take() throws ExecutionException, InterruptedException {
        List<E> oneList = take(1, 1000);
        return oneList.isEmpty() ? null : oneList.get(0);
    }

    public List<E> take(int size, long timeout) throws InterruptedException, ExecutionException {
        long start = System.currentTimeMillis();
        long fetchBlockTimeout = timeout - TIMEOUT_GAP;
        ExecutorService es = Executors.newFixedThreadPool(3);
        CountDownLatch countDownLatch = new CountDownLatch(1);
        List<Future<List<E>>> futureList = new ArrayList<Future<List<E>>>(3);
        futureList.add(es.submit(new MQTask<E>(Priority.HIGH.getValue(), countDownLatch, fetchBlockTimeout, size)));
        futureList.add(es.submit(new MQTask<E>(Priority.MEDIUM.getValue(), countDownLatch, fetchBlockTimeout, size)));
        futureList.add(es.submit(new MQTask<E>(Priority.LOW.getValue(), countDownLatch, fetchBlockTimeout, size)));
        es.shutdown();
        countDownLatch.await(fetchBlockTimeout, TimeUnit.MILLISECONDS); // 有其中一个返回，或者多个都有返回
        Thread.sleep(TIMEOUT_GAP); //  时间差，防止由于CPU切换的问题导致低优先级的先返回
        System.out.println(System.currentTimeMillis() - start);
        return handleFutureMsg(futureList, size);
    }

    private List<E> handleFutureMsg(List<Future<List<E>>> futureList, int size) throws ExecutionException, InterruptedException {
        List<E> list = new ArrayList<E>(0);
        for (Future<List<E>> future : futureList) {
            if (future.isDone()) {
                List<E> subList = future.get();
                System.out.println(subList);
                list.addAll(subList);
                // 按优先级处理返回值
                // 更新offset
                // 如果够size大小，则不在添加，设置其他的为无效
            } else {
                // cancel 未完成的任务
                System.out.println(future + "canceled");
                future.cancel(true);
            }
        }
        return list;
    }

    class MQTask<E> implements Callable<List<E>> {

        private int score;
        private CountDownLatch countDownLatch;
        private long fetchBlockTimeout;
        private int size;

        public MQTask(int score, CountDownLatch countDownLatch, long fetchBlockTimeout, int size) {
            this.score = score;
            this.countDownLatch = countDownLatch;
            this.fetchBlockTimeout = fetchBlockTimeout;
            this.size = size;
        }

        @Override
        public List<E> call() throws Exception {

            try {
                DefaultMQPullConsumer consumer = new DefaultMQPullConsumer("pullConsumer");
                consumer.setNamesrvAddr("10.31.90.114:9876");
                System.out.println(consumer.toString());
                consumer.setConsumerPullTimeoutMillis(3000);
                consumer.start();
                Set<MessageQueue> mqs = consumer.fetchSubscribeMessageQueues("TopicTest-2");
                for (MessageQueue mq : mqs) {
                    System.out.println("Consume from the queue: " + mq);
                    SINGLE_MQ:
                    while (true) {
                        try {
                            PullResult pullResult = consumer.pullBlockIfNotFound(mq, null, getMessageQueueOffset(mq), size);
                            System.out.println(pullResult);
//                            putMessageQueueOffset(mq, pullResult.getNextBeginOffset());
                            switch (pullResult.getPullStatus()) {
                                case FOUND:
                                    List<MessageExt> list = pullResult.getMsgFoundList();
                                    for (MessageExt messageExt : list) {
                                        System.out.println(Thread.currentThread().getName() + " ==> " + new String(messageExt.getBody()));
                                    }
                                    System.out.println();
                                    break;
                                case NO_MATCHED_MSG:
                                    break;
                                case NO_NEW_MSG:
                                    break SINGLE_MQ;
                                case OFFSET_ILLEGAL:
                                    break;
                                default:
                                    break;
                            }
                        } catch (Exception e) {
                            e.printStackTrace();
                        }
                    }
                }

                consumer.shutdown();


                Random r = new Random();
                Thread.sleep(r.nextInt(5) * 1000);
                // fetch msgs and set score of them
                // put all msgs into PriorityBlockingQueue, wait for returning.
                return new ArrayList<E>();
            } finally {
                countDownLatch.countDown();
                System.out.println(score + " finish");
            }
        }
    }

    private static final Map<MessageQueue, Long> offseTable = new ConcurrentHashMap<MessageQueue, Long>();


    private static void putMessageQueueOffset(MessageQueue mq, long offset) {
        offseTable.put(mq, offset);
    }


    private static long getMessageQueueOffset(MessageQueue mq) {
        Long offset = offseTable.get(mq);
        if (offset != null)
            return offset;

        return 0;
    }
}
