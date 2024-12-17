/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.linkis.scheduler.queue;

import org.apache.linkis.scheduler.queue.fifoqueue.FIFOGroup;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import scala.Option;
import scala.collection.IndexedSeq;

import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;

class PriorityLoopArrayQueueTest {
    private volatile AtomicInteger ac = new AtomicInteger();

    @Test
    public void testConcurrentPutAndTake() throws Exception {
        AtomicInteger counter = new AtomicInteger();
        FIFOGroup group = new FIFOGroup("test", 100, 100);
        PriorityLoopArrayQueue queue = new PriorityLoopArrayQueue(group);

        final long time = System.currentTimeMillis();
        // 获取开始时间的毫秒数
        long startTime = System.currentTimeMillis();
        // 三分钟的毫秒数
        long threeMinutesInMillis = 1 * 60 * 1000;


        int genLen = 2;
        int getLen = 1;
        final CountDownLatch latch = new CountDownLatch(genLen + getLen + 1);
        // 5 个生产者
        for (int i = 0; i < genLen; i++) {
            final int id = i;
            new Thread(() -> {
                try{
                    Thread.sleep(100 * id);
                    latch.countDown();
                    latch.await();
                } catch (InterruptedException e){
                    e.printStackTrace();
                }
                System.out.println(Thread.currentThread().getName() + "开始生产：");
                while ((System.currentTimeMillis() - startTime) < threeMinutesInMillis) {
                    //生产
                    try {
                        Thread.sleep(1000);
                        product(counter, queue);
                        product(counter, queue);
                    } catch (InterruptedException e) {
                        throw new RuntimeException(e);
                    }
                    //消费
                    //consume(queue);

                }
            }, "生产t-" + i).start();
        }
        // 5 个消费者
        for (int i = 0; i < getLen; i++) {
            final int id = i;
            new Thread(() -> {
                try{
                    Thread.sleep(500 * id);
                    latch.countDown();
                    latch.await();
                } catch (InterruptedException e){
                    e.printStackTrace();
                }
                System.out.println(Thread.currentThread().getName() + "开始消费：");
                while (true) {

                    try {
                        Thread.sleep(1000);
                        //消费
                        consume(queue);
                    } catch (InterruptedException e) {
                        throw new RuntimeException(e);
                    }

                }

            }, "消费t-" + i).start();
        }
        new Thread(() -> {
            try {
                Thread.sleep(100);
                latch.countDown();
                latch.await();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            System.out.println(Thread.currentThread().getName() + "开始获取当前队列元素：");
            while ((System.currentTimeMillis() - startTime) < threeMinutesInMillis * 2) {
                try {
                    Thread.sleep(5000);
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
                IndexedSeq<SchedulerEvent> schedulerEventIndexedSeq = queue.toIndexedSeq();
                //Object[] objects = queue.toArray();
                System.out.println("队列当前大小：" + queue.size());
                //Iterator<SchedulerEvent> it = schedulerEventIndexedSeq.iterator();
//                while (it.hasNext()) {
//                    SchedulerEvent event = it.next();
//                    printEvent("get:" , event);
//                }
            }
        }).start();
        Thread.sleep(threeMinutesInMillis * 3);
    }

    //消费
    private void consume(PriorityLoopArrayQueue queue) {
        SchedulerEvent take = null;
        try {
            take = queue.take();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        printEvent("消费" , take);
    }

    //生产
    private void product(AtomicInteger counter, PriorityLoopArrayQueue queue) {
        int i1 = counter.addAndGet(1);
        int priority = getRandom(10);
        String name = "item-" + i1 + "-" + priority;
        System.out.println("生产：" + name);
        Option<Object> offer = queue.offer(getJob(name, priority));
        if (offer.nonEmpty()) {
            System.out.println(offer);
        } else {
            System.out.println("当前队列已满，大小：" + queue.size());
        }

        //Option<SchedulerEvent> schedulerEventOption = queue.get((int) offer.get());
        //printEvent("生产-get：" + offer.get(), schedulerEventOption.get());
    }

    @Test
    void enqueue() {
        // 压测 offer take get
        FIFOGroup group = new FIFOGroup("test", 100, 100);
        PriorityLoopArrayQueue queue = new PriorityLoopArrayQueue(group);
        Option<Object> idx = queue.offer(getJob("job1-1", 1));
        //插入测试
        Assertions.assertEquals(1, (int)idx.get());
        queue.offer(getJob("job2", 2));
        queue.offer(getJob("job3", 3));
        queue.offer(getJob("job1-2", 1));
        queue.offer(getJob("job5", 5));
        queue.offer(getJob("item1-3", 1));
        queue.offer(getJob("item6-1", 6));
        queue.offer(getJob("item4", 4));
        queue.offer(getJob("item6-2", 6));
        //peek 测试
        Option<SchedulerEvent> peek = queue.peek();
        Assertions.assertEquals("item6-1", peek.get().getId());
        while (queue.size() > 1) {
            queue.take();
        }
        SchedulerEvent event = queue.take();
        //优先级，以及先进先出测试
        Assertions.assertEquals("item1-3", event.getId());
        Assertions.assertEquals(1, event.priority());
        Assertions.assertEquals(6, event.getIndex());
        //缓存测试，需要设置 linkis.fifo.priority.queue.max.cache.size 为 5
        Assertions.assertThrows(IllegalArgumentException.class, () -> {queue.get(7);});

    }

    private void printEvent(String opt, SchedulerEvent event) {
        System.out.println("【" + Thread.currentThread().getName() + "】" + opt + ":" + event.getId() + ", priority: " + event.getPriority() + ", index: " + event.getIndex());
    }
    private int getRandom(int bound){
        Random rand = new Random();
        int res = rand.nextInt(bound);
        return res;
    }
    private UserJob getJob(String name, int priority) {
        UserJob job = new UserJob();
        job.setId(name);
        job.setPriority(priority);
        return job;
    }
}