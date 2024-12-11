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

class PriorityLoopArrayQueueTest {

    @Test
    void enqueue() {
        FIFOGroup group = new FIFOGroup("test", 100, 1000);
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
        Assertions.assertEquals(6, event.getTimestamp());
        //缓存测试，需要设置 linkis.fifo.priority.queue.max.cache.size 为 5
        Assertions.assertThrows(IllegalArgumentException.class, () -> {queue.get(7);});

    }

    private void printEvent(SchedulerEvent event) {
        System.out.println(event.getId() + ", " + event.getPriority() + ", " + event.getTimestamp());
    }

    private UserJob getJob(String name, int priority) {
        UserJob job = new UserJob();
        job.setId(name);
        job.setPriority(priority);
        return job;
    }
}