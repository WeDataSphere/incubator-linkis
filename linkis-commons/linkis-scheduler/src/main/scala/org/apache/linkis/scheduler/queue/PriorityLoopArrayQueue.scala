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

package org.apache.linkis.scheduler.queue

import org.apache.linkis.common.utils.Logging
import org.apache.linkis.scheduler.conf.SchedulerConfiguration

import java.util

/**
 * 优先级队列元素
 * @param element 实际元素
 * @param priority 优先级
 * @param timestamp 唯一索引
 */
case class PriorityQueueElement(element: Any, priority: Int, timestamp: Int) extends Comparable[PriorityQueueElement] {
  override def compareTo(queueItem: PriorityQueueElement): Int = if (priority != queueItem.priority) priority - queueItem.priority
  else queueItem.timestamp - timestamp
}

/**
 * 固定大小集合，元素满后会移除最先插入集合的元素
 * @param maxSize 集合大小
 * @tparam K
 * @tparam V
 */
class FixedSizeCollection[K, V](val maxSize: Int) extends util.LinkedHashMap[K, V] {
  // 当集合大小超过最大值时，返回true，自动删除最老的元素
  protected override def removeEldestEntry(eldest: util.Map.Entry[K, V]): Boolean = size > maxSize
}

/**
 * 优先级队列，优先级相同时先进先出
 * @param group
 */
class PriorityLoopArrayQueue(var group: Group) extends ConsumeQueue with Logging {

  private val maxCapacity = group.getMaximumCapacity
  /** 优先级队列 */
  private val priorityEventQueue = new util.ArrayList[PriorityQueueElement](group.getMaximumCapacity)
  /** 累加器 1.越先进队列值越小，优先级相同时控制先进先出 2.队列元素唯一索引，不会重复 */
  private var timestamp = 0
  /** 记录队列中当前所有元素索引，元素存入优先级队列时添加，从优先级队列移除时删除 */
  private val indexSet = new util.HashSet[Int]
  /** 记录已经消费的元素，会有固定缓存大小，默认1000，元素从优先级队列移除时添加 */
  val fixedSizeCollection = new FixedSizeCollection[Integer, Any](SchedulerConfiguration.MAX_PRIORITY_QUEUE_CACHE_SIZE)

  private val writeLock = new Array[Byte](0)
  private val readLock = new Array[Byte](0)

  protected[this] var realSize = size
  override def isEmpty: Boolean = size == 0
  override def isFull: Boolean = size == maxCapacity
  def size: Int = priorityEventQueue.size

  /**
   * 将元素添加进队列
   * @param element
   * @return
   */
  private def addToPriorityQueue(element: PriorityQueueElement): Boolean = {
    val lastIndex: Int = size
    priorityEventQueue.add(lastIndex, element)
    indexSet.add(element.timestamp)
    // 节点上浮
    siftUp(lastIndex)
    // 新增成功
    true
  }

  /**
   * 从队列中获取并移除元素
   * @return
   */
  private def getAndRemoveTop: PriorityQueueElement = {
    // 根节点为最大值
    val top: PriorityQueueElement = priorityEventQueue.get(0)
    val lastIndex: Int = size - 1
    val lastNode: PriorityQueueElement = priorityEventQueue.get(lastIndex)
    priorityEventQueue.remove(lastIndex)
    if (size > 0) {
      priorityEventQueue.set(0, lastNode)
      // 节点下沉
      siftDown(0)
    }
    indexSet.remove(top.timestamp)
    fixedSizeCollection.put(top.timestamp, top.element)
    // 返回最大值
    top
  }

  override def remove(event: SchedulerEvent): Unit = {
    get(event).foreach(x => x.cancel())
  }

  override def getWaitingEvents: Array[SchedulerEvent] = {
    priorityEventQueue synchronized {
      toIndexedSeq
        .filter(x =>
          x.getState.equals(SchedulerEventState.Inited) || x.getState
            .equals(SchedulerEventState.Scheduled)
        )
        .toArray
    }
  }

  override def clearAll(): Unit = priorityEventQueue synchronized {
    realSize = 0
    priorityEventQueue.clear()
  }

  override def get(event: SchedulerEvent): Option[SchedulerEvent] = {
    priorityEventQueue synchronized {
      val eventSeq = toIndexedSeq.filter(x => x.getId.equals(event.getId)).seq
      if (eventSeq.size > 0) Some(eventSeq(0)) else None
    }
  }

  /**
   * 根据索引获取队列元素
   * @param index
   * @return
   */
  override def get(index: Int): Option[SchedulerEvent] = {
    var event: SchedulerEvent = null
    priorityEventQueue synchronized {
      if (!indexSet.contains(index) && !fixedSizeCollection.containsKey(index)) {
        throw new IllegalArgumentException(
          "The index " + index + " has already been deleted, now index must be better than " + timestamp
        )
      }
      event = fixedSizeCollection.get(index).asInstanceOf[SchedulerEvent]
      if (event == null) {
        val eventSeq = toIndexedSeq.filter(x => x.getTimestamp.equals(index)).seq
        if (eventSeq.size > 0) event = eventSeq(0)
      }
    }
    Option(event)
  }

  override def getGroup: Group = group

  override def setGroup(group: Group): Unit = {
    this.group = group
  }

  def toIndexedSeq: IndexedSeq[SchedulerEvent] = if (size == 0) {
    IndexedSeq.empty[SchedulerEvent]
  } else priorityEventQueue synchronized { (0 until  size).map(x => priorityEventQueue.get(x).element.asInstanceOf[SchedulerEvent]).filter(x => x != None) }

  def add(event: SchedulerEvent): Int = {
    priorityEventQueue synchronized {
      // 每次添加的时候需要给计数器+1，优先级相同时，控制先进先出
      event.setTimestamp(nextTimestamp())
      addToPriorityQueue(PriorityQueueElement(event, event.getPriority, event.getTimestamp))
    }
    event.getTimestamp
  }

  override def waitingSize: Int = size

  /**
   * Add one, if the queue is full, it will block until the queue is
   * available（添加一个，如果队列满了，将会一直阻塞，直到队列可用）
   *
   * @return
   *   Return index subscript（返回index下标）
   */
  override def put(event: SchedulerEvent): Int = {
    var index: Int = -1
    writeLock synchronized {
      while (isFull) writeLock.wait(1000)
      index = add(event)
    }
    readLock synchronized { readLock.notify() }
    index
  }

  /**
   * Add one, return None if the queue is full（添加一个，如果队列满了，返回None）
   *
   * @return
   */
  override def offer(event: SchedulerEvent): Option[Int] = {
    var index: Int = -1
    writeLock synchronized {
      if (isFull) return None
      else {
        index = add(event)
      }
    }
    readLock synchronized { readLock.notify() }
    Some(index)
  }

  /**
   * Get the latest SchedulerEvent of a group, if it does not exist, it will block
   * [<br>（获取某个group最新的SchedulerEvent，如果不存在，就一直阻塞<br>） This method will move the pointer（该方法会移动指针）
   *
   * @return
   */
  override def take(): SchedulerEvent = {
    val t: Option[SchedulerEvent] = readLock synchronized {
      while (waitingSize == 0) {
        readLock.wait(1000)
      }
      Option(getAndRemoveTop.element.asInstanceOf[SchedulerEvent])
    }
    writeLock synchronized { writeLock.notify() }
    t.get
  }

  /**
   * Get the latest SchedulerEvent of a group, if it does not exist, block the maximum waiting
   * time<br>（获取某个group最新的SchedulerEvent，如果不存在，就阻塞到最大等待时间<br>） This method will move the
   * pointer（该方法会移动指针）
   * @param mills
   *   Maximum waiting time（最大等待时间）
   * @return
   */
  override def take(mills: Long): Option[SchedulerEvent] = {
    val t: Option[SchedulerEvent] = readLock synchronized {
      if (waitingSize == 0) readLock.wait(mills)
      if (waitingSize == 0) return None
      Option(getAndRemoveTop.element.asInstanceOf[SchedulerEvent])
    }
    writeLock synchronized { writeLock.notify() }
    t
  }

  /**
   * Get the latest SchedulerEvent of a group and move the pointer to the next one. If not, return
   * directly to None 获取某个group最新的SchedulerEvent，并移动指针到下一个。如果没有，直接返回None
   *
   * @return
   */
  override def poll(): Option[SchedulerEvent] = {
    val event: Option[SchedulerEvent] = readLock synchronized {
      val t: Option[SchedulerEvent] = Option(getAndRemoveTop.element.asInstanceOf[SchedulerEvent])
      if (t == null) {
        logger.info("null, notice...")
      }
      t
    }
    writeLock synchronized { writeLock.notify() }
    event
  }

  /**
   * Only get the latest SchedulerEvent of a group, and do not move the pointer. If not, return
   * directly to None 只获取某个group最新的SchedulerEvent，并不移动指针。如果没有，直接返回None
   *
   * @return
   */
  override def peek(): Option[SchedulerEvent] = readLock synchronized {
    if (waitingSize == 0 ) None
    else Option(priorityEventQueue.get(0).element.asInstanceOf[SchedulerEvent])
  }

  /**
   * Get the latest SchedulerEvent whose group satisfies the condition and does not move the
   * pointer. If not, return directly to None 获取某个group满足条件的最新的SchedulerEvent，并不移动指针。如果没有，直接返回None
   * @param op
   *   满足的条件
   * @return
   */
  override def peek(op: (SchedulerEvent) => Boolean): Option[SchedulerEvent] = {
    if (waitingSize == 0) None
    else {
      val event: Option[SchedulerEvent] = Option(priorityEventQueue.get(0).element.asInstanceOf[SchedulerEvent])
      if (op(event.get)) event else None
    }
  }

  /**
   * 获取下一个计数器索引
   * @return
   */
  private def nextTimestamp() = {
    timestamp synchronized {
      timestamp += 1
      timestamp
    }
  }

  // 节点上浮
  private def siftUp(index: Int): Unit = {
    var flag = true
    var currentIndex = index
    while ( {
      currentIndex != 0 && flag
    }) { // 当前节点
      val currentValue: PriorityQueueElement = priorityEventQueue.get(currentIndex)
      // 父索引
      val parentIndex: Int = getParentIndex(currentIndex)
      // 父节点
      val parentValue: PriorityQueueElement = priorityEventQueue.get(parentIndex)
      // 当前节点小于父节点则退出循环
      if (currentValue.compareTo(parentValue) < 0) {
        flag = false
      } else {
        // 当前节点大于等于父节点则交换位置
        priorityEventQueue.set(parentIndex, currentValue)
        priorityEventQueue.set(currentIndex, parentValue)
        currentIndex = parentIndex
      }

    }
  }

  // 节点下沉
  private def siftDown(index: Int): Unit = { // 当前节点没有左子节点则无需下沉
    var currentIndex = index
    var flag = true
    while ( {
      getLeftChildIndex(currentIndex) < size && flag
    }) {
      val currentValue: PriorityQueueElement = priorityEventQueue.get(currentIndex)
      // 左子索引
      val leftChildIndex: Int = getLeftChildIndex(currentIndex)
      // 左子节点
      val leftChildValue: PriorityQueueElement = priorityEventQueue.get(leftChildIndex)
      // 右子索引
      var rightChildIndex: Integer = null
      // 右子节点
      var rightChildValue: PriorityQueueElement = null
      // 右子节点是否存在
      if (getRightChildIndex(currentIndex) < size) {
        rightChildIndex = getRightChildIndex(currentIndex)
        rightChildValue = priorityEventQueue.get(rightChildIndex)
      }
      // 右子节点存在
      if (null != rightChildIndex) { // 找出左右子节点较大节点
        var biggerIndex: Int = rightChildIndex
        if (leftChildValue.compareTo(rightChildValue) > 0) biggerIndex = leftChildIndex
        // 较大节点小于当前节点则退出循环
        val biggerValue: PriorityQueueElement = priorityEventQueue.get(biggerIndex)
        if (biggerValue.compareTo(currentValue) < 0) {
          flag = false
        } else {
          // 较大节点大于等于当前节点则交换位置
          priorityEventQueue.set(currentIndex, biggerValue)
          priorityEventQueue.set(biggerIndex, currentValue)
          currentIndex = biggerIndex
        }
      }
      else { // 右子节点不存在
        // 左子节点小于当前节点则退出循环
        if (leftChildValue.compareTo(currentValue) < 0) {
          flag = false
        } else {
          // 左子节点大于等于当前节点则交换位置
          priorityEventQueue.set(currentIndex, leftChildValue)
          priorityEventQueue.set(leftChildIndex, currentValue)
          currentIndex = leftChildIndex
        }
      }
    }
  }

  // 获取左子节点索引
  def getLeftChildIndex(currentIndex: Int): Int = currentIndex * 2 + 1

  // 获取右子节点索引
  def getRightChildIndex(currentIndex: Int): Int = currentIndex * 2 + 2

  // 获取父节点索引
  def getParentIndex(currentIndex: Int): Int = {
    if (currentIndex == 0) throw new RuntimeException("root node has no parent")
    (currentIndex - 1) / 2
  }
}
