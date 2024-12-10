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

package org.apache.linkis.monitor.jobhistory.analyze

import org.apache.linkis.common.utils.Logging
import org.apache.linkis.monitor.constants.Constants
import org.apache.linkis.monitor.core.ob.{Event, Observer}
import org.apache.linkis.monitor.jobhistory.entity.JobHistory
import org.apache.linkis.monitor.jobhistory.exception.AnomalyScannerException
import org.apache.linkis.monitor.utils.alert.ims.{MonitorAlertUtils, PooledImsAlertUtils}

import java.util

import scala.collection.JavaConverters._

/**
 * Scan the execution data within the first 20 minutes, judge the completed tasks,
 *   1. The parm field in jobhistory contains (task.notification.conditions) 2. If the result of
 *      executing the task is any one of (Succeed, Failed, Canceled, Timeout, ALL), an alarm will be
 *      triggered 3.The result of the job is that it has ended The alarm can be triggered if the
 *      above three conditions are met at the same time
 */
class JobHistoryAnalyzeAlertSender() extends Observer with Logging {
  override def update(e: Event, jobHistroyList: scala.Any): Unit = {}

}
