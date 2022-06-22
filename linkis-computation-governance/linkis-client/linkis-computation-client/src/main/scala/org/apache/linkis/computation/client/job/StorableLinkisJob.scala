/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 * 
 *   http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
 
package org.apache.linkis.computation.client.job

import org.apache.linkis.computation.client.LinkisJobMetrics
import org.apache.linkis.computation.client.operator.StorableOperator
import org.apache.linkis.governance.common.entity.task.RequestPersistTask
import org.apache.linkis.ujes.client.UJESClient
import org.apache.linkis.ujes.client.request.{JobSubmitAction, OpenLogAction}
import org.apache.linkis.ujes.client.response.{JobInfoResult, JobSubmitResult}


trait StorableLinkisJob extends AbstractLinkisJob {

  private var completedJobInfoResult: JobInfoResult = _

  protected def wrapperId[T](op: => T): T = super.wrapperObj(getId, "taskId must be exists.")(op)

  protected val ujesClient: UJESClient

  protected def getJobSubmitResult: JobSubmitResult

  override protected def wrapperObj[T](obj: Object, errorMsg: String)(op: => T): T = wrapperId {
    super.wrapperObj(obj, errorMsg)(op)
  }

  protected def getJobInfoResult: JobInfoResult = {
    if (completedJobInfoResult != null) return completedJobInfoResult
    val startTime = System.currentTimeMillis
    val jobInfoResult = wrapperId(ujesClient.getJobInfo(getJobSubmitResult))
    getJobMetrics.addClientGetJobInfoTime(System.currentTimeMillis - startTime)
    if(jobInfoResult.isCompleted) {
      getJobMetrics.setClientFinishedTime(System.currentTimeMillis)
      info(s"Job-$getId is completed with status " + completedJobInfoResult.getJobStatus)
      completedJobInfoResult = jobInfoResult
      getJobListeners.foreach(_.onJobFinished(this))
    } else if (jobInfoResult.isRunning) getJobListeners.foreach(_.onJobRunning(this))
    jobInfoResult
  }

  def getJobInfo: RequestPersistTask = getJobInfoResult.getRequestPersistTask

  def getAllLogs: Array[String] = wrapperId {
    val jobInfo = getJobInfo
    val action = OpenLogAction.newBuilder().setLogPath(jobInfo.getLogPath)
      .setProxyUser(jobInfo.getUmUser).build()
    ujesClient.openLog(action).getLog
  }

  override def doKill(): Unit = wrapperId(ujesClient.kill(getJobSubmitResult))

  override def isCompleted: Boolean = getJobInfoResult.isCompleted

  override def isSucceed: Boolean = getJobInfoResult.isSucceed
}

abstract class StorableSubmittableLinkisJob(override protected val ujesClient: UJESClient,
                                            jobSubmitAction: JobSubmitAction)
  extends StorableLinkisJob with AbstractSubmittableLinkisJob {

  private var taskId: String = _
  private var jobSubmitResult: JobSubmitResult = _
  private var maxRetry = 0
  private var retryNumber = 0

  private var finishedJobInfoResult: JobInfoResult = _

  override def getId: String = taskId

  override protected def getJobSubmitResult: JobSubmitResult = jobSubmitResult

  protected override def wrapperId[T](op: => T): T = super.wrapperObj(taskId, "Please submit job first.")(op)

  override protected def doSubmit(): Unit = {
    info("Ready to submit job: " + jobSubmitAction.getRequestPayload)
    jobSubmitResult = ujesClient.submit(jobSubmitAction)
    taskId = jobSubmitResult.taskID
    addOperatorAction {
      case operator: StorableOperator[_] => operator.setJobSubmitResult(jobSubmitResult).setUJESClient(ujesClient)
      case operator => operator
    }
    info("Job submitted with taskId: " + taskId)
  }

  override protected def getJobInfoResult: JobInfoResult = {
    if (finishedJobInfoResult != null) return finishedJobInfoResult
    val startTime = System.currentTimeMillis
    val oldJobSubmitResult = getJobSubmitResult
    val jobInfoResult = wrapperId(ujesClient.getJobInfo(getJobSubmitResult))
    getJobMetrics.addClientGetJobInfoTime(System.currentTimeMillis - startTime)
    if(jobInfoResult.isCompleted) {
      if (jobInfoResult.isFailed && jobInfoResult.canRetry && getRetryNumber < getMaxRetry) {
        logger.info(s"Task ${oldJobSubmitResult.taskID} failed to run, retrying automatically, retry number: ${getRetryNumber}")
        this.submit()
        addRetryNumber
        logger.info(s"The task has been retried, and the new task id is: ${getJobSubmitResult.taskID}")
        return  getJobInfoResult
      }
      getJobMetrics.setClientFinishedTime(System.currentTimeMillis)
      finishedJobInfoResult = jobInfoResult
      info(s"Job-$getId is completed with status " + finishedJobInfoResult.getJobStatus)
      getJobListeners.foreach(_.onJobFinished(this))
    } else if (jobInfoResult.isRunning) getJobListeners.foreach(_.onJobRunning(this))
    jobInfoResult
  }

  def getMaxRetry: Int = maxRetry

  def setMaxRetry(maxRetry: Int): Unit = this.maxRetry = maxRetry

  def addRetryNumber: Unit = retryNumber = retryNumber + 1

  def getRetryNumber: Int = retryNumber

}

abstract class StorableExistingLinkisJob(protected override val ujesClient: UJESClient,
                                         execId: String,
                                         taskId: String, user: String) extends StorableLinkisJob {

  private val jobSubmitResult = new JobSubmitResult
  private val jobMetrics: LinkisJobMetrics = new LinkisJobMetrics(taskId)
  jobSubmitResult.setUser(user)
  jobSubmitResult.setTaskID(taskId)
  jobSubmitResult.setExecID(execId)
  jobMetrics.setClientSubmitTime(System.currentTimeMillis)

  override protected def getJobSubmitResult: JobSubmitResult = jobSubmitResult

  override def getId: String = taskId

  override def getJobMetrics: LinkisJobMetrics = jobMetrics
}