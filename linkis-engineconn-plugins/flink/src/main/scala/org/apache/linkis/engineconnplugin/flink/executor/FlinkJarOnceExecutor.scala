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

package org.apache.linkis.engineconnplugin.flink.executor

import org.apache.linkis.common.utils.Utils
import org.apache.linkis.engineconn.executor.service.ManagerService
import org.apache.linkis.engineconn.once.executor.OnceExecutorExecutionContext
import org.apache.linkis.engineconnplugin.flink.client.deployment.YarnApplicationClusterDescriptorAdapter
import org.apache.linkis.engineconnplugin.flink.config.FlinkEnvConfiguration
import org.apache.linkis.engineconnplugin.flink.config.FlinkEnvConfiguration._
import org.apache.linkis.engineconnplugin.flink.context.FlinkEngineConnContext
import org.apache.linkis.governance.common.conf.GovernanceCommonConf
import org.apache.linkis.governance.common.constant.ec.ECConstants

import org.apache.commons.lang3.StringUtils

import java.util.concurrent.{Future, TimeUnit}

import scala.concurrent.duration.Duration

class FlinkJarOnceExecutor(
    override val id: Long,
    override protected val flinkEngineConnContext: FlinkEngineConnContext
) extends FlinkOnceExecutor[YarnApplicationClusterDescriptorAdapter] {

  private var daemonThread: Future[_] = _

  override def doSubmit(
      onceExecutorExecutionContext: OnceExecutorExecutionContext,
      options: Map[String, String]
  ): Unit = {
    val args = FLINK_APPLICATION_ARGS.getValue(options)
    val programArguments =
      if (StringUtils.isNotEmpty(args)) args.split(" ") else Array.empty[String]
    val mainClass = FLINK_APPLICATION_MAIN_CLASS.getValue(options)
    logger.info(s"Ready to submit flink application, mainClass: $mainClass, args: $args.")
    clusterDescriptor.deployCluster(programArguments, mainClass)
  }

  override protected def waitToRunning(): Unit = {
    Utils.waitUntil(() => clusterDescriptor.initJobId(), Duration.Inf)
    setJobID(clusterDescriptor.getJobId.toHexString)
    val extraParams = flinkEngineConnContext.getEnvironmentContext.getExtraParams()
    val clientType = extraParams
      .getOrDefault(
        GovernanceCommonConf.FLINK_CLIENT_TYPE.key,
        GovernanceCommonConf.FLINK_CLIENT_TYPE.getValue
      )
      .toString
    super.waitToRunning()
    logger.info(s"clientType : ${clientType}")
    clientType match {
      case ECConstants.EC_FLINK_CLIENT_TYPE_DETACH =>
        waitToExit()
      case _ =>
    }
  }

  private def waitToExit(): Unit = {
    // upload applicationId to manager and then exit
    val thisExecutor = this
    ManagerService
    if (!isCompleted) {
      daemonThread = Utils.defaultScheduler.scheduleWithFixedDelay(
        new Runnable {
          override def run(): Unit = {
            if (!isCompleted) {
              Utils.waitUntil(() => StringUtils.isNotBlank(getApplicationId), Duration.apply("10s"))
              if (StringUtils.isNotBlank(getApplicationId)) {
                Utils.tryAndWarn {
                  import org.springframework.web.context.ContextLoader
                  import org.springframework.web.context.WebApplicationContext
                  import org.apache.linkis.engineconn.acessible.executor.service.ExecutorHeartbeatService
                  val wac = ContextLoader.getCurrentWebApplicationContext
                  val heartbeatService = wac.getBean(classOf[ExecutorHeartbeatService])
                  val heartbeatMsg = heartbeatService.generateHeartBeatMsg(thisExecutor)
                  ManagerService.getManagerService.heartbeatReport(heartbeatMsg)
                  logger.info(
                    s"Succeed to report heatbeatMsg : ${heartbeatMsg.getHeartBeatMsg}, will exit."
                  )
                  trySucceed()
                }
              }
            }
          }
        },
        1000,
        FlinkEnvConfiguration.FLINK_ONCE_JAR_APP_REPORT_APPLICATIONID_INTERVAL.getValue.toLong,
        TimeUnit.MILLISECONDS
      )
    }
  }

}
