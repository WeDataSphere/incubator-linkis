/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.linkis.engineconn.computation.executor.hook

/**
 * 定义一个用于Spark引擎的Python模块加载与执行挂钩的类
 */
class PythonSparkEngineHook extends PythonModuleLoadEngineConnHook {
  
  // 设置engineType属性为"spark"，表示此挂钩适用于Spark数据处理引擎
  override val engineType: String = "spark"
  
  // 设置runType属性为RunType.PYSPARK，表示此挂钩将执行PySpark类型的代码
  override val runType: RunType = RunType.PYSPARK

  // 重写constructCode方法，用于根据Python模块信息构造加载模块的代码
  override protected def constructCode(pythonModuleInfo: PythonModuleInfoVo): String = {
    // 使用pythonModuleInfo的path属性，构造SparkContext.addPyFile的命令字符串
    // 这个命令在PySpark环境中将模块文件添加到所有worker上，以便在代码中可以使用
    s"sc.addPyFile(\"${pythonModuleInfo.path}\")"
  }

  // 重写getRealRunType方法，根据引擎类型动态确定实际的运行类型
  override protected def getRealRunType(engineType: String): RunType = {
    // 如果引擎类型为"spark"，则返回RunType.PYSPARK，以确保代码在正确的上下文中运行
    if (engineType.equals("spark")) {
      return RunType.PYSPARK
    }
    // 如果引擎类型不是"spark"，则返回默认的runType，这里默认为RunType.PYSPARK
    runType
  }
}

