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

package org.apache.linkis.scheduler.util;

import org.apache.linkis.scheduler.conf.SchedulerConfiguration;

import org.apache.commons.lang3.StringUtils;

public class SchedulerUtils {
  private static final String EVENT_ID_SPLIT = "_";

  public static boolean isSupportPriorityUsers(String groupName) {
    String users = SchedulerConfiguration.SUPPORT_PRIORITY_TASK_USERS();
    if (StringUtils.isEmpty(users)) {
      return false;
    }
    String userName = getUserFromGroupName(groupName);
    if (StringUtils.isEmpty(userName)) {
      return false;
    }
    return users.contains(userName);
  }

  public static String getGroupNameItem(String groupName, int index) {
    if (StringUtils.isEmpty(groupName)) {
      return "";
    }
    String[] groupItems = groupName.split(EVENT_ID_SPLIT);
    if (index < 0 || index >= groupItems.length) {
      return "";
    }
    return groupItems[index];
  }

  public static String getUserFromGroupName(String groupName) {
    return getGroupNameItem(groupName, 2);
  }

  public static String getEngineTypeFromGroupName(String groupName) {
    return getGroupNameItem(groupName, 3);
  }
}
