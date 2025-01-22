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

package org.apache.linkis.metadata.service.impl;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.linkis.metadata.domain.mdq.po.RangerPolicy;
import org.apache.linkis.metadata.hive.dao.RangerDao;
import org.apache.linkis.metadata.service.RangerPermissionService;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Service
public class RangerPermissionServiceImpl implements RangerPermissionService {
  private static final Logger log = LoggerFactory.getLogger(RangerPermissionServiceImpl.class);

  @Autowired private RangerDao rangerDao;

  private final ObjectMapper objectMapper = new ObjectMapper();

  @Override
  public List<String> getDbsByUsername(String username) throws Exception {
    List<String> rangerDbs = new ArrayList<>();
    List<String> policyTextList =
            rangerDao.getRangerPolicyText(username + "-hive", "0", new ArrayList<>());
    for (String policyTextStr : policyTextList) {
      RangerPolicy rangerPolicy = objectMapper.readValue(policyTextStr, RangerPolicy.class);
      if (rangerPolicy == null || rangerPolicy.getResources() == null || !rangerPolicy.getResources().containsKey("database")) {
        continue;
      }
      RangerPolicy.RangerPolicyResource databaseResource = rangerPolicy.getResources().get("database");
      List<String> values = databaseResource.getValues();
      for (String db : values) {
        if (!"*".equals(db) && !"default".equals(db)) {
          rangerDbs.add(db);
        }
      }
    }
    return rangerDbs;
  }
}