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

import org.apache.linkis.metadata.domain.mdq.po.RangerPolicy;
import org.apache.linkis.metadata.hive.dao.RangerDao;
import org.apache.linkis.metadata.hive.dto.MetadataQueryParam;
import org.apache.linkis.metadata.service.DataSourceService;
import org.apache.linkis.metadata.service.HiveMetaWithPermissionService;
import org.apache.linkis.metadata.service.RangerPermissionService;
import org.apache.linkis.metadata.util.DWSConfig;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.*;
import java.util.stream.Collectors;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Service
public class RangerPermissionServiceImpl implements RangerPermissionService {
  private static final Logger log = LoggerFactory.getLogger(RangerPermissionServiceImpl.class);

  @Autowired private RangerDao rangerDao;

  @Autowired private DataSourceService dataSourceService;

  @Autowired private HiveMetaWithPermissionService hiveMetaWithPermissionService;

  private final ObjectMapper objectMapper = new ObjectMapper();

  @Override
  public List<String> getDbsByUsername(String username) throws Exception {
    List<String> rangerDbs = new ArrayList<>();
    List<String> policyTextList =
        rangerDao.getRangerPolicyText(
            username + "-hive", RangerPolicy.POLICY_TYPE_ACCESS, new ArrayList<>());
    for (String policyTextStr : policyTextList) {
      RangerPolicy rangerPolicy = objectMapper.readValue(policyTextStr, RangerPolicy.class);
      if (rangerPolicy == null
          || rangerPolicy.getResources() == null
          || !rangerPolicy.getResources().containsKey("database")) {
        continue;
      }
      RangerPolicy.RangerPolicyResource databaseResource =
          rangerPolicy.getResources().get("database");
      List<String> values = databaseResource.getValues();
      for (String db : values) {
        if (!"*".equals(db) && !"default".equals(db)) {
          rangerDbs.add(db);
        }
      }
    }
    return rangerDbs;
  }

  @Override
  public List<String> queryRangerTables(MetadataQueryParam queryParam) throws Exception {
    List<String> rangerTables = new ArrayList<>();
    List<String> policyTextList =
        rangerDao.getRangerPolicyText(
            queryParam.getUserName() + "-hive",
            RangerPolicy.POLICY_TYPE_ACCESS,
            Collections.singletonList(queryParam.getDbName()));
    for (String policyTextStr : policyTextList) {
      RangerPolicy rangerPolicy = objectMapper.readValue(policyTextStr, RangerPolicy.class);
      if (rangerPolicy == null
          || rangerPolicy.getResources() == null
          || !rangerPolicy.getResources().containsKey("table")) {
        continue;
      }
      RangerPolicy.RangerPolicyResource tableResource = rangerPolicy.getResources().get("table");
      List<String> values = tableResource.getValues();
      for (String table : values) {
        if ("*".equals(table)) {
          MetadataQueryParam queryAllParam =
              MetadataQueryParam.of(DWSConfig.HIVE_DB_ADMIN_USER.getValue())
                  .withDbName(queryParam.getDbName());
          List<Map<String, Object>> hiveTables = dataSourceService.queryHiveTables(queryAllParam);
          List<String> tableNames =
              hiveTables.stream().map(tb -> (String) tb.get("NAME")).collect(Collectors.toList());
          rangerTables.addAll(tableNames);
          break;
        }
        rangerTables.add(table);
      }
    }
    return rangerTables;
  }

  @Override
  public List<String> queryRangerColumns(MetadataQueryParam queryParam) throws Exception {
    List<String> rangerColumns = new ArrayList<>();
    List<String> policyTextList =
        rangerDao.getRangerPolicyText(
            queryParam.getUserName() + "-hive",
            RangerPolicy.POLICY_TYPE_ROWFILTER,
            Arrays.asList(queryParam.getDbName(), queryParam.getTableName()));
    for (String policyTextStr : policyTextList) {
      RangerPolicy rangerPolicy = objectMapper.readValue(policyTextStr, RangerPolicy.class);
      if (rangerPolicy == null
          || rangerPolicy.getResources() == null
          || !rangerPolicy.getResources().containsKey("column")) {
        continue;
      }
      RangerPolicy.RangerPolicyResource columnResource = rangerPolicy.getResources().get("column");
      List<String> values = columnResource.getValues();
      for (String column : values) {
        if ("*".equals(column)) {
          MetadataQueryParam queryAllParam =
              MetadataQueryParam.of(DWSConfig.HIVE_DB_ADMIN_USER.getValue())
                  .withDbName(queryParam.getDbName())
                  .withTableName(queryParam.getTableName());
          JsonNode allColumns =
              hiveMetaWithPermissionService.getColumnsByDbTableNameAndOptionalUserName(
                  queryAllParam);
          for (int i = 0; i < allColumns.size(); i++) {
            JsonNode node = allColumns.get(i);
            rangerColumns.add(node.get("columnName").asText());
          }
          break;
        }
        rangerColumns.add(column);
      }
    }
    return rangerColumns;
  }
}
