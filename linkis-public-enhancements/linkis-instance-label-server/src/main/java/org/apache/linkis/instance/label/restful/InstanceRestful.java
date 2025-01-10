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

package org.apache.linkis.instance.label.restful;

import io.swagger.annotations.Api;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@Api(tags = "instance restful")
@RestController
@RequestMapping(path = "/microservice")
public class InstanceRestful {

  private static final Log logger = LogFactory.getLog(InstanceRestful.class);

//  private LabelBuilderFactory labelBuilderFactory =
//      LabelBuilderFactoryContext.getLabelBuilderFactory();
//
//  @Autowired private DefaultInsLabelService insLabelService;
//
//  @Autowired private DiscoveryClient discoveryClient;
//
//  @ApiOperation(
//      value = "listAllInstanceWithLabel",
//      notes = "list all instance with label",
//      response = Message.class)
//  @RequestMapping(path = "/allInstance", method = RequestMethod.GET)
//  public Message listAllInstanceWithLabel(HttpServletRequest req) throws Exception {
//    String userName = ModuleUserUtils.getOperationUser(req, "listAllInstanceWithLabel");
//    if (Configuration.isNotAdmin(userName)) {
//      throw new InstanceErrorException(
//          String.format(
//              ONLY_ADMIN_CAN_VIEW.getErrorDesc() + "The user [%s] is not admin.", userName));
//    }
//
//    logger.info("start to get all instance informations.....");
//    List<InstanceInfo> instances = insLabelService.listAllInstanceWithLabel();
//    insLabelService.markInstanceLabel(instances);
//    List<InstanceInfoVo> instanceVos = EntityParser.parseToInstanceVo(instances);
//    logger.info("Done, all instance:" + instances);
//    return Message.ok().data("instances", instanceVos);
//  }
//
//  @ApiOperation(
//      value = "upDateInstanceLabel",
//      notes = "up date instance label",
//      response = Message.class)
//  @ApiImplicitParams({
//    @ApiImplicitParam(name = "applicationName", dataType = "String"),
//    @ApiImplicitParam(name = "instance", required = false, dataType = "String", value = "instance"),
//    @ApiImplicitParam(name = "labels", required = false, dataType = "List", value = "labels"),
//    @ApiImplicitParam(name = "labelKey", dataType = "String"),
//    @ApiImplicitParam(name = "stringValue", dataType = "String")
//  })
//  @ApiOperationSupport(ignoreParameters = {"jsonNode"})
//  @RequestMapping(path = "/instanceLabel", method = RequestMethod.PUT)
//  public Message upDateInstanceLabel(HttpServletRequest req, @RequestBody JsonNode jsonNode)
//      throws Exception {
//    String userName = ModuleUserUtils.getOperationUser(req, "upDateInstanceLabel");
//    if (Configuration.isNotAdmin(userName)) {
//      throw new InstanceErrorException(
//          String.format(
//              ONLY_ADMIN_CAN_MODIFY.getErrorDesc() + " The user [%s] is not admin", userName));
//    }
//    String instanceName = jsonNode.get("instance").asText();
//    String instanceType = jsonNode.get("applicationName").asText();
//    if (StringUtils.isEmpty(instanceName)) {
//      return Message.error("instance cannot be empty(实例名不能为空");
//    }
//    if (StringUtils.isEmpty(instanceType)) {
//      return Message.error("instance cannot be empty(实例类型不能为空");
//    }
//    JsonNode labelsNode = jsonNode.get("labels");
//    Iterator<JsonNode> labelKeyIterator = labelsNode.iterator();
//    ServiceInstance instance = ServiceInstance.apply(instanceType, instanceName);
//    List<Label<? extends Label<?>>> labels = new ArrayList<>();
//    Set<String> keyList = LabelUtils.listAllUserModifiableLabel();
//    Set<String> labelKeySet = new HashSet<>();
//    // Traverse all labelKeys and take out labelValue.
//    while (labelKeyIterator.hasNext()) {
//      JsonNode label = labelKeyIterator.next();
//      String labelKey = label.get("labelKey").asText();
//      String labelStringValue = label.get("stringValue").asText();
//      if (labelStringValue != null && keyList.contains(labelKey)) {
//        Label realLabel = labelBuilderFactory.createLabel(labelKey, labelStringValue);
//        if (realLabel instanceof UserModifiable) {
//          ((UserModifiable) realLabel).valueCheck(labelStringValue);
//        }
//        labelKeySet.add(labelKey);
//        labels.add(realLabel);
//      }
//    }
//    if (labelKeySet.size() != labels.size()) {
//      throw new InstanceErrorException(INCLUDE_REPEAT.getErrorDesc());
//    }
//    insLabelService.refreshLabelsToInstance(labels, instance);
//    InstanceInfo instanceInfo = insLabelService.getInstanceInfoByServiceInstance(instance);
//    instanceInfo.setUpdateTime(new Date());
//    insLabelService.updateInstance(instanceInfo);
//    return Message.ok("success").data("labels", labels);
//  }
//
//  @ApiOperation(
//      value = "listAllModifiableLabelKey",
//      notes = "list all modifiable label key",
//      response = Message.class)
//  @RequestMapping(path = "/modifiableLabelKey", method = RequestMethod.GET)
//  public Message listAllModifiableLabelKey(HttpServletRequest req) {
//    ModuleUserUtils.getOperationUser(req, "upDateInstanceLabel");
//    Set<String> keyList = LabelUtils.listAllUserModifiableLabel();
//    return Message.ok().data("keyList", keyList);
//  }
//
//  @ApiOperation(value = "getServiceRegistryURL", response = Message.class)
//  @RequestMapping(path = "/serviceRegistryURL", method = RequestMethod.GET)
//  public Message getServiceRegistryURL(HttpServletRequest request) throws Exception {
//    String serviceRegistryURL = insLabelService.getServiceRegistryURL();
//    ModuleUserUtils.getOperationUser(request, "getServiceRegistryURL");
//    return Message.ok().data("url", serviceRegistryURL);
//  }

//  @ApiOperation(value = "getServiceInstances", response = Message.class)
//  @ApiImplicitParams({
//    @ApiImplicitParam(name = "serviceName", required = false, dataType = "String"),
//    @ApiImplicitParam(name = "hostName", required = false, dataType = "String"),
//    @ApiImplicitParam(name = "ip", required = false, dataType = "String"),
//    @ApiImplicitParam(name = "version", required = false, dataType = "String")
//  })
//  @RequestMapping(path = "/serviceInstances", method = RequestMethod.GET)
//  public Message getServiceInstance(
//      HttpServletRequest request,
//      @RequestParam(value = "serviceName", required = false) String serviceName,
//      @RequestParam(value = "hostName", required = false) String hostName,
//      @RequestParam(value = "ip", required = false) String ip,
//      @RequestParam(value = "version", required = false) String version) {
//    Stream<String> serviceStream = discoveryClient.getServices().stream();
//    serviceStream = serviceStream.filter(s -> s.toUpperCase().contains("LINKIS"));
//    if (StringUtils.isNotBlank(serviceName)) {
//      serviceStream =
//          serviceStream.filter(s -> s.toUpperCase().contains(serviceName.toUpperCase()));
//    }
//    Stream<EurekaServiceInstance> instanceList =
//        serviceStream
//            .flatMap(serviceId -> discoveryClient.getInstances(serviceId).stream())
//            .map(instance -> (EurekaServiceInstance) instance);
//    if (StringUtils.isNotBlank(ip)) {
//      instanceList = instanceList.filter(s -> s.getInstanceInfo().getIPAddr().equals(ip));
//    }
//    if (StringUtils.isNotBlank(hostName)) {
//      instanceList = instanceList.filter(s -> s.getInstanceInfo().getHostName().equals(hostName));
//    }
//    if (StringUtils.isNotBlank(version)) {
//      instanceList =
//          instanceList.filter(s -> s.getMetadata().get("linkis.app.version").contains(version));
//    }
//    return Message.ok().data("list", instanceList.collect(Collectors.toList()));
//  }
}
