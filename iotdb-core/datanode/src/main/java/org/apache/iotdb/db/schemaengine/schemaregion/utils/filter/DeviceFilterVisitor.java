/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.iotdb.db.schemaengine.schemaregion.utils.filter;

import org.apache.iotdb.commons.schema.filter.SchemaFilter;
import org.apache.iotdb.commons.schema.filter.SchemaFilterVisitor;
import org.apache.iotdb.commons.schema.filter.impl.DeviceAttributeFilter;
import org.apache.iotdb.commons.schema.filter.impl.DeviceIdFilter;
import org.apache.iotdb.commons.schema.filter.impl.PathContainsFilter;
import org.apache.iotdb.commons.schema.filter.impl.TemplateFilter;
import org.apache.iotdb.db.schemaengine.schemaregion.read.resp.info.IDeviceSchemaInfo;
import org.apache.iotdb.db.schemaengine.template.ClusterTemplateManager;

public class DeviceFilterVisitor extends SchemaFilterVisitor<IDeviceSchemaInfo> {
  @Override
  public boolean visitNode(SchemaFilter filter, IDeviceSchemaInfo info) {
    return true;
  }

  @Override
  public boolean visitPathContainsFilter(
      PathContainsFilter pathContainsFilter, IDeviceSchemaInfo info) {
    if (pathContainsFilter.getContainString() == null) {
      return true;
    }
    return info.getFullPath().toLowerCase().contains(pathContainsFilter.getContainString());
  }

  @Override
  public boolean visitTemplateFilter(TemplateFilter templateFilter, IDeviceSchemaInfo info) {
    boolean equalAns;
    int templateId = info.getTemplateId();
    String filterTemplateName = templateFilter.getTemplateName();
    if (templateId != -1) {
      equalAns =
          ClusterTemplateManager.getInstance()
              .getTemplate(templateId)
              .getName()
              .equals(filterTemplateName);
      return templateFilter.isEqual() == equalAns;
    } else if (filterTemplateName == null) {
      return templateFilter.isEqual();
    } else {
      return false;
    }
  }

  @Override
  public boolean visitDeviceIdFilter(DeviceIdFilter filter, IDeviceSchemaInfo info) {
    String[] nodes = info.getPartialPath().getNodes();
    if (nodes.length < filter.getIndex() + 3) {
      return false;
    } else {
      return nodes[filter.getIndex() + 3].equals(filter.getValue());
    }
  }

  @Override
  public boolean visitDeviceAttributeFilter(DeviceAttributeFilter filter, IDeviceSchemaInfo info) {
    return filter.getValue().equals(info.getAttributeValue(filter.getKey()));
  }
}
