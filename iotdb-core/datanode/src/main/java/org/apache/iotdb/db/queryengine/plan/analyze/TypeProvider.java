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

package org.apache.iotdb.db.queryengine.plan.analyze;

import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.utils.ReadWriteIOUtils;

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

public class TypeProvider {

  private final Map<String, TSDataType> typeMap;

  private TemplatedInfo templatedInfo;

  public TypeProvider() {
    this.typeMap = new HashMap<>();
  }

  public TypeProvider(Map<String, TSDataType> typeMap, TemplatedInfo templatedInfo) {
    this.typeMap = typeMap;
    this.templatedInfo = templatedInfo;
  }

  public TSDataType getType(String symbol) {
    return typeMap.get(symbol);
  }

  public void setType(String symbol, TSDataType dataType) {
    // DataType of NullOperand is null, we needn't put it into TypeProvider
    if (dataType != null) {
      this.typeMap.put(symbol, dataType);
    }
  }

  public Map<String, TSDataType> getTypeMap() {
    return this.typeMap;
  }

  public void setTemplatedInfo(TemplatedInfo templatedInfo) {
    this.templatedInfo = templatedInfo;
  }

  public TemplatedInfo getTemplatedInfo() {
    return this.templatedInfo;
  }

  public void serialize(ByteBuffer byteBuffer) {
    ReadWriteIOUtils.write(typeMap.size(), byteBuffer);
    for (Map.Entry<String, TSDataType> entry : typeMap.entrySet()) {
      ReadWriteIOUtils.write(entry.getKey(), byteBuffer);
      ReadWriteIOUtils.write(entry.getValue().ordinal(), byteBuffer);
    }

    if (templatedInfo == null) {
      ReadWriteIOUtils.write((byte) 0, byteBuffer);
    } else {
      ReadWriteIOUtils.write((byte) 1, byteBuffer);
      templatedInfo.serialize(byteBuffer);
    }
  }

  public void serialize(DataOutputStream stream) throws IOException {
    ReadWriteIOUtils.write(typeMap.size(), stream);
    for (Map.Entry<String, TSDataType> entry : typeMap.entrySet()) {
      ReadWriteIOUtils.write(entry.getKey(), stream);
      ReadWriteIOUtils.write(entry.getValue().ordinal(), stream);
    }

    if (templatedInfo == null) {
      ReadWriteIOUtils.write((byte) 0, stream);
    } else {
      ReadWriteIOUtils.write((byte) 1, stream);
      templatedInfo.serialize(stream);
    }
  }

  public static TypeProvider deserialize(ByteBuffer byteBuffer) {
    int mapSize = ReadWriteIOUtils.readInt(byteBuffer);
    Map<String, TSDataType> typeMap = new HashMap<>();
    while (mapSize > 0) {
      typeMap.put(
          ReadWriteIOUtils.readString(byteBuffer),
          TSDataType.values()[ReadWriteIOUtils.readInt(byteBuffer)]);
      mapSize--;
    }

    TemplatedInfo templatedInfo = null;
    byte hasTemplatedInfo = ReadWriteIOUtils.readByte(byteBuffer);
    if (hasTemplatedInfo == 1) {
      templatedInfo = TemplatedInfo.deserialize(byteBuffer);
    }

    return new TypeProvider(typeMap, templatedInfo);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    TypeProvider that = (TypeProvider) o;
    return Objects.equals(typeMap, that.typeMap);
  }

  @Override
  public int hashCode() {
    return Objects.hash(typeMap);
  }
}
