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
package org.apache.iotdb.db.draw;

import org.apache.iotdb.db.draw.utils.DrawUnit;
import org.apache.iotdb.db.utils.EnvironmentUtils;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class DrawUtilsTest {

  @Before
  public void setUp() throws Exception {
    EnvironmentUtils.envSetUp();
  }

  @After
  public void tearDown() throws Exception {
    EnvironmentUtils.cleanEnv();
  }

  @Test
  public void convertTimestampToISO8601Test() {
    long timel = 1000000;

    String times = DrawUnit.convertTimestampToISO8601(timel);

    // Assert.assertTrue("1970-01-01T08:16:40.000+08:00" == times);
  }

  private static float[] generateSampleData(int size) {
    // 生成示例数据
    float[] data = new float[size];
    for (int i = 0; i < size; i++) {
      data[i] = (float) (Math.sin(i * Math.PI / 50) * 100 + 200);
    }
    return data;
  }
}
