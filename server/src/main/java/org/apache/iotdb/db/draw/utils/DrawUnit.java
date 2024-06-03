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
package org.apache.iotdb.db.draw.utils;

import java.time.Instant;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;

public class DrawUnit {

  public static float[] convertToFloatArray(int[] intArray) {
    float[] floatArray = new float[intArray.length];
    for (int i = 0; i < intArray.length; i++) {
      floatArray[i] = intArray[i];
    }
    return floatArray;
  }

  public static float[] convertToFloatArray(double[] doubleArray) {
    float[] floatArray = new float[doubleArray.length];
    for (int i = 0; i < doubleArray.length; i++) {
      floatArray[i] = (float) doubleArray[i];
    }
    return floatArray;
  }

  public static String convertTimestampToISO8601(long timestamp) {
    // 创建Instant对象
    Instant instant = Instant.ofEpochMilli(timestamp);

    // 将Instant对象转换为ZonedDateTime对象，设置时区为+08:00
    ZonedDateTime zonedDateTime = instant.atZone(ZoneId.of("+08:00"));

    // 格式化为ISO 8601字符串
    DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss.SSSXXX");
    return zonedDateTime.format(formatter);
  }

  /**
   * @param data
   * @param width
   * @param height
   * @return +--------------------------------------------------+ | Draw data|
   *     +--------------------------------------------------+
   *     |┌────────────────────────────────────────────────┐| |│ ******* ******* │| |│ *** *** │| |│
   *     ** ** │| |│ ** ** │| |│ ** ** │| |│ ** ** │| |│ ** ** │| |│ * * │| |│ ** **│| |│* │|
   *     |└────────────────────────────────────────────────┘|
   *     +--------------------------------------------------+
   */
  public static List<String> drawTrendLineChart(float[] data, int width, int height) {
    List<String> chart = new ArrayList<>();

    // 找到数据中的最大值和最小值
    float max = Float.MIN_VALUE;
    float min = Float.MAX_VALUE;
    for (float value : data) {
      if (value > max) {
        max = value;
      }
      if (value < min) {
        min = value;
      }
    }

    // 创建一个 2D 图表数组
    char[][] plot = new char[height][width + 1];
    for (int i = 0; i < height; i++) {
      for (int j = 0; j < width; j++) {
        plot[i][j] = ' ';
      }
    }

    // 添加边框
    for (int i = 1; i < width; i++) {
      plot[0][i] = '─';
      plot[height - 1][i] = '─';
    }
    for (int i = 1; i < height - 1; i++) {
      plot[i][0] = '│';
      plot[i][width] = '│';
    }
    plot[0][0] = '┌';
    plot[0][width] = '┐';
    plot[height - 1][0] = '└';
    plot[height - 1][width] = '┘';

    // 绘制数据点
    int dataSize = data.length;
    if (dataSize <= 1) {
      int x = 1;
      int y = (int) ((data[0] - min) * (height - 3) / (max - min)) + 1;
      plot[height - y - 1][x] = '*';
    } else {
      for (int i = 0; i < dataSize; i++) {
        int x = i * (width - 2) / (dataSize - 1) + 1;
        int y = (int) ((data[i] - min) * (height - 3) / (max - min)) + 1;
        plot[height - y - 1][x] = '*';
      }
    }
    // 将图表数组转换为 List<String>
    for (int i = 0; i < height; i++) {
      chart.add(new String(plot[i]));
    }

    return chart;
  }
}
