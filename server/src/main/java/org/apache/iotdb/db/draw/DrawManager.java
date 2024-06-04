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

import org.apache.iotdb.commons.conf.IoTDBConstant;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.draw.utils.DrawUnit;
import org.apache.iotdb.db.mpp.plan.statement.Statement;
import org.apache.iotdb.db.mpp.plan.statement.sys.DrawStatement;
import org.apache.iotdb.rpc.RpcUtils;
import org.apache.iotdb.rpc.TSStatusCode;
import org.apache.iotdb.service.rpc.thrift.TSExecuteStatementResp;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.read.common.block.TsBlock;
import org.apache.iotdb.tsfile.read.common.block.TsBlockBuilder;
import org.apache.iotdb.tsfile.read.common.block.column.Column;
import org.apache.iotdb.tsfile.read.common.block.column.DoubleColumn;
import org.apache.iotdb.tsfile.read.common.block.column.FloatColumn;
import org.apache.iotdb.tsfile.read.common.block.column.IntColumn;
import org.apache.iotdb.tsfile.read.common.block.column.TsBlockSerde;
import org.apache.iotdb.tsfile.utils.Binary;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;

public class DrawManager {

  private static final Logger logger = LoggerFactory.getLogger(DrawManager.class);

  private static IoTDBDescriptor conf = IoTDBDescriptor.getInstance();

  public static final int GRAPH_MAX_WIDTH = conf.getConfig().getDrawWidth();
  public static final int GRAPH_WH_RATE = conf.getConfig().getDrawWHRate();

  public static final int GRAPH_MIN_WIDTH = 40;
  public static final int GRAPH_MIN_HEIGHT = 10;

  public static final String FLOAT_TYPE = "FLOAT";
  public static final String DOUBLE_TYPE = "DOUBLE";
  public static final String INT_TYPE = "INT";
  private static final String STARTTIME_PRE = "Start Time";
  private static final String ENDTIME_PRE = "End Time";

  private static final String COUNT_PRE = "type";

  private static final String TYPE_PRE = "count";

  public static TSExecuteStatementResp isNeedDraw(Statement statement, TSExecuteStatementResp resp)
      throws DrawException {
    if (Boolean.FALSE == statement instanceof DrawStatement) {
      return resp;
    }
    long startTime = System.currentTimeMillis();

    TSExecuteStatementResp newResp = KVToDraw(resp);

    long endTime = System.currentTimeMillis();

    String info =
        "Cost "
            + (endTime - startTime) / 1000
            + "seconds for drawing, query id is "
            + resp.getQueryId();

    logger.info(info);

    return newResp;
  }

  private static TSExecuteStatementResp getNewResp(TSExecuteStatementResp kv, DrawResult drawResult)
      throws IOException {
    TSExecuteStatementResp resp = RpcUtils.getTSExecuteStatementResp(TSStatusCode.SUCCESS_STATUS);
    resp.setQueryId(kv.queryId);
    resp.setStatus(kv.getStatus());
    resp.setColumns(new ArrayList<>(Collections.singleton(IoTDBConstant.COLUMN_Draw)));
    resp.setDataTypeList(new ArrayList<>(Collections.singleton(TSDataType.TEXT.toString())));
    resp.setIgnoreTimeStamp(true);

    TsBlockBuilder builder = new TsBlockBuilder(Collections.singletonList(TSDataType.TEXT));
    drawResult
        .getLines()
        .forEach(
            line -> {
              builder.getTimeColumnBuilder().writeLong(0L);
              builder.getColumnBuilder(0).writeBinary(new Binary(line));
              builder.declarePosition();
            });
    TsBlock tsBlock = builder.build();

    TsBlockSerde serde = new TsBlockSerde();

    Optional optional = Optional.of(serde.serialize(tsBlock));

    List<ByteBuffer> res = new ArrayList<>();
    Optional<ByteBuffer> optionalByteBuffer = optional;
    if (optionalByteBuffer.isPresent()) {
      ByteBuffer byteBuffer2 = optionalByteBuffer.get();
      byteBuffer2.mark();
      int valueColumnCount = byteBuffer2.getInt();
      for (int i = 0; i < valueColumnCount; i++) {
        byteBuffer2.get();
      }
      int positionCount = byteBuffer2.getInt();
      byteBuffer2.reset();
      if (positionCount != 0) {
        res.add(byteBuffer2);
      }
    }

    resp.setQueryResult(res);

    return resp;
  }

  public static TSExecuteStatementResp KVToDraw(TSExecuteStatementResp kv) throws DrawException {

    if (1 != kv.getColumns().size()) {
      String err =
          "The current result has "
              + kv.getColumns().size()
              + " columns, while drawing allows only one column.";
      throw new DrawException(err);
    }

    DrawSource source = getDrawSouceData(kv);

    DrawResult drawResult = getDrawResult(source);

    TSExecuteStatementResp resp;
    try {
      resp = getNewResp(kv, drawResult);
    } catch (IOException e) {
      throw new DrawException(e);
    }

    return resp;
  }

  private static DrawResult getDrawResult(DrawSource source) {

    DrawResult result = new DrawResult();

    result.setDataType(source.getDataType());
    result.setStartTime(DrawUnit.convertTimestampToISO8601(source.getStartTime()));
    result.setEndTime(DrawUnit.convertTimestampToISO8601(source.getEndTime()));
    result.setCount(source.getValueSize());

    float[] data = source.getStars();

    int graph_w = data.length;

    if (graph_w > GRAPH_MAX_WIDTH) {
      graph_w = GRAPH_MAX_WIDTH;
    } else if (graph_w <= GRAPH_MIN_WIDTH) {
      graph_w = GRAPH_MIN_WIDTH;
    }

    int graph_h = graph_w / GRAPH_WH_RATE;

    List<String> lines = DrawUnit.drawTrendLineChart(data, graph_w, graph_h);

    lines.add(STARTTIME_PRE + " : " + result.getStartTime());

    lines.add(ENDTIME_PRE + " : " + result.getEndTime());

    lines.add(TYPE_PRE + " : " + result.getDataType());

    lines.add(COUNT_PRE + " : " + result.getCount());

    result.setLines(lines);

    return result;
  }

  private static DrawSource getDrawSouceData(TSExecuteStatementResp kv) throws DrawException {

    DrawSource drawSource;

    TSDataType dataType = TSDataType.valueOf(kv.getDataTypeList().get(0));

    switch (dataType) {
      case FLOAT:
        drawSource = getDrawSouceDataWithFloat(kv);
        break;
      case DOUBLE:
        drawSource = getDrawSouceDataWithDouble(kv);
        break;
      case INT32:
      case INT64:
        drawSource = getDrawSouceDataWithInt(kv);
        break;
      default:
        String err =
            "The current column type is "
                + kv.getDataTypeList().get(0)
                + ", and we only support drawings of numeric types";
        throw new DrawException(err);
    }

    return drawSource;
  }

  private static DrawSource getDrawSouceDataWithInt(TSExecuteStatementResp kv) {
    DrawSource source = new DrawSource();
    source.setDataType(INT_TYPE);

    List<ByteBuffer> list = kv.getQueryResult();
    int queryResultIndex = 0;

    ByteBuffer byteBuffer = list.get(queryResultIndex);
    TsBlockSerde serde2 = new TsBlockSerde();
    TsBlock curTsBlock = serde2.deserialize(byteBuffer);

    source.setStartTime(curTsBlock.getStartTime());
    source.setEndTime(curTsBlock.getEndTime());

    Column[] c = curTsBlock.getValueColumns();
    IntColumn ic = (IntColumn) c[0];
    float[] fs = DrawUnit.convertToFloatArray(ic.getInts());
    source.setValueSize(fs.length);
    source.setStars(fs);
    return source;
  }

  private static DrawSource getDrawSouceDataWithFloat(TSExecuteStatementResp kv) {
    DrawSource source = new DrawSource();
    source.setDataType(FLOAT_TYPE);

    List<ByteBuffer> list = kv.getQueryResult();
    int queryResultIndex = 0;

    ByteBuffer byteBuffer = list.get(queryResultIndex);
    TsBlockSerde serde2 = new TsBlockSerde();
    TsBlock curTsBlock = serde2.deserialize(byteBuffer);

    source.setStartTime(curTsBlock.getStartTime());
    source.setEndTime(curTsBlock.getEndTime());

    Column[] c = curTsBlock.getValueColumns();
    FloatColumn fc = (FloatColumn) c[0];
    float[] fs = fc.getFloats();
    source.setValueSize(fs.length);
    source.setStars(fs);
    return source;
  }

  private static DrawSource getDrawSouceDataWithDouble(TSExecuteStatementResp kv) {
    DrawSource source = new DrawSource();
    source.setDataType(DOUBLE_TYPE);

    List<ByteBuffer> list = kv.getQueryResult();
    int queryResultIndex = 0;

    ByteBuffer byteBuffer = list.get(queryResultIndex);
    TsBlockSerde serde2 = new TsBlockSerde();
    TsBlock curTsBlock = serde2.deserialize(byteBuffer);

    source.setStartTime(curTsBlock.getStartTime());
    source.setEndTime(curTsBlock.getEndTime());

    Column[] c = curTsBlock.getValueColumns();
    DoubleColumn dc = (DoubleColumn) c[0];
    float[] fs = DrawUnit.convertToFloatArray(dc.getDoubles());
    source.setValueSize(fs.length);
    source.setStars(fs);
    return source;
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
