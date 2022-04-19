/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.drill.exec.store.kafka;

import org.apache.drill.exec.ExecConstants;
import org.apache.drill.exec.server.options.OptionSet;

import java.util.StringJoiner;

/**
 * Holds all system / session options that are used during data read from Kafka.
 *
 * @Deprecated should be changed for Drill2
 * TODO: all plugin specific options (like store.kafka.poll.timeout) recommended to be placed within Format
 * Plugin Configs and populated to {@link org.apache.drill.exec.store.easy.json.loader.JsonLoaderOptions} at runtime
 */
@Deprecated
public class ReadOptions {

  private final String messageReader;
  private final long pollTimeOut;
  private final boolean allTextMode;
  private final boolean readNumbersAsDouble;
  private final boolean enableUnionType;
  private final boolean skipInvalidRecords;
  private final boolean allowNanInf;
  private final boolean allowEscapeAnyChar;

  public ReadOptions(OptionSet options) {
    this.messageReader = options.getString(ExecConstants.KAFKA_RECORD_READER);
    this.pollTimeOut = options.getLong(ExecConstants.KAFKA_POLL_TIMEOUT);
    this.allTextMode = options.getBoolean(ExecConstants.KAFKA_ALL_TEXT_MODE);
    this.readNumbersAsDouble = options.getBoolean(ExecConstants.KAFKA_READER_READ_NUMBERS_AS_DOUBLE);
    this.enableUnionType = options.getBoolean(ExecConstants.ENABLE_UNION_TYPE_KEY);
    this.skipInvalidRecords = options.getBoolean(ExecConstants.KAFKA_READER_SKIP_INVALID_RECORDS);
    this.allowNanInf = options.getBoolean(ExecConstants.KAFKA_READER_NAN_INF_NUMBERS);
    this.allowEscapeAnyChar = options.getBoolean(ExecConstants.KAFKA_READER_ESCAPE_ANY_CHAR);
  }

  public String getMessageReader() {
    return messageReader;
  }

  public long getPollTimeOut() {
    return pollTimeOut;
  }

  public boolean isAllTextMode() {
    return allTextMode;
  }

  public boolean isReadNumbersAsDouble() {
    return readNumbersAsDouble;
  }

  public boolean isEnableUnionType() {
    return enableUnionType;
  }

  public boolean isSkipInvalidRecords() {
    return skipInvalidRecords;
  }

  public boolean isAllowNanInf() {
    return allowNanInf;
  }

  public boolean isAllowEscapeAnyChar() {
    return allowEscapeAnyChar;
  }

  @Override
  public String toString() {
    return new StringJoiner(", ", ReadOptions.class.getSimpleName() + "[", "]")
      .add("messageReader='" + messageReader + "'")
      .add("pollTimeOut=" + pollTimeOut)
      .add("allTextMode=" + allTextMode)
      .add("readNumbersAsDouble=" + readNumbersAsDouble)
      .add("enableUnionType=" + enableUnionType)
      .add("skipInvalidRecords=" + skipInvalidRecords)
      .add("allowNanInf=" + allowNanInf)
      .add("allowEscapeAnyChar=" + allowEscapeAnyChar)
      .toString();
  }
}
