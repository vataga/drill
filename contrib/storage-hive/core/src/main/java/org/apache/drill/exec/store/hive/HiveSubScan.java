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
package org.apache.drill.exec.store.hive;

import java.io.IOException;
import java.lang.reflect.Constructor;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import com.fasterxml.jackson.annotation.JacksonInject;
import org.apache.commons.codec.binary.Base64;
import org.apache.drill.common.exceptions.ExecutionSetupException;
import org.apache.drill.common.expression.SchemaPath;
import org.apache.drill.exec.physical.base.AbstractBase;
import org.apache.drill.exec.physical.base.PhysicalOperator;
import org.apache.drill.exec.physical.base.PhysicalVisitor;
import org.apache.drill.exec.physical.base.SubScan;
import org.apache.drill.exec.proto.UserBitShared.CoreOperatorType;
import org.apache.drill.exec.store.StoragePluginRegistry;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.mapred.InputSplit;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import com.google.common.collect.Iterators;
import com.google.common.io.ByteArrayDataInput;
import com.google.common.io.ByteStreams;

@JsonTypeName("hive-sub-scan")
public class HiveSubScan extends AbstractBase implements SubScan {
  protected HiveReadEntry hiveReadEntry;

  @JsonIgnore
  protected List<List<InputSplit>> inputSplits = new ArrayList<>();
  @JsonIgnore
  protected HiveTableWithColumnCache table;
  @JsonIgnore
  protected List<HivePartition> partitions;
  @JsonIgnore
  protected HiveStoragePlugin storagePlugin;

  private List<List<String>> splits;
  private List<String> splitClasses;
  protected List<SchemaPath> columns;

  @JsonCreator
  public HiveSubScan(@JacksonInject StoragePluginRegistry registry,
                     @JsonProperty("userName") String userName,
                     @JsonProperty("splits") List<List<String>> splits,
                     @JsonProperty("hiveReadEntry") HiveReadEntry hiveReadEntry,
                     @JsonProperty("splitClasses") List<String> splitClasses,
                     @JsonProperty("columns") List<SchemaPath> columns,
                     @JsonProperty("storagePluginName") String pluginName)
      throws IOException, ExecutionSetupException, ReflectiveOperationException {
    this(userName, splits, hiveReadEntry, splitClasses, columns, (HiveStoragePlugin)registry.getPlugin(pluginName));
  }

  public HiveSubScan(final String userName, final List<List<String>> splits, final HiveReadEntry hiveReadEntry,
      final List<String> splitClasses, final List<SchemaPath> columns, final HiveStoragePlugin plugin)
    throws IOException, ReflectiveOperationException {
    super(userName);
    this.hiveReadEntry = hiveReadEntry;
    this.table = hiveReadEntry.getTable();
    this.partitions = hiveReadEntry.getPartitions();
    this.splits = splits;
    this.splitClasses = splitClasses;
    this.columns = columns;
    this.storagePlugin = plugin;

    for (int i = 0; i < splits.size(); i++) {
      inputSplits.add(deserializeInputSplit(splits.get(i), splitClasses.get(i)));
    }
    logger.error("Input splits: ");
    for (List<InputSplit> inputSplit : inputSplits) {
      logger.error(inputSplit + " here");
    }

  }

  @JsonProperty("storagePluginName")
  @SuppressWarnings("unused")
  public String getStoragePluginName() {
    return storagePlugin.getName();
  }

  @JsonIgnore
  public HiveStoragePlugin getStoragePlugin() {
    return storagePlugin;
  }

  public List<List<String>> getSplits() {
    return splits;
  }

  public HiveTableWithColumnCache getTable() {
    return table;
  }

  public List<HivePartition> getPartitions() {
    return partitions;
  }

  public List<String> getSplitClasses() {
    return splitClasses;
  }

  public List<SchemaPath> getColumns() {
    return columns;
  }

  public List<List<InputSplit>> getInputSplits() {
    return inputSplits;
  }

  public HiveReadEntry getHiveReadEntry() {
    return hiveReadEntry;
  }

  public static List<InputSplit> deserializeInputSplit(List<String> base64, String className) throws IOException, ReflectiveOperationException{
    Constructor<?> constructor = Class.forName(className).getDeclaredConstructor();
    if (constructor == null) {
      throw new ReflectiveOperationException("Class " + className + " does not implement a default constructor.");
    }

    constructor.setAccessible(true);
    List<InputSplit> splits = new ArrayList<>();
    for (String str : base64) {
      InputSplit split = (InputSplit) constructor.newInstance();
      ByteArrayDataInput byteArrayDataInput = ByteStreams.newDataInput(Base64.decodeBase64(str));
      split.readFields(byteArrayDataInput);
      splits.add(split);
      logger.error(split + " added. HiveSubScan")
    }
    return splits;
  }

  @Override
  public <T, X, E extends Throwable> T accept(PhysicalVisitor<T, X, E> physicalVisitor, X value) throws E {
    return physicalVisitor.visitSubScan(this, value);
  }

  @Override
  public PhysicalOperator getNewWithChildren(List<PhysicalOperator> children) throws ExecutionSetupException {
    try {
      return new HiveSubScan(getUserName(), splits, hiveReadEntry, splitClasses, columns, storagePlugin);
    } catch (IOException | ReflectiveOperationException e) {
      throw new ExecutionSetupException(e);
    }
  }

  @Override
  public Iterator<PhysicalOperator> iterator() {
    return Iterators.emptyIterator();
  }

  @Override
  public int getOperatorType() {
    return CoreOperatorType.HIVE_SUB_SCAN_VALUE;
  }

  @JsonIgnore
  public HiveConf getHiveConf() {
    return storagePlugin.getHiveConf();
  }
}
