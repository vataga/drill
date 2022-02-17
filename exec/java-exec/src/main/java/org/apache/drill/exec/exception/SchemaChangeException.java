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
package org.apache.drill.exec.exception;

import org.apache.drill.common.exceptions.DrillException;
import org.apache.drill.exec.record.BatchSchema;

/**
 * Batch schema is changed and can't be handled by current operator
 */
public class SchemaChangeException extends DrillException {

  public SchemaChangeException() {
    super();
  }

  public SchemaChangeException(String message, Throwable cause, boolean enableSuppression, boolean writableStackTrace) {
    super(message, cause, enableSuppression, writableStackTrace);
  }

  public SchemaChangeException(String message, Throwable cause) {
    super(message, cause);
  }

  public SchemaChangeException(String message) {
    super(message);
  }

  public SchemaChangeException(Throwable cause) {
    super(cause);
  }

  public SchemaChangeException(String message, Object...objects){
    super(String.format(message, objects));
  }

  public SchemaChangeException(String message, Throwable cause, Object...objects){
    super(String.format(message, objects), cause);
  }

  public static SchemaChangeException schemaChanged(String message, BatchSchema priorSchema, BatchSchema newSchema) {
    final String errorMsg = message + "\n" +
      "Prior schema : \n" +
      priorSchema.toString() + "\n" +
      "New schema : \n" +
      newSchema.toString();
    return new SchemaChangeException(errorMsg);
  }
}
