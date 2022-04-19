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
package org.apache.drill.exec.physical.impl.protocol;

import java.util.ArrayList;
import java.util.List;

import org.apache.drill.exec.record.BatchSchema;
import org.apache.drill.exec.record.VectorContainer;
import org.apache.drill.exec.record.VectorWrapper;
import org.apache.drill.exec.vector.ValueVector;
import org.apache.drill.exec.vector.complex.AbstractMapVector;
import org.apache.drill.exec.vector.complex.VectorWithOrdinal;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Tracks changes to schemas via "snapshots" over time. That is, given
 * a schema, tracks if a new schema is the same as the current one. For
 * example, each batch output from a series of readers might be compared,
 * as they are returned, to detect schema changes from one batch to the
 * next. This class does not track vector-by-vector changes as a schema
 * is built, but rather periodic "snapshots" at times determined by the
 * operator.
 * <p>
 * If an operator is guaranteed to emit a consistent schema, then no
 * checks need be done, and this tracker will report no schema change.
 * On the other hand, a scanner might check schema more often. At least
 * once per reader, and more often if a reader is "late-schema": if the
 * reader can change schema batch-by-batch.
 * <p>
 * Drill defines "schema change" in a very specific way. Not only must
 * the set of columns be the same, and have the same types, it must also
 * be the case that the <b>vectors</b> that hold the columns be identical.
 * Generated code contains references to specific vector objects; passing
 * along different vectors requires new code to be generated and is treated
 * as a schema change.
 * <p>
 * Drill has no concept of "same schema, different vectors." A change in
 * vector is just as serious as a change in schema. Hence, operators
 * try to use the same vectors for their entire lives. That is the change
 * tracked here.
 * <p>
 * Schema versions start at 1. A schema version of 0 means that no
 * output batch was ever presented.
 */

// TODO: Does not handle SV4 situations

public class SchemaTracker {
  private static final Logger logger = LoggerFactory.getLogger(SchemaTracker.class);

  private int schemaVersion;
  private BatchSchema currentSchema;
  private List<ValueVector> currentVectors = new ArrayList<>();

  public void trackSchema(VectorContainer newBatch) {
    if (schemaVersion == 0 || ! isSameSchema(newBatch)) {
      schemaVersion++;
      captureSchema(newBatch);
    }
  }

  private boolean isSameSchema(VectorContainer newBatch) {
    if (currentVectors.size() != newBatch.getNumberOfColumns()) {
      return false;
    }

    // Compare vectors by identity: not just same type,
    // must be same instance.

    for (int i = 0; i < currentVectors.size(); i++) {
      ValueVector currentVector = currentVectors.get(i);
      ValueVector newVector = newBatch.getValueVector(i).getValueVector();
      if (currentVector != newVector) {
        if (currentVector instanceof AbstractMapVector && newVector instanceof AbstractMapVector) {
          if (!sameMapVectors((AbstractMapVector) currentVector, (AbstractMapVector) newVector)) {
            logger.debug("MapVectors are different");
            return false;
          }
          logger.debug("MapVectors are the same");
        } else {
          return false;
        }
      }
    }
    return true;
  }

  private boolean sameMapVectors(AbstractMapVector currentVector, AbstractMapVector newVector) {
    if (currentVector.getValueCapacity() != newVector.getValueCapacity()) {
      return false;
    } else if (currentVector.size() != newVector.size()) {
      return false;
    }
    for (int i = 0; i < currentVector.size(); i++) {
      ValueVector childCurrent = currentVector.getChildByOrdinal(i);
      String fieldName = childCurrent.getField().getName();
      VectorWithOrdinal childNew = newVector.getChildVectorWithOrdinal(fieldName);
      if (childNew == null) {
        return false;
      } else if (childNew.ordinal != i) {
        return false;
      } else if (childCurrent instanceof AbstractMapVector && childNew.vector instanceof AbstractMapVector) {
        if (!sameMapVectors((AbstractMapVector) childCurrent, (AbstractMapVector) childNew.vector)) {
          return false;
        }
      } else if (!childCurrent.equals(childNew.vector)) {
        return false;
      }
    }
    return true;
  }

  private void captureSchema(VectorContainer newBatch) {
    currentVectors.clear();
    for (VectorWrapper<?> vw : newBatch) {
      currentVectors.add(vw.getValueVector());
    }
    currentSchema = newBatch.getSchema();
  }

  public int schemaVersion() { return schemaVersion; }
  public BatchSchema schema() { return currentSchema; }
}
