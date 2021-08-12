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
package org.apache.drill.common.concurrent;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

/**
 * An extended CountDownLatch which allows us to await uninterruptibly.
 */
public class ExtendedLatch extends CountDownLatch {
  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(ExtendedLatch.class);

  public ExtendedLatch() {
    super(1);
    System.out.println("ExtendedLatch()");
  }

  public ExtendedLatch(final int count) {
    super(count);
    System.out.println("ExtendedLatch(1)");
  }

  /**
   * Await without interruption for a given time.
   * @param waitMillis
   *          Time in milliseconds to wait
   * @return Whether the countdown reached zero or not.
   */
  public boolean awaitUninterruptibly(long waitMillis) {
    System.out.println("enter awaitUninterruptibly(long waitMillis:" + waitMillis + ")");
    System.out.println("Thread.currentThread().getName(): " + Thread.currentThread().getName());
    final long targetMillis = System.currentTimeMillis() + waitMillis;
    System.out.println("targetMillis: " + targetMillis);
    while (System.currentTimeMillis() < targetMillis) {
      final long wait = targetMillis - System.currentTimeMillis();
      System.out.println("wait:" + wait);
      if (wait < 1) {
        System.out.println("wait < 1");
        return false;
      }

      try {
        System.out.println("exit awaitUninterruptibly(long waitMillis)");
        return await(wait, TimeUnit.MILLISECONDS);
      } catch (final InterruptedException e) {
        // if we weren't ready, the while loop will continue to wait
        logger.warn("Interrupted while waiting for event latch.", e);
        System.out.println("warn awaitUninterruptibly(long waitMillis)");
      }
    }
    System.out.println("System.currentTimeMillis() > targetMillis. " + System.currentTimeMillis() + " > " + targetMillis);
    return false;
  }

  /**
   * Await without interruption. In the case of interruption, log a warning and continue to wait.
   */
  public void awaitUninterruptibly() {
    System.out.println("enter awaitUninterruptibly");
    System.out.println("Thread.currentThread().getName()" + Thread.currentThread().getName());
    while (true) {
      try {
        await();
        System.out.println("exit awaitUninterruptibly");
        return;
      } catch (final InterruptedException e) {
        // if we're still not ready, the while loop will cause us to wait again
        logger.warn("Interrupted while waiting for event latch.", e);
        System.out.println("warn awaitUninterruptibly");
      }
    }
  }
}
