/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.nemo.runtime.master.scheduler;

import org.apache.beam.repackaged.core.org.apache.commons.lang3.mutable.MutableObject;
import org.apache.nemo.runtime.common.plan.Task;
import org.apache.nemo.runtime.master.resource.ExecutorRepresenter;
import org.apache.reef.annotations.audience.DriverSide;

import javax.annotation.concurrent.ThreadSafe;
import javax.inject.Inject;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Collection;
import java.util.OptionalInt;
import java.util.Set;

/**
 * This policy chooses a set of Executors, considering WAN environment.
 */
@ThreadSafe
@DriverSide
public final class WANAwareSchedulingPolicy implements SchedulingPolicy {
  private final ExecutorRegistry executorRegistry;
  private Collection<ExecutorRepresenter> allExecutors;

  @Inject
  private WANAwareSchedulingPolicy(final ExecutorRegistry executorRegistry) {
    this.executorRegistry = executorRegistry;
  }

  @Override
  public ExecutorRepresenter selectExecutor(final Collection<ExecutorRepresenter> executors, final Task task) {

    byte[] jsonData = null;

    try {
      jsonData = Files.readAllBytes(Paths.get("examples/resources/inputs/tree.txt"));
    } catch (IOException e) {
      e.printStackTrace();
    }

    String tree = new String(jsonData, StandardCharsets.UTF_8);

    System.out.println(tree);

    executorRegistry.viewExecutors(fromExecutors -> {
      final MutableObject<Set<ExecutorRepresenter>> allExecutorsSet = new MutableObject<>(fromExecutors);
      allExecutors = allExecutorsSet.getValue();
    });

    final OptionalInt minOccupancy =
      executors.stream()
        .map(ExecutorRepresenter::getNumOfRunningTasks)
        .mapToInt(i -> i).min();

    if (!minOccupancy.isPresent()) {
      throw new RuntimeException("Cannot find min occupancy");
    }

    return executors.stream()
      .filter(executor -> executor.getNumOfRunningTasks() == minOccupancy.getAsInt())
      .findFirst()
      .orElseThrow(() -> new RuntimeException("No such executor"));
  }
}
