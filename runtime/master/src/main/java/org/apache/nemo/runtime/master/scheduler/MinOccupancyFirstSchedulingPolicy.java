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

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.beam.repackaged.core.org.apache.commons.lang3.mutable.MutableObject;
import org.apache.nemo.runtime.common.plan.Task;
import org.apache.nemo.runtime.master.resource.ExecutorRepresenter;
import org.apache.nemo.runtime.master.resource.Link;
import org.apache.reef.annotations.audience.DriverSide;

import javax.annotation.concurrent.ThreadSafe;
import javax.inject.Inject;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.*;
import java.util.stream.Collectors;

/**
 * This policy chooses a set of Executors, considering WAN environment.
 */
@ThreadSafe
@DriverSide
public final class MinOccupancyFirstSchedulingPolicy implements SchedulingPolicy {
  private final ExecutorRegistry executorRegistry;
  private Collection<ExecutorRepresenter> allExecutors;

  @Inject
  private MinOccupancyFirstSchedulingPolicy(final ExecutorRegistry executorRegistry) {
    this.executorRegistry = executorRegistry;
  }

  @Override
  public ExecutorRepresenter selectExecutor(final Collection<ExecutorRepresenter> executors, final Task task) {
    System.out.println("++++++++++++++++++++++++++++++++++++++++++++");
    System.out.println("++++++++++++++++++++++++++++++++++++++++++++");
    System.out.println("++++++++++++++++++++++++++++++++++++++++++++");
    byte[] jsonData = null;

    try {
      jsonData = Files.readAllBytes(Paths.get("src/main/java/org/apache/nemo/runtime/master/resource/input/tree.txt"));
    } catch (IOException e) {
      e.printStackTrace();
    }

    String tree = new String(jsonData, StandardCharsets.UTF_8);

    Map<String, Link> links = new HashMap<>();
    try {
      Map<String, Map<String, Object>> map = (Map) new ObjectMapper().readValue(tree, Map.class);
      for (String key : map.keySet()) {
        if (key != "slaves" && key != "timestamp") {
          Map<String, Object> submap = map.get(key);
          Link tempLink = new Link(
            Integer.parseInt(((String) submap.get("bw")).split(" ")[0]),
            Integer.parseInt(((String) submap.get("latency")).split(" ")[0])
          );
          links.put(key, tempLink);
        }
      }
    } catch (IOException e) {
      e.printStackTrace();
    }

    Map<String, Map<String, Integer>> costSpace = new HashMap<>();
    Map<String, Long> latencySumSpace = new HashMap<>();

    for (String key : links.keySet()) {
      if (costSpace.get(key.split("/")[0]) == null) {
        costSpace.put(key.split("/")[0], new HashMap<>());
        latencySumSpace.put(key.split("/")[0], 0L);
      }
      if (costSpace.get(key.split("/")[1]) == null) {
        costSpace.put(key.split("/")[1], new HashMap<>());
        latencySumSpace.put(key.split("/")[1], 0L);
      }
      costSpace.get(key.split("/")[0]).put(key.split("/")[1], links.get(key).getLatency());
      latencySumSpace.put(key.split("/")[0], latencySumSpace.get(key.split("/")[0]) + links.get(key).getLatency());
      costSpace.get(key.split("/")[1]).put(key.split("/")[0], links.get(key).getLatency());
      latencySumSpace.put(key.split("/")[1], latencySumSpace.get(key.split("/")[1]) + links.get(key).getLatency());
    }

    executorRegistry.viewExecutors(fromExecutors -> {
      final MutableObject<Set<ExecutorRepresenter>> allExecutorsSet = new MutableObject<>(fromExecutors);
      allExecutors = allExecutorsSet.getValue();
    });

    Map<String, Map<String, Map<String, Map<String, String>>>> nodeSpace = new HashMap<>();

    Map<String, String> nodeExecutorMap = new HashMap<>();

    for (ExecutorRepresenter curExecutor : allExecutors) {
      nodeExecutorMap.put(curExecutor.getNodeName(), curExecutor.getExecutorId());
      for (Task curTask : curExecutor.getRunningTasks()) {
        String planId = curTask.getPlanId();
        String taskId = curTask.getTaskId();
        String stageId = taskId.split("Stage")[1].split("-")[0];
        String taskIndex = taskId.split("Stage")[1].split("-")[1];
        String attemptNum = taskId.split("Stage")[1].split("-")[2];
        if (nodeSpace.get(planId) == null) {
          nodeSpace.put(planId, new HashMap<>());
        }
        if (nodeSpace.get(planId).get(stageId) == null) {
          nodeSpace.get(planId).put(stageId, new HashMap<>());
        }
        if (nodeSpace.get(planId).get(stageId).get(taskIndex) == null) {
          nodeSpace.get(planId).get(stageId).put(taskIndex, new HashMap<>());
        }
        if (nodeSpace.get(planId).get(stageId).get(taskIndex).get(attemptNum) == null) {
          nodeSpace.get(planId).get(stageId).get(taskIndex).put(attemptNum, curExecutor.getNodeName());
        }
      }
    }

    Collection<String> closeNodes = getCloseNodes(nodeSpace, task);

    if (closeNodes.stream().count() == 0) {
      OptionalDouble averageLatency = latencySumSpace.entrySet().stream().mapToDouble(i -> i.getValue()).average();
      Collection<ExecutorRepresenter> lowLatencyExecutors;

      if (!averageLatency.isPresent()) {
        lowLatencyExecutors = executors;
      } else {
        Map<String, Long> lowLatencySumSpace = new HashMap<>();
        for (String node : latencySumSpace.keySet()) {
          if (latencySumSpace.get(node) <= averageLatency.getAsDouble()) {
            lowLatencySumSpace.put(node, latencySumSpace.get(node));
          }
        }
        Collection<String> executorIds = new ArrayList<String>();
        for (String node : lowLatencySumSpace.keySet()) {
          executorIds.add(nodeExecutorMap.get(node));
        }
        lowLatencyExecutors = executors.stream().filter(i-> executorIds.contains(i.getExecutorId())).collect(Collectors.toList());
      }

      if (lowLatencyExecutors.stream().count() == 0) {
        lowLatencyExecutors = executors;
      }

      return minOccupancySelectExecutor(lowLatencyExecutors, task);

    } else {
      String chosenCloseNode = closeNodes.stream().skip((int) (closeNodes.size() * Math.random())).findFirst().orElseThrow(() -> new RuntimeException("No such node"));
      Collection<ExecutorRepresenter> executorsWithoutClosest;

      Boolean closestNodeInExecutors = Boolean.FALSE;
      Integer occupancyOfClosestNode = Integer.MAX_VALUE;
      for (ExecutorRepresenter executor : executors) {
        if (executor.getNodeName() == chosenCloseNode) {
          closestNodeInExecutors = Boolean.TRUE;
          occupancyOfClosestNode = executor.getNumOfRunningTasks();
          break;
        }
      }
      if (closestNodeInExecutors) {
        executorsWithoutClosest = executors.stream().filter(i-> i.getNodeName() != chosenCloseNode).collect(Collectors.toList());
      } else {
        executorsWithoutClosest = executors;
      }

      if (executorsWithoutClosest.stream().count() == 0) {
        return executors.stream()
          .findFirst()
          .orElseThrow(() -> new RuntimeException("No such executor"));
      }

      final OptionalInt minLatency =
        executorsWithoutClosest.stream()
          .map(ExecutorRepresenter::getNodeName)
          .mapToInt(i -> costSpace
            .get(chosenCloseNode)
            .get(i))
          .min();

      if (!minLatency.isPresent()) {
        throw new RuntimeException("Cannot find min latency");
      }

      if (closestNodeInExecutors) {
        ExecutorRepresenter resultCandidateExecutor = executorsWithoutClosest.stream()
          .filter(executor -> costSpace.get(chosenCloseNode).get(executor.getNodeName()) == minLatency.getAsInt())
          .findFirst()
          .orElseThrow(() -> new RuntimeException("No such executor"));
        if (resultCandidateExecutor.getNumOfRunningTasks() > occupancyOfClosestNode) {
          return executors.stream()
            .filter(executor -> executor.getNodeName() == chosenCloseNode)
            .findFirst()
            .orElseThrow(() -> new RuntimeException("No such executor"));
        }
      }

      return executorsWithoutClosest.stream()
        .filter(executor -> costSpace.get(chosenCloseNode).get(executor.getNodeName()) == minLatency.getAsInt())
        .findFirst()
        .orElseThrow(() -> new RuntimeException("No such executor"));
    }
  }
  /**
   * Get the nodes where closest tasks to the current target task are running in.
   * Information about a task can be stratificated into "planId - taskId(stageId - taskIndex - attemptNum)"
   * Reading this hierarchy from the top, as two tasks' information has more stages in common, the two tasks can be seen as close.
   *
   * @param nodeSpace       the mapping of which tasks and its running executor's nodeName.
   * @param task            the task to schedule.
   */
  private Collection<String> getCloseNodes(final Map<String, Map<String, Map<String, Map<String, String>>>> nodeSpace, final Task task) {
    String planId = task.getPlanId();
    String taskId = task.getTaskId();
    String stageId = taskId.split("Stage")[1].split("-")[0];
    String taskIndex = taskId.split("Stage")[1].split("-")[1];
    String attemptNum = taskId.split("Stage")[1].split("-")[2];

    Collection<String> closeNodes = new ArrayList<String>();

    if (nodeSpace.get(planId) != null) {
      if (nodeSpace.get(planId).get(stageId) != null) {
        if (nodeSpace.get(planId).get(stageId).get(taskIndex) != null) {
          if (nodeSpace.get(planId).get(stageId).get(taskIndex).get(attemptNum) != null) {
            closeNodes.add(nodeSpace.get(planId).get(stageId).get(taskIndex).get(attemptNum));
          } else {
            for (String attemptNumRoot : nodeSpace.get(planId).get(stageId).get(taskIndex).keySet()) {
              closeNodes.add(nodeSpace.get(planId).get(stageId).get(taskIndex).get(attemptNumRoot));
            }
          }
        } else {
          for (String taskIndexRoot : nodeSpace.get(planId).get(stageId).keySet()) {
            for (String attemptNumRoot : nodeSpace.get(planId).get(stageId).get(taskIndexRoot).keySet()) {
              closeNodes.add(nodeSpace.get(planId).get(stageId).get(taskIndexRoot).get(attemptNumRoot));
            }
          }
        }
      } else {
        for (String stageRoot : nodeSpace.get(planId).keySet()) {
          for (String taskIndexRoot : nodeSpace.get(planId).get(stageRoot).keySet()) {
            for (String attemptNumRoot : nodeSpace.get(planId).get(stageRoot).get(taskIndexRoot).keySet()) {
              closeNodes.add(nodeSpace.get(planId).get(stageRoot).get(taskIndexRoot).get(attemptNumRoot));
            }
          }
        }
      }
    }
    return closeNodes;
  }

  /**
   * Chooses a set of Executors out of given executors, on which have minimum running Tasks.
   * @param executors   executors to choose from
   * @param task        task to schedule
   */
  private ExecutorRepresenter minOccupancySelectExecutor(final Collection<ExecutorRepresenter> executors, final Task task) {
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
