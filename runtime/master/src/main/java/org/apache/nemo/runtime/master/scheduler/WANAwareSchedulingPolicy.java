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

import avro.shaded.com.google.common.collect.Lists;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.beam.repackaged.core.org.apache.commons.lang3.SerializationUtils;
import org.apache.beam.repackaged.core.org.apache.commons.lang3.mutable.MutableObject;
import org.apache.nemo.common.dag.DAG;
import org.apache.nemo.common.ir.vertex.IRVertex;
import org.apache.nemo.common.ir.vertex.OperatorVertex;
import org.apache.nemo.common.ir.vertex.SourceVertex;
import org.apache.nemo.runtime.common.plan.RuntimeEdge;
import org.apache.nemo.runtime.common.plan.StageEdge;
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
public final class WANAwareSchedulingPolicy implements SchedulingPolicy {
  private final ExecutorRegistry executorRegistry;
  private Collection<ExecutorRepresenter> allExecutors;

  @Inject
  private WANAwareSchedulingPolicy(final ExecutorRegistry executorRegistry) {
    this.executorRegistry = executorRegistry;
  }

  @Override
  public ExecutorRepresenter selectExecutor(final Collection<ExecutorRepresenter> executors, final Task task) {
    final DAG<IRVertex, RuntimeEdge<IRVertex>> irDag = SerializationUtils.deserialize(task.getSerializedIRDag());
    final List<IRVertex> reverseTopologicallySorted = Lists.reverse(irDag.getTopologicalSort());
    Collection<String> currentIncomingEdges = new ArrayList<String>();

    // Read WAN environment
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

    // Build costSpace and latencySumSpace
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

    // Check if this task requires reading inputs(from source or parent)
    for (IRVertex irVertex : reverseTopologicallySorted) {
      if (irVertex instanceof SourceVertex) {
        return lessLatencySelectExecutor(latencySumSpace, executors, task);
      }

      if (!(irVertex instanceof OperatorVertex)) {
        return lessLatencySelectExecutor(latencySumSpace, executors, task);
      }

      // save incoming edges' ids
      task.getTaskIncomingEdges()
        .stream()
        .filter(inEdge -> inEdge.getDstIRVertex().getId().equals(irVertex.getId())).forEach(
        incomingEdge -> currentIncomingEdges.add(incomingEdge.getId())
      );
    }

    if (currentIncomingEdges.stream().count() == 0) {
      return lessLatencySelectExecutor(latencySumSpace, executors, task);
    }

    // Read all executors(even the ones that did not meet the constraints)
    executorRegistry.viewExecutors(fromExecutors -> {
      final MutableObject<Set<ExecutorRepresenter>> allExecutorsSet = new MutableObject<>(fromExecutors);
      allExecutors = allExecutorsSet.getValue();
    });

    // Build edge - node map
    Map<String, Collection<String>> edgeNodeSpace = new HashMap<>();

    for (ExecutorRepresenter iterExecutor : allExecutors) {

      for (Task curTask : iterExecutor.getRunningTasks()) {
        for (StageEdge outgoingEdge : curTask.getTaskOutgoingEdges()) {
          if (edgeNodeSpace.get(outgoingEdge.getId()) == null) {
            Collection<String> tempNodeList = new ArrayList<>();
            edgeNodeSpace.put(outgoingEdge.getId(), tempNodeList);
          }
          edgeNodeSpace.get(outgoingEdge.getId()).add(iterExecutor.getNodeName());
        }
      }
    }

    // have close nodes the nodes where the incoming edges started from
    Collection<String> closeNodes = getNodesFromIncomingEdges(edgeNodeSpace, currentIncomingEdges);

    if (closeNodes.stream().count() == 0) {
      return lessLatencySelectExecutor(latencySumSpace, executors, task);
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

      // the only node in close nodes is chosen itself
      if (executorsWithoutClosest.stream().count() == 0) {
        return executors.stream()
          .findFirst()
          .orElseThrow(() -> new RuntimeException("No such executor"));
      }

      // WAN environment information not complete
      if (costSpace.get(chosenCloseNode) == null) {
        return lessLatencySelectExecutor(latencySumSpace, executors, task);
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
   * Get the nodes where incoming edges start from
   * @param edgeNodeSpace        map of outgoing edge - node
   * @param incomingEdges        incoming edges of task to schedule
   */
  private Collection<String> getNodesFromIncomingEdges(final Map<String, Collection<String>> edgeNodeSpace, Collection<String> incomingEdges) {
    Collection<String> nodeList = new ArrayList<>();
    for (String incomingEdge : incomingEdges) {
      if (edgeNodeSpace.get(incomingEdge) != null) {
        for (String nodeName : edgeNodeSpace.get(incomingEdge)) {
          nodeList.add(nodeName);
        }
      }
    }
    return nodeList;
  }

  /**
   * Chooses a set of Executors out of given executors, on which have less latency sum.
   * @param latencySumSpace   map of node - latency sum
   * @param executors         executors to choose from
   * @param task              task to schedule
   */
  private ExecutorRepresenter lessLatencySelectExecutor(final Map<String, Long> latencySumSpace, final Collection<ExecutorRepresenter> executors, final Task task) {
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
      lowLatencyExecutors = executors.stream().filter(i-> lowLatencySumSpace.keySet().contains(i.getNodeName())).collect(Collectors.toList());
    }

    if (lowLatencyExecutors.stream().count() == 0) {
      lowLatencyExecutors = executors;
    }

    return minOccupancySelectExecutor(lowLatencyExecutors, task);
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
