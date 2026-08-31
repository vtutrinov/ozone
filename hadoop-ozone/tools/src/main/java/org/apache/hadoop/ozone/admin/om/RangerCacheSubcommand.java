/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package org.apache.hadoop.ozone.admin.om;

import java.io.IOException;
import java.net.InetAddress;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;

import org.apache.hadoop.hdds.cli.HddsVersionProvider;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.conf.TimeDurationUtil;
import org.apache.hadoop.ozone.OmUtils;
import org.apache.hadoop.ozone.om.OMConfigKeys;
import org.apache.hadoop.ozone.om.helpers.OMNodeDetails;
import org.apache.hadoop.ozone.om.protocolPB.OMAdminProtocolClientSideImpl;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerAdminProtocolProtos.RangerCacheControlResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerAdminProtocolProtos.RangerCacheOpType;
import org.apache.hadoop.security.UserGroupInformation;
import picocli.CommandLine;

import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_OM_SERVICE_IDS_KEY;

/**
 * Handler of ozone admin om rangercache command.
 *
 * Controls the Ranger authorizer policy-cache on OzoneManager nodes. The
 * cache state is local to each OM, so by default the command is applied to
 * every OM of the service; use -host to target a single node.
 */
@CommandLine.Command(
    name = "rangercache",
    customSynopsis = "ozone admin om rangercache [-id=<om-service-id>] " +
        "[-host=<om-host>] (--status | --invalidate [-p=<policy>|-r=<role>]" +
        " | --extend --ttl=<duration>) [--json]",
    description = "Control the Ranger authorizer policy-cache on " +
        "OzoneManager nodes (status, invalidation, validity extension). " +
        "The cache state is node-local: without -host the operation is " +
        "applied to ALL OzoneManagers of the service. " +
        "This operation requires Ozone administrator privilege.",
    mixinStandardHelpOptions = true,
    versionProvider = HddsVersionProvider.class)
public class RangerCacheSubcommand implements Callable<Void> {

  @CommandLine.ParentCommand
  private OMAdmin parent;

  @CommandLine.Option(names = {"-id", "--service-id"},
      description = "OM Service ID. May be omitted when exactly one service" +
          " id is configured.")
  private String omServiceId;

  @CommandLine.Option(names = {"-host", "--node-host"},
      description = "Host name/address of a single OM to target. Without " +
          "this option the operation is applied to all OMs of the service.")
  private String omHost;

  @CommandLine.ArgGroup(multiplicity = "1")
  private Operation operation;

  /**
   * Mutually exclusive top-level operations.
   */
  static class Operation {
    @CommandLine.Option(names = {"--status"},
        description = "Show the cache state of each OM.")
    private boolean status;

    @CommandLine.Option(names = {"-i", "--invalidate"},
        description = "Invalidate the whole policy cache, or a single " +
            "entry when --policy/--role is given.")
    private boolean invalidate;

    @CommandLine.Option(names = {"--extend"},
        description = "Extend the cache validity deadline by --ttl, " +
            "evaluated on each OM node clock.")
    private boolean extend;
  }

  @CommandLine.Option(names = {"-p", "--policy"},
      description = "Name of a single cached policy to invalidate " +
          "(only with --invalidate).")
  private String policyName;

  @CommandLine.Option(names = {"-r", "--role"},
      description = "Name of a single cached role to invalidate " +
          "(only with --invalidate).")
  private String roleName;

  @CommandLine.Option(names = {"--ttl"},
      description = "Cache validity extension duration (e.g. 30s, 10m, 2h;" +
          " plain numbers are milliseconds). Required with --extend.")
  private String ttlString;

  @CommandLine.Option(names = {"--json"},
      defaultValue = "false",
      description = "Format status output as json.")
  private boolean json;

  private OzoneConfiguration ozoneConf;
  private UserGroupInformation user;

  @Override
  public Void call() throws Exception {
    RangerCacheOpType op = validateAndResolveOp();
    long ttlMillis = operation.extend
        ? TimeDurationUtil.getTimeDurationHelper(
            "--ttl", ttlString, TimeUnit.MILLISECONDS)
        : 0;
    if (operation.extend && ttlMillis <= 0) {
      throw new IOException("--ttl must be a positive duration");
    }
    String entryName = policyName != null ? policyName : roleName;

    ozoneConf = new OzoneConfiguration(parent.getParent().getOzoneConf());
    // One unreachable OM should not stall the whole fan-out for minutes.
    ozoneConf.setInt(
        OMConfigKeys.OZONE_OM_ADMIN_PROTOCOL_MAX_RETRIES_KEY, 2);
    ozoneConf.setLong(
        OMConfigKeys.OZONE_OM_ADMIN_PROTOCOL_WAIT_BETWEEN_RETRIES_KEY, 1000);
    user = parent.getParent().getUser();

    List<OMNodeDetails> targetNodes = resolveTargetNodes();

    boolean anyFailure = false;
    List<String[]> rows = new ArrayList<>();
    StringBuilder jsonOut = new StringBuilder("{");
    for (OMNodeDetails node : targetNodes) {
      try (OMAdminProtocolClientSideImpl omAdminClient =
               OMAdminProtocolClientSideImpl.createProxyForSingleOM(
                   ozoneConf, user, node)) {
        RangerCacheControlResponse response =
            omAdminClient.rangerCacheControl(op, entryName, ttlMillis);
        anyFailure |= !renderNodeResult(node, response, rows, jsonOut);
      } catch (IOException e) {
        anyFailure = true;
        String msg = e.getMessage() == null
            ? e.getClass().getSimpleName() : firstLine(e.getMessage());
        if (msg.contains("Unknown method rangerCacheControl")) {
          msg = "rangercache is not supported by this OM version";
        }
        rows.add(new String[] {node.getNodeId(), node.getHostAddress(),
            "UNREACHABLE", msg});
      }
    }

    if (json && operation.status) {
      System.out.println(jsonOut.append("\n}"));
    } else {
      printTable(rows);
    }

    if (anyFailure) {
      throw new IOException(
          "rangercache operation failed on one or more OM nodes");
    }
    return null;
  }

  private RangerCacheOpType validateAndResolveOp() throws IOException {
    if ((policyName != null || roleName != null) && !operation.invalidate) {
      throw new IOException(
          "--policy/--role can only be used with --invalidate");
    }
    if (policyName != null && roleName != null) {
      throw new IOException("--policy and --role are mutually exclusive");
    }
    if (operation.extend && (ttlString == null || ttlString.isEmpty())) {
      throw new IOException("--extend requires --ttl <duration>");
    }
    if (!operation.extend && ttlString != null) {
      throw new IOException("--ttl can only be used with --extend");
    }

    if (operation.status) {
      return RangerCacheOpType.RANGER_CACHE_STATUS;
    } else if (operation.extend) {
      return RangerCacheOpType.RANGER_CACHE_EXTEND;
    } else if (policyName != null) {
      return RangerCacheOpType.RANGER_CACHE_INVALIDATE_POLICY;
    } else if (roleName != null) {
      return RangerCacheOpType.RANGER_CACHE_INVALIDATE_ROLE;
    } else {
      return RangerCacheOpType.RANGER_CACHE_INVALIDATE;
    }
  }

  private List<OMNodeDetails> resolveTargetNodes() throws IOException {
    if (omServiceId == null || omServiceId.isEmpty()) {
      Collection<String> serviceIds =
          ozoneConf.getTrimmedStringCollection(OZONE_OM_SERVICE_IDS_KEY);
      if (serviceIds.size() != 1) {
        throw new IOException("Please specify -id: there are either zero " +
            "or more than one om service ids configured: " + serviceIds);
      }
      omServiceId = serviceIds.iterator().next();
    }

    List<OMNodeDetails> allNodes = OmUtils.getAllOMHAAddresses(
        ozoneConf, omServiceId, false);
    if (allNodes.isEmpty()) {
      throw new IOException("No OM nodes found for service id " +
          omServiceId + " in the configuration.");
    }

    if (omHost == null || omHost.isEmpty()) {
      return allNodes;
    }

    InetAddress target = InetAddress.getByName(omHost);
    List<OMNodeDetails> matched = new ArrayList<>();
    for (OMNodeDetails node : allNodes) {
      InetAddress nodeAddress =
          InetAddress.getByName(node.getHostAddress());
      if (target.equals(nodeAddress)) {
        matched.add(node);
      }
    }
    if (matched.isEmpty()) {
      throw new IOException("Host " + omHost + " does not match any OM of " +
          "service " + omServiceId);
    }
    return matched;
  }

  /**
   * @return true when the node reported success.
   */
  private boolean renderNodeResult(OMNodeDetails node,
      RangerCacheControlResponse response, List<String[]> rows,
      StringBuilder jsonOut) {
    String nodeId = node.getNodeId();
    String host = node.getHostAddress();

    if (!response.getSuccess()) {
      rows.add(new String[] {nodeId, host, "FAILED",
          response.hasErrorMsg() ? response.getErrorMsg() : ""});
      return false;
    }

    if (operation.status) {
      String statusJson =
          response.hasStatusJson() ? response.getStatusJson() : "{}";
      if (jsonOut.length() > 1) {
        jsonOut.append(',');
      }
      jsonOut.append("\n  \"").append(nodeId).append("\": ")
          .append(statusJson);
      rows.add(new String[] {nodeId, host, "OK", statusJson});
      return true;
    }

    if (response.hasEntryFound() && !response.getEntryFound()) {
      rows.add(new String[] {nodeId, host, "NOT_FOUND",
          (policyName != null ? "policy " : "role ") +
              (policyName != null ? policyName : roleName) +
              " is not present in the cache"});
      return false;
    }

    rows.add(new String[] {nodeId, host, "OK", ""});
    return true;
  }

  private void printTable(List<String[]> rows) {
    int[] width = new int[] {4, 4, 6, 6};
    String[] header = {"NODE", "HOST", "RESULT", "DETAIL"};
    for (String[] row : rows) {
      for (int i = 0; i < row.length; i++) {
        width[i] = Math.max(width[i], row[i].length());
      }
    }
    printRow(header, width);
    for (String[] row : rows) {
      printRow(row, width);
    }
  }

  private void printRow(String[] row, int[] width) {
    StringBuilder sb = new StringBuilder();
    for (int i = 0; i < row.length; i++) {
      sb.append(String.format("%-" + (width[i] + 2) + "s", row[i]));
    }
    System.out.println(sb.toString().replaceAll("\\s+$", ""));
  }

  private static String firstLine(String s) {
    int newline = s.indexOf('\n');
    return newline < 0 ? s : s.substring(0, newline);
  }
}
