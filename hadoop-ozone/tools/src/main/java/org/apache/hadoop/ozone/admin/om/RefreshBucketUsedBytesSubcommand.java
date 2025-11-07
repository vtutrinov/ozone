package org.apache.hadoop.ozone.admin.om;

import org.apache.hadoop.hdds.cli.HddsVersionProvider;
import org.apache.hadoop.ozone.client.protocol.ClientProtocol;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.RefreshBucketUsedBytesResponse;
import picocli.CommandLine;

import java.util.concurrent.Callable;

/**
 * Refresh Bucket UsedBytes SubCommand.
 */
@CommandLine.Command(
        name = "refresh-usedbytes",
        description = "Recalculates and updates the usedBytes field of the bucket.",
        mixinStandardHelpOptions = true,
        versionProvider = HddsVersionProvider.class)
public class RefreshBucketUsedBytesSubcommand implements Callable<Void> {
  @CommandLine.ParentCommand
  private OMAdmin parent;

  @CommandLine.Option(names = {"-id", "--service-id"},
        description = "OM Service ID",
        required = true)
  private String omServiceId;

  @CommandLine.Parameters(description = "bucket name in the format <volume>/<bucket>.")
  private String bucketPath;

  @Override
  public Void call() throws Exception {
    String[] path = bucketPath.split("/");
    if (path.length != 2) {
      System.out.println("Invalid bucket.");
      return null;
    }

    String volumeName = path[0];
    String bucketName = path[1];
    ClientProtocol client = parent.createClient(omServiceId);
    RefreshBucketUsedBytesResponse response = client.refreshBucketUsedBytes(volumeName, bucketName);

    return null;
  }
}
