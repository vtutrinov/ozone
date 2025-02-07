package org.apache.hadoop.ozone.client.storage;

import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.scm.XceiverClientManager;
import org.apache.hadoop.hdds.scm.XceiverClientSpi;
import org.apache.hadoop.hdds.scm.pipeline.Pipeline;

import java.io.IOException;

/**
 * XceiverClientManager Stub.
 */
public class XceiverClientManagerStub extends XceiverClientManager {
  private final XceiverClientSpi xceiverClientSpi = new XceiverClientSpiStub();

  public XceiverClientManagerStub() throws IOException {
    super(new OzoneConfiguration());
  }

  @Override
  public XceiverClientSpi acquireClientForReadData(Pipeline pipeline) {
    return xceiverClientSpi;
  }

  @Override
  public void releaseClientForReadData(XceiverClientSpi xceiverClient, boolean isFailed) {
  }

  @Override
  public XceiverClientSpi acquireClient(Pipeline pipeline) {
    return xceiverClientSpi;
  }

  @Override
  public void releaseClient(XceiverClientSpi xceiverClient, boolean isFailed) {
  }

  @Override
  public void close() {
  }
}
