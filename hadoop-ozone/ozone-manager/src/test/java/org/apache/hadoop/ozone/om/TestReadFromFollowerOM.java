package org.apache.hadoop.ozone.om;

import com.google.protobuf.ProtocolMessageEnum;
import com.google.protobuf.ServiceException;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.utils.ProtocolMessageMetrics;
import org.apache.hadoop.ozone.om.helpers.KeyInfoWithVolumeContext;
import org.apache.hadoop.ozone.om.helpers.OmKeyArgs;
import org.apache.hadoop.ozone.om.ratis.OzoneManagerRatisServer;
import org.apache.hadoop.ozone.om.request.util.OmResponseUtil;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos;
import org.apache.hadoop.ozone.protocolPB.OzoneManagerProtocolServerSideTranslatorPB;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.io.IOException;

import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.anyBoolean;
import static org.mockito.Mockito.anyInt;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;


/**
 * Tests for OM follower read.
 */
public class TestReadFromFollowerOM {

  private OzoneManager ozoneManager = Mockito.mock(OzoneManager.class);
  private final OzoneConfiguration ozoneConfiguration = new OzoneConfiguration();

  @Test
  public void testDisabledReadFromFollowerOM() throws IOException {
    OzoneManagerProtocolProtos.OMRequest readRequest =
          OzoneManagerProtocolProtos.OMRequest.newBuilder()
                .setCmdType(OzoneManagerProtocolProtos.Type.GetKeyInfo)
                .setClientId("test-client-id")
                .build();
    ozoneConfiguration.set(OMConfigKeys.OZONE_OM_FOLLOWER_READ_ENABLED, String.valueOf(false));
    OzoneManagerRatisServer ratisServer = Mockito.mock(OzoneManagerRatisServer.class);
    when(ozoneManager.getConfiguration()).thenReturn(ozoneConfiguration);
    when(ratisServer.checkOmLeaderStatus()).thenReturn(OzoneManagerRatisServer.RaftServerStatus.NOT_LEADER);
    ProtocolMessageMetrics<ProtocolMessageEnum> protocolMessageMetrics =
          Mockito.mock(ProtocolMessageMetrics.class);
    long transactionIndex = 100L;

    OzoneManagerProtocolServerSideTranslatorPB translator =
          new OzoneManagerProtocolServerSideTranslatorPB(ozoneManager,
                ratisServer, protocolMessageMetrics, true,
                transactionIndex);

    assertThrows(ServiceException.class, () -> translator.processRequest(readRequest));
    verify(ozoneManager, times(0)).getKeyInfo(any(OmKeyArgs.class), anyBoolean());
  }

  @Test
  public void testEnabledReadFromFollowerOM() throws Exception {
    OzoneManagerProtocolProtos.OMRequest readRequest =
          OzoneManagerProtocolProtos.OMRequest.newBuilder()
                .setCmdType(OzoneManagerProtocolProtos.Type.GetKeyInfo)
                .setClientId("test-client-id")
                .build();
    ozoneConfiguration.setBoolean(OMConfigKeys.OZONE_OM_FOLLOWER_READ_ENABLED, true);
    when(ozoneManager.getConfiguration()).thenReturn(ozoneConfiguration);
    OzoneManagerRatisServer ratisServer = Mockito.mock(OzoneManagerRatisServer.class);
    when(ratisServer.checkOmLeaderStatus()).thenReturn(OzoneManagerRatisServer.RaftServerStatus.NOT_LEADER);
    ProtocolMessageMetrics<ProtocolMessageEnum> protocolMessageMetrics =
          Mockito.mock(ProtocolMessageMetrics.class);
    long transactionIndex = 100L;

    OzoneManagerProtocolServerSideTranslatorPB translator =
          new OzoneManagerProtocolServerSideTranslatorPB(ozoneManager,
                ratisServer, protocolMessageMetrics, true,
                transactionIndex);

    when(ratisServer.getOzoneManager()).thenReturn(ozoneManager);

    KeyInfoWithVolumeContext keyInfo = mock(KeyInfoWithVolumeContext.class);
    doReturn(keyInfo).when(ozoneManager).getKeyInfo(any(OmKeyArgs.class), anyBoolean());

    OzoneManagerProtocolProtos.GetKeyInfoResponse response = mock(OzoneManagerProtocolProtos.GetKeyInfoResponse.class);
    doReturn(response).when(keyInfo).toProtobuf(anyInt());

    try (MockedStatic<OmResponseUtil> omResponseUtil = mockStatic(OmResponseUtil.class)) {
      OzoneManagerProtocolProtos.OMResponse.Builder responseBuilder =
              mock(OzoneManagerProtocolProtos.OMResponse.Builder.class);

      OzoneManagerProtocolProtos.OMResponse expectedResponse = OzoneManagerProtocolProtos.OMResponse.newBuilder()
              .setStatus(OzoneManagerProtocolProtos.Status.OK)
              .setCmdType(OzoneManagerProtocolProtos.Type.GetKeyInfo)
              .build();
      doReturn(expectedResponse).when(responseBuilder).build();

      omResponseUtil.when(() -> OmResponseUtil.getOMResponseBuilder(eq(readRequest))).thenReturn(responseBuilder);
      OzoneManagerProtocolProtos.OMResponse actual = translator.processRequest(readRequest);
      assertSame(expectedResponse, actual);
      Mockito.verify(ozoneManager, Mockito.times(1)).getKeyInfo(any(OmKeyArgs.class), anyBoolean());
    }
  }
}
