package org.apache.hadoop.ozone.om.ratis;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.apache.hadoop.hdds.tracing.TracingUtil;
import org.apache.hadoop.ozone.om.OMConfigKeys;
import org.apache.hadoop.ozone.om.OzoneManager;
import org.apache.hadoop.ozone.om.exceptions.OMException;
import org.apache.hadoop.ozone.om.execution.flowcontrol.ExecutionContext;
import org.apache.hadoop.ozone.om.helpers.OMRatisHelper;
import org.apache.hadoop.ozone.om.lock.OMLockDetails;
import org.apache.hadoop.ozone.om.ratis.utils.OzoneManagerRatisUtils;
import org.apache.hadoop.ozone.om.response.DummyOMClientResponse;
import org.apache.hadoop.ozone.om.response.OMClientResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos;
import org.apache.hadoop.ozone.protocolPB.OzoneManagerRequestHandler;
import org.apache.hadoop.ozone.protocolPB.RequestHandler;
import org.apache.hadoop.util.concurrent.HadoopExecutors;
import org.apache.ratis.protocol.Message;
import org.apache.ratis.protocol.RaftGroupId;
import org.apache.ratis.server.RaftServer;
import org.apache.ratis.server.protocol.TermIndex;
import org.apache.ratis.server.storage.RaftStorage;
import org.apache.ratis.statemachine.TransactionContext;
import org.apache.ratis.statemachine.impl.BaseStateMachine;
import org.apache.ratis.statemachine.impl.SimpleStateMachineStorage;
import org.apache.ratis.util.ExitUtils;
import java.io.IOException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ThreadFactory;

import static org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.Status.INTERNAL_ERROR;
import static org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.Status.METADATA_ERROR;

public class BucketStateMachine extends BaseStateMachine {

  private final OzoneManager ozoneManager;

  private final SimpleStateMachineStorage storage = new SimpleStateMachineStorage();

  private final OzoneManagerDoubleBuffer ozoneManagerDoubleBuffer;

  private final String threadNamePrefix;

  private final ExecutorService executorService;

  private RequestHandler handler;

  public BucketStateMachine(String bucketName, OzoneManager om) {
    this.ozoneManager = om;
    this.ozoneManagerDoubleBuffer =  buildDoubleBufferForRatis();
    this.threadNamePrefix = om.getThreadNamePrefix() + "-" + bucketName;

    ThreadFactory build = new ThreadFactoryBuilder().setDaemon(true)
        .setNameFormat(threadNamePrefix +
            "OMStateMachineApplyTransactionThread - %d").build();
    this.executorService = HadoopExecutors.newSingleThreadExecutor(build);
    this.handler = new OzoneManagerRequestHandler(ozoneManager);
  }

  @Override
  public void initialize(RaftServer raftServer, RaftGroupId raftGroupId, RaftStorage raftStorage) throws IOException {
    getLifeCycle().startAndTransition(() -> {
      super.initialize(raftServer, raftGroupId, raftStorage);
      storage.init(raftStorage);
      LOG.info("{}: initialize {} with {}", getId(), raftGroupId, getLastAppliedTermIndex());
    });
  }

  @Override
  public CompletableFuture<Message> applyTransaction(TransactionContext trx) {
    try {
      LOG.info("Apply transaction in bucket state machine");
      // For the Leader, the OMRequest is set in trx in startTransaction.
      // For Followers, the OMRequest hast to be converted from the log entry.
      final Object context = trx.getStateMachineContext();
      final OzoneManagerProtocolProtos.OMRequest request = context != null ? (OzoneManagerProtocolProtos.OMRequest) context
          : OMRatisHelper.convertByteStringToOMRequest(
          trx.getStateMachineLogEntry().getLogData());
      final TermIndex termIndex = TermIndex.valueOf(trx.getLogEntry());
      LOG.debug("{}: applyTransaction {}", getId(), termIndex);
      // In the current approach we have one single global thread executor.
      // with single thread. Right now this is being done for correctness, as
      // applyTransaction will be run on multiple OM's we want to execute the
      // transactions in the same order on all OM's, otherwise there is a
      // chance that OM replica's can be out of sync.
      // TODO: In this way we are making all applyTransactions in
      // OM serial order. Revisit this in future to use multiple executors for
      // volume/bucket.

      // Reason for not immediately implementing executor per volume is, if
      // one executor operations are slow, we cannot update the
      // lastAppliedIndex in OzoneManager StateMachine, even if other
      // executor has completed the transactions with id more.

      //if there are too many pending requests, wait for doubleBuffer flushing
      ozoneManagerDoubleBuffer.acquireUnFlushedTransactions(1);

      return CompletableFuture.supplyAsync(() -> runCommand(request, termIndex), executorService)
          .thenApply(this::processResponse);
    } catch (Exception e) {
      return completeExceptionally(e);
    }
  }

  /**
   * Submits write request to OM and returns the response Message.
   * @param request OMRequest
   * @return response from OM
   */
  private OzoneManagerProtocolProtos.OMResponse runCommand(OzoneManagerProtocolProtos.OMRequest request, TermIndex termIndex) {
    try {
      ExecutionContext context = ExecutionContext.of(termIndex.getIndex(), termIndex);
      final OMClientResponse omClientResponse = handler.handleWriteRequest(
          request, context, ozoneManagerDoubleBuffer);
      OMLockDetails omLockDetails = omClientResponse.getOmLockDetails();
      OzoneManagerProtocolProtos.OMResponse omResponse = omClientResponse.getOMResponse();
      if (omLockDetails != null) {
        return omResponse.toBuilder()
            .setOmLockDetails(omLockDetails.toProtobufBuilder()).build();
      } else {
        return omResponse;
      }
    } catch (IOException e) {
      LOG.warn("Failed to write, Exception occurred ", e);
      return createErrorResponse(request, e, termIndex);
    } catch (Throwable e) {
      // For any Runtime exceptions, terminate OM.
      String errorMessage = "Request " + request + " failed with exception";
      ExitUtils.terminate(1, errorMessage, e, LOG);
    }
    return null;
  }

  private OzoneManagerProtocolProtos.OMResponse createErrorResponse(
      OzoneManagerProtocolProtos.OMRequest omRequest, IOException exception, TermIndex termIndex) {
    OzoneManagerProtocolProtos.OMResponse.Builder omResponseBuilder = OzoneManagerProtocolProtos.OMResponse.newBuilder()
        .setStatus(OzoneManagerRatisUtils.exceptionToResponseStatus(exception))
        .setCmdType(omRequest.getCmdType())
        .setTraceID(omRequest.getTraceID())
        .setSuccess(false);
    if (exception.getMessage() != null) {
      omResponseBuilder.setMessage(exception.getMessage());
    }
    OzoneManagerProtocolProtos.OMResponse omResponse = omResponseBuilder.build();
    OMClientResponse omClientResponse = new DummyOMClientResponse(omResponse);
    ozoneManagerDoubleBuffer.add(omClientResponse, termIndex);
    return omResponse;
  }

  private Message processResponse(OzoneManagerProtocolProtos.OMResponse omResponse) {
    if (!omResponse.getSuccess()) {
      // INTERNAL_ERROR or METADATA_ERROR are considered as critical errors.
      // In such cases, OM must be terminated instead of completing the future exceptionally,
      // Otherwise, OM may continue applying transactions which leads to an inconsistent state.
      if (omResponse.getStatus() == INTERNAL_ERROR) {
        terminate(omResponse, OMException.ResultCodes.INTERNAL_ERROR);
      } else if (omResponse.getStatus() == METADATA_ERROR) {
        terminate(omResponse, OMException.ResultCodes.METADATA_ERROR);
      }
    }

    // For successful response and non-critical errors, convert the response.
    return OMRatisHelper.convertResponseToMessage(omResponse);
  }

  private static <T> CompletableFuture<T> completeExceptionally(Exception e) {
    final CompletableFuture<T> future = new CompletableFuture<>();
    future.completeExceptionally(e);
    return future;
  }

  private static void terminate(OzoneManagerProtocolProtos.OMResponse omResponse, OMException.ResultCodes resultCode) {
    OMException exception = new OMException(omResponse.getMessage(),
        resultCode);
    String errorMessage = "OM Ratis Server has received unrecoverable " +
        "error, to avoid further DB corruption, terminating OM. Error " +
        "Response received is:" + omResponse;
    ExitUtils.terminate(1, errorMessage, exception, LOG);
  }

  public OzoneManagerDoubleBuffer buildDoubleBufferForRatis() {
    final int maxUnFlushedTransactionCount = ozoneManager.getConfiguration()
        .getInt(OMConfigKeys.OZONE_OM_UNFLUSHED_TRANSACTION_MAX_COUNT,
            OMConfigKeys.OZONE_OM_UNFLUSHED_TRANSACTION_MAX_COUNT_DEFAULT);
    return OzoneManagerDoubleBuffer.newBuilder()
        .setOmMetadataManager(ozoneManager.getMetadataManager())
        .setUpdateLastAppliedIndex(this::updateLastAppliedTermIndex)
        .setMaxUnFlushedTransactionCount(maxUnFlushedTransactionCount)
        .setThreadPrefix(threadNamePrefix)
        .setS3SecretManager(ozoneManager.getS3SecretManager())
        .enableTracing(TracingUtil.isTracingEnabled(ozoneManager.getConfiguration()))
        .build()
        .start();
  }
}
