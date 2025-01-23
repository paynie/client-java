package org.tikv.common.util;

import com.google.common.collect.ImmutableSet;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.tikv.common.exception.*;
import org.tikv.common.region.RegionManager;
import org.tikv.common.region.TiRegion;
import org.tikv.common.region.TiStore;
import org.tikv.common.store.StoreClientManager;

public class ExceptionHandlerUtils {
  private static final Logger logger = LoggerFactory.getLogger(ExceptionHandlerUtils.class);

  public static final ImmutableSet<Status.Code> unrecoverableStatus =
      ImmutableSet.of(
          Status.Code.ALREADY_EXISTS, Status.Code.PERMISSION_DENIED,
          Status.Code.INVALID_ARGUMENT, Status.Code.NOT_FOUND,
          Status.Code.UNIMPLEMENTED, Status.Code.OUT_OF_RANGE,
          Status.Code.UNAUTHENTICATED, Status.Code.CANCELLED);

  public static void handleTiKVException(
      TiKVException e, BackOffer backOffer, RegionManager regionManager) {
    logger.error("rpc error", e);
    // Sleep and retry
    backOffer.doBackOff(BackOffFunction.BackOffFuncType.BoUnknownError, e);
  }

  public static void handleNeedNotRetryException(NeedNotRetryException e) {
    logger.error("rpc error", e);
    throw e;
  }

  public static void handleNeedRetryException(
      NeedRetryException e, RegionBatch batch, TiRegion region, RegionManager regionManager) {
    logger.error("rpc error", e);

    if (region != null) {
      // Update region for batch and retry again
      TiRegion newRegion = regionManager.getRegionById(region.getId());
      logger.info(
          String.format(
              "New region is {region id: %d, leader: %d}",
              newRegion.getId(), newRegion.getLeader().getStoreId()));
      if (batch != null) {
        batch.setRegion(newRegion);
      }
    }
  }

  public static void handleNeedResplitAndRetryException(
      NeedResplitAndRetryException e,
      RegionBatch batch,
      TiRegion region,
      BackOffer backOffer,
      RegionManager regionManager) {
    logger.error("rpc error", e);

    // Sleep and retry
    backOffer.doBackOff(BackOffFunction.BackOffFuncType.BoRegionMiss, e);

    if (region != null) {
      // Update region for batch and retry again
      TiRegion newRegion = regionManager.getRegionById(region.getId());
      logger.info(
          String.format(
              "New region is {region id: %d, leader: %d}",
              newRegion.getId(), newRegion.getLeader().getStoreId()));
      if (batch != null) {
        batch.setRegion(newRegion);
      }
    }
  }

  public static void handleStatusRuntimeException(
      StatusRuntimeException e,
      RegionBatch batch,
      TiStore store,
      TiRegion region,
      BackOffer backOffer,
      RegionManager regionManager,
      StoreClientManager storeClientManager) {
    logger.error("rpc error", e);
    if (unrecoverableStatus.contains(e.getStatus().getCode())) {
      // Not recoverable, just throw
      throw new GrpcException(e);
    }

    // Sleep and retry
    backOffer.doBackOff(BackOffFunction.BackOffFuncType.BoTiKVRPC, e);

    if (store != null) {
      logger.error("rpc error for store " + store.getAddress() + ", state = " + e.getStatus());
      if (e.getStatus() == Status.UNAVAILABLE) {
        // Store is unavailable
        storeClientManager.storeUnreachable(store);
      }
    }

    if (region != null) {
      regionManager.invalidateRegion(region);
      // Check timeout or not, if not timeout, Retry again
      TiRegion newRegion = regionManager.getRegionById(region.getId());
      logger.info(
          String.format(
              "New region is {region id: %d, leader: %d}",
              newRegion.getId(), newRegion.getLeader().getStoreId()));
      if (batch != null) {
        batch.setRegion(newRegion);
      }
    }
  }

  public static void checkStore(
      TiStore store, TiRegion region, BackOffer backOffer, RegionManager regionManager) {
    if (store == null) {
      backOffer.doBackOff(
          BackOffFunction.BackOffFuncType.BoRegionMiss,
          new GrpcException("Store is null, clear region and retry"));
      throw new NeedRetryException("Store is null, clear region and retry");
    }

    if (!store.isValid()) {
      regionManager.invalidateRegion(region);
      String errorMsg =
          String.format(
              "Store {id:%d, address:%s} is invalid, clear region and retry",
              store.getId(), store.getAddress());
      backOffer.doBackOff(
          BackOffFunction.BackOffFuncType.BoRegionMiss, new GrpcException(errorMsg));
      throw new NeedRetryException(errorMsg);
    }

    if (!store.isReachable()) {
      regionManager.invalidateRegion(region);
      String errorMsg =
          String.format(
              "Store {id:%d, address:%s} is not reachable, clear region and retry",
              store.getId(), store.getAddress());
      backOffer.doBackOff(
          BackOffFunction.BackOffFuncType.BoRegionMiss, new GrpcException(errorMsg));
      throw new NeedRetryException(errorMsg);
    }
  }

  public static void handleRpcException(StatusRuntimeException e, BackOffer backOffer) {
    if (unrecoverableStatus.contains(e.getStatus().getCode())) {
      // Not recoverable, just throw
      throw new GrpcException(e);
    }

    // Check timeout or not, if not timeout, Retry again
    backOffer.doBackOff(BackOffFunction.BackOffFuncType.BoTiKVRPC, e);
  }
}
