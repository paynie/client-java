package org.tikv.common.operation;

import com.google.protobuf.ByteString;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.function.Function;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.tikv.common.codec.KeyUtils;
import org.tikv.common.event.CacheInvalidateEvent;
import org.tikv.common.exception.*;
import org.tikv.common.region.RegionErrorReceiver;
import org.tikv.common.region.RegionManager;
import org.tikv.common.region.TiRegion;
import org.tikv.common.util.BackOffFunction;
import org.tikv.common.util.BackOffer;
import org.tikv.kvproto.Errorpb;
import org.tikv.kvproto.Metapb;

public class RegionErrorHandlerV2<RespT> implements IErrorHandlerV2<RespT> {
  private static final Logger logger = LoggerFactory.getLogger(RegionErrorHandlerV2.class);
  // if a store does not have leader currently, store id is set to 0
  public static final int NO_LEADER_STORE_ID = 0;
  public static final int INVALID_STORE_ID = 0;
  public static final int INVALID_REGION_ID = 0;

  private final Function<RespT, Errorpb.Error> getRegionError;
  private final RegionManager regionManager;
  private final RegionErrorReceiver recv;
  private final List<Function<CacheInvalidateEvent, Void>> cacheInvalidateCallBackList;
  private final ExecutorService callBackThreadPool;

  public RegionErrorHandlerV2(
      RegionManager regionManager,
      RegionErrorReceiver recv,
      Function<RespT, Errorpb.Error> getRegionError) {
    this.recv = recv;
    this.regionManager = regionManager;
    this.getRegionError = getRegionError;
    this.cacheInvalidateCallBackList = regionManager.getCacheInvalidateCallbackList();
    this.callBackThreadPool = regionManager.getCallBackThreadPool();
  }

  @Override
  public void handleResponse(BackOffer backOffer, RespT resp) {
    if (resp == null) {
      String msg = String.format("Request Failed with unknown reason for [%s]", recv.getRegion());
      handleRequestError(backOffer, new GrpcException(msg));
      return;
    }
    // Region error handling logic
    Errorpb.Error error = getRegionError(resp);
    if (error != null) {
      handleRegionError(backOffer, error);
    }
  }

  public void handleRegionError(BackOffer backOffer, Errorpb.Error error) {
    if (error.hasNotLeader()) {
      // this error is reported from raftstore:
      // peer of current request is not leader, the following might be its causes:
      // 1. cache is outdated, region has changed its leader, can be solved by re-fetching from PD
      // 2. leader of current region is missing, need to wait and then fetch region info from PD
      long newStoreId = error.getNotLeader().getLeader().getStoreId();

      String errorMsg =
          String.format(
              "NotLeader Error with region id %d and store id %d, new store id %d",
              recv.getRegion().getId(), recv.getRegion().getLeader().getStoreId(), newStoreId);

      // update Leader here
      logger.warn(errorMsg);

      BackOffFunction.BackOffFuncType backOffFuncType;
      // if there's current no leader, we do not trigger update pd cache logic
      // since issuing store = NO_LEADER_STORE_ID requests to pd will definitely fail.
      if (newStoreId != NO_LEADER_STORE_ID) {
        backOffFuncType = BackOffFunction.BackOffFuncType.BoUpdateLeader;
        if (regionManager.updateLeader2(recv.getRegion(), newStoreId)) {
          // Update region leader success, just retry
          throw new NeedRetryException(errorMsg);
        }
      } else {
        logger.warn(
            String.format(
                "Received zero store id, from region %d try next time", recv.getRegion().getId()));
        backOffFuncType = BackOffFunction.BackOffFuncType.BoRegionMiss;
      }

      // If update region failed, just invalid the region in cache first
      this.regionManager.invalidateRegion(recv.getRegion());
      notifyRegionLeaderError(recv.getRegion());
      backOffer.doBackOff(backOffFuncType, new GrpcException(error.toString()));

      // Retry
      throw new NeedRetryException(errorMsg);
    } else if (error.hasStoreNotMatch()) {
      // this error is reported from raftstore:
      // store_id requested at the moment is inconsistent with that expected
      // Solution：re-fetch from PD
      long storeId = error.getStoreNotMatch().getRequestStoreId();
      long actualStoreId = error.getStoreNotMatch().getActualStoreId();
      String errorMsg =
          String.format(
              "Store Not Match happened with region id %d, store id %d, actual store id %d",
              recv.getRegion().getId(), storeId, actualStoreId);
      logger.warn(errorMsg);
      // may request store which is not leader.
      invalidateRegionStoreCache(recv.getRegion(), storeId);
      // assume this is a low probability error, do not retry, just re-split the request by
      // throwing it out.

      // Retry
      throw new NeedRetryException(errorMsg);
    } else if (error.hasEpochNotMatch()) {
      String errorMsg =
          String.format("tikv reports `EpochNotMatch` retry later, region: %s", recv.getRegion());
      logger.warn(errorMsg);
      onRegionEpochNotMatch(backOffer, error.getEpochNotMatch().getCurrentRegionsList());

      // Retry
      throw new NeedRetryException(errorMsg);
    } else if (error.hasServerIsBusy()) {
      // this error is reported from kv:
      // will occur when write pressure is high. Please try later.
      String errorMsg =
          String.format(
              "Server is busy for region [%s], reason: %s",
              recv.getRegion(), error.getServerIsBusy().getReason());
      logger.warn(errorMsg);
      backOffer.doBackOff(
          BackOffFunction.BackOffFuncType.BoServerBusy,
          new StatusRuntimeException(
              Status.fromCode(Status.Code.UNAVAILABLE).withDescription(error.toString())));

      // Retry
      throw new NeedRetryException(errorMsg);
    } else if (error.hasStaleCommand()) {
      // this error is reported from raftstore:
      // command outdated, please try later
      String errorMsg = String.format("Stale command for region [%s]", recv.getRegion());
      logger.warn(errorMsg);
      backOffer.doBackOff(
          BackOffFunction.BackOffFuncType.BoRegionMiss, new GrpcException(error.getMessage()));

      // Retry
      throw new NeedRetryException(errorMsg);
    } else if (error.hasRaftEntryTooLarge()) {
      String errorMsg = String.format("Raft too large for region [%s]", recv.getRegion());
      logger.error(errorMsg);

      // Not retry
      throw new NeedNotRetryException(errorMsg);
    } else if (error.hasDiskFull()) {
      String errorMsg = String.format("Disk is full for region [%s]", recv.getRegion());
      logger.error(errorMsg);

      // Not retry
      throw new NeedNotRetryException(errorMsg);
    } else if (error.hasKeyNotInRegion()) {
      // this error is reported from raftstore:
      // key requested is not in current region
      // should not happen here.
      ByteString invalidKey = error.getKeyNotInRegion().getKey();
      String errorMsg =
          String.format(
              "Key not in region [%s] for key [%s], this error should not happen here.",
              recv.getRegion(), KeyUtils.formatBytesUTF8(invalidKey));
      logger.error(errorMsg);
      regionManager.clearRegionCache();

      // Retry
      throw new NeedResplitAndRetryException(errorMsg);
    }
    // The tso cache is used up in TiKV servers, we should backoff and wait its cache is renewed.
    else if (error.getMessage().contains("TsoBatchUsedUp")) {
      String errorMsg = String.format("tso batch used up for region [%s]", recv.getRegion());
      logger.warn(errorMsg);
      backOffer.doBackOff(
          BackOffFunction.BackOffFuncType.BoTsoBatchUsedUp, new GrpcException(error.getMessage()));

      // Retry
      throw new NeedRetryException(errorMsg);
    }

    String errorMsg = String.format("Unknown error %s for region [%s]", error, recv.getRegion());
    logger.warn(errorMsg);
    // For other errors, we only drop cache here.
    // Upper level may split this task.
    invalidateRegionStoreCache(recv.getRegion(), recv.getRegion().getLeader().getStoreId());
    // retry if raft proposal is dropped, it indicates the store is in the middle of transition
    if (error.getMessage().contains("Raft ProposalDropped")) {
      backOffer.doBackOff(
          BackOffFunction.BackOffFuncType.BoRegionMiss, new GrpcException(error.getMessage()));

      // Retry
      throw new NeedRetryException(errorMsg);
    }

    backOffer.doBackOff(
        BackOffFunction.BackOffFuncType.BoUnknownError, new GrpcException(error.getMessage()));

    // Retry
    throw new NeedRetryException(errorMsg);
  }

  private void onRegionEpochNotMatch(BackOffer backOffer, List<Metapb.Region> currentRegions) {
    if (currentRegions == null || currentRegions.isEmpty()) {
      // Invalid the region in the cache
      this.regionManager.onRegionStale(recv.getRegion());
      notifyRegionCacheInvalidate(recv.getRegion());
      return;
    }

    // Find whether the region epoch in `ctx` is ahead of TiKV's. If so, backoff.
    for (Metapb.Region meta : currentRegions) {
      if (meta.getId() == recv.getRegion().getId()
          && (meta.getRegionEpoch().getConfVer() < recv.getRegion().getVerID().getConfVer()
              || meta.getRegionEpoch().getVersion() < recv.getRegion().getVerID().getVer())) {
        String errorMsg =
            String.format(
                "region epoch is ahead of tikv, region: %s, currentRegions: %s",
                recv.getRegion(), currentRegions);
        logger.info(errorMsg);
        backOffer.doBackOff(
            BackOffFunction.BackOffFuncType.BoRegionMiss, new TiKVException(errorMsg));
        return;
      }
    }

    boolean needInvalidateOld = true;
    List<TiRegion> newRegions = new ArrayList<>(currentRegions.size());
    // If the region epoch is not ahead of TiKV's, replace region meta in region cache.
    for (Metapb.Region meta : currentRegions) {
      // The region needs to be decoded to plain format.
      meta = regionManager.getPDClient().getCodec().decodeRegion(meta);
      TiRegion region = regionManager.createRegion(meta, backOffer);
      newRegions.add(region);
      if (recv.getRegion().getVerID() == region.getVerID()) {
        needInvalidateOld = false;
      }
    }

    if (needInvalidateOld) {
      notifyRegionCacheInvalidate(recv.getRegion());
      this.regionManager.onRegionStale(recv.getRegion());
    }

    for (TiRegion region : newRegions) {
      regionManager.insertRegionToCache(region);
    }
  }

  @Override
  public void handleRequestError(BackOffer backOffer, Exception e) {
    if (recv.onStoreUnreachable(backOffer)) {
      if (!backOffer.canRetryAfterSleep(BackOffFunction.BackOffFuncType.BoTiKVRPC)) {
        regionManager.onRequestFail(recv.getRegion());
        throw new GrpcException("retry is exhausted.", e);
      }
      return;
    }

    logger.warn("request failed because of: " + e.getMessage());
    if (!backOffer.canRetryAfterSleep(BackOffFunction.BackOffFuncType.BoTiKVRPC)) {
      regionManager.onRequestFail(recv.getRegion());
      throw new GrpcException(
          "send tikv request error: " + e.getMessage() + ", try next peer later", e);
    }
  }

  public Errorpb.Error getRegionError(RespT resp) {
    if (getRegionError != null) {
      return getRegionError.apply(resp);
    }
    return null;
  }

  public TiRegion getRegion() {
    return recv.getRegion();
  }

  private void notifyRegionRequestError(
      TiRegion ctxRegion, long storeId, CacheInvalidateEvent.CacheType type) {
    CacheInvalidateEvent event;
    // When store(region) id is invalid,
    // it implies that the error was not caused by store(region) error.
    switch (type) {
      case REGION:
      case LEADER:
        event = new CacheInvalidateEvent(ctxRegion.getId(), INVALID_STORE_ID, true, false, type);
        break;
      case REGION_STORE:
        event = new CacheInvalidateEvent(ctxRegion.getId(), storeId, true, true, type);
        break;
      case REQ_FAILED:
        event = new CacheInvalidateEvent(INVALID_REGION_ID, INVALID_STORE_ID, false, false, type);
        break;
      default:
        throw new IllegalArgumentException("Unexpect invalid cache invalid type " + type);
    }
    if (cacheInvalidateCallBackList != null) {
      for (Function<CacheInvalidateEvent, Void> cacheInvalidateCallBack :
          cacheInvalidateCallBackList) {
        callBackThreadPool.submit(
            () -> {
              try {
                cacheInvalidateCallBack.apply(event);
              } catch (Exception e) {
                logger.error(String.format("CacheInvalidCallBack failed %s", e));
              }
            });
      }
    }
  }

  private void invalidateRegionStoreCache(TiRegion ctxRegion, long storeId) {
    regionManager.invalidateRegion(ctxRegion);
    regionManager.invalidateStore(storeId);
    notifyRegionRequestError(ctxRegion, storeId, CacheInvalidateEvent.CacheType.REGION_STORE);
  }

  private void notifyRegionCacheInvalidate(TiRegion ctxRegion) {
    notifyRegionRequestError(ctxRegion, 0, CacheInvalidateEvent.CacheType.REGION);
  }

  private void notifyRegionLeaderError(TiRegion ctxRegion) {
    notifyRegionRequestError(ctxRegion, 0, CacheInvalidateEvent.CacheType.LEADER);
  }
}
