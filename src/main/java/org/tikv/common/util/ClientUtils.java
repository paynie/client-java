/*
 * Copyright 2021 TiKV Project Authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

package org.tikv.common.util;

import com.google.protobuf.ByteString;
import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.tikv.common.exception.TiKVException;
import org.tikv.common.region.RegionManager;
import org.tikv.common.region.RegionStoreClient;
import org.tikv.common.region.TiRegion;
import org.tikv.kvproto.Kvrpcpb;
import org.tikv.kvproto.Kvrpcpb.Op;
import org.tikv.kvproto.Kvrpcpb.WriteOp;

public class ClientUtils {
  /**
   * Append batch to list and split them according to batch limit
   *
   * @param backOffer backOffer
   * @param batches a grouped batch
   * @param region region
   * @param keys keys
   * @param batchMaxSizeInBytes batch max limit
   */
  public static void appendBatches(
      BackOffer backOffer,
      List<Batch> batches,
      TiRegion region,
      List<ByteString> keys,
      int batchMaxSizeInBytes,
      int batchLimit) {
    if (keys == null) {
      return;
    }
    int len = keys.size();
    for (int start = 0, end; start < len; start = end) {
      int size = 0;
      for (end = start;
          end < len && size < batchMaxSizeInBytes && end - start < batchLimit;
          end++) {
        size += keys.get(end).size();
      }
      Batch batch = new Batch(backOffer, region, keys.subList(start, end));
      batches.add(batch);
    }
  }

  /**
   * Append batch to list and split them according to batch limit
   *
   * @param backOffer backOffer
   * @param batches a grouped batch
   * @param region region
   * @param keys keys
   * @param values values
   * @param batchMaxSizeInBytes batch max limit
   */
  public static void appendBatches(
      BackOffer backOffer,
      List<Batch> batches,
      TiRegion region,
      List<ByteString> keys,
      List<ByteString> values,
      int batchMaxSizeInBytes,
      int batchLimit) {
    if (keys == null) {
      return;
    }
    int len = keys.size();
    for (int start = 0, end; start < len; start = end) {
      int size = 0;
      for (end = start;
          end < len && size < batchMaxSizeInBytes && end - start < batchLimit;
          end++) {
        size += keys.get(end).size();
        size += values.get(end).size();
      }
      Batch batch =
          new Batch(backOffer, region, keys.subList(start, end), values.subList(start, end));
      batches.add(batch);
    }
  }

  /**
   * Append batch to list and split them according to batch limit
   *
   * @param backOffer backOffer
   * @param batches a grouped batch
   * @param region region
   * @param writeOps writeOps write batch
   * @param batchMaxSizeInBytes batch max limit
   */
  public static void appendWriteBatches(
      BackOffer backOffer,
      List<WriteBatch> batches,
      TiRegion region,
      List<WriteOp> writeOps,
      int batchMaxSizeInBytes,
      int batchLimit) {
    if (writeOps == null) {
      return;
    }
    int len = writeOps.size();
    for (int start = 0, end; start < len; start = end) {
      int size = 0;
      for (end = start;
          end < len && size < batchMaxSizeInBytes && end - start < batchLimit;
          end++) {
        WriteOp writeOp = writeOps.get(end);
        size += writeOp.getKey().size();
        if (writeOp.getOp() == Op.Put) {
          size += writeOp.getValue().size();
        }
      }
      WriteBatch batch = new WriteBatch(backOffer, region, writeOps.subList(start, end));
      batches.add(batch);
    }
  }

  public static List<Batch> getBatches(
      BackOffer backOffer,
      List<ByteString> keys,
      int batchSize,
      int batchLimit,
      RegionStoreClient.RegionStoreClientBuilder clientBuilder) {
    Map<TiRegion, List<ByteString>> groupKeys =
        groupKeysByRegion(clientBuilder.getRegionManager(), keys, backOffer);
    List<Batch> retryBatches = new ArrayList<>();

    for (Map.Entry<TiRegion, List<ByteString>> entry : groupKeys.entrySet()) {
      appendBatches(
          backOffer, retryBatches, entry.getKey(), entry.getValue(), batchSize, batchLimit);
    }

    return retryBatches;
  }

  public static Map<TiRegion, List<ByteString>> groupKeysByRegion(
      RegionManager regionManager, Set<ByteString> keys, BackOffer backoffer) {
    return groupKeysByRegion(regionManager, new ArrayList<>(keys), backoffer, true);
  }

  public static Map<TiRegion, List<ByteString>> groupKeysByRegion(
      RegionManager regionManager, List<ByteString> keys, BackOffer backoffer) {
    return groupKeysByRegion(regionManager, keys, backoffer, false);
  }

  public static Map<TiRegion, List<WriteOp>> groupWriteOpsByRegion(
      RegionManager regionManager, List<WriteOp> batch, BackOffer backoffer) {
    return groupWriteOpsByRegion(regionManager, batch, backoffer, false);
  }

  /**
   * Group by list of keys according to its region
   *
   * @param batch write batch
   * @return a mapping of keys and their region
   */
  public static Map<TiRegion, List<WriteOp>> groupWriteOpsByRegion(
      RegionManager regionManager, List<WriteOp> batch, BackOffer backoffer, boolean sorted) {
    Map<TiRegion, List<WriteOp>> groups = new HashMap<>();
    if (!sorted) {
      batch.sort(
          (k1, k2) ->
              FastByteComparisons.compareTo(k1.getKey().toByteArray(), k2.getKey().toByteArray()));
    }
    TiRegion lastRegion = null;
    for (WriteOp writeOp : batch) {
      if (lastRegion == null || !lastRegion.contains(writeOp.getKey())) {
        lastRegion = regionManager.getRegionByKey(writeOp.getKey(), backoffer);
      }
      groups.computeIfAbsent(lastRegion, k -> new ArrayList<>()).add(writeOp);
    }
    return groups;
  }

  public static Map<TiRegion, List<ByteString>> groupKeysByRegion(
      RegionManager regionManager, List<ByteString> keys, BackOffer backoffer, boolean sorted) {
    Map<TiRegion, List<ByteString>> groups = new HashMap<>();
    if (!sorted) {
      keys.sort((k1, k2) -> FastByteComparisons.compareTo(k1.toByteArray(), k2.toByteArray()));
    }
    TiRegion lastRegion = null;
    for (ByteString key : keys) {
      if (lastRegion == null || !lastRegion.contains(key)) {
        lastRegion = regionManager.getRegionByKey(key, backoffer);
      }
      groups.computeIfAbsent(lastRegion, k -> new ArrayList<>()).add(key);
    }
    return groups;
  }

  public static List<Kvrpcpb.KvPair> getKvPairs(
      ExecutorCompletionService<List<Kvrpcpb.KvPair>> completionService,
      List<Batch> batches,
      int backOff) {
    try {
      List<Kvrpcpb.KvPair> result = new ArrayList<>();
      for (int i = 0; i < batches.size(); i++) {
        result.addAll(completionService.take().get(backOff, TimeUnit.MILLISECONDS));
      }
      return result;
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new TiKVException("Current thread interrupted.", e);
    } catch (TimeoutException e) {
      throw new TiKVException("TimeOut Exceeded for current operation. ", e);
    } catch (ExecutionException e) {
      throw new TiKVException("Execution exception met.", e);
    }
  }

  public static <T> void getTasks(
      ExecutorCompletionService<List<T>> completionService,
      Queue<List<T>> taskQueue,
      List<T> batches,
      long backOff) {
    try {
      for (int i = 0; i < batches.size(); i++) {
        Future<List<T>> future = completionService.poll(backOff, TimeUnit.MILLISECONDS);
        if (future == null) {
          throw new TiKVException("TimeOut Exceeded for current operation.");
        }
        List<T> task = future.get();
        if (!task.isEmpty()) {
          taskQueue.offer(task);
        }
      }
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new TiKVException("Current thread interrupted.", e);
    } catch (ExecutionException e) {
      throw new TiKVException("Execution exception met.", e);
    }
  }

  public static <T, U> List<U> getTasksWithOutput(
      ExecutorCompletionService<Pair<List<T>, List<U>>> completionService,
      Queue<List<T>> taskQueue,
      List<T> batches,
      long backOff) {
    try {
      List<U> result = new ArrayList<>();
      for (int i = 0; i < batches.size(); i++) {
        Future<Pair<List<T>, List<U>>> future =
            completionService.poll(backOff, TimeUnit.MILLISECONDS);
        if (future == null) {
          throw new TiKVException("TimeOut Exceeded for current operation.");
        }
        Pair<List<T>, List<U>> task = future.get();
        if (!task.first.isEmpty()) {
          taskQueue.offer(task.first);
        } else {
          result.addAll(task.second);
        }
      }
      return result;
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new TiKVException("Current thread interrupted.", e);
    } catch (ExecutionException e) {
      throw new TiKVException("Execution exception met.", e);
    }
  }

  public static byte[] genUUID() {
    UUID uuid = UUID.randomUUID();

    byte[] out = new byte[16];
    long msb = uuid.getMostSignificantBits();
    long lsb = uuid.getLeastSignificantBits();
    for (int i = 0; i < 8; i++) {
      out[i] = (byte) ((msb >> ((7 - i) * 8)) & 0xff);
    }
    for (int i = 8; i < 16; i++) {
      out[i] = (byte) ((lsb >> ((15 - i) * 8)) & 0xff);
    }
    return out;
  }
}
