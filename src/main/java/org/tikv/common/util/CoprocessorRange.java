package org.tikv.common.util;

import com.google.protobuf.ByteString;
import org.tikv.common.region.TiRegion;

public class CoprocessorRange {
  private final BackOffer backOffer;
  private final TiRegion region;
  private final ByteString startKey;
  private final ByteString endKey;

  public CoprocessorRange(
      BackOffer backOffer, TiRegion region, ByteString startKey, ByteString endKey) {
    this.backOffer = backOffer;
    this.region = region;
    this.startKey = startKey;
    this.endKey = endKey;
  }

  public BackOffer getBackOffer() {
    return backOffer;
  }

  public TiRegion getRegion() {
    return region;
  }

  public ByteString getStartKey() {
    return startKey;
  }

  public ByteString getEndKey() {
    return endKey;
  }

  @Override
  public String toString() {
    return "CoprocessorRange{"
        + "backOffer="
        + backOffer
        + ", region="
        + region
        + ", startKey="
        + startKey
        + ", endKey="
        + endKey
        + '}';
  }
}
