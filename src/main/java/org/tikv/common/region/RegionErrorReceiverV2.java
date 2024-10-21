package org.tikv.common.region;

import org.tikv.common.util.BackOffer;

public class RegionErrorReceiverV2 implements RegionErrorReceiver {
  private final TiRegion region;

  public RegionErrorReceiverV2(TiRegion region) {
    this.region = region;
  }

  @Override
  public boolean onNotLeader(TiRegion region, BackOffer backOffer) {
    return false;
  }

  @Override
  public boolean onStoreUnreachable(BackOffer backOffer) {
    return false;
  }

  @Override
  public TiRegion getRegion() {
    return region;
  }
}
