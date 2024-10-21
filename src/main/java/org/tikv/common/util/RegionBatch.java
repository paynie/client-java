package org.tikv.common.util;

import org.tikv.common.region.TiRegion;

public class RegionBatch {
  protected final BackOffer backOffer;
  protected volatile TiRegion region;

  public RegionBatch(BackOffer backOffer, TiRegion region) {
    this.backOffer = backOffer;
    this.region = region;
  }

  public BackOffer getBackOffer() {
    return ConcreteBackOffer.create(backOffer);
  }

  public TiRegion getRegion() {
    return region;
  }

  public void setRegion(TiRegion region) {
    this.region = region;
  }
}
