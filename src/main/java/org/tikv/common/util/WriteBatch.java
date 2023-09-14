package org.tikv.common.util;

import java.util.List;
import org.tikv.common.region.TiRegion;
import org.tikv.kvproto.Kvrpcpb.WriteOp;

public class WriteBatch {

  private final BackOffer backOffer;
  private final TiRegion region;
  private final List<WriteOp> writeOps;

  public WriteBatch(BackOffer backOffer, TiRegion region, List<WriteOp> writeOps) {
    this.backOffer = backOffer;
    this.region = region;
    this.writeOps = writeOps;
  }

  public BackOffer getBackOffer() {
    return backOffer;
  }

  public TiRegion getRegion() {
    return region;
  }

  public List<WriteOp> getWriteOps() {
    return writeOps;
  }
}
