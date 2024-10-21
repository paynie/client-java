package org.tikv.common.util;

import java.util.List;
import org.tikv.common.region.TiRegion;
import org.tikv.kvproto.Kvrpcpb.WriteOp;

public class WriteBatch extends RegionBatch {
  private final List<WriteOp> writeOps;

  public WriteBatch(BackOffer backOffer, TiRegion region, List<WriteOp> writeOps) {
    super(ConcreteBackOffer.create(backOffer), region);
    this.writeOps = writeOps;
  }

  public List<WriteOp> getWriteOps() {
    return writeOps;
  }
}
