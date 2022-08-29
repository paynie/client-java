package org.tikv.common.region;

public class RawCASUseVersionResponse {
  private final boolean isSuccess;
  private final long currentVersion;

  public RawCASUseVersionResponse(boolean isSuccess, long currentVersion) {
    this.isSuccess = isSuccess;
    this.currentVersion = currentVersion;
  }

  public boolean isSuccess() {
    return isSuccess;
  }

  public long getCurrentVersion() {
    return currentVersion;
  }

  @Override
  public String toString() {
    return "RawCASUseVersionResponse{"
        + "isSuccess="
        + isSuccess
        + ", currentVersion="
        + currentVersion
        + '}';
  }
}
