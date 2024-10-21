package org.tikv.common.exception;

public class NeedResplitAndRetryException extends TiKVException {
  public NeedResplitAndRetryException(Throwable e) {
    super(e);
  }

  public NeedResplitAndRetryException(String msg) {
    super(msg);
  }

  public NeedResplitAndRetryException(String msg, Throwable e) {
    super(msg, e);
  }
}
