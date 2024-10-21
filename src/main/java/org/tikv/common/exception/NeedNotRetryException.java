package org.tikv.common.exception;

public class NeedNotRetryException extends TiKVException {
  public NeedNotRetryException(Throwable e) {
    super(e);
  }

  public NeedNotRetryException(String msg) {
    super(msg);
  }

  public NeedNotRetryException(String msg, Throwable e) {
    super(msg, e);
  }
}
