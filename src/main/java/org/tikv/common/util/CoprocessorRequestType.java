package org.tikv.common.util;

public enum CoprocessorRequestType {
  UPDATE_GRAPH_META(0),
  GET_GRAPH_META(1),
  COUNT(2);

  private final int value;

  CoprocessorRequestType(int i) {
    this.value = i;
  }

  public int getValue() {
    return value;
  }
}
