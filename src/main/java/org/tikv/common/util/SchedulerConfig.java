package org.tikv.common.util;

import java.util.Map;

public class SchedulerConfig {
  private Map<String, Object> config;

  public Map<String, Object> getConfig() {
    return config;
  }

  public void setConfig(Map<String, Object> config) {
    this.config = config;
  }
}
