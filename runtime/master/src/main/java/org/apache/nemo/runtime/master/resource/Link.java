package org.apache.nemo.runtime.master.resource;

public class Link {

  private final Integer bw;
  private final Integer latency;

  public Link(Integer bw, Integer latency) {
    super();
    this.bw = bw;
    this.latency = latency;
  }

  public Integer getBw() {
    return bw;
  }

  public Integer getLatency() {
    return latency;
  }

  @Override
  public String toString() {
    return "Link{" +
      "bw='" + bw + '\'' +
      ", latency='" + latency + '\'' +
      '}';
  }
}
