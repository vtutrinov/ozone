package org.apache.hadoop.hdds.protocol;

import java.io.IOException;

public class OMInSafeModeException extends IOException {

  public OMInSafeModeException(String message) {
    super(message);
  }
}
