package org.apache.hadoop.ozone.shell.fsck.writer;

import java.io.IOException;

/**
 * Functional interface representing a printer function that performs a print action.
 * This interface is used to encapsulate a piece of printing logic that can throw an {@link IOException}.
 * <p>
 * Implementations of this interface can be used in contexts where printing operations need to be performed and handled,
 * especially in scenarios involving I/O operations.
 */
@FunctionalInterface
public interface PrinterFunction {
  void print() throws IOException;
}
