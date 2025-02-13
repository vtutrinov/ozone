package org.apache.hadoop.ozone.shell.fsck;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import picocli.CommandLine;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.util.stream.Stream;

import static org.apache.ratis.util.Preconditions.assertTrue;
import static org.junit.jupiter.params.provider.Arguments.arguments;

class TestOzoneFsckCommandInvalidArgs {

  private static Stream<Arguments> invalidArgs() {
    return Stream.of(
      arguments((Object) new String[]{}),
            arguments((Object) new String[]{"--bucket-prefix=bucket"})
    );
  }

  @ParameterizedTest
  @MethodSource("invalidArgs")
  void testFsckInvalidArgs(String[] args) {
    ByteArrayOutputStream errContent = new ByteArrayOutputStream();
    System.setErr(new PrintStream(errContent));
    CommandLine cmdLine = new CommandLine(new OzoneFsckCommand());
    cmdLine.execute(args);
    String actualErr = errContent.toString();
    assertTrue(actualErr.contains("Missing required option: '--volume-prefix=<volumePrefix>'"));
  }
}
