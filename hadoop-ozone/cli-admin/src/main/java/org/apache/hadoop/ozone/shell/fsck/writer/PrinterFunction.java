/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

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
