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

package org.apache.hadoop.ozone;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import org.junit.jupiter.api.Test;

/**
 * Test for VolumeIterator.
 */
public class TestVolumeIterator {

  @Test
  public void testIterator() throws IOException {
    // given
    Volume volume1 = mockVolume("volume1");
    Volume volume2 = mockVolume("volume2");
    Volume volume3 = mockVolume("volume3");
    Volume volume4 = mockVolume("volume4");

    List<Volume> volumesShouldBeHandled = Arrays.asList(volume1, volume2, volume3, volume4);
    List<Volume> firstBunchOfVolumes = volumesShouldBeHandled.subList(0, 2);
    List<Volume> secondBunchOfVolumes = volumesShouldBeHandled.subList(2, 4);
    List<Volume> thirdBunchOfVolumes = Collections.emptyList();

    VolumeIterator.VolumeListProvider<Volume> volumeListProvider = mock(VolumeIterator.VolumeListProvider.class);
    when(volumeListProvider.getList(eq("user1"), eq(""), eq(""), eq(2)))
        .thenReturn(firstBunchOfVolumes);
    when(volumeListProvider.getList(eq("user1"), eq(""), eq("volume2"), eq(2)))
        .thenReturn(secondBunchOfVolumes);
    when(volumeListProvider.getList(eq("user1"), eq(""), eq("volume4"), eq(2)))
        .thenReturn(thirdBunchOfVolumes);

    VolumeIterator<Volume> volumeIterator = spy(new VolumeIterator<>("", "", "user1", 2, volumeListProvider));
    int iterationIndex = 0;

    // when
    while (volumeIterator.hasNext()) {
      Volume volume = volumeIterator.next();
      // then
      assertEquals(volumesShouldBeHandled.get(iterationIndex), volume);
      iterationIndex++;
    }

    // and also
    verify(volumeListProvider, times(3)).getList(eq("user1"), eq(""), anyString(), eq(2));
    verify(volumeIterator, times(4)).next();
  }

  private Volume mockVolume(String volumeName) {
    Volume volume = mock(Volume.class);
    when(volume.getName()).thenReturn(volumeName);
    return volume;
  }

}
