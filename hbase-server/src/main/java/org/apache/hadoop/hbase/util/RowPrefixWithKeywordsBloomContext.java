/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hbase.util;

import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.io.hfile.HFile;
import org.apache.yetus.audience.InterfaceAudience;

import java.io.IOException;
import java.util.Arrays;

import static org.apache.hadoop.hbase.regionserver.HStoreFile.LAST_BLOOM_KEY;

/**
 * Handles ROWPREFIX bloom related context.
 * It works with both ByteBufferedCell and byte[] backed cells
 */
@InterfaceAudience.Private
public class RowPrefixWithKeywordsBloomContext extends BloomContext {
  private final int prefixLength;

  public RowPrefixWithKeywordsBloomContext(BloomFilterWriter bloomFilterWriter,
                                           CellComparator comparator, int prefixLength) {
    super(bloomFilterWriter, comparator);
    this.prefixLength = prefixLength;
  }

  @Override
  public void addLastBloomKey(HFile.Writer writer) throws IOException {
    if (this.getLastCell() != null) {
      Cell firstOnRow = PrivateCellUtil.createFirstOnRowCol(this.getLastCell());
      // This copy happens only once when the writer is closed
      byte[] key = PrivateCellUtil.getCellKeySerializedAsKeyValueKey(firstOnRow);
      writer.appendFileInfo(LAST_BLOOM_KEY, key);
    }
  }

  @Override
  protected boolean isNewKey(Cell cell) {
    if (this.getLastCell() != null) {
      return Arrays.equals(cell.getQualifierArray(), "keywords".getBytes());
    }
    return true;
  }
}
