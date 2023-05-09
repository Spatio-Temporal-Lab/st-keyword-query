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
import org.apache.yetus.audience.InterfaceAudience;

import java.io.IOException;

/**
 * Handles ROWPREFIX bloom related context.
 * It works with both ByteBufferedCell and byte[] backed cells
 */
@InterfaceAudience.Private
public class RowPrefixWithKeywordsBloomContext extends RowBloomContext {
  private final int prefixLength;

  public RowPrefixWithKeywordsBloomContext(BloomFilterWriter bloomFilterWriter,
                                           CellComparator comparator, int prefixLength) {
    super(bloomFilterWriter, comparator);
    this.prefixLength = prefixLength;
  }

  public void writeBloom(Cell cell) throws IOException {
    super.writeBloom(getRowPrefixCell(cell));
  }

  /**
   * @param cell the cell
   * @return the new cell created by row prefix
   */
  private Cell getRowPrefixCell(Cell cell) {
    byte[] row = CellUtil.copyRow(cell);
    int rowLength = row.length;
    int lastByte = row[prefixLength] & 0xFF;
    lastByte >>>= 2;
    row[prefixLength] = (byte) lastByte;
    return ExtendedCellBuilderFactory.create(CellBuilderType.DEEP_COPY)
        .setRow(row, 0, Math.min(prefixLength, row.length))
        .setType(Cell.Type.Put)
        .build();
  }
}
