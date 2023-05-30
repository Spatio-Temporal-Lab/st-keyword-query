/*
 *
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

package org.apache.hadoop.hbase.io.hfile;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.nio.ByteBuff;
import org.apache.hadoop.hbase.regionserver.BloomType;
import org.apache.hadoop.hbase.util.*;
import org.apache.yetus.audience.InterfaceAudience;

import java.io.DataInput;
import java.io.IOException;

/**
 * A Bloom filter implementation built on top of 
 * {@link org.apache.hadoop.hbase.util.BloomFilterChunk}, encapsulating
 * a set of fixed-size Bloom filters written out at the time of
 * {@link org.apache.hadoop.hbase.io.hfile.HFile} generation into the data
 * block stream, and loaded on demand at query time. This class only provides
 * reading capabilities.
 */
@InterfaceAudience.Private
public class CompoundBloomFilter extends CompoundBloomFilterBase
    implements BloomFilter {

  /** Used to load chunks on demand */
  private HFile.Reader reader;

  private HFileBlockIndex.BlockIndexReader index;

  private int hashCount;
  private Hash hash;

  private long[] numQueriesPerChunk;
  private long[] numPositivesPerChunk;

  /**
   * De-serialization for compound Bloom filter metadata. Must be consistent
   * with what {@link CompoundBloomFilterWriter} does.
   *
   * @param meta serialized Bloom filter metadata without any magic blocks
   * @throws IOException
   */
  public CompoundBloomFilter(DataInput meta, HFile.Reader reader)
      throws IOException {
    this.reader = reader;

    totalByteSize = meta.readLong();
    hashCount = meta.readInt();
    hashType = meta.readInt();
    totalKeyCount = meta.readLong();
    totalMaxKeys = meta.readLong();
    numChunks = meta.readInt();
    byte[] comparatorClassName = Bytes.readByteArray(meta);
    // The writer would have return 0 as the vint length for the case of 
    // Bytes.BYTES_RAWCOMPARATOR.  In such cases do not initialize comparator, it can be
    // null
    if (comparatorClassName.length != 0) {
      comparator = FixedFileTrailer.createComparator(Bytes.toString(comparatorClassName));
    }

    hash = Hash.getInstance(hashType);
    if (hash == null) {
      throw new IllegalArgumentException("Invalid hash type: " + hashType);
    }
    // We will pass null for ROW block
    if(comparator == null) {
      index = new HFileBlockIndex.ByteArrayKeyBlockIndexReader(1);
    } else {
      index = new HFileBlockIndex.CellBasedKeyBlockIndexReader(comparator, 1);
    }
    index.readRootIndex(meta, numChunks);
  }

  @Override
  public boolean contains(byte[] key, int keyOffset, int keyLength, ByteBuff bloom) {
    int block = index.rootBlockContainingKey(key, keyOffset, keyLength);
    if (block < 0) {
      return false; // This key is not in the file.
    }
    boolean result;
    HFileBlock bloomBlock = getBloomBlock(block);
    try {
      ByteBuff bloomBuf = bloomBlock.getBufferReadOnly();
      result = BloomFilterUtil.contains(key, keyOffset, keyLength, bloomBuf,
          bloomBlock.headerSize(), bloomBlock.getUncompressedSizeWithoutHeader(), hash, hashCount);
    } finally {
      // After the use return back the block if it was served from a cache.
      reader.returnBlock(bloomBlock);
    }
    if (numPositivesPerChunk != null && result) {
      // Update statistics. Only used in unit tests.
      ++numPositivesPerChunk[block];
    }
    return result;
  }

  @Override
  public boolean containsWithKeywords(byte[] key, byte[][] keywordsByte, int keyOffset, int keyLength, ByteBuff bloom) {
    byte[] maxKeyWithID = ByteUtil.concat(key, ByteUtil.longToByte(Long.MAX_VALUE));
    int maxBlock = index.rootBlockContainingKey(maxKeyWithID, keyOffset, keyLength + 8);
    if (maxBlock < 0) {
      System.out.println("error!");
      return false; // This key is not in the file.
    }
    byte[] minKeyWithID = ByteUtil.concat(key, new byte[]{0, 0, 0, 0, 0, 0, 0, 0});
    int minBlock = Math.max(0, index.rootBlockContainingKey(minKeyWithID, keyOffset, keyLength + 8));
//    int block = index.rootBlockContainingKey(key, keyOffset, keyLength);

//    System.out.println("query bloom block count: " + (maxBlock - minBlock + 1));
    for (int block = minBlock; block <= maxBlock; ++block) {
      HFileBlock bloomBlock = getBloomBlock(block);
      try {
        ByteBuff bloomBuf = bloomBlock.getBufferReadOnly();
        for (byte[] bytes : keywordsByte) {
//          System.out.println("query st key: " + Arrays.toString(key));
//          System.out.println("query keyword key: " + Arrays.toString(bytes));
          if (BloomFilterUtil.contains(ByteUtil.concat(bytes, key), keyOffset, keyLength + 4, bloomBuf,
                  bloomBlock.headerSize(), bloomBlock.getUncompressedSizeWithoutHeader(), hash, hashCount)) {
            return true;
          }
        }
      } finally {
        // After the use return back the block if it was served from a cache.
        reader.returnBlock(bloomBlock);
      }
    }
    return false;
  }

  private HFileBlock getBloomBlock(int block) {
    HFileBlock bloomBlock;
    try {
      // We cache the block and use a positional read.
      bloomBlock = reader.readBlock(index.getRootBlockOffset(block),
          index.getRootBlockDataSize(block), true, true, false, true, BlockType.BLOOM_CHUNK, null);
    } catch (IOException ex) {
      // The Bloom filter is broken, turn it off.
      throw new IllegalArgumentException("Failed to load Bloom block", ex);
    }

    if (numQueriesPerChunk != null) {
      // Update statistics. Only used in unit tests.
      ++numQueriesPerChunk[block];
    }
    return bloomBlock;
  }

  @Override
  public boolean contains(Cell keyCell, ByteBuff bloom, BloomType type) {
    int block = index.rootBlockContainingKey(keyCell);
    if (block < 0) {
      return false; // This key is not in the file.
    }
    boolean result;
    HFileBlock bloomBlock = getBloomBlock(block);
    try {
      ByteBuff bloomBuf = bloomBlock.getBufferReadOnly();
      result = BloomFilterUtil.contains(keyCell, bloomBuf, bloomBlock.headerSize(),
          bloomBlock.getUncompressedSizeWithoutHeader(), hash, hashCount, type);
    } finally {
      // After the use return back the block if it was served from a cache.
      reader.returnBlock(bloomBlock);
    }
    if (numPositivesPerChunk != null && result) {
      // Update statistics. Only used in unit tests.
      ++numPositivesPerChunk[block];
    }
    return result;
  }

  @Override
  public boolean supportsAutoLoading() {
    return true;
  }


  public int getNumChunks() {
    return numChunks;
  }

  public void enableTestingStats() {
    numQueriesPerChunk = new long[numChunks];
    numPositivesPerChunk = new long[numChunks];
  }

  public String formatTestingStats() {
    StringBuilder sb = new StringBuilder();
    for (int i = 0; i < numChunks; ++i) {
      sb.append("chunk #");
      sb.append(i);
      sb.append(": queries=");
      sb.append(numQueriesPerChunk[i]);
      sb.append(", positives=");
      sb.append(numPositivesPerChunk[i]);
      sb.append(", positiveRatio=");
      sb.append(numPositivesPerChunk[i] * 1.0 / numQueriesPerChunk[i]);
      sb.append(";\n");
    }
    return sb.toString();
  }

  public long getNumQueriesForTesting(int chunk) {
    return numQueriesPerChunk[chunk];
  }

  public long getNumPositivesForTesting(int chunk) {
    return numPositivesPerChunk[chunk];
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append(BloomFilterUtil.formatStats(this));
    sb.append(BloomFilterUtil.STATS_RECORD_SEP + 
        "Number of chunks: " + numChunks);
    sb.append(BloomFilterUtil.STATS_RECORD_SEP + 
        ((comparator != null) ? "Comparator: "
        + comparator.getClass().getSimpleName() : "Comparator: "
        + Bytes.BYTES_RAWCOMPARATOR.getClass().getSimpleName()));
    return sb.toString();
  }

}
