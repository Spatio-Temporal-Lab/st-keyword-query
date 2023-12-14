package com.github.nivdayan.FilterLibrary.bitmap_implementations;

import org.urbcomp.startdb.stkq.util.ByteUtil;

import java.io.Serializable;
import java.util.Arrays;

public class QuickBitVectorWrapper extends Bitmap implements Serializable {

	public long[] bs;

	@Override
	public byte[] getArray() {
		int n = bs.length;
		byte[] result = new byte[n * 8];
		for (int i = 0; i < n; ++i) {
			byte[] temp = ByteUtil.longToBytes(bs[i]);
            System.arraycopy(temp, 0, result, i * 8, 8);
		}
		return result;
	}

	public QuickBitVectorWrapper(byte[] bytes) {
		int n = bytes.length;
		bs = new long[n / 8];
		for (int i = 0; i < n; i += 8) {
			bs[i >> 3] = ByteUtil.toLong(Arrays.copyOfRange(bytes, i, i + 8));
		}
	}

	public QuickBitVectorWrapper(int bits_per_entry, long num_entries) {
		bs = QuickBitVector.makeBitVector(num_entries, bits_per_entry);
	}

	public QuickBitVectorWrapper() {
		super();
	}

	@Override
	public long size() {
		return (long)bs.length * Long.BYTES * 8L;
	}

	@Override
	public void set(long bit_index, boolean value) {
		if (value) {
			QuickBitVector.set(bs, bit_index);
		}
		else {
			QuickBitVector.clear(bs, bit_index);
		}
	}

	@Override
	public void setFromTo(long from, long to, long value) {
		QuickBitVector.putLongFromTo(bs, value, from, to - 1);
	}

	@Override
	public boolean get(long bit_index) {
		return QuickBitVector.get(bs, bit_index);
	}

	@Override
	public long getFromTo(long from, long to) {
		return QuickBitVector.getLongFromTo(bs, from, to - 1);
	}
	

}
