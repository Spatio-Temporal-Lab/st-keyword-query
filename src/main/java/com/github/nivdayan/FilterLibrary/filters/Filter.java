package com.github.nivdayan.FilterLibrary.filters;

import com.github.nivdayan.FilterLibrary.bitmap_implementations.Bitmap;

import java.io.Serializable;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

public abstract class Filter implements Serializable {
	
	HashType hash_type;
	
	abstract boolean rejuvenate(long key);
	abstract boolean expand();
	protected abstract boolean _delete(long large_hash);
	abstract protected boolean _insert(long large_hash, boolean insert_only_if_no_match);
	protected boolean _insert(long large_hash, int t, boolean insert_only_if_no_match) {return true;}
	abstract protected boolean _search(long large_hash);
	protected boolean _search(long large_hash, int t) { return true; }


	public boolean delete(long input) {
		return _delete(get_hash(input));
	}

	public boolean delete(String input) {
		ByteBuffer input_buffer = ByteBuffer.wrap(input.getBytes(StandardCharsets.UTF_8));
		return _delete(HashFunctions.xxhash(input_buffer));
	}

	public boolean delete(byte[] input) {
		ByteBuffer input_buffer = ByteBuffer.wrap(input);
		return _delete(HashFunctions.xxhash(input_buffer));
	}
	
	public boolean insert(long input, boolean insert_only_if_no_match) {		
		long hash = get_hash(input);
		return _insert(hash, insert_only_if_no_match);
	}

	public boolean insert(String input, boolean insert_only_if_no_match) {
		ByteBuffer input_buffer = ByteBuffer.wrap(input.getBytes(StandardCharsets.UTF_8));
		return _insert(HashFunctions.xxhash(input_buffer), insert_only_if_no_match);
	}

	public boolean insert(byte[] input, boolean insert_only_if_no_match) {
		ByteBuffer input_buffer = ByteBuffer.wrap(input);
		return _insert(HashFunctions.xxhash(input_buffer), insert_only_if_no_match);
	}

	public boolean insert(byte[] input, int t, boolean insert_only_if_no_match, int seed) {
		ByteBuffer input_buffer = ByteBuffer.wrap(input);
		return _insert(HashFunctions.xxhash(input_buffer, seed), t, insert_only_if_no_match);
	}
	
	public boolean search(long input) {
		return _search(get_hash(input));
	}

	public boolean search(String input) {
		ByteBuffer input_buffer = ByteBuffer.wrap(input.getBytes(StandardCharsets.UTF_8));
		return _search(HashFunctions.xxhash(input_buffer));
	}

	public boolean search(byte[] input) {
		ByteBuffer input_buffer = ByteBuffer.wrap(input);
		return _search(HashFunctions.xxhash(input_buffer));
	}

	public boolean search(byte[] input, int t, int seed) {
		ByteBuffer input_buffer = ByteBuffer.wrap(input);
		return _search(HashFunctions.xxhash(input_buffer, seed), t);
	}

	public boolean search(byte[] input, int l, int r, int seed) {
		for (int i = l; i <= r; ++i) {
			if (search(input, i, seed)) {
				return true;
			}
		}
		return false;
	}
	
	long get_hash(long input) {
		long hash = 0;
		if (hash_type == HashType.arbitrary) {
			hash = HashFunctions.normal_hash((int)input);
		}
		else if (hash_type == HashType.xxh) {
			hash = HashFunctions.xxhash(input);
		}
		else {
			System.exit(1);
		}
		return hash;
	}
	
	public abstract long get_num_entries(boolean include_all_internal_filters);

	public double get_utilization() {
		return 0;
	}
	
	public double measure_num_bits_per_entry() {
		return 0;
	}
	
	 static void print_int_in_binary(int num, int length) {
		String str = "";
		for (int i = 0; i < length; i++) {
			int mask = (int)Math.pow(2, i);
			int masked = num & mask;
			str += masked > 0 ? "1" : "0";
		}
		System.out.println(str);
	}
	
	 static void print_long_in_binary(long num, int length) {
		String str = "";
		for (int i = 0; i < length; i++) {
			long mask = (long)Math.pow(2, i);
			long masked = num & mask;
			str += masked > 0 ? "1" : "0";
		}
		System.out.println(str);
	}
	
	String get_fingerprint_str(long fp, int length) {
		String str = "";
		for (int i = 0; i < length; i++) {
			str += Bitmap.get_fingerprint_bit(i, fp) ? "1" : "0";
		}
		return str;
	}
	
	public void pretty_print() {

	}
}
