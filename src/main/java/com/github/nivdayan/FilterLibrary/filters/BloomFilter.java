package com.github.nivdayan.FilterLibrary.filters;

import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import com.github.nivdayan.FilterLibrary.bitmap_implementations.Bitmap;
import com.github.nivdayan.FilterLibrary.bitmap_implementations.QuickBitVectorWrapper;

import java.io.InputStream;
import java.io.OutputStream;

public class BloomFilter extends Filter {

	Bitmap filter;
	long num_bits; 
	long max_num_entries;
	long current_num_entries;
	long bits_per_entry;
	int num_hash_functions;

	public void writeTo(OutputStream os) {

		Output output = new Output(os);

		QuickBitVectorWrapper quickBitVectorWrapper = (QuickBitVectorWrapper) filter;
		output.writeInt(quickBitVectorWrapper.bs.length);

		if (quickBitVectorWrapper.bs.length > 0) {
			output.writeLongs(quickBitVectorWrapper.bs, 0, quickBitVectorWrapper.bs.length);
		}

		output.writeLong(num_bits);
		output.writeLong(max_num_entries);
		output.writeLong(current_num_entries);
		output.writeLong(bits_per_entry);
		output.writeInt(num_hash_functions);
		output.close();
	}

	public BloomFilter() {}

	public BloomFilter read(InputStream is) {
		BloomFilter bf = new BloomFilter();

		Input input = new Input(is);

		int len = input.readInt();
		bf.filter = new QuickBitVectorWrapper();
		if (len > 0) {
			((QuickBitVectorWrapper) (bf.filter)).bs = input.readLongs(len);
		}
		bf.num_bits = input.readLong();
		bf.max_num_entries = input.readLong();
		bf.current_num_entries = input.readLong();
		bf.bits_per_entry = input.readLong();
		bf.num_hash_functions = input.readInt();
		bf.hash_type = HashType.xxh;
		input.close();
		return bf;
	}

	public BloomFilter(int new_num_entries, int new_bits_per_entry) {
		max_num_entries = new_num_entries;
		filter = new QuickBitVectorWrapper(new_bits_per_entry,  (int)max_num_entries);
		num_bits = new_bits_per_entry * max_num_entries;
		bits_per_entry = new_bits_per_entry;
		num_hash_functions = (int) Math.round( bits_per_entry * Math.log(2) );
		hash_type = HashType.xxh;		
		current_num_entries = 0;
	}

	public BloomFilter(int new_num_entries, int new_bits_per_entry, int seed) {
		max_num_entries = new_num_entries;
		filter = new QuickBitVectorWrapper(new_bits_per_entry,  (int)max_num_entries);
		num_bits = new_bits_per_entry * max_num_entries;
		bits_per_entry = new_bits_per_entry;
		num_hash_functions = 1;
		hash_type = HashType.xxh;
		current_num_entries = 0;
	}
	
	@Override
	boolean rejuvenate(long key) {
		return false;
	}

	@Override
	boolean expand() {
		return false;
	}

	@Override
	protected boolean _delete(long large_hash) {
		return false;
	}
	
	long get_target_bit(long large_hash, int hash_num) {
		long this_hash = HashFunctions.xxhash(large_hash, hash_num);
		return Math.abs(this_hash % num_bits);
	}
	
	@Override
	protected boolean _insert(long large_hash, boolean insert_only_if_no_match) {
		
		long target_bit = Math.abs(large_hash % num_bits);
		filter.set(target_bit, true);
		
		for (int i = 1; i < num_hash_functions; i++) {
			target_bit = get_target_bit(large_hash, i);
			//System.out.println(target_bit);
			filter.set(target_bit, true);
		}
		current_num_entries++;
		return true;
	}

	@Override
	protected boolean _insert(long large_hash, int t, boolean insert_only_if_no_match) {
		long target_bit = Math.abs((large_hash + t) % num_bits);
		filter.set(target_bit, true);
		current_num_entries++;
		return true;
	}

	@Override
	protected boolean _search(long large_hash) {
		
		long target_bit = Math.abs(large_hash % num_bits);
		if (! filter.get(target_bit) ) {
			return false;
		}
		
		for (int i = 1; i < num_hash_functions; i++) {
			target_bit = get_target_bit(large_hash, i);
			if (! filter.get(target_bit) ) {
				return false;
			}
		}
		return true;
	}

	@Override
	protected boolean _search(long large_hash, int t) {
		long target_bit = Math.abs((large_hash + t) % num_bits);
        return filter.get(target_bit);
    }

	@Override
	public long get_num_entries(boolean include_all_internal_filters) {
		return current_num_entries;
	}
	
	public double get_utilization() {
		return current_num_entries / max_num_entries;
	}
	
	public double measure_num_bits_per_entry() {
		return (max_num_entries * bits_per_entry) / (double)current_num_entries;
	}
}
