package com.github.nivdayan.FilterLibrary.filters;

import com.github.nivdayan.FilterLibrary.bitmap_implementations.QuickBitVectorWrapper;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;

import java.io.Serializable;

public class BasicInfiniFilter extends QuotientFilter implements Serializable
{

	protected long empty_fingerprint;
	int num_void_entries = 0;
	FingerprintGrowthStrategy.FalsePositiveRateExpansion fprStyle = FingerprintGrowthStrategy.FalsePositiveRateExpansion.UNIFORM;
	int num_distinct_void_entries = 0;
	
	public void set_fpr_style(FingerprintGrowthStrategy.FalsePositiveRateExpansion val) {
		fprStyle = val;
	}

	BasicInfiniFilter() {super();}

	BasicInfiniFilter(int power_of_two, int bits_per_entry) {
		super(power_of_two, bits_per_entry);
		max_entries_before_expansion = (long)(Math.pow(2, power_of_two_size) * expansion_threshold);
		set_empty_fingerprint(fingerprintLength);
	}
	
	void set_empty_fingerprint(long fp_length) {
		empty_fingerprint = (1L << fp_length) - 2L;
	}
	
	protected boolean compare(long index, long fingerprint) {
		long generation = parse_unary(index);
		long first_fp_bit = index * bitPerEntry + 3;
		long last_fp_bit = index * bitPerEntry + 3 + fingerprintLength - (generation + 1);
		long actual_fp_length = last_fp_bit - first_fp_bit;
		long existing_fingerprint = filter.getFromTo(first_fp_bit, last_fp_bit);
		long mask = (1L << actual_fp_length) - 1L;
		long adjusted_saught_fp = fingerprint & mask;
		return existing_fingerprint == adjusted_saught_fp;
	}
		
	// this is the newer version of parsing the unary encoding. 
	// it is done using just binary operations and no loop. 
	// however, this optimization didn't yield much performance benefit 
	long parse_unary(long slot_index) {
		long f = get_fingerprint(slot_index);
		//.out.println();
		//System.out.println(get_pretty_str(slot_index));
		//print_long_in_binary(f, 32);
		long inverted_fp = ~f;
		//print_long_in_binary(inverted_fp, 32);
		long mask = (1L << fingerprintLength) - 1;
		//print_long_in_binary(mask, 32);
		long masked = mask & inverted_fp;
		//print_long_in_binary(masked, 32);
		long highest = Long.highestOneBit(masked);
		//print_long_in_binary(highest, 32);
		long leading_zeros = Long.numberOfTrailingZeros(highest);
		//System.out.println( leading_zeros );
		long age = fingerprintLength - leading_zeros - 1;
		//System.out.println( age );
		return age;
	}

	long parse_unary_by_finger_print(long fingerprint) {
		long inverted_fp = ~fingerprint;
		//print_long_in_binary(inverted_fp, 32);
		long mask = (1L << fingerprintLength) - 1;
		//print_long_in_binary(mask, 32);
		long masked = mask & inverted_fp;
		//print_long_in_binary(masked, 32);
		long highest = Long.highestOneBit(masked);
		//print_long_in_binary(highest, 32);
		long leading_zeros = Long.numberOfTrailingZeros(highest);
		//System.out.println( leading_zeros );
		long age = fingerprintLength - leading_zeros - 1;
		//System.out.println( age );
		return age;
	}
	
	// TODO if we rejuvenate a void entry, we should subtract from num_void_entries 
	// as if this count reaches zero, we can have shorter chains
	public boolean rejuvenate(long key) {
		long large_hash = get_hash(key);
		long fingerprint = gen_fingerprint(large_hash);
		long ideal_index = get_slot_index(large_hash);
		
		boolean does_run_exist = is_occupied(ideal_index);
		if (!does_run_exist) {
			return false;
		}
		
		long run_start_index = find_run_start(ideal_index);
		long smallest_index = find_largest_matching_fingerprint_in_run(run_start_index, fingerprint);
		if (smallest_index == -1) {
			return false;
		}
		swap_fingerprints(smallest_index, fingerprint);
		return true; 
	}

	
	long decide_which_fingerprint_to_delete(long index, long fingerprint) {
		return find_largest_matching_fingerprint_in_run(index, fingerprint);
	}
	
	// returns the index of the entry if found, -1 otherwise
	long find_largest_matching_fingerprint_in_run(long index, long fingerprint) {
		assert(!is_continuation(index));
		long matching_fingerprint_index = -1;
		long lowest_age = Integer.MAX_VALUE;
		do {
			if (compare(index, fingerprint)) {
				//System.out.println("found matching FP at index " + index);
				long age = parse_unary(index);
				if (age < lowest_age) {
					lowest_age = age;
					matching_fingerprint_index = index;
				}
			}
			index++;
		} while (is_continuation(index));
		return matching_fingerprint_index; 
	}
	
	long gen_fingerprint(long large_hash) {
		long fingerprint_mask = (1L << fingerprintLength) - 1L;
		fingerprint_mask = fingerprint_mask << power_of_two_size;
		long fingerprint = (large_hash & fingerprint_mask) >> power_of_two_size;
		long unary_mask = ~(1L << (fingerprintLength - 1L));
		long updated_fingerprint = fingerprint & unary_mask;
		/*System.out.println(); 
		print_long_in_binary(unary_mask, fingerprintLength);
		print_long_in_binary( fingerprint, fingerprintLength);
		print_long_in_binary( updated_fingerprint, fingerprintLength);*/
		return updated_fingerprint;
	}
	
	void handle_empty_fingerprint(long bucket_index, QuotientFilter insertee) {
//		System.out.println("called");
		/*long bucket1 = bucket_index;
		long bucket_mask = 1L << power_of_two_size; 		// setting this bit to the proper offset of the slot address field
		long bucket2 = bucket1 | bucket_mask;	// adding the pivot bit to the slot address field
		insertee.insert(empty_fingerprint, bucket1, false);
		insertee.insert(empty_fingerprint, bucket2, false);*/
	}
	
	private static int prep_unary_mask(int prev_FP_size, int new_FP_size) {
		int fingerprint_diff = new_FP_size - prev_FP_size;
		
		int unary_mask = 0;
		for (int i = 0; i < fingerprint_diff + 1; i++) {
			unary_mask <<= 1;
			unary_mask |= 1;
		}
		unary_mask <<= new_FP_size - 1 - fingerprint_diff;
		return unary_mask;
	}
	
	int get_num_void_entries() {
		int num = 0;
		for (long i = 0; i < get_physcial_num_slots(); i++) {
			long fp = get_fingerprint(i);
			if (fp == empty_fingerprint) {
				num++;
			}
		}
		return num;
	}
	
	void report_void_entry_creation(long slot) {
		num_distinct_void_entries++;
	}
	
	boolean expand() {
		if (is_full()) {
			return false;
		}
		int new_fingerprint_size = FingerprintGrowthStrategy.get_new_fingerprint_size(original_fingerprint_size, num_expansions, fprStyle);
		//System.out.println("FP size: " + new_fingerprint_size);
		new_fingerprint_size = Math.max(new_fingerprint_size, fingerprintLength);
		QuotientFilter new_qf = new QuotientFilter(power_of_two_size + 1, new_fingerprint_size + 3);
		Iterator it = new Iterator(this);
		long unary_mask = prep_unary_mask(fingerprintLength, new_fingerprint_size);
		
		long current_empty_fingerprint = empty_fingerprint;
		set_empty_fingerprint(new_fingerprint_size);
		//print_long_in_binary(current_empty_fingerprint, 32);
		//print_long_in_binary(empty_fingerprint, 32);
		num_void_entries = 0;
		
		while (it.next()) {
			long bucket = it.bucket_index;
			long fingerprint = it.fingerprint;
			if (it.fingerprint != current_empty_fingerprint) {
				long pivot_bit = (1 & fingerprint);	// getting the bit of the fingerprint we'll be sacrificing 
				long bucket_mask = pivot_bit << power_of_two_size; // setting this bit to the proper offset of the slot address field
				long updated_bucket = bucket | bucket_mask;	 // adding the pivot bit to the slot address field
				long chopped_fingerprint = fingerprint >> 1; // getting rid of this pivot bit from the fingerprint 
				long updated_fingerprint = chopped_fingerprint | unary_mask;				
				new_qf.insert(updated_fingerprint, updated_bucket, false);
				
				num_void_entries += updated_fingerprint == empty_fingerprint ? 1 : 0;
				//print_long_in_binary(updated_fingerprint, 32);
				if (updated_fingerprint == empty_fingerprint) {
					report_void_entry_creation(updated_bucket);
				}
				//if (updated_fingerprint == empty_fingerprint) {
				//	num_void_entries++;
					//is_full = true;
				//}
				/*System.out.println(bucket); 
				System.out.print("bucket1      : ");
				print_int_in_binary( bucket, power_of_two_size);
				System.out.print("fingerprint1 : ");
				print_int_in_binary((int) fingerprint, fingerprintLength);
				System.out.print("pivot        : ");
				print_int_in_binary((int) pivot_bit, 1);
				System.out.print("mask        : ");
				print_int_in_binary((int) unary_mask, new_fingerprint_size);
				System.out.print("bucket2      : ");
				print_int_in_binary((int) updated_bucket, power_of_two_size + 1);
				System.out.print("fingerprint2 : ");
				print_int_in_binary((int) updated_fingerprint, new_fingerprint_size);
				System.out.println();
				System.out.println();*/
			}
			else {
				handle_empty_fingerprint(it.bucket_index, new_qf);
			}
		}
		
		empty_fingerprint = (1L << new_fingerprint_size) - 2 ;
		fingerprintLength = new_fingerprint_size;
		bitPerEntry = new_fingerprint_size + 3;
		filter = new_qf.filter;
		num_existing_entries = new_qf.num_existing_entries;
		power_of_two_size++;
		num_extension_slots += 2;
		max_entries_before_expansion = (int)(Math.pow(2, power_of_two_size) * expansion_threshold);
		last_empty_slot = new_qf.last_empty_slot;
		last_cluster_start = new_qf.last_cluster_start;
		backward_steps = new_qf.backward_steps;
		if (num_void_entries > 0) {
			//is_full = true;
		}
		return true;
	}

	private long setZero(long x, int pos) {
		return x & ~(1L << pos);
	}

	boolean sacrifice() {
		if (is_full()) {
			return false;
		}
		int new_fingerprint_size = fingerprintLength - 1;

		QuotientFilter new_qf = new QuotientFilter(power_of_two_size, new_fingerprint_size + 3);
		Iterator it = new Iterator(this);

		long current_empty_fingerprint = empty_fingerprint;
		set_empty_fingerprint(new_fingerprint_size);
		num_void_entries = 0;

		while (it.next()) {
			long bucket = it.bucket_index;
			long fingerprint = it.fingerprint;
			if (it.fingerprint != current_empty_fingerprint) {

				long generation = parse_unary_by_finger_print(fingerprint);
				long actual_fp_length = fingerprintLength - (generation + 1);
				long real_fingerprint = fingerprint & ((1L << actual_fp_length) - 1);

				long updated_fingerprint = ((1L << (generation + 1)) - 2) << (actual_fp_length - 1) | setZero(real_fingerprint, (int) (actual_fp_length - 1));

				new_qf.insert(updated_fingerprint, bucket, false);

				num_void_entries += updated_fingerprint == empty_fingerprint ? 1 : 0;
				if (updated_fingerprint == empty_fingerprint) {
					report_void_entry_creation(bucket);
				}
			}
			else {
				handle_empty_fingerprint(it.bucket_index, new_qf);
			}
		}

		empty_fingerprint = (1L << new_fingerprint_size) - 2 ;
		fingerprintLength = new_fingerprint_size;
		bitPerEntry = new_fingerprint_size + 3;
		filter = new_qf.filter;
		num_existing_entries = new_qf.num_existing_entries;
//		num_extension_slots += 2;
		max_entries_before_expansion = (int)(Math.pow(2, power_of_two_size) * expansion_threshold);
		last_empty_slot = new_qf.last_empty_slot;
		last_cluster_start = new_qf.last_cluster_start;
		backward_steps = new_qf.backward_steps;
		if (num_void_entries > 0) {
			//is_full = true;
		}
		return true;
	}
	
	boolean is_full() {
		return num_void_entries > 0;
	}
	
	public void print_filter_summary() {
		super.print_filter_summary();
		int num_void_entries = get_num_void_entries();
		System.out.println("void entries: " + num_void_entries);
		System.out.println("is full: " + is_full);
		System.out.println("original fingerprint size: " + original_fingerprint_size);
		System.out.println("num expansions : " + num_expansions);
	}

	protected void writeTo(Output output) {
//		Output output = new Output(os);
		output.writeLong(empty_fingerprint);
		output.writeInt(num_void_entries);
		output.writeInt(num_distinct_void_entries);
		output.writeInt(bitPerEntry);
		output.writeInt(fingerprintLength);
		output.writeInt(power_of_two_size);
		output.writeInt(num_extension_slots);
		output.writeInt(num_existing_entries);

		QuickBitVectorWrapper quickBitVectorWrapper = (QuickBitVectorWrapper) filter;
		output.writeInt(quickBitVectorWrapper.bs.length);
//		System.out.println("length output = " + quickBitVectorWrapper.bs.length);
		if (quickBitVectorWrapper.bs.length > 0) {
			output.writeLongs(quickBitVectorWrapper.bs, 0, quickBitVectorWrapper.bs.length);
		}

		output.writeLong(last_empty_slot);
		output.writeLong(last_cluster_start);
		output.writeLong(backward_steps);
		output.writeDouble(expansion_threshold);
		output.writeLong(max_entries_before_expansion);
		output.writeBoolean(expand_autonomously);
		output.writeBoolean(is_full);
		output.writeLong(num_runs);
		output.writeLong(num_clusters);
		output.writeDouble(avg_run_length);
		output.writeDouble(avg_cluster_length);
		output.writeInt(original_fingerprint_size);
		output.writeInt(num_expansions);
//		System.out.println("output num_expansions: " + num_expansions);
	}

	protected BasicInfiniFilter read(Input input) {
//		BasicInfiniFilter result = new BasicInfiniFilter(3, 10);
		BasicInfiniFilter result = new BasicInfiniFilter();
//		Input input = new Input(is);

		result.empty_fingerprint = input.readLong();
		result.num_void_entries = input.readInt();
		result.num_distinct_void_entries = input.readInt();
		result.bitPerEntry = input.readInt();
		result.fingerprintLength = input.readInt();
		result.power_of_two_size = input.readInt();
		result.num_extension_slots = input.readInt();
		result.num_existing_entries = input.readInt();

		int length = input.readInt();
		result.filter = new QuickBitVectorWrapper();
//		System.out.println("length input = " + length);
		if (length > 0) {
			((QuickBitVectorWrapper) (result.filter)).bs = input.readLongs(length);
		}

		result.last_empty_slot = input.readLong();
		result.last_cluster_start = input.readLong();
		result.backward_steps = input.readLong();
		result.expansion_threshold = input.readDouble();
		result.max_entries_before_expansion = input.readLong();
		result.expand_autonomously = input.readBoolean();
		result.is_full = input.readBoolean();
		result.num_runs = input.readLong();
		result.num_clusters = input.readLong();
		result.avg_run_length = input.readDouble();
		result.avg_cluster_length = input.readDouble();
		result.original_fingerprint_size = input.readInt();
		result.num_expansions = input.readInt();
//		System.out.println("input num_expansions: " + result.num_expansions);

		return result;
	}

	protected void read(Input input, ChainedInfiniFilter result) {
		result.empty_fingerprint = input.readLong();
		result.num_void_entries = input.readInt();
		result.num_distinct_void_entries = input.readInt();
		result.bitPerEntry = input.readInt();
		result.fingerprintLength = input.readInt();
		result.power_of_two_size = input.readInt();
		result.num_extension_slots = input.readInt();
		result.num_existing_entries = input.readInt();

		int length = input.readInt();
//		System.out.println("length input = " + length);
		result.filter = new QuickBitVectorWrapper();

		if (length > 0) {
			((QuickBitVectorWrapper) (result.filter)).bs = input.readLongs(length);
		}

		result.last_empty_slot = input.readLong();
		result.last_cluster_start = input.readLong();
		result.backward_steps = input.readLong();
		result.expansion_threshold = input.readDouble();
		result.max_entries_before_expansion = input.readLong();
		result.expand_autonomously = input.readBoolean();
		result.is_full = input.readBoolean();
		result.num_runs = input.readLong();
		result.num_clusters = input.readLong();
		result.avg_run_length = input.readDouble();
		result.avg_cluster_length = input.readDouble();
		result.original_fingerprint_size = input.readInt();
		result.num_expansions = input.readInt();
//		System.out.println("input num_expansions: " + result.num_expansions);
	}
	
	/*public void print_filter_summary() {	
		super.print_filter_summary();
		
		TreeMap<Integer, Integer> histogram = new TreeMap<Integer, Integer>();
		
		for (int i = 0; i <= fingerprintLength; i++) {
			histogram.put(i, 0);
		}
		
		
		for (int i = 0; i < get_logical_num_slots_plus_extensions(); i++) {
			int age = parse_unary(i); 
			int count = histogram.get(age);
			histogram.put(age, count + 1);
		}
		
		System.out.println("fingerprint sizes histogram");
		System.out.println("\tFP size" + "\t" + "count");
		for ( Entry<Integer, Integer> e : histogram.entrySet() ) {
			int fingerprint_size = fingerprintLength - e.getKey() - 1;
			if (fingerprint_size >= 0) {
				System.out.println("\t" + fingerprint_size + "\t" + e.getValue());
			}
		}
		
	}*/

}
