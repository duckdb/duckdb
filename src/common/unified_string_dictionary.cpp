#include "duckdb/common/unified_string_dictionary.hpp"
#include "duckdb/common/types/hash.hpp"
#include "duckdb/common/helper.hpp"
#include <cmath>

namespace duckdb {

UnifiedStringsDictionary::UnifiedStringsDictionary(idx_t usd_sf) {
	// usd_sf must be a power of two
	D_ASSERT((usd_sf & (usd_sf - 1)) == 0);

	usd_scale_factor = usd_sf;
	if (usd_scale_factor == 0) {
		return;
	}

	// The extra bits are used to increase the slot_bits that determine how many bits are needed to index into the data
	// region. The baseline size of data region in USD is 512kB (64k chunks of 8-bytes) and require 16 bits to index
	// into. Now for example, if the usd_sf is 2, we require 17 bits to slot into the 128k chunks of 8-byte and so on.
	auto extra_bits = static_cast<idx_t>(std::log2(usd_sf));
	slot_bits = SLOT_BITS_BASELINE + extra_bits;
	slot_mask = (1ULL << (slot_bits)) - 1ULL;

	total_slots = usd_sf * BASELINE_SLOT_COUNT;

	auto buffer_bytes_needed = total_slots * DATA_REGION_SLOT_SIZE + total_slots * HT_BUCKET_SIZE + 8;
	buffer = make_unsafe_uniq_array_uninitialized<data_t>(buffer_bytes_needed);

	HT = reinterpret_cast<atomic<uint32_t> *>(buffer.get());
	// should be 8-byte aligned
	DataRegion = reinterpret_cast<uint64_t *>(
	    AlignValue(reinterpret_cast<uint64_t>(buffer.get() + total_slots * HT_BUCKET_SIZE)));

	// We zero the hashtable, since we need an indicator if a bucket has been filled or not
	memset(buffer.get(), 0, total_slots * HT_BUCKET_SIZE);
	// As we store the hash in the slot before the start of the string, slot 0 cannot be used. Also, value 1 is
	// reserved as the SENTINEL value, therefore, it is initialized with 2.
	current_empty_slot.store(2);
	failed_attempts = 0;
}

bool UnifiedStringsDictionary::CheckEqualityAndUpdatePtr(string_t &str, idx_t bucket_idx) {
	auto slot_ptr = data_ptr_cast(DataRegion + (HT[bucket_idx].load(std::memory_order_relaxed) & slot_mask));
	if (memcmp(slot_ptr, str.GetDataUnsafe(), str.GetSize()) == 0) {
		// make sure the string in the USD is null-terminated
		if (slot_ptr[str.GetSize()] == '\0') {
			str.SetPointer(AddTag(char_ptr_cast(slot_ptr)));
			return true;
		}
	}
	return false;
}

bool UnifiedStringsDictionary::WaitUntilSlotResolves(idx_t bucket_idx) {
	while (true) {
		auto bucket = HT[bucket_idx].load(std::memory_order_acquire);
		if (bucket == 0) { // bucket is rolled back, unable to insert string due to full size
			return false;
		}
		if ((bucket & slot_mask) != HT_DIRTY_SENTINEL) {
			return true;
		}
	}
}

USDInsertResult UnifiedStringsDictionary::Insert(string_t &str) {
	// no support for inlined strings
	// FIXME: the first condition should be IsInlined, change for bug test
	if (str.GetSize() <= 12 || str.GetSize() > MAX_STRING_LENGTH) {
		return USDInsertResult::INVALID;
	}

	// disable Unified string dictionary if passed attempt threshold to stop performance loss
	if (failed_attempts > FAILED_ATTEMPT_THRESHOLD) {
		return USDInsertResult::REJECTED_FULL;
	}

	// FIXME: technically, there is no need for a USD with size zero,
	//  but there are some tests that use verification which causes CI failure
	if (usd_scale_factor == 0) {
		return USDInsertResult::INVALID;
	}

	return InsertInternal(str);
}

USDInsertResult UnifiedStringsDictionary::InsertInternal(string_t &str) {
	hash_t string_hash = Hash(str.GetData(), str.GetSize());
	uint32_t string_hash_prefix = Load<uint32_t>(reinterpret_cast<const_data_ptr_t>(&string_hash));

	// Used as the index into the linear probing hash table
	uint32_t bucket_index = string_hash_prefix & slot_mask;
	// Will be compared to the salt in the HT bucket to be even more sure before performing memcmp of full strings
	uint32_t string_hash_salt = string_hash_prefix >> slot_bits;
	// Combines the salt with a sentinel value to mark the bucket as "dirty" to signal the other thread that a string is
	// being inserted
	uint32_t dirty_bucket_value = (string_hash_salt << slot_bits) | HT_DIRTY_SENTINEL;

	D_ASSERT(bucket_index <= total_slots);

	for (idx_t i = 0; i < PROBING_LIMIT; i++) {
		idx_t prob_index = i;
		if (bucket_index + i >= total_slots) {
			prob_index = (bucket_index + i) % total_slots;
		}
		uint32_t HT_bucket = HT[bucket_index + prob_index].load(std::memory_order_acquire);
		uint32_t HT_bucket_salt = HT_bucket >> slot_bits;
		if (HT_bucket == 0) {
			// dirty the bucket
			uint32_t expected = 0;
			if (HT[bucket_index + prob_index].compare_exchange_strong(
			        expected, dirty_bucket_value, std::memory_order_release, std::memory_order_relaxed)) {
				// calculates how many 8-bytes slots is needed for the hash + string + null character
				auto total_bytes_needed = str.GetSize() + sizeof(hash_t) + 1;
				auto slots_needed =
				    (total_bytes_needed % 8 == 0) ? total_bytes_needed / 8 : 1 + (total_bytes_needed / 8);
				// reserve the capacity needed in the data region
				auto slot_to_insert = current_empty_slot.fetch_add(slots_needed);
				// check if enough space is left
				if (slot_to_insert + slots_needed - 1 > total_slots) {
					// give back the reserved slots
					current_empty_slot.fetch_sub(slots_needed, std::memory_order_relaxed);
					// clear the dirtied bucket
					HT[bucket_index + prob_index].store(0, std::memory_order_release);
					return USDInsertResult::REJECTED_FULL;
				}
				// build the new bucket value
				uint32_t new_bucket = UnsafeNumericCast<uint32_t>(string_hash_salt);
				new_bucket = new_bucket << (slot_bits);
				new_bucket |= slot_to_insert;

				auto slot_ptr = data_ptr_cast(DataRegion + slot_to_insert);
				// modify the data region
				memcpy(slot_ptr, str.GetData(), str.GetSize());
				memset(slot_ptr + str.GetSize(), '\0', 1);
				Store<uint64_t>(string_hash, slot_ptr - sizeof(hash_t));
				// finalize the hash table bucket
				HT[bucket_index + prob_index].store(new_bucket, std::memory_order_release);
				// Input string points into the USD backed string
				str.SetPointer(AddTag(char_ptr_cast(slot_ptr)));
				return USDInsertResult::SUCCESS;
			} else { // lost the race to dirty the bucket, check if the winner is trying to insert the same string, if
				     // so wait, else continue probing
				if (expected == dirty_bucket_value) {
					// the thread that won is most likely inserting the same string, wait
					if (!WaitUntilSlotResolves(bucket_index + prob_index)) {
						return USDInsertResult::REJECTED_FULL;
					}
					if (CheckEqualityAndUpdatePtr(str, bucket_index + prob_index)) {
						return USDInsertResult::ALREADY_EXISTS;
					} else {
						continue;
					}
				} else {
					continue;
				}
			}
		} else if (HT_bucket_salt == string_hash_salt &&
		           (HT_bucket & slot_mask) == HT_DIRTY_SENTINEL) { // dirtied but the salt matches, wait until the other
			                                                       // thread finishes, then check again
			if (!WaitUntilSlotResolves(bucket_index + prob_index)) {
				return USDInsertResult::REJECTED_FULL;
			}
			if (CheckEqualityAndUpdatePtr(str, bucket_index + prob_index)) {
				return USDInsertResult::ALREADY_EXISTS;
			} else {
				continue;
			}
		} else if (HT_bucket_salt ==
		           string_hash_salt) { // the salt matches, string already exists, set the input string to
			// point to the materialized string
			if (CheckEqualityAndUpdatePtr(str, bucket_index + prob_index)) {
				return USDInsertResult::ALREADY_EXISTS;
			} else {
				continue;
			}
		}
	}
	return USDInsertResult::REJECTED_PROBING;
}

void UnifiedStringsDictionary::UpdateFailedAttempts(idx_t n_failed) {
	failed_attempts += n_failed;
}

UnifiedStringsDictionary::~UnifiedStringsDictionary() {
	this->buffer.reset();
}

hash_t UnifiedStringsDictionary::LoadHash(string_t &str) {
	return *(reinterpret_cast<uint64_t *>(data_ptr_cast(str.GetPointer()) - (sizeof(hash_t))));
}

char *UnifiedStringsDictionary::AddTag(char *ptr) {
#ifndef DUCKDB_DISABLE_POINTER_SALT
	return reinterpret_cast<char *>(reinterpret_cast<uint64_t>(ptr) | string_t::UNIFIED_STRING_DICTIONARY_SALT_MASK);
#else
	return ptr;
#endif
}
} // namespace duckdb
