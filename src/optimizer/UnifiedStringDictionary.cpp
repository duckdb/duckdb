#include "duckdb/optimizer/UnifiedStringDictionary.h"
#include "duckdb/common/types/hash.hpp"
#include <cstring>
#include <algorithm>
#include <iostream>
#include <string>
#include "duckdb/common/helper.hpp"
#include <cmath>

namespace duckdb {

UnifiedStringsDictionary::UnifiedStringsDictionary(idx_t size) {
	if (size == 0) {
		USSR_MASK = 0;
		USSR_prefix = 0xFFFFFFFFFF;
		return;
	}

	auto extra_bits = static_cast<idx_t>(std::log(size) / std::log(2));

	required_bits += extra_bits;
	slot_bits += extra_bits;

	USSR_MASK = ~((1ULL << (required_bits)) - 1);
	USSR_SIZE = size * 65536;
	HT_SIZE = USSR_SIZE;

	slot_mask = (1ULL << (slot_bits)) - 1ULL;
	salt_mask = ~slot_mask;

	buffer = make_unsafe_uniq_array_uninitialized<data_t>((size)*BUFFER_SIZE);
	USSR_prefix = cast_pointer_to_uint64(buffer.get() + USSR_SIZE * USSR_SLOT_SIZE) & USSR_MASK;

	DataRegion = reinterpret_cast<uint64_t *>(USSR_prefix);

	// Double check that the DataRegion is contained within the buffer
	D_ASSERT(cast_pointer_to_uint64(buffer.get()) < cast_pointer_to_uint64(DataRegion));
	D_ASSERT(cast_pointer_to_uint64(DataRegion) < cast_pointer_to_uint64(buffer.get()) + size * BUFFER_SIZE);
	D_ASSERT(cast_pointer_to_uint64(DataRegion) + USSR_SIZE * USSR_SLOT_SIZE <=
	         cast_pointer_to_uint64(buffer.get()) + size * BUFFER_SIZE);

	data_ptr_t HT_address;
	// The hash table can be either before or after the data region
	if (USSR_prefix - cast_pointer_to_uint64(buffer.get()) >= HT_SIZE * HT_BUCKET_SIZE) {
		HT_address = buffer.get();
	} else {
		HT_address = cast_uint64_to_pointer(USSR_prefix) + USSR_SIZE * USSR_SLOT_SIZE;
	}

	//	const auto buffer_start = cast_pointer_to_uint64(buffer.get());
	//	const auto buffer_end   = buffer_start + size * BUFFER_SIZE;
	//	const auto ht_start     = cast_pointer_to_uint64(HT_address);
	//	const auto ht_end       = ht_start + HT_SIZE * HT_BUCKET_SIZE;
	//
	//	D_ASSERT(ht_start >= buffer_start);          // HT begins inside the buffer
	//	D_ASSERT(ht_end   <= buffer_end);            // HT ends   inside the buffer
	// We zero the hashtable, since we need an indicator if a bucket as been filled or not
	memset(HT_address, '\0', HT_SIZE * HT_BUCKET_SIZE);

	HT = reinterpret_cast<atomic<uint32_t> *>(HT_address);

	currentEmptySlot.store(2);

	candidates = 0;
	accepted = 0;
	nRejections_Probing = 0;
	nRejections_SizeFull = 0;
	already_in = 0;
}

string_t UnifiedStringsDictionary::insert(string_t str) {
	//	return str;
	// no support for short strings now
	if (str.GetSize() <= 12 || str.GetSize() > MAX_STRING_LENGTH) {
		return str;
	}
	return insertInternal(str);
}

string_t UnifiedStringsDictionary::insertInternal(string_t str) {

	//	if (nRejections_SizeFull.load(std::memory_order_relaxed) > ATTEMPT_THRESHOLD + 1000000) {
	//		return str;
	//	}

	hash_t h = Hash(str);
	//					candidates++;

	uint32_t hash_prefix;
	memcpy(&hash_prefix, &h, HT_BUCKET_SIZE);

	uint32_t HT_slot;
	HT_slot = hash_prefix & slot_mask;
	D_ASSERT(HT_slot <= USSR_SIZE);

	uint32_t HT_salt = hash_prefix >> slot_bits;

	for (idx_t i = 0; i < PROBING_LIMIT + 16; i++) {
		idx_t prob_index = i;

		if (HT_slot + i >= USSR_SIZE) {
			prob_index = (HT_slot + i) % USSR_SIZE;
		}
		uint32_t HT_bucket = (HT + (HT_slot + prob_index))->load(std::memory_order_acquire);

		uint32_t HT_bucket_salt = HT_bucket >> (slot_bits);

		if (HT_bucket == 0) {
			// dirty the bucket
			uint32_t expected = 0;
			if ((HT + (HT_slot + prob_index))
			        ->compare_exchange_strong(expected, (HT_salt << slot_bits) | 1, std::memory_order_release,
			                                  std::memory_order_relaxed)) {
				auto str_len = str.GetSize() + STR_LENGTH_BYTES;
				auto increasedSlot = (str_len % 8 == 0) ? 1 + (str_len / 8) : 2 + (str_len / 8);
				auto slot_to_insert = currentEmptySlot.fetch_add(increasedSlot);
				if (slot_to_insert + increasedSlot > USSR_SIZE || str_len > (USSR_SIZE - slot_to_insert) * 8) {
					//																					nRejections_SizeFull++;
					currentEmptySlot.fetch_sub(increasedSlot, std::memory_order_relaxed);
					(HT + HT_slot + prob_index)->store(0, std::memory_order_release);

					return str;
				}
				uint32_t newBucket = UnsafeNumericCast<uint32_t>(HT_salt);
				newBucket = newBucket << (slot_bits);
				newBucket |= slot_to_insert;

				auto slot_ptr = data_ptr_cast(DataRegion + slot_to_insert);

				const uint16_t len16 = UnsafeNumericCast<uint16_t>(str.GetSize());

				Store<uint16_t>(len16, slot_ptr);
				memcpy(slot_ptr + STR_LENGTH_BYTES, str.GetData(), str.GetSize());
				Store<uint64_t>(h, slot_ptr - 8);
				//				accepted++;
				// not sure if needed, maybe just be non-atomic store, just need to suppress TSan
				(HT + HT_slot + prob_index)->store(newBucket, std::memory_order_release);
//				return string_t(const_char_ptr_cast(AddSalt(slot_ptr + STR_LENGTH_BYTES)),
//				                UnsafeNumericCast<uint32_t>(str.GetSize()));
				auto res_str = string_t(const_char_ptr_cast(slot_ptr + STR_LENGTH_BYTES),
				                        UnsafeNumericCast<uint32_t>(str.GetSize()));
				res_str.SetPointer(AddSalt(res_str.GetPointer()));
				return res_str;
			} else { // lost the race to dirty the bucket, check if the dirt = HT_salt, if so wait, else continue
				     // probing
				if (expected == ((HT_salt << slot_bits) | 1)) {
					while (true) {
						auto bucket = (HT + HT_slot + prob_index)->load(std::memory_order_acquire);
						if (bucket == 0) {
							return str;
						}
						if ((bucket & slot_mask) != 1) {
							break;
						}
					};
					auto slot_ptr = data_ptr_cast(
					    DataRegion + ((HT + HT_slot + prob_index)->load(std::memory_order_relaxed) & slot_mask));
					auto materialized_str_length = UnsafeNumericCast<uint16_t>(*reinterpret_cast<uint16_t *>(slot_ptr));
					if (materialized_str_length == str.GetSize() &&
					    memcmp(slot_ptr + STR_LENGTH_BYTES, str.GetDataUnsafe(), str.GetSize()) == 0) {
						//						already_in++;
						auto res_str = string_t(const_char_ptr_cast(slot_ptr + STR_LENGTH_BYTES),
						                        UnsafeNumericCast<uint32_t>(materialized_str_length));
						res_str.SetPointer(AddSalt(res_str.GetPointer()));
						return res_str;

					} else {
						continue;
					}
				} else {
					continue;
				}
			}
		} else if (HT_bucket_salt == HT_salt && (HT_bucket & slot_mask) == 1) { // dirtied
			while (true) {
				auto bucket = (HT + HT_slot + prob_index)->load(std::memory_order_acquire);
				if (bucket == 0) {
					return str;
				}
				if ((bucket & slot_mask) != 1) {
					break;
				}
			}
			auto slot_ptr =
			    data_ptr_cast(DataRegion + ((HT + HT_slot + prob_index)->load(std::memory_order_relaxed) & slot_mask));
			auto materialized_str_length = UnsafeNumericCast<uint16_t>(*reinterpret_cast<uint16_t *>(slot_ptr));
			if (materialized_str_length == str.GetSize() &&
			    memcmp(slot_ptr + STR_LENGTH_BYTES, str.GetDataUnsafe(), str.GetSize()) == 0) {
				//						already_in++;
				auto res_str = string_t(const_char_ptr_cast(slot_ptr + STR_LENGTH_BYTES),
				                UnsafeNumericCast<uint32_t>(materialized_str_length));
				res_str.SetPointer(AddSalt(res_str.GetPointer()));
				return res_str;
			} else {
				continue;
			}
		} else if (HT_bucket_salt == HT_salt) {
			auto slot_ptr = data_ptr_cast(DataRegion + (HT_bucket & slot_mask));
			auto materialized_str_length = UnsafeNumericCast<uint16_t>(*reinterpret_cast<uint16_t *>(slot_ptr));
			if (materialized_str_length == str.GetSize() &&
			    memcmp(slot_ptr + STR_LENGTH_BYTES, str.GetDataUnsafe(), str.GetSize()) == 0) {
				//				already_in++;
				auto res_str = string_t(const_char_ptr_cast(slot_ptr + STR_LENGTH_BYTES),
				                        UnsafeNumericCast<uint32_t>(materialized_str_length));
				res_str.SetPointer(AddSalt(res_str.GetPointer()));
				return res_str;
			} else {
				continue;
			}
		}
	}
	//					nRejections_Probing++;
	return str;
}

UnifiedStringsDictionary::~UnifiedStringsDictionary() {
	//	Printer::Print("USSR deleted be careful!");
	this->buffer.reset();
	//	this->LinearProbingHT.reset();
	//				this->getStatistics();
}

char * UnifiedStringsDictionary::AddSalt(char * ptr) {
#ifndef DUCKDB_DISABLE_POINTER_SALT
	return reinterpret_cast<char *>(reinterpret_cast<uint64_t>(ptr) |
	                                    string_t::UNIFIED_STRING_DICTIONARY_SALT_MASK);
#else
	return ptr;
#endif
}

void UnifiedStringsDictionary::getStatistics() {
	// A small helper to pad strings on the right
	Printer::Print("");
	auto padRight = [](const std::string &text, std::size_t width) {
		if (text.size() >= width) {
			return text; // If it already exceeds or matches the width, just return it
		}
		return text + std::string(width - text.size(), ' ');
	};

	// Specify column widths as needed
	const std::size_t w1 = 15;
	const std::size_t w2 = 15;
	const std::size_t w3 = 20;
	const std::size_t w4 = 25;
	const std::size_t w5 = 15;

	// Build header row
	std::string header;
	header += padRight("candidates", w1);
	header += padRight("accepted", w5);
	header += padRight("already in", w2);
	header += padRight("Rejected(full USSR)", w3);
	header += padRight("Rejected(failed probing)", w4);

	Printer::Print(header);

	// Build stats row
	std::string statsStr;
	statsStr += padRight(std::to_string(candidates), w1);
	statsStr += padRight(std::to_string(accepted), w2);
	statsStr += padRight(std::to_string(already_in), w5);
	statsStr += padRight(std::to_string(nRejections_SizeFull), w3);
	statsStr += padRight(std::to_string(nRejections_Probing), w4);
	//
	//
	Printer::Print(statsStr);

	Printer::PrintF("faster hash path triggered: %d, equal pointers for strings: %d",
	                string_t::StringComparisonOperators::faster_hash.load(),
	                string_t::StringComparisonOperators::faster_equality.load());
	string_t::StringComparisonOperators::faster_equality = 0;
	string_t::StringComparisonOperators::faster_hash = 0;
}

} // namespace duckdb
