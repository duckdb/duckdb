#include "duckdb/common/types/validity_mask.hpp"

namespace duckdb {

ValidityData::ValidityData(idx_t count) : TemplatedValidityData(count) {
}
ValidityData::ValidityData(const ValidityMask &original, idx_t count)
    : TemplatedValidityData(original.GetData(), count) {
}

void ValidityMask::Combine(const ValidityMask &other, idx_t count) {
	if (other.AllValid()) {
		// X & 1 = X
		return;
	}
	if (AllValid()) {
		// 1 & Y = Y
		Initialize(other);
		return;
	}
	if (validity_mask == other.validity_mask) {
		// X & X == X
		return;
	}
	// have to merge
	// create a new validity mask that contains the combined mask
	auto owned_data = std::move(validity_data);
	auto data = GetData();
	auto other_data = other.GetData();

	Initialize(count);
	auto result_data = GetData();

	auto entry_count = ValidityData::EntryCount(count);
	for (idx_t entry_idx = 0; entry_idx < entry_count; entry_idx++) {
		result_data[entry_idx] = data[entry_idx] & other_data[entry_idx];
	}
}

// LCOV_EXCL_START
string ValidityMask::ToString(idx_t count) const {
	string result = "Validity Mask (" + to_string(count) + ") [";
	for (idx_t i = 0; i < count; i++) {
		result += RowIsValid(i) ? "." : "X";
	}
	result += "]";
	return result;
}
// LCOV_EXCL_STOP

void ValidityMask::Resize(idx_t old_size, idx_t new_size) {
	D_ASSERT(new_size >= old_size);
	if (validity_mask) {
		auto new_size_count = EntryCount(new_size);
		auto old_size_count = EntryCount(old_size);
		auto new_validity_data = make_buffer<ValidityBuffer>(new_size);
		auto new_owned_data = new_validity_data->owned_data.get();
		for (idx_t entry_idx = 0; entry_idx < old_size_count; entry_idx++) {
			new_owned_data[entry_idx] = validity_mask[entry_idx];
		}
		for (idx_t entry_idx = old_size_count; entry_idx < new_size_count; entry_idx++) {
			new_owned_data[entry_idx] = ValidityData::MAX_ENTRY;
		}
		validity_data = std::move(new_validity_data);
		validity_mask = validity_data->owned_data.get();
	} else {
		Initialize(new_size);
	}
}

void ValidityMask::Slice(const ValidityMask &other, idx_t offset, idx_t end) {
	if (other.AllValid()) {
		validity_mask = nullptr;
		validity_data.reset();
		return;
	}
	if (offset == 0) {
		Initialize(other);
		return;
	}
	ValidityMask new_mask(end - offset);

// FIXME THIS NEEDS FIXING!
#if 1
	for (idx_t i = offset; i < end; i++) {
		new_mask.Set(i - offset, other.RowIsValid(i));
	}
	Initialize(new_mask);
#else
	// first shift the "whole" units
	idx_t entire_units = offset / BITS_PER_VALUE;
	idx_t sub_units = offset - entire_units * BITS_PER_VALUE;
	if (entire_units > 0) {
		idx_t validity_idx;
		for (validity_idx = 0; validity_idx + entire_units < STANDARD_ENTRY_COUNT; validity_idx++) {
			new_mask.validity_mask[validity_idx] = other.validity_mask[validity_idx + entire_units];
		}
	}
	// now we shift the remaining sub units
	// this gets a bit more complicated because we have to shift over the borders of the entries
	// e.g. suppose we have 2 entries of length 4 and we left-shift by two
	// 0101|1010
	// a regular left-shift of both gets us:
	// 0100|1000
	// we then OR the overflow (right-shifted by BITS_PER_VALUE - offset) together to get the correct result
	// 0100|1000 ->
	// 0110|1000
	if (sub_units > 0) {
		idx_t validity_idx;
		for (validity_idx = 0; validity_idx + 1 < STANDARD_ENTRY_COUNT; validity_idx++) {
			new_mask.validity_mask[validity_idx] =
			    (other.validity_mask[validity_idx] >> sub_units) |
			    (other.validity_mask[validity_idx + 1] << (BITS_PER_VALUE - sub_units));
		}
		new_mask.validity_mask[validity_idx] >>= sub_units;
	}
#ifdef DEBUG
	for (idx_t i = offset; i < STANDARD_VECTOR_SIZE; i++) {
		D_ASSERT(new_mask.RowIsValid(i - offset) == other.RowIsValid(i));
	}
	Initialize(new_mask);
#endif
#endif
}

} // namespace duckdb
