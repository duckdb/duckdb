#include "duckdb/common/types/validity_mask.hpp"

namespace duckdb {

ValidityData::ValidityData(idx_t count) {
	auto entry_count = EntryCount(count);
	owned_data = unique_ptr<validity_t[]>(new validity_t[entry_count]);
	for(size_t entry_idx = 0; entry_idx < entry_count; entry_idx++) {
		owned_data[entry_idx] = MAX_ENTRY;
	}
}
ValidityData::ValidityData(const ValidityMask &original, idx_t count) {
	D_ASSERT(original.validity_mask);
	auto entry_count = EntryCount(count);
	owned_data = unique_ptr<validity_t[]>(new validity_t[entry_count]);
	for(size_t entry_idx = 0; entry_idx < entry_count; entry_idx++) {
		owned_data[entry_idx] = original.validity_mask[entry_idx];
	}
}

void ValidityMask::Combine(const ValidityMask& other, idx_t count) {
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
	auto owned_data = move(validity_data);
	auto data = GetData();
	auto other_data = other.GetData();

	Initialize(count);
	auto result_data = GetData();

	auto entry_count = ValidityData::EntryCount(count);
	for(idx_t entry_idx = 0; entry_idx < entry_count; entry_idx++) {
		result_data[entry_idx] = data[entry_idx] & other_data[entry_idx];
	}
}

string ValidityMask::ToString(idx_t count) const {
	string result = "Validity Mask (" + to_string(count) + ") [";
	for (idx_t i = 0; i < count; i++) {
		result += RowIsValid(i) ? "." : "X";
	}
	result += "]";
	return result;
}

}
