#include "duckdb/common/types/selection_vector.hpp"

#include "duckdb/common/printer.hpp"
#include "duckdb/common/to_string.hpp"
#include "duckdb/common/algorithm.hpp"

namespace duckdb {

SelectionData::SelectionData(idx_t count) {
	owned_data = Allocator::DefaultAllocator().Allocate(MaxValue<idx_t>(count, 1) * sizeof(sel_t));
#ifdef DEBUG
	auto data_ptr = reinterpret_cast<sel_t *>(owned_data.get());
	for (idx_t i = 0; i < count; i++) {
		data_ptr[i] = std::numeric_limits<sel_t>::max();
	}
#endif
}

// LCOV_EXCL_START
string SelectionVector::ToString(idx_t count) const {
	string result = "Selection Vector (" + to_string(count) + ") [";
	for (idx_t i = 0; i < count; i++) {
		if (i != 0) {
			result += ", ";
		}
		result += to_string(get_index(i));
	}
	result += "]";
	return result;
}

void SelectionVector::Sort(idx_t count) {
	std::sort(sel_vector, sel_vector + count);
}

void SelectionVector::Print(idx_t count) const {
	Printer::Print(ToString(count));
}
// LCOV_EXCL_STOP

buffer_ptr<SelectionData> SelectionVector::Slice(const SelectionVector &sel, idx_t count) const {
	auto data = make_buffer<SelectionData>(count);
	auto result_ptr = reinterpret_cast<sel_t *>(data->owned_data.get());
	// for every element, we perform result[i] = target[new[i]]
	for (idx_t i = 0; i < count; i++) {
		auto new_idx = sel.get_index(i);
		auto idx = this->get_index(new_idx);
		result_ptr[i] = UnsafeNumericCast<sel_t>(idx);
	}
	return data;
}

idx_t SelectionVector::SliceInPlace(const SelectionVector &source, idx_t count) {
	for (idx_t i = 0; i < count; ++i) {
		set_index(i, get_index(source.get_index(i)));
	}

	return count;
}

void SelectionVector::Verify(idx_t count, idx_t vector_size) const {
#ifdef DEBUG
	D_ASSERT(vector_size >= 1);
	for (idx_t i = 0; i < count; i++) {
		auto index = get_index(i);
		if (index >= vector_size) {
			throw InternalException(
			    "Provided SelectionVector is invalid, index %d points to %d, which is out of range. "
			    "the valid range (0-%d)",
			    i, index, vector_size - 1);
		}
	}
#endif
}

} // namespace duckdb
