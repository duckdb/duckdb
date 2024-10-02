#include "duckdb/common/sort/comparators.hpp"

#include "duckdb/common/fast_mem.hpp"
#include "duckdb/common/sort/sort.hpp"
#include "duckdb/common/uhugeint.hpp"

namespace duckdb {

bool Comparators::TieIsBreakable(const idx_t &tie_col, const data_ptr_t &row_ptr, const SortLayout &sort_layout) {
	const auto &col_idx = sort_layout.sorting_to_blob_col.at(tie_col);
	// Check if the blob is NULL
	ValidityBytes row_mask(row_ptr);
	idx_t entry_idx;
	idx_t idx_in_entry;
	ValidityBytes::GetEntryIndex(col_idx, entry_idx, idx_in_entry);
	if (!row_mask.RowIsValid(row_mask.GetValidityEntry(entry_idx), idx_in_entry)) {
		// Can't break a NULL tie
		return false;
	}
	auto &row_layout = sort_layout.blob_layout;
	if (row_layout.GetTypes()[col_idx].InternalType() != PhysicalType::VARCHAR) {
		// Nested type, must be broken
		return true;
	}
	const auto &tie_col_offset = row_layout.GetOffsets()[col_idx];
	auto tie_string = Load<string_t>(row_ptr + tie_col_offset);
	if (tie_string.GetSize() < sort_layout.prefix_lengths[tie_col]) {
		// No need to break the tie - we already compared the full string
		return false;
	}
	return true;
}

int Comparators::CompareTuple(const SBScanState &left, const SBScanState &right, const data_ptr_t &l_ptr,
                              const data_ptr_t &r_ptr, const SortLayout &sort_layout, const bool &external_sort) {
	// Compare the sorting columns one by one
	int comp_res = 0;
	data_ptr_t l_ptr_offset = l_ptr;
	data_ptr_t r_ptr_offset = r_ptr;
	for (idx_t col_idx = 0; col_idx < sort_layout.column_count; col_idx++) {
		comp_res = FastMemcmp(l_ptr_offset, r_ptr_offset, sort_layout.column_sizes[col_idx]);
		if (comp_res == 0 && !sort_layout.constant_size[col_idx]) {
			comp_res = BreakBlobTie(col_idx, left, right, sort_layout, external_sort);
		}
		if (comp_res != 0) {
			break;
		}
		l_ptr_offset += sort_layout.column_sizes[col_idx];
		r_ptr_offset += sort_layout.column_sizes[col_idx];
	}
	return comp_res;
}

int Comparators::CompareVal(const data_ptr_t l_ptr, const data_ptr_t r_ptr, const LogicalType &type) {
	switch (type.InternalType()) {
	case PhysicalType::VARCHAR:
		return TemplatedCompareVal<string_t>(l_ptr, r_ptr);
	case PhysicalType::LIST:
	case PhysicalType::ARRAY:
	case PhysicalType::STRUCT: {
		auto l_nested_ptr = Load<data_ptr_t>(l_ptr);
		auto r_nested_ptr = Load<data_ptr_t>(r_ptr);
		return CompareValAndAdvance(l_nested_ptr, r_nested_ptr, type, true);
	}
	default:
		throw NotImplementedException("Unimplemented CompareVal for type %s", type.ToString());
	}
}

int Comparators::BreakBlobTie(const idx_t &tie_col, const SBScanState &left, const SBScanState &right,
                              const SortLayout &sort_layout, const bool &external) {
	data_ptr_t l_data_ptr = left.DataPtr(*left.sb->blob_sorting_data);
	data_ptr_t r_data_ptr = right.DataPtr(*right.sb->blob_sorting_data);
	if (!TieIsBreakable(tie_col, l_data_ptr, sort_layout)) {
		// Quick check to see if ties can be broken
		return 0;
	}
	// Align the pointers
	const idx_t &col_idx = sort_layout.sorting_to_blob_col.at(tie_col);
	const auto &tie_col_offset = sort_layout.blob_layout.GetOffsets()[col_idx];
	l_data_ptr += tie_col_offset;
	r_data_ptr += tie_col_offset;
	// Do the comparison
	const int order = sort_layout.order_types[tie_col] == OrderType::DESCENDING ? -1 : 1;
	const auto &type = sort_layout.blob_layout.GetTypes()[col_idx];
	int result;
	if (external) {
		// Store heap pointers
		data_ptr_t l_heap_ptr = left.HeapPtr(*left.sb->blob_sorting_data);
		data_ptr_t r_heap_ptr = right.HeapPtr(*right.sb->blob_sorting_data);
		// Unswizzle offset to pointer
		UnswizzleSingleValue(l_data_ptr, l_heap_ptr, type);
		UnswizzleSingleValue(r_data_ptr, r_heap_ptr, type);
		// Compare
		result = CompareVal(l_data_ptr, r_data_ptr, type);
		// Swizzle the pointers back to offsets
		SwizzleSingleValue(l_data_ptr, l_heap_ptr, type);
		SwizzleSingleValue(r_data_ptr, r_heap_ptr, type);
	} else {
		result = CompareVal(l_data_ptr, r_data_ptr, type);
	}
	return order * result;
}

template <class T>
int Comparators::TemplatedCompareVal(const data_ptr_t &left_ptr, const data_ptr_t &right_ptr) {
	const auto left_val = Load<T>(left_ptr);
	const auto right_val = Load<T>(right_ptr);
	if (Equals::Operation<T>(left_val, right_val)) {
		return 0;
	} else if (LessThan::Operation<T>(left_val, right_val)) {
		return -1;
	} else {
		return 1;
	}
}

int Comparators::CompareValAndAdvance(data_ptr_t &l_ptr, data_ptr_t &r_ptr, const LogicalType &type, bool valid) {
	switch (type.InternalType()) {
	case PhysicalType::BOOL:
	case PhysicalType::INT8:
		return TemplatedCompareAndAdvance<int8_t>(l_ptr, r_ptr);
	case PhysicalType::INT16:
		return TemplatedCompareAndAdvance<int16_t>(l_ptr, r_ptr);
	case PhysicalType::INT32:
		return TemplatedCompareAndAdvance<int32_t>(l_ptr, r_ptr);
	case PhysicalType::INT64:
		return TemplatedCompareAndAdvance<int64_t>(l_ptr, r_ptr);
	case PhysicalType::UINT8:
		return TemplatedCompareAndAdvance<uint8_t>(l_ptr, r_ptr);
	case PhysicalType::UINT16:
		return TemplatedCompareAndAdvance<uint16_t>(l_ptr, r_ptr);
	case PhysicalType::UINT32:
		return TemplatedCompareAndAdvance<uint32_t>(l_ptr, r_ptr);
	case PhysicalType::UINT64:
		return TemplatedCompareAndAdvance<uint64_t>(l_ptr, r_ptr);
	case PhysicalType::INT128:
		return TemplatedCompareAndAdvance<hugeint_t>(l_ptr, r_ptr);
	case PhysicalType::UINT128:
		return TemplatedCompareAndAdvance<uhugeint_t>(l_ptr, r_ptr);
	case PhysicalType::FLOAT:
		return TemplatedCompareAndAdvance<float>(l_ptr, r_ptr);
	case PhysicalType::DOUBLE:
		return TemplatedCompareAndAdvance<double>(l_ptr, r_ptr);
	case PhysicalType::INTERVAL:
		return TemplatedCompareAndAdvance<interval_t>(l_ptr, r_ptr);
	case PhysicalType::VARCHAR:
		return CompareStringAndAdvance(l_ptr, r_ptr, valid);
	case PhysicalType::LIST:
		return CompareListAndAdvance(l_ptr, r_ptr, ListType::GetChildType(type), valid);
	case PhysicalType::STRUCT:
		return CompareStructAndAdvance(l_ptr, r_ptr, StructType::GetChildTypes(type), valid);
	case PhysicalType::ARRAY:
		return CompareArrayAndAdvance(l_ptr, r_ptr, ArrayType::GetChildType(type), valid, ArrayType::GetSize(type));
	default:
		throw NotImplementedException("Unimplemented CompareValAndAdvance for type %s", type.ToString());
	}
}

template <class T>
int Comparators::TemplatedCompareAndAdvance(data_ptr_t &left_ptr, data_ptr_t &right_ptr) {
	auto result = TemplatedCompareVal<T>(left_ptr, right_ptr);
	left_ptr += sizeof(T);
	right_ptr += sizeof(T);
	return result;
}

int Comparators::CompareStringAndAdvance(data_ptr_t &left_ptr, data_ptr_t &right_ptr, bool valid) {
	if (!valid) {
		return 0;
	}
	uint32_t left_string_size = Load<uint32_t>(left_ptr);
	uint32_t right_string_size = Load<uint32_t>(right_ptr);
	left_ptr += sizeof(uint32_t);
	right_ptr += sizeof(uint32_t);
	auto memcmp_res = memcmp(const_char_ptr_cast(left_ptr), const_char_ptr_cast(right_ptr),
	                         std::min<uint32_t>(left_string_size, right_string_size));

	left_ptr += left_string_size;
	right_ptr += right_string_size;

	if (memcmp_res != 0) {
		return memcmp_res;
	}
	if (left_string_size == right_string_size) {
		return 0;
	}
	if (left_string_size < right_string_size) {
		return -1;
	}
	return 1;
}

int Comparators::CompareStructAndAdvance(data_ptr_t &left_ptr, data_ptr_t &right_ptr,
                                         const child_list_t<LogicalType> &types, bool valid) {
	idx_t count = types.size();
	// Load validity masks
	ValidityBytes left_validity(left_ptr);
	ValidityBytes right_validity(right_ptr);
	left_ptr += (count + 7) / 8;
	right_ptr += (count + 7) / 8;
	// Initialize variables
	bool left_valid;
	bool right_valid;
	idx_t entry_idx;
	idx_t idx_in_entry;
	// Compare
	int comp_res = 0;
	for (idx_t i = 0; i < count; i++) {
		ValidityBytes::GetEntryIndex(i, entry_idx, idx_in_entry);
		left_valid = left_validity.RowIsValid(left_validity.GetValidityEntry(entry_idx), idx_in_entry);
		right_valid = right_validity.RowIsValid(right_validity.GetValidityEntry(entry_idx), idx_in_entry);
		auto &type = types[i].second;
		if ((left_valid == right_valid) || TypeIsConstantSize(type.InternalType())) {
			comp_res = CompareValAndAdvance(left_ptr, right_ptr, types[i].second, left_valid && valid);
		}
		if (!left_valid && !right_valid) {
			comp_res = 0;
		} else if (!left_valid) {
			comp_res = 1;
		} else if (!right_valid) {
			comp_res = -1;
		}
		if (comp_res != 0) {
			break;
		}
	}
	return comp_res;
}

int Comparators::CompareArrayAndAdvance(data_ptr_t &left_ptr, data_ptr_t &right_ptr, const LogicalType &type,
                                        bool valid, idx_t array_size) {
	if (!valid) {
		return 0;
	}

	// Load array validity masks
	ValidityBytes left_validity(left_ptr);
	ValidityBytes right_validity(right_ptr);
	left_ptr += (array_size + 7) / 8;
	right_ptr += (array_size + 7) / 8;

	int comp_res = 0;
	if (TypeIsConstantSize(type.InternalType())) {
		// Templated code for fixed-size types
		switch (type.InternalType()) {
		case PhysicalType::BOOL:
		case PhysicalType::INT8:
			comp_res = TemplatedCompareListLoop<int8_t>(left_ptr, right_ptr, left_validity, right_validity, array_size);
			break;
		case PhysicalType::INT16:
			comp_res =
			    TemplatedCompareListLoop<int16_t>(left_ptr, right_ptr, left_validity, right_validity, array_size);
			break;
		case PhysicalType::INT32:
			comp_res =
			    TemplatedCompareListLoop<int32_t>(left_ptr, right_ptr, left_validity, right_validity, array_size);
			break;
		case PhysicalType::INT64:
			comp_res =
			    TemplatedCompareListLoop<int64_t>(left_ptr, right_ptr, left_validity, right_validity, array_size);
			break;
		case PhysicalType::UINT8:
			comp_res =
			    TemplatedCompareListLoop<uint8_t>(left_ptr, right_ptr, left_validity, right_validity, array_size);
			break;
		case PhysicalType::UINT16:
			comp_res =
			    TemplatedCompareListLoop<uint16_t>(left_ptr, right_ptr, left_validity, right_validity, array_size);
			break;
		case PhysicalType::UINT32:
			comp_res =
			    TemplatedCompareListLoop<uint32_t>(left_ptr, right_ptr, left_validity, right_validity, array_size);
			break;
		case PhysicalType::UINT64:
			comp_res =
			    TemplatedCompareListLoop<uint64_t>(left_ptr, right_ptr, left_validity, right_validity, array_size);
			break;
		case PhysicalType::INT128:
			comp_res =
			    TemplatedCompareListLoop<hugeint_t>(left_ptr, right_ptr, left_validity, right_validity, array_size);
			break;
		case PhysicalType::FLOAT:
			comp_res = TemplatedCompareListLoop<float>(left_ptr, right_ptr, left_validity, right_validity, array_size);
			break;
		case PhysicalType::DOUBLE:
			comp_res = TemplatedCompareListLoop<double>(left_ptr, right_ptr, left_validity, right_validity, array_size);
			break;
		case PhysicalType::INTERVAL:
			comp_res =
			    TemplatedCompareListLoop<interval_t>(left_ptr, right_ptr, left_validity, right_validity, array_size);
			break;
		default:
			throw NotImplementedException("CompareListAndAdvance for fixed-size type %s", type.ToString());
		}
	} else {
		// Variable-sized array entries
		bool left_valid;
		bool right_valid;
		idx_t entry_idx;
		idx_t idx_in_entry;
		// Size (in bytes) of all variable-sizes entries is stored before the entries begin,
		// to make deserialization easier. We need to skip over them
		left_ptr += array_size * sizeof(idx_t);
		right_ptr += array_size * sizeof(idx_t);
		for (idx_t i = 0; i < array_size; i++) {
			ValidityBytes::GetEntryIndex(i, entry_idx, idx_in_entry);
			left_valid = left_validity.RowIsValid(left_validity.GetValidityEntry(entry_idx), idx_in_entry);
			right_valid = right_validity.RowIsValid(right_validity.GetValidityEntry(entry_idx), idx_in_entry);
			if (left_valid && right_valid) {
				switch (type.InternalType()) {
				case PhysicalType::LIST:
					comp_res = CompareListAndAdvance(left_ptr, right_ptr, ListType::GetChildType(type), left_valid);
					break;
				case PhysicalType::ARRAY:
					comp_res = CompareArrayAndAdvance(left_ptr, right_ptr, ArrayType::GetChildType(type), left_valid,
					                                  ArrayType::GetSize(type));
					break;
				case PhysicalType::VARCHAR:
					comp_res = CompareStringAndAdvance(left_ptr, right_ptr, left_valid);
					break;
				case PhysicalType::STRUCT:
					comp_res =
					    CompareStructAndAdvance(left_ptr, right_ptr, StructType::GetChildTypes(type), left_valid);
					break;
				default:
					throw NotImplementedException("CompareArrayAndAdvance for variable-size type %s", type.ToString());
				}
			} else if (!left_valid && !right_valid) {
				comp_res = 0;
			} else if (left_valid) {
				comp_res = -1;
			} else {
				comp_res = 1;
			}
			if (comp_res != 0) {
				break;
			}
		}
	}
	return comp_res;
}

int Comparators::CompareListAndAdvance(data_ptr_t &left_ptr, data_ptr_t &right_ptr, const LogicalType &type,
                                       bool valid) {
	if (!valid) {
		return 0;
	}
	// Load list lengths
	auto left_len = Load<idx_t>(left_ptr);
	auto right_len = Load<idx_t>(right_ptr);
	left_ptr += sizeof(idx_t);
	right_ptr += sizeof(idx_t);
	// Load list validity masks
	ValidityBytes left_validity(left_ptr);
	ValidityBytes right_validity(right_ptr);
	left_ptr += (left_len + 7) / 8;
	right_ptr += (right_len + 7) / 8;
	// Compare
	int comp_res = 0;
	idx_t count = MinValue(left_len, right_len);
	if (TypeIsConstantSize(type.InternalType())) {
		// Templated code for fixed-size types
		switch (type.InternalType()) {
		case PhysicalType::BOOL:
		case PhysicalType::INT8:
			comp_res = TemplatedCompareListLoop<int8_t>(left_ptr, right_ptr, left_validity, right_validity, count);
			break;
		case PhysicalType::INT16:
			comp_res = TemplatedCompareListLoop<int16_t>(left_ptr, right_ptr, left_validity, right_validity, count);
			break;
		case PhysicalType::INT32:
			comp_res = TemplatedCompareListLoop<int32_t>(left_ptr, right_ptr, left_validity, right_validity, count);
			break;
		case PhysicalType::INT64:
			comp_res = TemplatedCompareListLoop<int64_t>(left_ptr, right_ptr, left_validity, right_validity, count);
			break;
		case PhysicalType::UINT8:
			comp_res = TemplatedCompareListLoop<uint8_t>(left_ptr, right_ptr, left_validity, right_validity, count);
			break;
		case PhysicalType::UINT16:
			comp_res = TemplatedCompareListLoop<uint16_t>(left_ptr, right_ptr, left_validity, right_validity, count);
			break;
		case PhysicalType::UINT32:
			comp_res = TemplatedCompareListLoop<uint32_t>(left_ptr, right_ptr, left_validity, right_validity, count);
			break;
		case PhysicalType::UINT64:
			comp_res = TemplatedCompareListLoop<uint64_t>(left_ptr, right_ptr, left_validity, right_validity, count);
			break;
		case PhysicalType::INT128:
			comp_res = TemplatedCompareListLoop<hugeint_t>(left_ptr, right_ptr, left_validity, right_validity, count);
			break;
		case PhysicalType::UINT128:
			comp_res = TemplatedCompareListLoop<uhugeint_t>(left_ptr, right_ptr, left_validity, right_validity, count);
			break;
		case PhysicalType::FLOAT:
			comp_res = TemplatedCompareListLoop<float>(left_ptr, right_ptr, left_validity, right_validity, count);
			break;
		case PhysicalType::DOUBLE:
			comp_res = TemplatedCompareListLoop<double>(left_ptr, right_ptr, left_validity, right_validity, count);
			break;
		case PhysicalType::INTERVAL:
			comp_res = TemplatedCompareListLoop<interval_t>(left_ptr, right_ptr, left_validity, right_validity, count);
			break;
		default:
			throw NotImplementedException("CompareListAndAdvance for fixed-size type %s", type.ToString());
		}
	} else {
		// Variable-sized list entries
		bool left_valid;
		bool right_valid;
		idx_t entry_idx;
		idx_t idx_in_entry;
		// Size (in bytes) of all variable-sizes entries is stored before the entries begin,
		// to make deserialization easier. We need to skip over them
		left_ptr += left_len * sizeof(idx_t);
		right_ptr += right_len * sizeof(idx_t);
		for (idx_t i = 0; i < count; i++) {
			ValidityBytes::GetEntryIndex(i, entry_idx, idx_in_entry);
			left_valid = left_validity.RowIsValid(left_validity.GetValidityEntry(entry_idx), idx_in_entry);
			right_valid = right_validity.RowIsValid(right_validity.GetValidityEntry(entry_idx), idx_in_entry);
			if (left_valid && right_valid) {
				switch (type.InternalType()) {
				case PhysicalType::LIST:
					comp_res = CompareListAndAdvance(left_ptr, right_ptr, ListType::GetChildType(type), left_valid);
					break;
				case PhysicalType::ARRAY:
					comp_res = CompareArrayAndAdvance(left_ptr, right_ptr, ArrayType::GetChildType(type), left_valid,
					                                  ArrayType::GetSize(type));
					break;
				case PhysicalType::VARCHAR:
					comp_res = CompareStringAndAdvance(left_ptr, right_ptr, left_valid);
					break;
				case PhysicalType::STRUCT:
					comp_res =
					    CompareStructAndAdvance(left_ptr, right_ptr, StructType::GetChildTypes(type), left_valid);
					break;
				default:
					throw NotImplementedException("CompareListAndAdvance for variable-size type %s", type.ToString());
				}
			} else if (!left_valid && !right_valid) {
				comp_res = 0;
			} else if (left_valid) {
				comp_res = -1;
			} else {
				comp_res = 1;
			}
			if (comp_res != 0) {
				break;
			}
		}
	}
	// All values that we looped over were equal
	if (comp_res == 0 && left_len != right_len) {
		// Smaller lists first
		if (left_len < right_len) {
			comp_res = -1;
		} else {
			comp_res = 1;
		}
	}
	return comp_res;
}

template <class T>
int Comparators::TemplatedCompareListLoop(data_ptr_t &left_ptr, data_ptr_t &right_ptr,
                                          const ValidityBytes &left_validity, const ValidityBytes &right_validity,
                                          const idx_t &count) {
	int comp_res = 0;
	bool left_valid;
	bool right_valid;
	idx_t entry_idx;
	idx_t idx_in_entry;
	for (idx_t i = 0; i < count; i++) {
		ValidityBytes::GetEntryIndex(i, entry_idx, idx_in_entry);
		left_valid = left_validity.RowIsValid(left_validity.GetValidityEntry(entry_idx), idx_in_entry);
		right_valid = right_validity.RowIsValid(right_validity.GetValidityEntry(entry_idx), idx_in_entry);
		comp_res = TemplatedCompareAndAdvance<T>(left_ptr, right_ptr);
		if (!left_valid && !right_valid) {
			comp_res = 0;
		} else if (!left_valid) {
			comp_res = 1;
		} else if (!right_valid) {
			comp_res = -1;
		}
		if (comp_res != 0) {
			break;
		}
	}
	return comp_res;
}

void Comparators::UnswizzleSingleValue(data_ptr_t data_ptr, const data_ptr_t &heap_ptr, const LogicalType &type) {
	if (type.InternalType() == PhysicalType::VARCHAR) {
		data_ptr += string_t::HEADER_SIZE;
	}
	Store<data_ptr_t>(heap_ptr + Load<idx_t>(data_ptr), data_ptr);
}

void Comparators::SwizzleSingleValue(data_ptr_t data_ptr, const data_ptr_t &heap_ptr, const LogicalType &type) {
	if (type.InternalType() == PhysicalType::VARCHAR) {
		data_ptr += string_t::HEADER_SIZE;
	}
	Store<idx_t>(UnsafeNumericCast<idx_t>(Load<data_ptr_t>(data_ptr) - heap_ptr), data_ptr);
}

} // namespace duckdb
