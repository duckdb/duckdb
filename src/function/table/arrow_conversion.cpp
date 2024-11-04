#include "duckdb/common/exception/conversion_exception.hpp"
#include "duckdb/common/limits.hpp"
#include "duckdb/common/operator/multiply.hpp"
#include "duckdb/common/types/arrow_aux_data.hpp"
#include "duckdb/common/types/arrow_string_view_type.hpp"
#include "duckdb/common/types/hugeint.hpp"
#include "duckdb/function/scalar/nested_functions.hpp"
#include "duckdb/function/table/arrow.hpp"

#include "duckdb/common/bswap.hpp"

namespace duckdb {

namespace {

enum class ArrowArrayPhysicalType : uint8_t { DICTIONARY_ENCODED, RUN_END_ENCODED, DEFAULT };

ArrowArrayPhysicalType GetArrowArrayPhysicalType(const ArrowType &type) {
	if (type.HasDictionary()) {
		return ArrowArrayPhysicalType::DICTIONARY_ENCODED;
	}
	if (type.RunEndEncoded()) {
		return ArrowArrayPhysicalType::RUN_END_ENCODED;
	}
	return ArrowArrayPhysicalType::DEFAULT;
}

} // namespace

static void ShiftRight(unsigned char *ar, int size, int shift) {
	int carry = 0;
	while (shift--) {
		for (int i = size - 1; i >= 0; --i) {
			int next = (ar[i] & 1) ? 0x80 : 0;
			ar[i] = UnsafeNumericCast<unsigned char>(carry | (ar[i] >> 1));
			carry = next;
		}
	}
}

idx_t GetEffectiveOffset(const ArrowArray &array, int64_t parent_offset, const ArrowScanLocalState &state,
                         int64_t nested_offset = -1) {
	if (nested_offset != -1) {
		// The parent of this array is a list
		// We just ignore the parent offset, it's already applied to the list
		return UnsafeNumericCast<idx_t>(array.offset + nested_offset);
	}
	// Parent offset is set in the case of a struct, it applies to all child arrays
	// 'chunk_offset' is how much of the chunk we've already scanned, in case the chunk size exceeds
	// STANDARD_VECTOR_SIZE
	return UnsafeNumericCast<idx_t>(array.offset + parent_offset) + state.chunk_offset;
}

template <class T>
T *ArrowBufferData(ArrowArray &array, idx_t buffer_idx) {
	return (T *)array.buffers[buffer_idx]; // NOLINT
}

static void GetValidityMask(ValidityMask &mask, ArrowArray &array, const ArrowScanLocalState &scan_state, idx_t size,
                            int64_t parent_offset, int64_t nested_offset = -1, bool add_null = false) {
	// In certains we don't need to or cannot copy arrow's validity mask to duckdb.
	//
	// The conditions where we do want to copy arrow's mask to duckdb are:
	// 1. nulls exist
	// 2. n_buffers > 0, meaning the array's arrow type is not `null`
	// 3. the validity buffer (the first buffer) is not a nullptr
	if (array.null_count != 0 && array.n_buffers > 0 && array.buffers[0]) {
		auto bit_offset = GetEffectiveOffset(array, parent_offset, scan_state, nested_offset);
		mask.EnsureWritable();
#if STANDARD_VECTOR_SIZE > 64
		auto n_bitmask_bytes = (size + 8 - 1) / 8;
		if (bit_offset % 8 == 0) {
			//! just memcpy nullmask
			memcpy((void *)mask.GetData(), ArrowBufferData<uint8_t>(array, 0) + bit_offset / 8, n_bitmask_bytes);
		} else {
			//! need to re-align nullmask
			vector<uint8_t> temp_nullmask(n_bitmask_bytes + 1);
			memcpy(temp_nullmask.data(), ArrowBufferData<uint8_t>(array, 0) + bit_offset / 8, n_bitmask_bytes + 1);
			ShiftRight(temp_nullmask.data(), NumericCast<int>(n_bitmask_bytes + 1),
			           NumericCast<int>(bit_offset % 8ull)); //! why this has to be a right shift is a mystery to me
			memcpy((void *)mask.GetData(), data_ptr_cast(temp_nullmask.data()), n_bitmask_bytes);
		}
#else
		auto byte_offset = bit_offset / 8;
		auto source_data = ArrowBufferData<uint8_t>(array, 0);
		bit_offset %= 8;
		for (idx_t i = 0; i < size; i++) {
			mask.Set(i, source_data[byte_offset] & (1 << bit_offset));
			bit_offset++;
			if (bit_offset == 8) {
				bit_offset = 0;
				byte_offset++;
			}
		}
#endif
	}
	if (add_null) {
		//! We are setting a validity mask of the data part of dictionary vector
		//! For some reason, Nulls are allowed to be indexes, hence we need to set the last element here to be null
		//! We might have to resize the mask
		mask.Resize(size, size + 1);
		mask.SetInvalid(size);
	}
}

static void SetValidityMask(Vector &vector, ArrowArray &array, const ArrowScanLocalState &scan_state, idx_t size,
                            int64_t parent_offset, int64_t nested_offset, bool add_null = false) {
	D_ASSERT(vector.GetVectorType() == VectorType::FLAT_VECTOR);
	auto &mask = FlatVector::Validity(vector);
	GetValidityMask(mask, array, scan_state, size, parent_offset, nested_offset, add_null);
}

static void ColumnArrowToDuckDBRunEndEncoded(Vector &vector, const ArrowArray &array, ArrowArrayScanState &array_state,
                                             idx_t size, const ArrowType &arrow_type, int64_t nested_offset = -1,
                                             ValidityMask *parent_mask = nullptr, uint64_t parent_offset = 0);

static void ColumnArrowToDuckDB(Vector &vector, ArrowArray &array, ArrowArrayScanState &array_state, idx_t size,
                                const ArrowType &arrow_type, int64_t nested_offset = -1,
                                ValidityMask *parent_mask = nullptr, uint64_t parent_offset = 0);

static void ColumnArrowToDuckDBDictionary(Vector &vector, ArrowArray &array, ArrowArrayScanState &array_state,
                                          idx_t size, const ArrowType &arrow_type, int64_t nested_offset = -1,
                                          const ValidityMask *parent_mask = nullptr, uint64_t parent_offset = 0);

namespace {

struct ArrowListOffsetData {
	idx_t list_size = 0;
	idx_t start_offset = 0;
};

} // namespace

template <class BUFFER_TYPE>
static ArrowListOffsetData ConvertArrowListOffsetsTemplated(Vector &vector, ArrowArray &array, idx_t size,
                                                            idx_t effective_offset) {
	ArrowListOffsetData result;
	auto &start_offset = result.start_offset;
	auto &list_size = result.list_size;

	if (size == 0) {
		start_offset = 0;
		list_size = 0;
		return result;
	}

	idx_t cur_offset = 0;
	auto offsets = ArrowBufferData<BUFFER_TYPE>(array, 1) + effective_offset;
	start_offset = offsets[0];
	auto list_data = FlatVector::GetData<list_entry_t>(vector);
	for (idx_t i = 0; i < size; i++) {
		auto &le = list_data[i];
		le.offset = cur_offset;
		le.length = offsets[i + 1] - offsets[i];
		cur_offset += le.length;
	}
	list_size = offsets[size];
	list_size -= start_offset;
	return result;
}

template <class BUFFER_TYPE>
static ArrowListOffsetData ConvertArrowListViewOffsetsTemplated(Vector &vector, ArrowArray &array, idx_t size,
                                                                idx_t effective_offset) {
	ArrowListOffsetData result;
	auto &start_offset = result.start_offset;
	auto &list_size = result.list_size;

	list_size = 0;
	auto offsets = ArrowBufferData<BUFFER_TYPE>(array, 1) + effective_offset;
	auto sizes = ArrowBufferData<BUFFER_TYPE>(array, 2) + effective_offset;

	// In ListArrays the offsets have to be sequential
	// ListViewArrays do not have this same constraint
	// for that reason we need to keep track of the lowest offset, so we can skip all the data that comes before it
	// when we scan the child data

	auto lowest_offset = size ? offsets[0] : 0;
	auto list_data = FlatVector::GetData<list_entry_t>(vector);
	for (idx_t i = 0; i < size; i++) {
		auto &le = list_data[i];
		le.offset = offsets[i];
		le.length = sizes[i];
		list_size += le.length;
		if (sizes[i] != 0) {
			lowest_offset = MinValue(lowest_offset, offsets[i]);
		}
	}
	start_offset = lowest_offset;
	if (start_offset) {
		// We start scanning the child data at the 'start_offset' so we need to fix up the created list entries
		for (idx_t i = 0; i < size; i++) {
			auto &le = list_data[i];
			le.offset = le.offset <= start_offset ? 0 : le.offset - start_offset;
		}
	}
	return result;
}

static ArrowListOffsetData ConvertArrowListOffsets(Vector &vector, ArrowArray &array, idx_t size,
                                                   const ArrowType &arrow_type, idx_t effective_offset) {
	auto &list_info = arrow_type.GetTypeInfo<ArrowListInfo>();
	auto size_type = list_info.GetSizeType();
	if (list_info.IsView()) {
		if (size_type == ArrowVariableSizeType::NORMAL) {
			return ConvertArrowListViewOffsetsTemplated<uint32_t>(vector, array, size, effective_offset);
		} else {
			D_ASSERT(size_type == ArrowVariableSizeType::SUPER_SIZE);
			return ConvertArrowListViewOffsetsTemplated<uint64_t>(vector, array, size, effective_offset);
		}
	} else {
		if (size_type == ArrowVariableSizeType::NORMAL) {
			return ConvertArrowListOffsetsTemplated<uint32_t>(vector, array, size, effective_offset);
		} else {
			D_ASSERT(size_type == ArrowVariableSizeType::SUPER_SIZE);
			return ConvertArrowListOffsetsTemplated<uint64_t>(vector, array, size, effective_offset);
		}
	}
}

static void ArrowToDuckDBList(Vector &vector, ArrowArray &array, ArrowArrayScanState &array_state, idx_t size,
                              const ArrowType &arrow_type, int64_t nested_offset, const ValidityMask *parent_mask,
                              int64_t parent_offset) {
	auto &scan_state = array_state.state;

	auto &list_info = arrow_type.GetTypeInfo<ArrowListInfo>();
	SetValidityMask(vector, array, scan_state, size, parent_offset, nested_offset);

	auto effective_offset = GetEffectiveOffset(array, parent_offset, scan_state, nested_offset);
	auto list_data = ConvertArrowListOffsets(vector, array, size, arrow_type, effective_offset);
	auto &start_offset = list_data.start_offset;
	auto &list_size = list_data.list_size;

	ListVector::Reserve(vector, list_size);
	ListVector::SetListSize(vector, list_size);
	auto &child_vector = ListVector::GetEntry(vector);
	SetValidityMask(child_vector, *array.children[0], scan_state, list_size, array.offset,
	                NumericCast<int64_t>(start_offset));
	auto &list_mask = FlatVector::Validity(vector);
	if (parent_mask) {
		//! Since this List is owned by a struct we must guarantee their validity map matches on Null
		if (!parent_mask->AllValid()) {
			for (idx_t i = 0; i < size; i++) {
				if (!parent_mask->RowIsValid(i)) {
					list_mask.SetInvalid(i);
				}
			}
		}
	}
	auto &child_state = array_state.GetChild(0);
	auto &child_array = *array.children[0];
	auto &child_type = list_info.GetChild();

	if (list_size == 0 && start_offset == 0) {
		D_ASSERT(!child_array.dictionary);
		ColumnArrowToDuckDB(child_vector, child_array, child_state, list_size, child_type, -1);
		return;
	}

	auto array_physical_type = GetArrowArrayPhysicalType(child_type);
	switch (array_physical_type) {
	case ArrowArrayPhysicalType::DICTIONARY_ENCODED:
		// TODO: add support for offsets
		ColumnArrowToDuckDBDictionary(child_vector, child_array, child_state, list_size, child_type,
		                              NumericCast<int64_t>(start_offset));
		break;
	case ArrowArrayPhysicalType::RUN_END_ENCODED:
		ColumnArrowToDuckDBRunEndEncoded(child_vector, child_array, child_state, list_size, child_type,
		                                 NumericCast<int64_t>(start_offset));
		break;
	case ArrowArrayPhysicalType::DEFAULT:
		ColumnArrowToDuckDB(child_vector, child_array, child_state, list_size, child_type,
		                    NumericCast<int64_t>(start_offset));
		break;
	default:
		throw NotImplementedException("ArrowArrayPhysicalType not recognized");
	}
}

static void ArrowToDuckDBArray(Vector &vector, ArrowArray &array, ArrowArrayScanState &array_state, idx_t size,
                               const ArrowType &arrow_type, int64_t nested_offset, const ValidityMask *parent_mask,
                               int64_t parent_offset) {

	auto &array_info = arrow_type.GetTypeInfo<ArrowArrayInfo>();
	auto &scan_state = array_state.state;
	auto array_size = array_info.FixedSize();
	auto child_count = array_size * size;
	auto child_offset = GetEffectiveOffset(array, parent_offset, scan_state, nested_offset) * array_size;

	SetValidityMask(vector, array, scan_state, size, parent_offset, nested_offset);

	auto &child_vector = ArrayVector::GetEntry(vector);
	SetValidityMask(child_vector, *array.children[0], scan_state, child_count, array.offset,
	                NumericCast<int64_t>(child_offset));

	auto &array_mask = FlatVector::Validity(vector);
	if (parent_mask) {
		//! Since this List is owned by a struct we must guarantee their validity map matches on Null
		if (!parent_mask->AllValid()) {
			for (idx_t i = 0; i < size; i++) {
				if (!parent_mask->RowIsValid(i)) {
					array_mask.SetInvalid(i);
				}
			}
		}
	}

	// Broadcast the validity mask to the child vector
	if (!array_mask.AllValid()) {
		auto &child_validity_mask = FlatVector::Validity(child_vector);
		for (idx_t i = 0; i < size; i++) {
			if (!array_mask.RowIsValid(i)) {
				for (idx_t j = 0; j < array_size; j++) {
					child_validity_mask.SetInvalid(i * array_size + j);
				}
			}
		}
	}

	auto &child_state = array_state.GetChild(0);
	auto &child_array = *array.children[0];
	auto &child_type = array_info.GetChild();
	if (child_count == 0 && child_offset == 0) {
		D_ASSERT(!child_array.dictionary);
		ColumnArrowToDuckDB(child_vector, child_array, child_state, child_count, child_type, -1);
	} else {
		if (child_array.dictionary) {
			ColumnArrowToDuckDBDictionary(child_vector, child_array, child_state, child_count, child_type,
			                              NumericCast<int64_t>(child_offset));
		} else {
			ColumnArrowToDuckDB(child_vector, child_array, child_state, child_count, child_type,
			                    NumericCast<int64_t>(child_offset));
		}
	}
}

static void ArrowToDuckDBBlob(Vector &vector, ArrowArray &array, const ArrowScanLocalState &scan_state, idx_t size,
                              const ArrowType &arrow_type, int64_t nested_offset, int64_t parent_offset) {
	SetValidityMask(vector, array, scan_state, size, parent_offset, nested_offset);
	auto &string_info = arrow_type.GetTypeInfo<ArrowStringInfo>();
	auto size_type = string_info.GetSizeType();
	if (size_type == ArrowVariableSizeType::FIXED_SIZE) {
		auto fixed_size = string_info.FixedSize();
		//! Have to check validity mask before setting this up
		idx_t offset = GetEffectiveOffset(array, parent_offset, scan_state, nested_offset) * fixed_size;
		auto cdata = ArrowBufferData<char>(array, 1);
		for (idx_t row_idx = 0; row_idx < size; row_idx++) {
			if (FlatVector::IsNull(vector, row_idx)) {
				continue;
			}
			auto bptr = cdata + offset;
			auto blob_len = fixed_size;
			FlatVector::GetData<string_t>(vector)[row_idx] = StringVector::AddStringOrBlob(vector, bptr, blob_len);
			offset += blob_len;
		}
	} else if (size_type == ArrowVariableSizeType::NORMAL) {
		auto offsets =
		    ArrowBufferData<uint32_t>(array, 1) + GetEffectiveOffset(array, parent_offset, scan_state, nested_offset);
		auto cdata = ArrowBufferData<char>(array, 2);
		for (idx_t row_idx = 0; row_idx < size; row_idx++) {
			if (FlatVector::IsNull(vector, row_idx)) {
				continue;
			}
			auto bptr = cdata + offsets[row_idx];
			auto blob_len = offsets[row_idx + 1] - offsets[row_idx];
			FlatVector::GetData<string_t>(vector)[row_idx] = StringVector::AddStringOrBlob(vector, bptr, blob_len);
		}
	} else {
		//! Check if last offset is higher than max uint32
		if (ArrowBufferData<uint64_t>(array, 1)[array.length] > NumericLimits<uint32_t>::Maximum()) { // LCOV_EXCL_START
			throw ConversionException("DuckDB does not support Blobs over 4GB");
		} // LCOV_EXCL_STOP
		auto offsets =
		    ArrowBufferData<uint64_t>(array, 1) + GetEffectiveOffset(array, parent_offset, scan_state, nested_offset);
		auto cdata = ArrowBufferData<char>(array, 2);
		for (idx_t row_idx = 0; row_idx < size; row_idx++) {
			if (FlatVector::IsNull(vector, row_idx)) {
				continue;
			}
			auto bptr = cdata + offsets[row_idx];
			auto blob_len = offsets[row_idx + 1] - offsets[row_idx];
			FlatVector::GetData<string_t>(vector)[row_idx] = StringVector::AddStringOrBlob(vector, bptr, blob_len);
		}
	}
}

static void ArrowToDuckDBMapVerify(Vector &vector, idx_t count) {
	auto valid_check = MapVector::CheckMapValidity(vector, count);
	switch (valid_check) {
	case MapInvalidReason::VALID:
		break;
	case MapInvalidReason::DUPLICATE_KEY: {
		throw InvalidInputException("Arrow map contains duplicate key, which isn't supported by DuckDB map type");
	}
	case MapInvalidReason::NULL_KEY: {
		throw InvalidInputException("Arrow map contains NULL as map key, which isn't supported by DuckDB map type");
	}
	default: {
		throw InternalException("MapInvalidReason not implemented");
	}
	}
}

template <class T>
static void SetVectorString(Vector &vector, idx_t size, char *cdata, T *offsets) {
	auto strings = FlatVector::GetData<string_t>(vector);
	for (idx_t row_idx = 0; row_idx < size; row_idx++) {
		if (FlatVector::IsNull(vector, row_idx)) {
			continue;
		}
		auto cptr = cdata + offsets[row_idx];
		auto str_len = offsets[row_idx + 1] - offsets[row_idx];
		if (str_len > NumericLimits<uint32_t>::Maximum()) { // LCOV_EXCL_START
			throw ConversionException("DuckDB does not support Strings over 4GB");
		} // LCOV_EXCL_STOP
		strings[row_idx] = string_t(cptr, UnsafeNumericCast<uint32_t>(str_len));
	}
}

static void SetVectorStringView(Vector &vector, idx_t size, ArrowArray &array, idx_t current_pos) {
	auto strings = FlatVector::GetData<string_t>(vector);
	auto arrow_string = ArrowBufferData<arrow_string_view_t>(array, 1) + current_pos;

	for (idx_t row_idx = 0; row_idx < size; row_idx++) {
		if (FlatVector::IsNull(vector, row_idx)) {
			continue;
		}
		auto length = UnsafeNumericCast<uint32_t>(arrow_string[row_idx].Length());
		if (arrow_string[row_idx].IsInline()) {
			//	This string is inlined
			//  | Bytes 0-3  | Bytes 4-15                            |
			//  |------------|---------------------------------------|
			//  | length     | data (padded with 0)                  |
			strings[row_idx] = string_t(arrow_string[row_idx].GetInlineData(), length);
		} else {
			//  This string is not inlined, we have to check a different buffer and offsets
			//  | Bytes 0-3  | Bytes 4-7  | Bytes 8-11 | Bytes 12-15 |
			//  |------------|------------|------------|-------------|
			//  | length     | prefix     | buf. index | offset      |
			auto buffer_index = UnsafeNumericCast<uint32_t>(arrow_string[row_idx].GetBufferIndex());
			int32_t offset = arrow_string[row_idx].GetOffset();
			D_ASSERT(array.n_buffers > 2 + buffer_index);
			auto c_data = ArrowBufferData<char>(array, 2 + buffer_index);
			strings[row_idx] = string_t(&c_data[offset], length);
		}
	}
}

static void DirectConversion(Vector &vector, ArrowArray &array, const ArrowScanLocalState &scan_state,
                             int64_t nested_offset, uint64_t parent_offset) {
	auto internal_type = GetTypeIdSize(vector.GetType().InternalType());
	auto data_ptr =
	    ArrowBufferData<data_t>(array, 1) +
	    internal_type * GetEffectiveOffset(array, NumericCast<int64_t>(parent_offset), scan_state, nested_offset);
	FlatVector::SetData(vector, data_ptr);
}

template <class T>
static void TimeConversion(Vector &vector, ArrowArray &array, const ArrowScanLocalState &scan_state,
                           int64_t nested_offset, int64_t parent_offset, idx_t size, int64_t conversion) {
	auto tgt_ptr = FlatVector::GetData<dtime_t>(vector);
	auto &validity_mask = FlatVector::Validity(vector);
	auto src_ptr =
	    static_cast<const T *>(array.buffers[1]) + GetEffectiveOffset(array, parent_offset, scan_state, nested_offset);
	for (idx_t row = 0; row < size; row++) {
		if (!validity_mask.RowIsValid(row)) {
			continue;
		}
		if (!TryMultiplyOperator::Operation(static_cast<int64_t>(src_ptr[row]), conversion, tgt_ptr[row].micros)) {
			throw ConversionException("Could not convert Time to Microsecond");
		}
	}
}

static void UUIDConversion(Vector &vector, const ArrowArray &array, const ArrowScanLocalState &scan_state,
                           int64_t nested_offset, int64_t parent_offset, idx_t size) {
	auto tgt_ptr = FlatVector::GetData<hugeint_t>(vector);
	auto &validity_mask = FlatVector::Validity(vector);
	auto src_ptr = static_cast<const hugeint_t *>(array.buffers[1]) +
	               GetEffectiveOffset(array, parent_offset, scan_state, nested_offset);
	for (idx_t row = 0; row < size; row++) {
		if (!validity_mask.RowIsValid(row)) {
			continue;
		}
		tgt_ptr[row].lower = static_cast<uint64_t>(BSwap(src_ptr[row].upper));
		// flip Upper MSD
		tgt_ptr[row].upper =
		    static_cast<int64_t>(static_cast<uint64_t>(BSwap(src_ptr[row].lower)) ^ (static_cast<uint64_t>(1) << 63));
	}
}

static void TimestampTZConversion(Vector &vector, ArrowArray &array, const ArrowScanLocalState &scan_state,
                                  int64_t nested_offset, int64_t parent_offset, idx_t size, int64_t conversion) {
	auto tgt_ptr = FlatVector::GetData<timestamp_t>(vector);
	auto &validity_mask = FlatVector::Validity(vector);
	auto src_ptr =
	    ArrowBufferData<int64_t>(array, 1) + GetEffectiveOffset(array, parent_offset, scan_state, nested_offset);
	for (idx_t row = 0; row < size; row++) {
		if (!validity_mask.RowIsValid(row)) {
			continue;
		}
		if (!TryMultiplyOperator::Operation(src_ptr[row], conversion, tgt_ptr[row].value)) {
			throw ConversionException("Could not convert TimestampTZ to Microsecond");
		}
	}
}

static void IntervalConversionUs(Vector &vector, ArrowArray &array, const ArrowScanLocalState &scan_state,
                                 int64_t nested_offset, int64_t parent_offset, idx_t size, int64_t conversion) {
	auto tgt_ptr = FlatVector::GetData<interval_t>(vector);
	auto src_ptr =
	    ArrowBufferData<int64_t>(array, 1) + GetEffectiveOffset(array, parent_offset, scan_state, nested_offset);
	for (idx_t row = 0; row < size; row++) {
		tgt_ptr[row].days = 0;
		tgt_ptr[row].months = 0;
		if (!TryMultiplyOperator::Operation(src_ptr[row], conversion, tgt_ptr[row].micros)) {
			throw ConversionException("Could not convert Interval to Microsecond");
		}
	}
}

static void IntervalConversionMonths(Vector &vector, ArrowArray &array, const ArrowScanLocalState &scan_state,
                                     int64_t nested_offset, int64_t parent_offset, idx_t size) {
	auto tgt_ptr = FlatVector::GetData<interval_t>(vector);
	auto src_ptr =
	    ArrowBufferData<int32_t>(array, 1) + GetEffectiveOffset(array, parent_offset, scan_state, nested_offset);
	for (idx_t row = 0; row < size; row++) {
		tgt_ptr[row].days = 0;
		tgt_ptr[row].micros = 0;
		tgt_ptr[row].months = src_ptr[row];
	}
}

static void IntervalConversionMonthDayNanos(Vector &vector, ArrowArray &array, const ArrowScanLocalState &scan_state,
                                            int64_t nested_offset, int64_t parent_offset, idx_t size) {
	auto tgt_ptr = FlatVector::GetData<interval_t>(vector);
	auto src_ptr =
	    ArrowBufferData<ArrowInterval>(array, 1) + GetEffectiveOffset(array, parent_offset, scan_state, nested_offset);
	for (idx_t row = 0; row < size; row++) {
		tgt_ptr[row].days = src_ptr[row].days;
		tgt_ptr[row].micros = src_ptr[row].nanoseconds / Interval::NANOS_PER_MICRO;
		tgt_ptr[row].months = src_ptr[row].months;
	}
}

// Find the index of the first run-end that is strictly greater than the offset.
// count is returned if no such run-end is found.
template <class RUN_END_TYPE>
static idx_t FindRunIndex(const RUN_END_TYPE *run_ends, idx_t count, idx_t offset) {
	// Binary-search within the [0, count) range. For example:
	// [0, 0, 0, 1, 1, 2] encoded as
	// run_ends: [3, 5, 6]:
	// 0, 1, 2 -> 0
	//    3, 4 -> 1
	//       5 -> 2
	// 6, 7 .. -> 3 (3 == count [not found])
	idx_t begin = 0;
	idx_t end = count;
	while (begin < end) {
		idx_t middle = (begin + end) / 2;
		// begin < end implies middle < end
		if (offset >= static_cast<idx_t>(run_ends[middle])) {
			// keep searching in [middle + 1, end)
			begin = middle + 1;
		} else {
			// offset < run_ends[middle], so keep searching in [begin, middle)
			end = middle;
		}
	}
	return begin;
}

template <class RUN_END_TYPE, class VALUE_TYPE>
static void FlattenRunEnds(Vector &result, ArrowRunEndEncodingState &run_end_encoding, idx_t compressed_size,
                           idx_t scan_offset, idx_t count) {
	auto &runs = *run_end_encoding.run_ends;
	auto &values = *run_end_encoding.values;

	UnifiedVectorFormat run_end_format;
	UnifiedVectorFormat value_format;
	runs.ToUnifiedFormat(compressed_size, run_end_format);
	values.ToUnifiedFormat(compressed_size, value_format);
	auto run_ends_data = run_end_format.GetData<RUN_END_TYPE>(run_end_format);
	auto values_data = value_format.GetData<VALUE_TYPE>(value_format);
	auto result_data = FlatVector::GetData<VALUE_TYPE>(result);
	auto &validity = FlatVector::Validity(result);

	// According to the arrow spec, the 'run_ends' array is always valid
	// so we will assume this is true and not check the validity map

	// Now construct the result vector from the run_ends and the values

	auto run = FindRunIndex(run_ends_data, compressed_size, scan_offset);
	idx_t logical_index = scan_offset;
	idx_t index = 0;
	if (value_format.validity.AllValid()) {
		// None of the compressed values are NULL
		for (; run < compressed_size; ++run) {
			auto run_end_index = run_end_format.sel->get_index(run);
			auto value_index = value_format.sel->get_index(run);
			auto &value = values_data[value_index];
			auto run_end = static_cast<idx_t>(run_ends_data[run_end_index]);

			D_ASSERT(run_end > (logical_index + index));
			auto to_scan = run_end - (logical_index + index);
			// Cap the amount to scan so we don't go over size
			to_scan = MinValue<idx_t>(to_scan, (count - index));

			for (idx_t i = 0; i < to_scan; i++) {
				result_data[index + i] = value;
			}
			index += to_scan;
			if (index >= count) {
				if (logical_index + index >= run_end) {
					// The last run was completed, forward the run index
					++run;
				}
				break;
			}
		}
	} else {
		for (; run < compressed_size; ++run) {
			auto run_end_index = run_end_format.sel->get_index(run);
			auto value_index = value_format.sel->get_index(run);
			auto run_end = static_cast<idx_t>(run_ends_data[run_end_index]);

			D_ASSERT(run_end > (logical_index + index));
			auto to_scan = run_end - (logical_index + index);
			// Cap the amount to scan so we don't go over size
			to_scan = MinValue<idx_t>(to_scan, (count - index));

			if (value_format.validity.RowIsValidUnsafe(value_index)) {
				auto &value = values_data[value_index];
				for (idx_t i = 0; i < to_scan; i++) {
					result_data[index + i] = value;
					validity.SetValid(index + i);
				}
			} else {
				for (idx_t i = 0; i < to_scan; i++) {
					validity.SetInvalid(index + i);
				}
			}
			index += to_scan;
			if (index >= count) {
				if (logical_index + index >= run_end) {
					// The last run was completed, forward the run index
					++run;
				}
				break;
			}
		}
	}
}

template <class RUN_END_TYPE>
static void FlattenRunEndsSwitch(Vector &result, ArrowRunEndEncodingState &run_end_encoding, idx_t compressed_size,
                                 idx_t scan_offset, idx_t size) {
	auto &values = *run_end_encoding.values;
	auto physical_type = values.GetType().InternalType();

	switch (physical_type) {
	case PhysicalType::INT8:
		FlattenRunEnds<RUN_END_TYPE, int8_t>(result, run_end_encoding, compressed_size, scan_offset, size);
		break;
	case PhysicalType::INT16:
		FlattenRunEnds<RUN_END_TYPE, int16_t>(result, run_end_encoding, compressed_size, scan_offset, size);
		break;
	case PhysicalType::INT32:
		FlattenRunEnds<RUN_END_TYPE, int32_t>(result, run_end_encoding, compressed_size, scan_offset, size);
		break;
	case PhysicalType::INT64:
		FlattenRunEnds<RUN_END_TYPE, int64_t>(result, run_end_encoding, compressed_size, scan_offset, size);
		break;
	case PhysicalType::INT128:
		FlattenRunEnds<RUN_END_TYPE, hugeint_t>(result, run_end_encoding, compressed_size, scan_offset, size);
		break;
	case PhysicalType::UINT8:
		FlattenRunEnds<RUN_END_TYPE, uint8_t>(result, run_end_encoding, compressed_size, scan_offset, size);
		break;
	case PhysicalType::UINT16:
		FlattenRunEnds<RUN_END_TYPE, uint16_t>(result, run_end_encoding, compressed_size, scan_offset, size);
		break;
	case PhysicalType::UINT32:
		FlattenRunEnds<RUN_END_TYPE, uint32_t>(result, run_end_encoding, compressed_size, scan_offset, size);
		break;
	case PhysicalType::UINT64:
		FlattenRunEnds<RUN_END_TYPE, uint64_t>(result, run_end_encoding, compressed_size, scan_offset, size);
		break;
	case PhysicalType::BOOL:
		FlattenRunEnds<RUN_END_TYPE, bool>(result, run_end_encoding, compressed_size, scan_offset, size);
		break;
	case PhysicalType::FLOAT:
		FlattenRunEnds<RUN_END_TYPE, float>(result, run_end_encoding, compressed_size, scan_offset, size);
		break;
	case PhysicalType::DOUBLE:
		FlattenRunEnds<RUN_END_TYPE, double>(result, run_end_encoding, compressed_size, scan_offset, size);
		break;
	case PhysicalType::INTERVAL:
		FlattenRunEnds<RUN_END_TYPE, interval_t>(result, run_end_encoding, compressed_size, scan_offset, size);
		break;
	case PhysicalType::VARCHAR: {
		// Share the string heap, we don't need to allocate new strings, we just reference the existing ones
		result.SetAuxiliary(values.GetAuxiliary());
		FlattenRunEnds<RUN_END_TYPE, string_t>(result, run_end_encoding, compressed_size, scan_offset, size);
		break;
	}
	default:
		throw NotImplementedException("RunEndEncoded value type '%s' not supported yet", TypeIdToString(physical_type));
	}
}

static void ColumnArrowToDuckDBRunEndEncoded(Vector &vector, const ArrowArray &array, ArrowArrayScanState &array_state,
                                             idx_t size, const ArrowType &arrow_type, int64_t nested_offset,
                                             ValidityMask *parent_mask, uint64_t parent_offset) {
	// Scan the 'run_ends' array
	D_ASSERT(array.n_children == 2);
	auto &run_ends_array = *array.children[0];
	auto &values_array = *array.children[1];

	auto &struct_info = arrow_type.GetTypeInfo<ArrowStructInfo>();
	auto &run_ends_type = struct_info.GetChild(0);
	auto &values_type = struct_info.GetChild(1);
	D_ASSERT(vector.GetType() == values_type.GetDuckType());

	auto &scan_state = array_state.state;

	D_ASSERT(run_ends_array.length == values_array.length);
	auto compressed_size = NumericCast<idx_t>(run_ends_array.length);
	// Create a vector for the run ends and the values
	auto &run_end_encoding = array_state.RunEndEncoding();
	if (!run_end_encoding.run_ends) {
		// The run ends and values have not been scanned yet for this array
		D_ASSERT(!run_end_encoding.values);
		run_end_encoding.run_ends = make_uniq<Vector>(run_ends_type.GetDuckType(), compressed_size);
		run_end_encoding.values = make_uniq<Vector>(values_type.GetDuckType(), compressed_size);

		ColumnArrowToDuckDB(*run_end_encoding.run_ends, run_ends_array, array_state, compressed_size, run_ends_type);
		auto &values = *run_end_encoding.values;
		SetValidityMask(values, values_array, scan_state, compressed_size, NumericCast<int64_t>(parent_offset),
		                nested_offset);
		ColumnArrowToDuckDB(values, values_array, array_state, compressed_size, values_type);
	}

	idx_t scan_offset = GetEffectiveOffset(array, NumericCast<int64_t>(parent_offset), scan_state, nested_offset);
	auto physical_type = run_ends_type.GetDuckType().InternalType();
	switch (physical_type) {
	case PhysicalType::INT16:
		FlattenRunEndsSwitch<int16_t>(vector, run_end_encoding, compressed_size, scan_offset, size);
		break;
	case PhysicalType::INT32:
		FlattenRunEndsSwitch<int32_t>(vector, run_end_encoding, compressed_size, scan_offset, size);
		break;
	case PhysicalType::INT64:
		FlattenRunEndsSwitch<int32_t>(vector, run_end_encoding, compressed_size, scan_offset, size);
		break;
	default:
		throw NotImplementedException("Type '%s' not implemented for RunEndEncoding", TypeIdToString(physical_type));
	}
}

static void ColumnArrowToDuckDB(Vector &vector, ArrowArray &array, ArrowArrayScanState &array_state, idx_t size,
                                const ArrowType &arrow_type, int64_t nested_offset, ValidityMask *parent_mask,
                                uint64_t parent_offset) {
	auto &scan_state = array_state.state;
	D_ASSERT(!array.dictionary);

	switch (vector.GetType().id()) {
	case LogicalTypeId::SQLNULL:
		vector.Reference(Value());
		break;
	case LogicalTypeId::BOOLEAN: {
		//! Arrow bit-packs boolean values
		//! Lets first figure out where we are in the source array
		auto effective_offset =
		    GetEffectiveOffset(array, NumericCast<int64_t>(parent_offset), scan_state, nested_offset);
		auto src_ptr = ArrowBufferData<uint8_t>(array, 1) + effective_offset / 8;
		auto tgt_ptr = (uint8_t *)FlatVector::GetData(vector);
		int src_pos = 0;
		idx_t cur_bit = effective_offset % 8;
		for (idx_t row = 0; row < size; row++) {
			if ((src_ptr[src_pos] & (1 << cur_bit)) == 0) {
				tgt_ptr[row] = 0;
			} else {
				tgt_ptr[row] = 1;
			}
			cur_bit++;
			if (cur_bit == 8) {
				src_pos++;
				cur_bit = 0;
			}
		}
		break;
	}
	case LogicalTypeId::TINYINT:
	case LogicalTypeId::SMALLINT:
	case LogicalTypeId::INTEGER:
	case LogicalTypeId::FLOAT:
	case LogicalTypeId::DOUBLE:
	case LogicalTypeId::UTINYINT:
	case LogicalTypeId::USMALLINT:
	case LogicalTypeId::UINTEGER:
	case LogicalTypeId::UBIGINT:
	case LogicalTypeId::BIGINT:
	case LogicalTypeId::HUGEINT:
	case LogicalTypeId::UHUGEINT:
	case LogicalTypeId::TIMESTAMP:
	case LogicalTypeId::TIMESTAMP_SEC:
	case LogicalTypeId::TIMESTAMP_MS:
	case LogicalTypeId::TIMESTAMP_NS:
	case LogicalTypeId::TIME_TZ: {
		DirectConversion(vector, array, scan_state, nested_offset, parent_offset);
		break;
	}
	case LogicalTypeId::UUID:
		UUIDConversion(vector, array, scan_state, nested_offset, NumericCast<int64_t>(parent_offset), size);
		break;
	case LogicalTypeId::VARCHAR: {
		auto &string_info = arrow_type.GetTypeInfo<ArrowStringInfo>();
		auto size_type = string_info.GetSizeType();
		switch (size_type) {
		case ArrowVariableSizeType::SUPER_SIZE: {
			auto cdata = ArrowBufferData<char>(array, 2);
			auto offsets = ArrowBufferData<uint64_t>(array, 1) +
			               GetEffectiveOffset(array, NumericCast<int64_t>(parent_offset), scan_state, nested_offset);
			SetVectorString(vector, size, cdata, offsets);
			break;
		}
		case ArrowVariableSizeType::NORMAL:
		case ArrowVariableSizeType::FIXED_SIZE: {
			auto cdata = ArrowBufferData<char>(array, 2);
			auto offsets = ArrowBufferData<uint32_t>(array, 1) +
			               GetEffectiveOffset(array, NumericCast<int64_t>(parent_offset), scan_state, nested_offset);
			SetVectorString(vector, size, cdata, offsets);
			break;
		}
		case ArrowVariableSizeType::VIEW: {
			SetVectorStringView(
			    vector, size, array,
			    GetEffectiveOffset(array, NumericCast<int64_t>(parent_offset), scan_state, nested_offset));
			break;
		}
		}
		break;
	}
	case LogicalTypeId::DATE: {
		auto &datetime_info = arrow_type.GetTypeInfo<ArrowDateTimeInfo>();
		auto precision = datetime_info.GetDateTimeType();
		switch (precision) {
		case ArrowDateTimeType::DAYS: {
			DirectConversion(vector, array, scan_state, nested_offset, parent_offset);
			break;
		}
		case ArrowDateTimeType::MILLISECONDS: {
			//! convert date from nanoseconds to days
			auto src_ptr = ArrowBufferData<uint64_t>(array, 1) +
			               GetEffectiveOffset(array, NumericCast<int64_t>(parent_offset), scan_state, nested_offset);
			auto tgt_ptr = FlatVector::GetData<date_t>(vector);
			for (idx_t row = 0; row < size; row++) {
				tgt_ptr[row] = date_t(UnsafeNumericCast<int32_t>(static_cast<int64_t>(src_ptr[row]) /
				                                                 static_cast<int64_t>(1000 * 60 * 60 * 24)));
			}
			break;
		}
		default:
			throw NotImplementedException("Unsupported precision for Date Type ");
		}
		break;
	}
	case LogicalTypeId::TIME: {
		auto &datetime_info = arrow_type.GetTypeInfo<ArrowDateTimeInfo>();
		auto precision = datetime_info.GetDateTimeType();
		switch (precision) {
		case ArrowDateTimeType::SECONDS: {
			TimeConversion<int32_t>(vector, array, scan_state, nested_offset, NumericCast<int64_t>(parent_offset), size,
			                        1000000);
			break;
		}
		case ArrowDateTimeType::MILLISECONDS: {
			TimeConversion<int32_t>(vector, array, scan_state, nested_offset, NumericCast<int64_t>(parent_offset), size,
			                        1000);
			break;
		}
		case ArrowDateTimeType::MICROSECONDS: {
			TimeConversion<int64_t>(vector, array, scan_state, nested_offset, NumericCast<int64_t>(parent_offset), size,
			                        1);
			break;
		}
		case ArrowDateTimeType::NANOSECONDS: {
			auto tgt_ptr = FlatVector::GetData<dtime_t>(vector);
			auto src_ptr = ArrowBufferData<int64_t>(array, 1) +
			               GetEffectiveOffset(array, NumericCast<int64_t>(parent_offset), scan_state, nested_offset);
			for (idx_t row = 0; row < size; row++) {
				tgt_ptr[row].micros = src_ptr[row] / 1000;
			}
			break;
		}
		default:
			throw NotImplementedException("Unsupported precision for Time Type ");
		}
		break;
	}
	case LogicalTypeId::TIMESTAMP_TZ: {
		auto &datetime_info = arrow_type.GetTypeInfo<ArrowDateTimeInfo>();
		auto precision = datetime_info.GetDateTimeType();
		switch (precision) {
		case ArrowDateTimeType::SECONDS: {
			TimestampTZConversion(vector, array, scan_state, nested_offset, NumericCast<int64_t>(parent_offset), size,
			                      1000000);
			break;
		}
		case ArrowDateTimeType::MILLISECONDS: {
			TimestampTZConversion(vector, array, scan_state, nested_offset, NumericCast<int64_t>(parent_offset), size,
			                      1000);
			break;
		}
		case ArrowDateTimeType::MICROSECONDS: {
			DirectConversion(vector, array, scan_state, nested_offset, parent_offset);
			break;
		}
		case ArrowDateTimeType::NANOSECONDS: {
			auto tgt_ptr = FlatVector::GetData<timestamp_t>(vector);
			auto src_ptr = ArrowBufferData<int64_t>(array, 1) +
			               GetEffectiveOffset(array, NumericCast<int64_t>(parent_offset), scan_state, nested_offset);
			for (idx_t row = 0; row < size; row++) {
				tgt_ptr[row].value = src_ptr[row] / 1000;
			}
			break;
		}
		default:
			throw NotImplementedException("Unsupported precision for TimestampTZ Type ");
		}
		break;
	}
	case LogicalTypeId::INTERVAL: {
		auto &datetime_info = arrow_type.GetTypeInfo<ArrowDateTimeInfo>();
		auto precision = datetime_info.GetDateTimeType();
		switch (precision) {
		case ArrowDateTimeType::SECONDS: {
			IntervalConversionUs(vector, array, scan_state, nested_offset, NumericCast<int64_t>(parent_offset), size,
			                     1000000);
			break;
		}
		case ArrowDateTimeType::DAYS:
		case ArrowDateTimeType::MILLISECONDS: {
			IntervalConversionUs(vector, array, scan_state, nested_offset, NumericCast<int64_t>(parent_offset), size,
			                     1000);
			break;
		}
		case ArrowDateTimeType::MICROSECONDS: {
			IntervalConversionUs(vector, array, scan_state, nested_offset, NumericCast<int64_t>(parent_offset), size,
			                     1);
			break;
		}
		case ArrowDateTimeType::NANOSECONDS: {
			auto tgt_ptr = FlatVector::GetData<interval_t>(vector);
			auto src_ptr = ArrowBufferData<int64_t>(array, 1) +
			               GetEffectiveOffset(array, NumericCast<int64_t>(parent_offset), scan_state, nested_offset);
			for (idx_t row = 0; row < size; row++) {
				tgt_ptr[row].micros = src_ptr[row] / 1000;
				tgt_ptr[row].days = 0;
				tgt_ptr[row].months = 0;
			}
			break;
		}
		case ArrowDateTimeType::MONTHS: {
			IntervalConversionMonths(vector, array, scan_state, nested_offset, NumericCast<int64_t>(parent_offset),
			                         size);
			break;
		}
		case ArrowDateTimeType::MONTH_DAY_NANO: {
			IntervalConversionMonthDayNanos(vector, array, scan_state, nested_offset,
			                                NumericCast<int64_t>(parent_offset), size);
			break;
		}
		default:
			throw NotImplementedException("Unsupported precision for Interval/Duration Type ");
		}
		break;
	}
	case LogicalTypeId::DECIMAL: {
		auto val_mask = FlatVector::Validity(vector);
		//! We have to convert from INT128
		auto src_ptr = ArrowBufferData<hugeint_t>(array, 1) +
		               GetEffectiveOffset(array, NumericCast<int64_t>(parent_offset), scan_state, nested_offset);
		switch (vector.GetType().InternalType()) {
		case PhysicalType::INT16: {
			auto tgt_ptr = FlatVector::GetData<int16_t>(vector);
			for (idx_t row = 0; row < size; row++) {
				if (val_mask.RowIsValid(row)) {
					auto result = Hugeint::TryCast(src_ptr[row], tgt_ptr[row]);
					D_ASSERT(result);
					(void)result;
				}
			}
			break;
		}
		case PhysicalType::INT32: {
			auto tgt_ptr = FlatVector::GetData<int32_t>(vector);
			for (idx_t row = 0; row < size; row++) {
				if (val_mask.RowIsValid(row)) {
					auto result = Hugeint::TryCast(src_ptr[row], tgt_ptr[row]);
					D_ASSERT(result);
					(void)result;
				}
			}
			break;
		}
		case PhysicalType::INT64: {
			auto tgt_ptr = FlatVector::GetData<int64_t>(vector);
			for (idx_t row = 0; row < size; row++) {
				if (val_mask.RowIsValid(row)) {
					auto result = Hugeint::TryCast(src_ptr[row], tgt_ptr[row]);
					D_ASSERT(result);
					(void)result;
				}
			}
			break;
		}
		case PhysicalType::INT128: {
			FlatVector::SetData(vector, ArrowBufferData<data_t>(array, 1) +
			                                GetTypeIdSize(vector.GetType().InternalType()) *
			                                    GetEffectiveOffset(array, NumericCast<int64_t>(parent_offset),
			                                                       scan_state, nested_offset));
			break;
		}
		default:
			throw NotImplementedException("Unsupported physical type for Decimal: %s",
			                              TypeIdToString(vector.GetType().InternalType()));
		}
		break;
	}
	case LogicalTypeId::BLOB:
	case LogicalTypeId::BIT:
	case LogicalTypeId::VARINT: {
		ArrowToDuckDBBlob(vector, array, scan_state, size, arrow_type, nested_offset,
		                  NumericCast<int64_t>(parent_offset));
		break;
	}
	case LogicalTypeId::LIST: {
		ArrowToDuckDBList(vector, array, array_state, size, arrow_type, nested_offset, parent_mask,
		                  NumericCast<int64_t>(parent_offset));
		break;
	}
	case LogicalTypeId::ARRAY: {
		ArrowToDuckDBArray(vector, array, array_state, size, arrow_type, nested_offset, parent_mask,
		                   NumericCast<int64_t>(parent_offset));
		break;
	}
	case LogicalTypeId::MAP: {
		ArrowToDuckDBList(vector, array, array_state, size, arrow_type, nested_offset, parent_mask,
		                  NumericCast<int64_t>(parent_offset));
		ArrowToDuckDBMapVerify(vector, size);
		break;
	}
	case LogicalTypeId::STRUCT: {
		//! Fill the children
		auto &struct_info = arrow_type.GetTypeInfo<ArrowStructInfo>();
		auto &child_entries = StructVector::GetEntries(vector);
		auto &struct_validity_mask = FlatVector::Validity(vector);
		for (idx_t child_idx = 0; child_idx < NumericCast<idx_t>(array.n_children); child_idx++) {
			auto &child_entry = *child_entries[child_idx];
			auto &child_array = *array.children[child_idx];
			auto &child_type = struct_info.GetChild(child_idx);
			auto &child_state = array_state.GetChild(child_idx);

			SetValidityMask(child_entry, child_array, scan_state, size, array.offset, nested_offset);
			if (!struct_validity_mask.AllValid()) {
				auto &child_validity_mark = FlatVector::Validity(child_entry);
				for (idx_t i = 0; i < size; i++) {
					if (!struct_validity_mask.RowIsValid(i)) {
						child_validity_mark.SetInvalid(i);
					}
				}
			}

			auto array_physical_type = GetArrowArrayPhysicalType(child_type);
			switch (array_physical_type) {
			case ArrowArrayPhysicalType::DICTIONARY_ENCODED:
				ColumnArrowToDuckDBDictionary(child_entry, child_array, child_state, size, child_type, nested_offset,
				                              &struct_validity_mask, NumericCast<uint64_t>(array.offset));
				break;
			case ArrowArrayPhysicalType::RUN_END_ENCODED:
				ColumnArrowToDuckDBRunEndEncoded(child_entry, child_array, child_state, size, child_type, nested_offset,
				                                 &struct_validity_mask, NumericCast<uint64_t>(array.offset));
				break;
			case ArrowArrayPhysicalType::DEFAULT:
				ColumnArrowToDuckDB(child_entry, child_array, child_state, size, child_type, nested_offset,
				                    &struct_validity_mask, NumericCast<uint64_t>(array.offset));
				break;
			default:
				throw NotImplementedException("ArrowArrayPhysicalType not recognized");
			}
		}
		break;
	}
	case LogicalTypeId::UNION: {
		auto type_ids = ArrowBufferData<int8_t>(array, array.n_buffers == 1 ? 0 : 1);
		D_ASSERT(type_ids);
		auto members = UnionType::CopyMemberTypes(vector.GetType());

		auto &validity_mask = FlatVector::Validity(vector);
		auto &union_info = arrow_type.GetTypeInfo<ArrowStructInfo>();
		duckdb::vector<Vector> children;
		for (idx_t child_idx = 0; child_idx < NumericCast<idx_t>(array.n_children); child_idx++) {
			Vector child(members[child_idx].second, size);
			auto &child_array = *array.children[child_idx];
			auto &child_state = array_state.GetChild(child_idx);
			auto &child_type = union_info.GetChild(child_idx);

			SetValidityMask(child, child_array, scan_state, size, NumericCast<int64_t>(parent_offset), nested_offset);
			auto array_physical_type = GetArrowArrayPhysicalType(child_type);

			switch (array_physical_type) {
			case ArrowArrayPhysicalType::DICTIONARY_ENCODED:
				ColumnArrowToDuckDBDictionary(child, child_array, child_state, size, child_type);
				break;
			case ArrowArrayPhysicalType::RUN_END_ENCODED:
				ColumnArrowToDuckDBRunEndEncoded(child, child_array, child_state, size, child_type);
				break;
			case ArrowArrayPhysicalType::DEFAULT:
				ColumnArrowToDuckDB(child, child_array, child_state, size, child_type, nested_offset, &validity_mask);
				break;
			default:
				throw NotImplementedException("ArrowArrayPhysicalType not recognized");
			}

			children.push_back(std::move(child));
		}

		for (idx_t row_idx = 0; row_idx < size; row_idx++) {
			auto tag = NumericCast<uint8_t>(type_ids[row_idx]);

			auto out_of_range = tag >= array.n_children;
			if (out_of_range) {
				throw InvalidInputException("Arrow union tag out of range: %d", tag);
			}

			const Value &value = children[tag].GetValue(row_idx);
			vector.SetValue(row_idx, value.IsNull() ? Value() : Value::UNION(members, tag, value));
		}

		break;
	}
	default:
		throw NotImplementedException("Unsupported type for arrow conversion: %s", vector.GetType().ToString());
	}
}

template <class T>
static void SetSelectionVectorLoop(SelectionVector &sel, data_ptr_t indices_p, idx_t size) {
	auto indices = reinterpret_cast<T *>(indices_p);
	for (idx_t row = 0; row < size; row++) {
		sel.set_index(row, UnsafeNumericCast<idx_t>(indices[row]));
	}
}

template <class T>
static void SetSelectionVectorLoopWithChecks(SelectionVector &sel, data_ptr_t indices_p, idx_t size) {

	auto indices = reinterpret_cast<T *>(indices_p);
	for (idx_t row = 0; row < size; row++) {
		if (indices[row] > NumericLimits<uint32_t>::Maximum()) {
			throw ConversionException("DuckDB only supports indices that fit on an uint32");
		}
		sel.set_index(row, NumericCast<idx_t>(indices[row]));
	}
}

template <class T>
static void SetMaskedSelectionVectorLoop(SelectionVector &sel, data_ptr_t indices_p, idx_t size, ValidityMask &mask,
                                         idx_t last_element_pos) {
	auto indices = reinterpret_cast<T *>(indices_p);
	for (idx_t row = 0; row < size; row++) {
		if (mask.RowIsValid(row)) {
			sel.set_index(row, UnsafeNumericCast<idx_t>(indices[row]));
		} else {
			//! Need to point out to last element
			sel.set_index(row, last_element_pos);
		}
	}
}

static void SetSelectionVector(SelectionVector &sel, data_ptr_t indices_p, const LogicalType &logical_type, idx_t size,
                               ValidityMask *mask = nullptr, idx_t last_element_pos = 0) {
	sel.Initialize(size);

	if (mask) {
		switch (logical_type.id()) {
		case LogicalTypeId::UTINYINT:
			SetMaskedSelectionVectorLoop<uint8_t>(sel, indices_p, size, *mask, last_element_pos);
			break;
		case LogicalTypeId::TINYINT:
			SetMaskedSelectionVectorLoop<int8_t>(sel, indices_p, size, *mask, last_element_pos);
			break;
		case LogicalTypeId::USMALLINT:
			SetMaskedSelectionVectorLoop<uint16_t>(sel, indices_p, size, *mask, last_element_pos);
			break;
		case LogicalTypeId::SMALLINT:
			SetMaskedSelectionVectorLoop<int16_t>(sel, indices_p, size, *mask, last_element_pos);
			break;
		case LogicalTypeId::UINTEGER:
			if (last_element_pos > NumericLimits<uint32_t>::Maximum()) {
				//! Its guaranteed that our indices will point to the last element, so just throw an error
				throw ConversionException("DuckDB only supports indices that fit on an uint32");
			}
			SetMaskedSelectionVectorLoop<uint32_t>(sel, indices_p, size, *mask, last_element_pos);
			break;
		case LogicalTypeId::INTEGER:
			SetMaskedSelectionVectorLoop<int32_t>(sel, indices_p, size, *mask, last_element_pos);
			break;
		case LogicalTypeId::UBIGINT:
			if (last_element_pos > NumericLimits<uint32_t>::Maximum()) {
				//! Its guaranteed that our indices will point to the last element, so just throw an error
				throw ConversionException("DuckDB only supports indices that fit on an uint32");
			}
			SetMaskedSelectionVectorLoop<uint64_t>(sel, indices_p, size, *mask, last_element_pos);
			break;
		case LogicalTypeId::BIGINT:
			if (last_element_pos > NumericLimits<uint32_t>::Maximum()) {
				//! Its guaranteed that our indices will point to the last element, so just throw an error
				throw ConversionException("DuckDB only supports indices that fit on an uint32");
			}
			SetMaskedSelectionVectorLoop<int64_t>(sel, indices_p, size, *mask, last_element_pos);
			break;

		default:
			throw NotImplementedException("(Arrow) Unsupported type for selection vectors %s", logical_type.ToString());
		}

	} else {
		switch (logical_type.id()) {
		case LogicalTypeId::UTINYINT:
			SetSelectionVectorLoop<uint8_t>(sel, indices_p, size);
			break;
		case LogicalTypeId::TINYINT:
			SetSelectionVectorLoop<int8_t>(sel, indices_p, size);
			break;
		case LogicalTypeId::USMALLINT:
			SetSelectionVectorLoop<uint16_t>(sel, indices_p, size);
			break;
		case LogicalTypeId::SMALLINT:
			SetSelectionVectorLoop<int16_t>(sel, indices_p, size);
			break;
		case LogicalTypeId::UINTEGER:
			SetSelectionVectorLoop<uint32_t>(sel, indices_p, size);
			break;
		case LogicalTypeId::INTEGER:
			SetSelectionVectorLoop<int32_t>(sel, indices_p, size);
			break;
		case LogicalTypeId::UBIGINT:
			if (last_element_pos > NumericLimits<uint32_t>::Maximum()) {
				//! We need to check if our indexes fit in a uint32_t
				SetSelectionVectorLoopWithChecks<uint64_t>(sel, indices_p, size);
			} else {
				SetSelectionVectorLoop<uint64_t>(sel, indices_p, size);
			}
			break;
		case LogicalTypeId::BIGINT:
			if (last_element_pos > NumericLimits<uint32_t>::Maximum()) {
				//! We need to check if our indexes fit in a uint32_t
				SetSelectionVectorLoopWithChecks<int64_t>(sel, indices_p, size);
			} else {
				SetSelectionVectorLoop<int64_t>(sel, indices_p, size);
			}
			break;
		default:
			throw ConversionException("(Arrow) Unsupported type for selection vectors %s", logical_type.ToString());
		}
	}
}

static bool CanContainNull(const ArrowArray &array, const ValidityMask *parent_mask) {
	if (array.null_count > 0) {
		return true;
	}
	if (!parent_mask) {
		return false;
	}
	return !parent_mask->AllValid();
}

static void ColumnArrowToDuckDBDictionary(Vector &vector, ArrowArray &array, ArrowArrayScanState &array_state,
                                          idx_t size, const ArrowType &arrow_type, int64_t nested_offset,
                                          const ValidityMask *parent_mask, uint64_t parent_offset) {
	D_ASSERT(arrow_type.HasDictionary());
	auto &scan_state = array_state.state;
	const bool has_nulls = CanContainNull(array, parent_mask);
	if (array_state.CacheOutdated(array.dictionary)) {
		//! We need to set the dictionary data for this column
		auto base_vector = make_uniq<Vector>(vector.GetType(), NumericCast<idx_t>(array.dictionary->length));
		SetValidityMask(*base_vector, *array.dictionary, scan_state, NumericCast<idx_t>(array.dictionary->length), 0, 0,
		                has_nulls);
		auto &dictionary_type = arrow_type.GetDictionary();
		auto arrow_physical_type = GetArrowArrayPhysicalType(dictionary_type);
		switch (arrow_physical_type) {
		case ArrowArrayPhysicalType::DICTIONARY_ENCODED:
			ColumnArrowToDuckDBDictionary(*base_vector, *array.dictionary, array_state,
			                              NumericCast<idx_t>(array.dictionary->length), dictionary_type);
			break;
		case ArrowArrayPhysicalType::RUN_END_ENCODED:
			ColumnArrowToDuckDBRunEndEncoded(*base_vector, *array.dictionary, array_state,
			                                 NumericCast<idx_t>(array.dictionary->length), dictionary_type);
			break;
		case ArrowArrayPhysicalType::DEFAULT:
			ColumnArrowToDuckDB(*base_vector, *array.dictionary, array_state,
			                    NumericCast<idx_t>(array.dictionary->length), dictionary_type);
			break;
		default:
			throw NotImplementedException("ArrowArrayPhysicalType not recognized");
		};
		array_state.AddDictionary(std::move(base_vector), array.dictionary);
	}
	auto offset_type = arrow_type.GetDuckType();
	//! Get Pointer to Indices of Dictionary
	auto indices = ArrowBufferData<data_t>(array, 1) +
	               GetTypeIdSize(offset_type.InternalType()) *
	                   GetEffectiveOffset(array, NumericCast<int64_t>(parent_offset), scan_state, nested_offset);

	SelectionVector sel;
	if (has_nulls) {
		ValidityMask indices_validity;
		GetValidityMask(indices_validity, array, scan_state, size, NumericCast<int64_t>(parent_offset));
		if (parent_mask && !parent_mask->AllValid()) {
			auto &struct_validity_mask = *parent_mask;
			for (idx_t i = 0; i < size; i++) {
				if (!struct_validity_mask.RowIsValid(i)) {
					indices_validity.SetInvalid(i);
				}
			}
		}
		SetSelectionVector(sel, indices, offset_type, size, &indices_validity,
		                   NumericCast<idx_t>(array.dictionary->length));
	} else {
		SetSelectionVector(sel, indices, offset_type, size);
	}
	vector.Slice(array_state.GetDictionary(), sel, size);
	vector.Verify(size);
}

void ArrowTableFunction::ArrowToDuckDB(ArrowScanLocalState &scan_state, const arrow_column_map_t &arrow_convert_data,
                                       DataChunk &output, idx_t start, bool arrow_scan_is_projected) {
	for (idx_t idx = 0; idx < output.ColumnCount(); idx++) {
		auto col_idx = scan_state.column_ids[idx];

		// If projection was not pushed down into the arrow scanner, but projection pushdown is enabled on the
		// table function, we need to use original column ids here.
		auto arrow_array_idx = arrow_scan_is_projected ? idx : col_idx;

		if (col_idx == COLUMN_IDENTIFIER_ROW_ID) {
			// This column is skipped by the projection pushdown
			continue;
		}

		auto &parent_array = scan_state.chunk->arrow_array;
		auto &array = *scan_state.chunk->arrow_array.children[arrow_array_idx];
		if (!array.release) {
			throw InvalidInputException("arrow_scan: released array passed");
		}
		if (array.length != scan_state.chunk->arrow_array.length) {
			throw InvalidInputException("arrow_scan: array length mismatch");
		}

		D_ASSERT(arrow_convert_data.find(col_idx) != arrow_convert_data.end());
		auto &arrow_type = *arrow_convert_data.at(col_idx);
		auto &array_state = scan_state.GetState(col_idx);

		// Make sure this Vector keeps the Arrow chunk alive in case we can zero-copy the data
		if (!array_state.owned_data) {
			array_state.owned_data = scan_state.chunk;
		}
		output.data[idx].GetBuffer()->SetAuxiliaryData(make_uniq<ArrowAuxiliaryData>(array_state.owned_data));

		auto array_physical_type = GetArrowArrayPhysicalType(arrow_type);

		switch (array_physical_type) {
		case ArrowArrayPhysicalType::DICTIONARY_ENCODED:
			ColumnArrowToDuckDBDictionary(output.data[idx], array, array_state, output.size(), arrow_type);
			break;
		case ArrowArrayPhysicalType::RUN_END_ENCODED:
			ColumnArrowToDuckDBRunEndEncoded(output.data[idx], array, array_state, output.size(), arrow_type);
			break;
		case ArrowArrayPhysicalType::DEFAULT:
			SetValidityMask(output.data[idx], array, scan_state, output.size(), parent_array.offset, -1);
			ColumnArrowToDuckDB(output.data[idx], array, array_state, output.size(), arrow_type);
			break;
		default:
			throw NotImplementedException("ArrowArrayPhysicalType not recognized");
		}
	}
}

} // namespace duckdb
