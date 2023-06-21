#include "duckdb/function/table/arrow.hpp"
#include "duckdb/common/limits.hpp"
#include "duckdb/common/operator/multiply.hpp"
#include "duckdb/common/types/hugeint.hpp"
#include "duckdb/common/types/arrow_aux_data.hpp"
#include "duckdb/function/scalar/nested_functions.hpp"

namespace {
using duckdb::idx_t;
struct ArrowConvertDataIndices {
	//! The index that refers to 'variable_sz_type' in ArrowConvertData
	idx_t variable_sized_index;
	//! The index that refers to 'date_time_precision' in ArrowConvertData
	idx_t datetime_precision_index;
};
} // namespace

namespace duckdb {

static void ShiftRight(unsigned char *ar, int size, int shift) {
	int carry = 0;
	while (shift--) {
		for (int i = size - 1; i >= 0; --i) {
			int next = (ar[i] & 1) ? 0x80 : 0;
			ar[i] = carry | (ar[i] >> 1);
			carry = next;
		}
	}
}

template <class T>
T *ArrowBufferData(ArrowArray &array, idx_t buffer_idx) {
	return (T *)array.buffers[buffer_idx]; // NOLINT
}

static void GetValidityMask(ValidityMask &mask, ArrowArray &array, ArrowScanLocalState &scan_state, idx_t size,
                            int64_t nested_offset = -1, bool add_null = false) {
	// In certains we don't need to or cannot copy arrow's validity mask to duckdb.
	//
	// The conditions where we do want to copy arrow's mask to duckdb are:
	// 1. nulls exist
	// 2. n_buffers > 0, meaning the array's arrow type is not `null`
	// 3. the validity buffer (the first buffer) is not a nullptr
	if (array.null_count != 0 && array.n_buffers > 0 && array.buffers[0]) {
		auto bit_offset = scan_state.chunk_offset + array.offset;
		if (nested_offset != -1) {
			bit_offset = nested_offset;
		}
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
			ShiftRight(temp_nullmask.data(), n_bitmask_bytes + 1,
			           bit_offset % 8); //! why this has to be a right shift is a mystery to me
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

static void SetValidityMask(Vector &vector, ArrowArray &array, ArrowScanLocalState &scan_state, idx_t size,
                            int64_t nested_offset, bool add_null = false) {
	D_ASSERT(vector.GetVectorType() == VectorType::FLAT_VECTOR);
	auto &mask = FlatVector::Validity(vector);
	GetValidityMask(mask, array, scan_state, size, nested_offset, add_null);
}

static void ColumnArrowToDuckDB(Vector &vector, ArrowArray &array, ArrowScanLocalState &scan_state, idx_t size,
                                std::unordered_map<idx_t, unique_ptr<ArrowConvertData>> &arrow_convert_data,
                                idx_t col_idx, ArrowConvertDataIndices &arrow_convert_idx, int64_t nested_offset = -1,
                                ValidityMask *parent_mask = nullptr);

static void ArrowToDuckDBList(Vector &vector, ArrowArray &array, ArrowScanLocalState &scan_state, idx_t size,
                              std::unordered_map<idx_t, unique_ptr<ArrowConvertData>> &arrow_convert_data,
                              idx_t col_idx, ArrowConvertDataIndices &arrow_convert_idx, int64_t nested_offset,
                              ValidityMask *parent_mask) {
	auto original_type = arrow_convert_data[col_idx]->variable_sz_type[arrow_convert_idx.variable_sized_index++];
	idx_t list_size = 0;
	SetValidityMask(vector, array, scan_state, size, nested_offset);
	idx_t start_offset = 0;
	idx_t cur_offset = 0;
	if (original_type.first == ArrowVariableSizeType::FIXED_SIZE) {
		//! Have to check validity mask before setting this up
		idx_t offset = (scan_state.chunk_offset + array.offset) * original_type.second;
		if (nested_offset != -1) {
			offset = original_type.second * nested_offset;
		}
		start_offset = offset;
		auto list_data = FlatVector::GetData<list_entry_t>(vector);
		for (idx_t i = 0; i < size; i++) {
			auto &le = list_data[i];
			le.offset = cur_offset;
			le.length = original_type.second;
			cur_offset += original_type.second;
		}
		list_size = start_offset + cur_offset;
	} else if (original_type.first == ArrowVariableSizeType::NORMAL) {
		auto offsets = ArrowBufferData<uint32_t>(array, 1) + array.offset + scan_state.chunk_offset;
		if (nested_offset != -1) {
			offsets = ArrowBufferData<uint32_t>(array, 1) + nested_offset;
		}
		start_offset = offsets[0];
		auto list_data = FlatVector::GetData<list_entry_t>(vector);
		for (idx_t i = 0; i < size; i++) {
			auto &le = list_data[i];
			le.offset = cur_offset;
			le.length = offsets[i + 1] - offsets[i];
			cur_offset += le.length;
		}
		list_size = offsets[size];
	} else {
		auto offsets = ArrowBufferData<uint64_t>(array, 1) + array.offset + scan_state.chunk_offset;
		if (nested_offset != -1) {
			offsets = ArrowBufferData<uint64_t>(array, 1) + nested_offset;
		}
		start_offset = offsets[0];
		auto list_data = FlatVector::GetData<list_entry_t>(vector);
		for (idx_t i = 0; i < size; i++) {
			auto &le = list_data[i];
			le.offset = cur_offset;
			le.length = offsets[i + 1] - offsets[i];
			cur_offset += le.length;
		}
		list_size = offsets[size];
	}
	list_size -= start_offset;
	ListVector::Reserve(vector, list_size);
	ListVector::SetListSize(vector, list_size);
	auto &child_vector = ListVector::GetEntry(vector);
	SetValidityMask(child_vector, *array.children[0], scan_state, list_size, start_offset);
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
	if (list_size == 0 && start_offset == 0) {
		ColumnArrowToDuckDB(child_vector, *array.children[0], scan_state, list_size, arrow_convert_data, col_idx,
		                    arrow_convert_idx, -1);
	} else {
		ColumnArrowToDuckDB(child_vector, *array.children[0], scan_state, list_size, arrow_convert_data, col_idx,
		                    arrow_convert_idx, start_offset);
	}
}

static void ArrowToDuckDBBlob(Vector &vector, ArrowArray &array, ArrowScanLocalState &scan_state, idx_t size,
                              std::unordered_map<idx_t, unique_ptr<ArrowConvertData>> &arrow_convert_data,
                              idx_t col_idx, ArrowConvertDataIndices &arrow_convert_idx, int64_t nested_offset) {
	auto original_type = arrow_convert_data[col_idx]->variable_sz_type[arrow_convert_idx.variable_sized_index++];
	SetValidityMask(vector, array, scan_state, size, nested_offset);
	if (original_type.first == ArrowVariableSizeType::FIXED_SIZE) {
		//! Have to check validity mask before setting this up
		idx_t offset = (scan_state.chunk_offset + array.offset) * original_type.second;
		if (nested_offset != -1) {
			offset = original_type.second * nested_offset;
		}
		auto cdata = ArrowBufferData<char>(array, 1);
		for (idx_t row_idx = 0; row_idx < size; row_idx++) {
			if (FlatVector::IsNull(vector, row_idx)) {
				continue;
			}
			auto bptr = cdata + offset;
			auto blob_len = original_type.second;
			FlatVector::GetData<string_t>(vector)[row_idx] = StringVector::AddStringOrBlob(vector, bptr, blob_len);
			offset += blob_len;
		}
	} else if (original_type.first == ArrowVariableSizeType::NORMAL) {
		auto offsets = ArrowBufferData<uint32_t>(array, 1) + array.offset + scan_state.chunk_offset;
		if (nested_offset != -1) {
			offsets = ArrowBufferData<uint32_t>(array, 1) + array.offset + nested_offset;
		}
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
		auto offsets = ArrowBufferData<uint64_t>(array, 1) + array.offset + scan_state.chunk_offset;
		if (nested_offset != -1) {
			offsets = ArrowBufferData<uint64_t>(array, 1) + array.offset + nested_offset;
		}
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
	case MapInvalidReason::NULL_KEY_LIST: {
		throw InvalidInputException("Arrow map contains NULL as key list, which isn't supported by DuckDB map type");
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
		strings[row_idx] = string_t(cptr, str_len);
	}
}

static void DirectConversion(Vector &vector, ArrowArray &array, ArrowScanLocalState &scan_state,
                             int64_t nested_offset) {
	auto internal_type = GetTypeIdSize(vector.GetType().InternalType());
	auto data_ptr = ArrowBufferData<data_t>(array, 1) + internal_type * (scan_state.chunk_offset + array.offset);
	if (nested_offset != -1) {
		data_ptr = ArrowBufferData<data_t>(array, 1) + internal_type * (array.offset + nested_offset);
	}
	FlatVector::SetData(vector, data_ptr);
}

template <class T>
static void TimeConversion(Vector &vector, ArrowArray &array, ArrowScanLocalState &scan_state, int64_t nested_offset,
                           idx_t size, int64_t conversion) {
	auto tgt_ptr = FlatVector::GetData<dtime_t>(vector);
	auto &validity_mask = FlatVector::Validity(vector);
	auto src_ptr = (T *)array.buffers[1] + scan_state.chunk_offset + array.offset;
	if (nested_offset != -1) {
		src_ptr = (T *)array.buffers[1] + nested_offset + array.offset;
	}
	for (idx_t row = 0; row < size; row++) {
		if (!validity_mask.RowIsValid(row)) {
			continue;
		}
		if (!TryMultiplyOperator::Operation((int64_t)src_ptr[row], conversion, tgt_ptr[row].micros)) {
			throw ConversionException("Could not convert Time to Microsecond");
		}
	}
}

static void TimestampTZConversion(Vector &vector, ArrowArray &array, ArrowScanLocalState &scan_state,
                                  int64_t nested_offset, idx_t size, int64_t conversion) {
	auto tgt_ptr = FlatVector::GetData<timestamp_t>(vector);
	auto &validity_mask = FlatVector::Validity(vector);
	auto src_ptr = ArrowBufferData<int64_t>(array, 1) + scan_state.chunk_offset + array.offset;
	if (nested_offset != -1) {
		src_ptr = ArrowBufferData<int64_t>(array, 1) + nested_offset + array.offset;
	}
	for (idx_t row = 0; row < size; row++) {
		if (!validity_mask.RowIsValid(row)) {
			continue;
		}
		if (!TryMultiplyOperator::Operation(src_ptr[row], conversion, tgt_ptr[row].value)) {
			throw ConversionException("Could not convert TimestampTZ to Microsecond");
		}
	}
}

static void IntervalConversionUs(Vector &vector, ArrowArray &array, ArrowScanLocalState &scan_state,
                                 int64_t nested_offset, idx_t size, int64_t conversion) {
	auto tgt_ptr = FlatVector::GetData<interval_t>(vector);
	auto src_ptr = ArrowBufferData<int64_t>(array, 1) + scan_state.chunk_offset + array.offset;
	if (nested_offset != -1) {
		src_ptr = ArrowBufferData<int64_t>(array, 1) + nested_offset + array.offset;
	}
	for (idx_t row = 0; row < size; row++) {
		tgt_ptr[row].days = 0;
		tgt_ptr[row].months = 0;
		if (!TryMultiplyOperator::Operation(src_ptr[row], conversion, tgt_ptr[row].micros)) {
			throw ConversionException("Could not convert Interval to Microsecond");
		}
	}
}

static void IntervalConversionMonths(Vector &vector, ArrowArray &array, ArrowScanLocalState &scan_state,
                                     int64_t nested_offset, idx_t size) {
	auto tgt_ptr = FlatVector::GetData<interval_t>(vector);
	auto src_ptr = ArrowBufferData<int32_t>(array, 1) + scan_state.chunk_offset + array.offset;
	if (nested_offset != -1) {
		src_ptr = ArrowBufferData<int32_t>(array, 1) + nested_offset + array.offset;
	}
	for (idx_t row = 0; row < size; row++) {
		tgt_ptr[row].days = 0;
		tgt_ptr[row].micros = 0;
		tgt_ptr[row].months = src_ptr[row];
	}
}

static void IntervalConversionMonthDayNanos(Vector &vector, ArrowArray &array, ArrowScanLocalState &scan_state,
                                            int64_t nested_offset, idx_t size) {
	auto tgt_ptr = FlatVector::GetData<interval_t>(vector);
	auto src_ptr = ArrowBufferData<ArrowInterval>(array, 1) + scan_state.chunk_offset + array.offset;
	if (nested_offset != -1) {
		src_ptr = ArrowBufferData<ArrowInterval>(array, 1) + nested_offset + array.offset;
	}
	for (idx_t row = 0; row < size; row++) {
		tgt_ptr[row].days = src_ptr[row].days;
		tgt_ptr[row].micros = src_ptr[row].nanoseconds / Interval::NANOS_PER_MICRO;
		tgt_ptr[row].months = src_ptr[row].months;
	}
}

static void ColumnArrowToDuckDB(Vector &vector, ArrowArray &array, ArrowScanLocalState &scan_state, idx_t size,
                                std::unordered_map<idx_t, unique_ptr<ArrowConvertData>> &arrow_convert_data,
                                idx_t col_idx, ArrowConvertDataIndices &arrow_convert_idx, int64_t nested_offset,
                                ValidityMask *parent_mask) {
	switch (vector.GetType().id()) {
	case LogicalTypeId::SQLNULL:
		vector.Reference(Value());
		break;
	case LogicalTypeId::BOOLEAN: {
		//! Arrow bit-packs boolean values
		//! Lets first figure out where we are in the source array
		auto src_ptr = ArrowBufferData<uint8_t>(array, 1) + (scan_state.chunk_offset + array.offset) / 8;

		if (nested_offset != -1) {
			src_ptr = ArrowBufferData<uint8_t>(array, 1) + (nested_offset + array.offset) / 8;
		}
		auto tgt_ptr = (uint8_t *)FlatVector::GetData(vector);
		int src_pos = 0;
		idx_t cur_bit = scan_state.chunk_offset % 8;
		if (nested_offset != -1) {
			cur_bit = nested_offset % 8;
		}
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
	case LogicalTypeId::TIMESTAMP:
	case LogicalTypeId::TIMESTAMP_SEC:
	case LogicalTypeId::TIMESTAMP_MS:
	case LogicalTypeId::TIMESTAMP_NS: {
		DirectConversion(vector, array, scan_state, nested_offset);
		break;
	}
	case LogicalTypeId::VARCHAR: {
		auto original_type = arrow_convert_data[col_idx]->variable_sz_type[arrow_convert_idx.variable_sized_index++];
		auto cdata = ArrowBufferData<char>(array, 2);
		if (original_type.first == ArrowVariableSizeType::SUPER_SIZE) {
			auto offsets = ArrowBufferData<uint64_t>(array, 1) + array.offset + scan_state.chunk_offset;
			if (nested_offset != -1) {
				offsets = ArrowBufferData<uint64_t>(array, 1) + array.offset + nested_offset;
			}
			SetVectorString(vector, size, cdata, offsets);
		} else {
			auto offsets = ArrowBufferData<uint32_t>(array, 1) + array.offset + scan_state.chunk_offset;
			if (nested_offset != -1) {
				offsets = ArrowBufferData<uint32_t>(array, 1) + array.offset + nested_offset;
			}
			SetVectorString(vector, size, cdata, offsets);
		}
		break;
	}
	case LogicalTypeId::DATE: {
		auto precision = arrow_convert_data[col_idx]->date_time_precision[arrow_convert_idx.datetime_precision_index++];
		switch (precision) {
		case ArrowDateTimeType::DAYS: {
			DirectConversion(vector, array, scan_state, nested_offset);
			break;
		}
		case ArrowDateTimeType::MILLISECONDS: {
			//! convert date from nanoseconds to days
			auto src_ptr = ArrowBufferData<uint64_t>(array, 1) + scan_state.chunk_offset + array.offset;
			if (nested_offset != -1) {
				src_ptr = ArrowBufferData<uint64_t>(array, 1) + nested_offset + array.offset;
			}
			auto tgt_ptr = FlatVector::GetData<date_t>(vector);
			for (idx_t row = 0; row < size; row++) {
				tgt_ptr[row] = date_t(int64_t(src_ptr[row]) / static_cast<int64_t>(1000 * 60 * 60 * 24));
			}
			break;
		}
		default:
			throw NotImplementedException("Unsupported precision for Date Type ");
		}
		break;
	}
	case LogicalTypeId::TIME: {
		auto precision = arrow_convert_data[col_idx]->date_time_precision[arrow_convert_idx.datetime_precision_index++];
		switch (precision) {
		case ArrowDateTimeType::SECONDS: {
			TimeConversion<int32_t>(vector, array, scan_state, nested_offset, size, 1000000);
			break;
		}
		case ArrowDateTimeType::MILLISECONDS: {
			TimeConversion<int32_t>(vector, array, scan_state, nested_offset, size, 1000);
			break;
		}
		case ArrowDateTimeType::MICROSECONDS: {
			TimeConversion<int64_t>(vector, array, scan_state, nested_offset, size, 1);
			break;
		}
		case ArrowDateTimeType::NANOSECONDS: {
			auto tgt_ptr = FlatVector::GetData<dtime_t>(vector);
			auto src_ptr = ArrowBufferData<int64_t>(array, 1) + scan_state.chunk_offset + array.offset;
			if (nested_offset != -1) {
				src_ptr = ArrowBufferData<int64_t>(array, 1) + nested_offset + array.offset;
			}
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
		auto precision = arrow_convert_data[col_idx]->date_time_precision[arrow_convert_idx.datetime_precision_index++];
		switch (precision) {
		case ArrowDateTimeType::SECONDS: {
			TimestampTZConversion(vector, array, scan_state, nested_offset, size, 1000000);
			break;
		}
		case ArrowDateTimeType::MILLISECONDS: {
			TimestampTZConversion(vector, array, scan_state, nested_offset, size, 1000);
			break;
		}
		case ArrowDateTimeType::MICROSECONDS: {
			DirectConversion(vector, array, scan_state, nested_offset);
			break;
		}
		case ArrowDateTimeType::NANOSECONDS: {
			auto tgt_ptr = FlatVector::GetData<timestamp_t>(vector);
			auto src_ptr = ArrowBufferData<int64_t>(array, 1) + scan_state.chunk_offset + array.offset;
			if (nested_offset != -1) {
				src_ptr = ArrowBufferData<int64_t>(array, 1) + nested_offset + array.offset;
			}
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
		auto precision = arrow_convert_data[col_idx]->date_time_precision[arrow_convert_idx.datetime_precision_index++];
		switch (precision) {
		case ArrowDateTimeType::SECONDS: {
			IntervalConversionUs(vector, array, scan_state, nested_offset, size, 1000000);
			break;
		}
		case ArrowDateTimeType::DAYS:
		case ArrowDateTimeType::MILLISECONDS: {
			IntervalConversionUs(vector, array, scan_state, nested_offset, size, 1000);
			break;
		}
		case ArrowDateTimeType::MICROSECONDS: {
			IntervalConversionUs(vector, array, scan_state, nested_offset, size, 1);
			break;
		}
		case ArrowDateTimeType::NANOSECONDS: {
			auto tgt_ptr = FlatVector::GetData<interval_t>(vector);
			auto src_ptr = ArrowBufferData<int64_t>(array, 1) + scan_state.chunk_offset + array.offset;
			if (nested_offset != -1) {
				src_ptr = ArrowBufferData<int64_t>(array, 1) + nested_offset + array.offset;
			}
			for (idx_t row = 0; row < size; row++) {
				tgt_ptr[row].micros = src_ptr[row] / 1000;
				tgt_ptr[row].days = 0;
				tgt_ptr[row].months = 0;
			}
			break;
		}
		case ArrowDateTimeType::MONTHS: {
			IntervalConversionMonths(vector, array, scan_state, nested_offset, size);
			break;
		}
		case ArrowDateTimeType::MONTH_DAY_NANO: {
			IntervalConversionMonthDayNanos(vector, array, scan_state, nested_offset, size);
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
		auto src_ptr = ArrowBufferData<hugeint_t>(array, 1) + scan_state.chunk_offset + array.offset;
		if (nested_offset != -1) {
			src_ptr = ArrowBufferData<hugeint_t>(array, 1) + nested_offset + array.offset;
		}
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
			FlatVector::SetData(vector,
			                    ArrowBufferData<data_t>(array, 1) + GetTypeIdSize(vector.GetType().InternalType()) *
			                                                            (scan_state.chunk_offset + array.offset));
			break;
		}
		default:
			throw NotImplementedException("Unsupported physical type for Decimal: %s",
			                              TypeIdToString(vector.GetType().InternalType()));
		}
		break;
	}
	case LogicalTypeId::BLOB: {
		ArrowToDuckDBBlob(vector, array, scan_state, size, arrow_convert_data, col_idx, arrow_convert_idx,
		                  nested_offset);
		break;
	}
	case LogicalTypeId::LIST: {
		ArrowToDuckDBList(vector, array, scan_state, size, arrow_convert_data, col_idx, arrow_convert_idx,
		                  nested_offset, parent_mask);
		break;
	}
	case LogicalTypeId::MAP: {
		ArrowToDuckDBList(vector, array, scan_state, size, arrow_convert_data, col_idx, arrow_convert_idx,
		                  nested_offset, parent_mask);
		ArrowToDuckDBMapVerify(vector, size);
		break;
	}
	case LogicalTypeId::STRUCT: {
		//! Fill the children
		auto &child_entries = StructVector::GetEntries(vector);
		auto &struct_validity_mask = FlatVector::Validity(vector);
		for (idx_t type_idx = 0; type_idx < (idx_t)array.n_children; type_idx++) {
			SetValidityMask(*child_entries[type_idx], *array.children[type_idx], scan_state, size, nested_offset);
			if (!struct_validity_mask.AllValid()) {
				auto &child_validity_mark = FlatVector::Validity(*child_entries[type_idx]);
				for (idx_t i = 0; i < size; i++) {
					if (!struct_validity_mask.RowIsValid(i)) {
						child_validity_mark.SetInvalid(i);
					}
				}
			}
			ColumnArrowToDuckDB(*child_entries[type_idx], *array.children[type_idx], scan_state, size,
			                    arrow_convert_data, col_idx, arrow_convert_idx, nested_offset, &struct_validity_mask);
		}
		break;
	}
	default:
		throw NotImplementedException("Unsupported type %s", vector.GetType().ToString());
	}
}

template <class T>
static void SetSelectionVectorLoop(SelectionVector &sel, data_ptr_t indices_p, idx_t size) {
	auto indices = reinterpret_cast<T *>(indices_p);
	for (idx_t row = 0; row < size; row++) {
		sel.set_index(row, indices[row]);
	}
}

template <class T>
static void SetSelectionVectorLoopWithChecks(SelectionVector &sel, data_ptr_t indices_p, idx_t size) {

	auto indices = reinterpret_cast<T *>(indices_p);
	for (idx_t row = 0; row < size; row++) {
		if (indices[row] > NumericLimits<uint32_t>::Maximum()) {
			throw ConversionException("DuckDB only supports indices that fit on an uint32");
		}
		sel.set_index(row, indices[row]);
	}
}

template <class T>
static void SetMaskedSelectionVectorLoop(SelectionVector &sel, data_ptr_t indices_p, idx_t size, ValidityMask &mask,
                                         idx_t last_element_pos) {
	auto indices = reinterpret_cast<T *>(indices_p);
	for (idx_t row = 0; row < size; row++) {
		if (mask.RowIsValid(row)) {
			sel.set_index(row, indices[row]);
		} else {
			//! Need to point out to last element
			sel.set_index(row, last_element_pos);
		}
	}
}

static void SetSelectionVector(SelectionVector &sel, data_ptr_t indices_p, LogicalType &logical_type, idx_t size,
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

static void ColumnArrowToDuckDBDictionary(Vector &vector, ArrowArray &array, ArrowScanLocalState &scan_state,
                                          idx_t size,
                                          std::unordered_map<idx_t, unique_ptr<ArrowConvertData>> &arrow_convert_data,
                                          idx_t col_idx, ArrowConvertDataIndices &arrow_convert_idx) {
	SelectionVector sel;
	auto &dict_vectors = scan_state.arrow_dictionary_vectors;
	if (!dict_vectors.count(col_idx)) {
		//! We need to set the dictionary data for this column
		auto base_vector = make_uniq<Vector>(vector.GetType(), array.dictionary->length);
		SetValidityMask(*base_vector, *array.dictionary, scan_state, array.dictionary->length, 0, array.null_count > 0);
		ColumnArrowToDuckDB(*base_vector, *array.dictionary, scan_state, array.dictionary->length, arrow_convert_data,
		                    col_idx, arrow_convert_idx);
		dict_vectors[col_idx] = std::move(base_vector);
	}
	auto dictionary_type = arrow_convert_data[col_idx]->dictionary_type;
	//! Get Pointer to Indices of Dictionary
	auto indices = ArrowBufferData<data_t>(array, 1) +
	               GetTypeIdSize(dictionary_type.InternalType()) * (scan_state.chunk_offset + array.offset);
	if (array.null_count > 0) {
		ValidityMask indices_validity;
		GetValidityMask(indices_validity, array, scan_state, size);
		SetSelectionVector(sel, indices, dictionary_type, size, &indices_validity, array.dictionary->length);
	} else {
		SetSelectionVector(sel, indices, dictionary_type, size);
	}
	vector.Slice(*dict_vectors[col_idx], sel, size);
}

void ArrowTableFunction::ArrowToDuckDB(ArrowScanLocalState &scan_state,
                                       unordered_map<idx_t, unique_ptr<ArrowConvertData>> &arrow_convert_data,
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

		ArrowConvertDataIndices arrow_convert_idx {0, 0};
		auto &array = *scan_state.chunk->arrow_array.children[arrow_array_idx];
		if (!array.release) {
			throw InvalidInputException("arrow_scan: released array passed");
		}
		if (array.length != scan_state.chunk->arrow_array.length) {
			throw InvalidInputException("arrow_scan: array length mismatch");
		}
		// Make sure this Vector keeps the Arrow chunk alive in case we can zero-copy the data
		output.data[idx].GetBuffer()->SetAuxiliaryData(make_uniq<ArrowAuxiliaryData>(scan_state.chunk));
		if (array.dictionary) {
			ColumnArrowToDuckDBDictionary(output.data[idx], array, scan_state, output.size(), arrow_convert_data,
			                              col_idx, arrow_convert_idx);
		} else {
			SetValidityMask(output.data[idx], array, scan_state, output.size(), -1);
			ColumnArrowToDuckDB(output.data[idx], array, scan_state, output.size(), arrow_convert_data, col_idx,
			                    arrow_convert_idx);
		}
	}
}

} // namespace duckdb
