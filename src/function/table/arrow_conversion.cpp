#include "duckdb/function/table/arrow.hpp"
#include "duckdb/common/limits.hpp"
#include "duckdb/common/operator/multiply.hpp"
#include "duckdb/common/types/hugeint.hpp"
#include "duckdb/common/types/arrow_aux_data.hpp"
#include "duckdb/function/scalar/nested_functions.hpp"

namespace duckdb {

void ShiftRight(unsigned char *ar, int size, int shift) {
	int carry = 0;
	while (shift--) {
		for (int i = size - 1; i >= 0; --i) {
			int next = (ar[i] & 1) ? 0x80 : 0;
			ar[i] = carry | (ar[i] >> 1);
			carry = next;
		}
	}
}

void GetValidityMask(ValidityMask &mask, ArrowArray &array, ArrowScanLocalState &scan_state, idx_t size,
                     int64_t nested_offset = -1, bool add_null = false) {
	if (array.null_count != 0 && array.buffers[0]) {
		auto bit_offset = scan_state.chunk_offset + array.offset;
		if (nested_offset != -1) {
			bit_offset = nested_offset;
		}
		mask.EnsureWritable();
#if STANDARD_VECTOR_SIZE > 64
		auto n_bitmask_bytes = (size + 8 - 1) / 8;
		if (bit_offset % 8 == 0) {
			//! just memcpy nullmask
			memcpy((void *)mask.GetData(), (uint8_t *)array.buffers[0] + bit_offset / 8, n_bitmask_bytes);
		} else {
			//! need to re-align nullmask
			std::vector<uint8_t> temp_nullmask(n_bitmask_bytes + 1);
			memcpy(temp_nullmask.data(), (uint8_t *)array.buffers[0] + bit_offset / 8, n_bitmask_bytes + 1);
			ShiftRight(temp_nullmask.data(), n_bitmask_bytes + 1,
			           bit_offset % 8); //! why this has to be a right shift is a mystery to me
			memcpy((void *)mask.GetData(), (data_ptr_t)temp_nullmask.data(), n_bitmask_bytes);
		}
#else
		auto byte_offset = bit_offset / 8;
		auto source_data = (uint8_t *)array.buffers[0];
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

void SetValidityMask(Vector &vector, ArrowArray &array, ArrowScanLocalState &scan_state, idx_t size,
                     int64_t nested_offset, bool add_null = false) {
	D_ASSERT(vector.GetVectorType() == VectorType::FLAT_VECTOR);
	auto &mask = FlatVector::Validity(vector);
	GetValidityMask(mask, array, scan_state, size, nested_offset, add_null);
}

void ColumnArrowToDuckDB(Vector &vector, ArrowArray &array, ArrowScanLocalState &scan_state, idx_t size,
                         std::unordered_map<idx_t, unique_ptr<ArrowConvertData>> &arrow_convert_data, idx_t col_idx,
                         std::pair<idx_t, idx_t> &arrow_convert_idx, int64_t nested_offset = -1,
                         ValidityMask *parent_mask = nullptr);

void ArrowToDuckDBList(Vector &vector, ArrowArray &array, ArrowScanLocalState &scan_state, idx_t size,
                       std::unordered_map<idx_t, unique_ptr<ArrowConvertData>> &arrow_convert_data, idx_t col_idx,
                       std::pair<idx_t, idx_t> &arrow_convert_idx, int64_t nested_offset, ValidityMask *parent_mask) {
	auto original_type = arrow_convert_data[col_idx]->variable_sz_type[arrow_convert_idx.first++];
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
		auto offsets = (uint32_t *)array.buffers[1] + array.offset + scan_state.chunk_offset;
		if (nested_offset != -1) {
			offsets = (uint32_t *)array.buffers[1] + nested_offset;
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
		auto offsets = (uint64_t *)array.buffers[1] + array.offset + scan_state.chunk_offset;
		if (nested_offset != -1) {
			offsets = (uint64_t *)array.buffers[1] + nested_offset;
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

void ArrowToDuckDBBlob(Vector &vector, ArrowArray &array, ArrowScanLocalState &scan_state, idx_t size,
                       std::unordered_map<idx_t, unique_ptr<ArrowConvertData>> &arrow_convert_data, idx_t col_idx,
                       std::pair<idx_t, idx_t> &arrow_convert_idx, int64_t nested_offset) {
	auto original_type = arrow_convert_data[col_idx]->variable_sz_type[arrow_convert_idx.first++];
	SetValidityMask(vector, array, scan_state, size, nested_offset);
	if (original_type.first == ArrowVariableSizeType::FIXED_SIZE) {
		//! Have to check validity mask before setting this up
		idx_t offset = (scan_state.chunk_offset + array.offset) * original_type.second;
		if (nested_offset != -1) {
			offset = original_type.second * nested_offset;
		}
		auto cdata = (char *)array.buffers[1];
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
		auto offsets = (uint32_t *)array.buffers[1] + array.offset + scan_state.chunk_offset;
		if (nested_offset != -1) {
			offsets = (uint32_t *)array.buffers[1] + array.offset + nested_offset;
		}
		auto cdata = (char *)array.buffers[2];
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
		if (((uint64_t *)array.buffers[1])[array.length] > NumericLimits<uint32_t>::Maximum()) { // LCOV_EXCL_START
			throw std::runtime_error("DuckDB does not support Blobs over 4GB");
		} // LCOV_EXCL_STOP
		auto offsets = (uint64_t *)array.buffers[1] + array.offset + scan_state.chunk_offset;
		if (nested_offset != -1) {
			offsets = (uint64_t *)array.buffers[1] + array.offset + nested_offset;
		}
		auto cdata = (char *)array.buffers[2];
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

void ArrowToDuckDBMapVerify(Vector &vector, idx_t count) {
	auto valid_check = CheckMapValidity(vector, count);
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

void ArrowToDuckDBMapList(Vector &vector, ArrowArray &array, ArrowScanLocalState &scan_state, idx_t size,
                          unordered_map<idx_t, unique_ptr<ArrowConvertData>> &arrow_convert_data, idx_t col_idx,
                          pair<idx_t, idx_t> &arrow_convert_idx, uint32_t *offsets, ValidityMask *parent_mask) {
	idx_t list_size = offsets[size] - offsets[0];
	ListVector::Reserve(vector, list_size);

	auto &child_vector = ListVector::GetEntry(vector);
	auto list_data = FlatVector::GetData<list_entry_t>(vector);
	auto cur_offset = 0;
	for (idx_t i = 0; i < size; i++) {
		auto &le = list_data[i];
		le.offset = cur_offset;
		le.length = offsets[i + 1] - offsets[i];
		cur_offset += le.length;
	}
	ListVector::SetListSize(vector, list_size);
	if (list_size == 0 && offsets[0] == 0) {
		SetValidityMask(child_vector, array, scan_state, list_size, -1);
	} else {
		SetValidityMask(child_vector, array, scan_state, list_size, offsets[0]);
	}

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
	if (list_size == 0 && offsets[0] == 0) {
		ColumnArrowToDuckDB(child_vector, array, scan_state, list_size, arrow_convert_data, col_idx, arrow_convert_idx,
		                    -1);
	} else {
		ColumnArrowToDuckDB(child_vector, array, scan_state, list_size, arrow_convert_data, col_idx, arrow_convert_idx,
		                    offsets[0]);
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
		strings[row_idx] = string_t(cptr, str_len);
	}
}

void DirectConversion(Vector &vector, ArrowArray &array, ArrowScanLocalState &scan_state, int64_t nested_offset) {
	auto internal_type = GetTypeIdSize(vector.GetType().InternalType());
	auto data_ptr = (data_ptr_t)array.buffers[1] + internal_type * (scan_state.chunk_offset + array.offset);
	if (nested_offset != -1) {
		data_ptr = (data_ptr_t)array.buffers[1] + internal_type * (array.offset + nested_offset);
	}
	FlatVector::SetData(vector, data_ptr);
}

template <class T>
void TimeConversion(Vector &vector, ArrowArray &array, ArrowScanLocalState &scan_state, int64_t nested_offset,
                    idx_t size, int64_t conversion) {
	auto tgt_ptr = (dtime_t *)FlatVector::GetData(vector);
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

void TimestampTZConversion(Vector &vector, ArrowArray &array, ArrowScanLocalState &scan_state, int64_t nested_offset,
                           idx_t size, int64_t conversion) {
	auto tgt_ptr = (timestamp_t *)FlatVector::GetData(vector);
	auto &validity_mask = FlatVector::Validity(vector);
	auto src_ptr = (int64_t *)array.buffers[1] + scan_state.chunk_offset + array.offset;
	if (nested_offset != -1) {
		src_ptr = (int64_t *)array.buffers[1] + nested_offset + array.offset;
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

void IntervalConversionUs(Vector &vector, ArrowArray &array, ArrowScanLocalState &scan_state, int64_t nested_offset,
                          idx_t size, int64_t conversion) {
	auto tgt_ptr = (interval_t *)FlatVector::GetData(vector);
	auto src_ptr = (int64_t *)array.buffers[1] + scan_state.chunk_offset + array.offset;
	if (nested_offset != -1) {
		src_ptr = (int64_t *)array.buffers[1] + nested_offset + array.offset;
	}
	for (idx_t row = 0; row < size; row++) {
		tgt_ptr[row].days = 0;
		tgt_ptr[row].months = 0;
		if (!TryMultiplyOperator::Operation(src_ptr[row], conversion, tgt_ptr[row].micros)) {
			throw ConversionException("Could not convert Interval to Microsecond");
		}
	}
}

void IntervalConversionMonths(Vector &vector, ArrowArray &array, ArrowScanLocalState &scan_state, int64_t nested_offset,
                              idx_t size) {
	auto tgt_ptr = (interval_t *)FlatVector::GetData(vector);
	auto src_ptr = (int32_t *)array.buffers[1] + scan_state.chunk_offset + array.offset;
	if (nested_offset != -1) {
		src_ptr = (int32_t *)array.buffers[1] + nested_offset + array.offset;
	}
	for (idx_t row = 0; row < size; row++) {
		tgt_ptr[row].days = 0;
		tgt_ptr[row].micros = 0;
		tgt_ptr[row].months = src_ptr[row];
	}
}

void ColumnArrowToDuckDB(Vector &vector, ArrowArray &array, ArrowScanLocalState &scan_state, idx_t size,
                         std::unordered_map<idx_t, unique_ptr<ArrowConvertData>> &arrow_convert_data, idx_t col_idx,
                         std::pair<idx_t, idx_t> &arrow_convert_idx, int64_t nested_offset, ValidityMask *parent_mask) {
	switch (vector.GetType().id()) {
	case LogicalTypeId::SQLNULL:
		vector.Reference(Value());
		break;
	case LogicalTypeId::BOOLEAN: {
		//! Arrow bit-packs boolean values
		//! Lets first figure out where we are in the source array
		auto src_ptr = (uint8_t *)array.buffers[1] + (scan_state.chunk_offset + array.offset) / 8;

		if (nested_offset != -1) {
			src_ptr = (uint8_t *)array.buffers[1] + (nested_offset + array.offset) / 8;
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
	case LogicalTypeId::JSON:
	case LogicalTypeId::VARCHAR: {
		auto original_type = arrow_convert_data[col_idx]->variable_sz_type[arrow_convert_idx.first++];
		auto cdata = (char *)array.buffers[2];
		if (original_type.first == ArrowVariableSizeType::SUPER_SIZE) {
			if (((uint64_t *)array.buffers[1])[array.length] > NumericLimits<uint32_t>::Maximum()) { // LCOV_EXCL_START
				throw std::runtime_error("DuckDB does not support Strings over 4GB");
			} // LCOV_EXCL_STOP
			auto offsets = (uint64_t *)array.buffers[1] + array.offset + scan_state.chunk_offset;
			if (nested_offset != -1) {
				offsets = (uint64_t *)array.buffers[1] + array.offset + nested_offset;
			}
			SetVectorString(vector, size, cdata, offsets);
		} else {
			auto offsets = (uint32_t *)array.buffers[1] + array.offset + scan_state.chunk_offset;
			if (nested_offset != -1) {
				offsets = (uint32_t *)array.buffers[1] + array.offset + nested_offset;
			}
			SetVectorString(vector, size, cdata, offsets);
		}
		break;
	}
	case LogicalTypeId::DATE: {
		auto precision = arrow_convert_data[col_idx]->date_time_precision[arrow_convert_idx.second++];
		switch (precision) {
		case ArrowDateTimeType::DAYS: {
			DirectConversion(vector, array, scan_state, nested_offset);
			break;
		}
		case ArrowDateTimeType::MILLISECONDS: {
			//! convert date from nanoseconds to days
			auto src_ptr = (uint64_t *)array.buffers[1] + scan_state.chunk_offset + array.offset;
			if (nested_offset != -1) {
				src_ptr = (uint64_t *)array.buffers[1] + nested_offset + array.offset;
			}
			auto tgt_ptr = (date_t *)FlatVector::GetData(vector);
			for (idx_t row = 0; row < size; row++) {
				tgt_ptr[row] = date_t(int64_t(src_ptr[row]) / (1000 * 60 * 60 * 24));
			}
			break;
		}
		default:
			throw std::runtime_error("Unsupported precision for Date Type ");
		}
		break;
	}
	case LogicalTypeId::TIME: {
		auto precision = arrow_convert_data[col_idx]->date_time_precision[arrow_convert_idx.second++];
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
			auto tgt_ptr = (dtime_t *)FlatVector::GetData(vector);
			auto src_ptr = (int64_t *)array.buffers[1] + scan_state.chunk_offset + array.offset;
			if (nested_offset != -1) {
				src_ptr = (int64_t *)array.buffers[1] + nested_offset + array.offset;
			}
			for (idx_t row = 0; row < size; row++) {
				tgt_ptr[row].micros = src_ptr[row] / 1000;
			}
			break;
		}
		default:
			throw std::runtime_error("Unsupported precision for Time Type ");
		}
		break;
	}
	case LogicalTypeId::TIMESTAMP_TZ: {
		auto precision = arrow_convert_data[col_idx]->date_time_precision[arrow_convert_idx.second++];
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
			auto tgt_ptr = (timestamp_t *)FlatVector::GetData(vector);
			auto src_ptr = (int64_t *)array.buffers[1] + scan_state.chunk_offset + array.offset;
			if (nested_offset != -1) {
				src_ptr = (int64_t *)array.buffers[1] + nested_offset + array.offset;
			}
			for (idx_t row = 0; row < size; row++) {
				tgt_ptr[row].value = src_ptr[row] / 1000;
			}
			break;
		}
		default:
			throw std::runtime_error("Unsupported precision for TimestampTZ Type ");
		}
		break;
	}
	case LogicalTypeId::INTERVAL: {
		auto precision = arrow_convert_data[col_idx]->date_time_precision[arrow_convert_idx.second++];
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
			auto tgt_ptr = (interval_t *)FlatVector::GetData(vector);
			auto src_ptr = (int64_t *)array.buffers[1] + scan_state.chunk_offset + array.offset;
			if (nested_offset != -1) {
				src_ptr = (int64_t *)array.buffers[1] + nested_offset + array.offset;
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
		default:
			throw std::runtime_error("Unsupported precision for Interval/Duration Type ");
		}
		break;
	}
	case LogicalTypeId::DECIMAL: {
		auto val_mask = FlatVector::Validity(vector);
		//! We have to convert from INT128
		auto src_ptr = (hugeint_t *)array.buffers[1] + scan_state.chunk_offset + array.offset;
		if (nested_offset != -1) {
			src_ptr = (hugeint_t *)array.buffers[1] + nested_offset + array.offset;
		}
		switch (vector.GetType().InternalType()) {
		case PhysicalType::INT16: {
			auto tgt_ptr = (int16_t *)FlatVector::GetData(vector);
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
			auto tgt_ptr = (int32_t *)FlatVector::GetData(vector);
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
			auto tgt_ptr = (int64_t *)FlatVector::GetData(vector);
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
			FlatVector::SetData(vector, (data_ptr_t)array.buffers[1] + GetTypeIdSize(vector.GetType().InternalType()) *
			                                                               (scan_state.chunk_offset + array.offset));
			break;
		}
		default:
			throw std::runtime_error("Unsupported physical type for Decimal: " +
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
		//! Since this is a map we skip first child, because its a struct
		auto &struct_arrow = *array.children[0];
		auto &child_entries = StructVector::GetEntries(vector);
		D_ASSERT(child_entries.size() == 2);
		auto offsets = (uint32_t *)array.buffers[1] + array.offset + scan_state.chunk_offset;
		if (nested_offset != -1) {
			offsets = (uint32_t *)array.buffers[1] + nested_offset;
		}
		auto &struct_validity_mask = FlatVector::Validity(vector);
		//! Fill the children
		for (idx_t type_idx = 0; type_idx < (idx_t)struct_arrow.n_children; type_idx++) {
			ArrowToDuckDBMapList(*child_entries[type_idx], *struct_arrow.children[type_idx], scan_state, size,
			                     arrow_convert_data, col_idx, arrow_convert_idx, offsets, &struct_validity_mask);
		}
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
		throw std::runtime_error("Unsupported type " + vector.GetType().ToString());
	}
}

template <class T>
static void SetSelectionVectorLoop(SelectionVector &sel, data_ptr_t indices_p, idx_t size) {
	auto indices = (T *)indices_p;
	for (idx_t row = 0; row < size; row++) {
		sel.set_index(row, indices[row]);
	}
}

template <class T>
static void SetSelectionVectorLoopWithChecks(SelectionVector &sel, data_ptr_t indices_p, idx_t size) {

	auto indices = (T *)indices_p;
	for (idx_t row = 0; row < size; row++) {
		if (indices[row] > NumericLimits<uint32_t>::Maximum()) {
			throw std::runtime_error("DuckDB only supports indices that fit on an uint32");
		}
		sel.set_index(row, indices[row]);
	}
}

template <class T>
static void SetMaskedSelectionVectorLoop(SelectionVector &sel, data_ptr_t indices_p, idx_t size, ValidityMask &mask,
                                         idx_t last_element_pos) {
	auto indices = (T *)indices_p;
	for (idx_t row = 0; row < size; row++) {
		if (mask.RowIsValid(row)) {
			sel.set_index(row, indices[row]);
		} else {
			//! Need to point out to last element
			sel.set_index(row, last_element_pos);
		}
	}
}

void SetSelectionVector(SelectionVector &sel, data_ptr_t indices_p, LogicalType &logical_type, idx_t size,
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
				throw std::runtime_error("DuckDB only supports indices that fit on an uint32");
			}
			SetMaskedSelectionVectorLoop<uint32_t>(sel, indices_p, size, *mask, last_element_pos);
			break;
		case LogicalTypeId::INTEGER:
			SetMaskedSelectionVectorLoop<int32_t>(sel, indices_p, size, *mask, last_element_pos);
			break;
		case LogicalTypeId::UBIGINT:
			if (last_element_pos > NumericLimits<uint32_t>::Maximum()) {
				//! Its guaranteed that our indices will point to the last element, so just throw an error
				throw std::runtime_error("DuckDB only supports indices that fit on an uint32");
			}
			SetMaskedSelectionVectorLoop<uint64_t>(sel, indices_p, size, *mask, last_element_pos);
			break;
		case LogicalTypeId::BIGINT:
			if (last_element_pos > NumericLimits<uint32_t>::Maximum()) {
				//! Its guaranteed that our indices will point to the last element, so just throw an error
				throw std::runtime_error("DuckDB only supports indices that fit on an uint32");
			}
			SetMaskedSelectionVectorLoop<int64_t>(sel, indices_p, size, *mask, last_element_pos);
			break;

		default:
			throw std::runtime_error("(Arrow) Unsupported type for selection vectors " + logical_type.ToString());
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
			throw std::runtime_error("(Arrow) Unsupported type for selection vectors " + logical_type.ToString());
		}
	}
}

void ColumnArrowToDuckDBDictionary(Vector &vector, ArrowArray &array, ArrowScanLocalState &scan_state, idx_t size,
                                   std::unordered_map<idx_t, unique_ptr<ArrowConvertData>> &arrow_convert_data,
                                   idx_t col_idx, std::pair<idx_t, idx_t> &arrow_convert_idx) {
	SelectionVector sel;
	auto &dict_vectors = scan_state.arrow_dictionary_vectors;
	if (dict_vectors.find(col_idx) == dict_vectors.end()) {
		//! We need to set the dictionary data for this column
		auto base_vector = make_unique<Vector>(vector.GetType(), array.dictionary->length);
		SetValidityMask(*base_vector, *array.dictionary, scan_state, array.dictionary->length, 0, array.null_count > 0);
		ColumnArrowToDuckDB(*base_vector, *array.dictionary, scan_state, array.dictionary->length, arrow_convert_data,
		                    col_idx, arrow_convert_idx);
		dict_vectors[col_idx] = move(base_vector);
	}
	auto dictionary_type = arrow_convert_data[col_idx]->dictionary_type;
	//! Get Pointer to Indices of Dictionary
	auto indices = (data_ptr_t)array.buffers[1] +
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
                                       DataChunk &output, idx_t start) {
	for (idx_t idx = 0; idx < output.ColumnCount(); idx++) {
		auto col_idx = scan_state.column_ids[idx];
		std::pair<idx_t, idx_t> arrow_convert_idx {0, 0};
		auto &array = *scan_state.chunk->arrow_array.children[idx];
		if (!array.release) {
			throw InvalidInputException("arrow_scan: released array passed");
		}
		if (array.length != scan_state.chunk->arrow_array.length) {
			throw InvalidInputException("arrow_scan: array length mismatch");
		}
		output.data[idx].GetBuffer()->SetAuxiliaryData(make_unique<ArrowAuxiliaryData>(scan_state.chunk));
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
