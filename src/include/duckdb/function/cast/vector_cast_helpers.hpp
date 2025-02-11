//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/function/cast/vector_cast_helpers.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/function/cast/default_casts.hpp"
#include "duckdb/common/vector_operations/unary_executor.hpp"
#include "duckdb/common/vector_operations/general_cast.hpp"
#include "duckdb/common/operator/decimal_cast_operators.hpp"
#include "duckdb/common/likely.hpp"
#include "duckdb/common/string_map_set.hpp"

namespace duckdb {

template <class OP>
struct VectorStringCastOperator {
	template <class INPUT_TYPE, class RESULT_TYPE>
	static RESULT_TYPE Operation(INPUT_TYPE input, ValidityMask &mask, idx_t idx, void *dataptr) {
		auto result = reinterpret_cast<Vector *>(dataptr);
		return OP::template Operation<INPUT_TYPE>(input, *result);
	}
};

template <class OP>
struct VectorTryCastOperator {
	template <class INPUT_TYPE, class RESULT_TYPE>
	static RESULT_TYPE Operation(INPUT_TYPE input, ValidityMask &mask, idx_t idx, void *dataptr) {
		RESULT_TYPE output;
		if (DUCKDB_LIKELY(OP::template Operation<INPUT_TYPE, RESULT_TYPE>(input, output))) {
			return output;
		}
		auto data = reinterpret_cast<VectorTryCastData *>(dataptr);
		return HandleVectorCastError::Operation<RESULT_TYPE>(CastExceptionText<INPUT_TYPE, RESULT_TYPE>(input), mask,
		                                                     idx, *data);
	}
};

template <class OP>
struct VectorTryCastStrictOperator {
	template <class INPUT_TYPE, class RESULT_TYPE>
	static RESULT_TYPE Operation(INPUT_TYPE input, ValidityMask &mask, idx_t idx, void *dataptr) {
		auto data = reinterpret_cast<VectorTryCastData *>(dataptr);
		RESULT_TYPE output;
		if (DUCKDB_LIKELY(OP::template Operation<INPUT_TYPE, RESULT_TYPE>(input, output, data->parameters.strict))) {
			return output;
		}
		return HandleVectorCastError::Operation<RESULT_TYPE>(CastExceptionText<INPUT_TYPE, RESULT_TYPE>(input), mask,
		                                                     idx, *data);
	}
};

template <class OP>
struct VectorTryCastErrorOperator {
	template <class INPUT_TYPE, class RESULT_TYPE>
	static RESULT_TYPE Operation(INPUT_TYPE input, ValidityMask &mask, idx_t idx, void *dataptr) {
		auto data = reinterpret_cast<VectorTryCastData *>(dataptr);
		RESULT_TYPE output;
		if (DUCKDB_LIKELY(OP::template Operation<INPUT_TYPE, RESULT_TYPE>(input, output, data->parameters))) {
			return output;
		}
		bool has_error = data->parameters.error_message && !data->parameters.error_message->empty();
		return HandleVectorCastError::Operation<RESULT_TYPE>(
		    has_error ? *data->parameters.error_message : CastExceptionText<INPUT_TYPE, RESULT_TYPE>(input), mask, idx,
		    *data);
	}
};

template <class OP>
struct VectorTryCastStringOperator {
	template <class INPUT_TYPE, class RESULT_TYPE>
	static RESULT_TYPE Operation(INPUT_TYPE input, ValidityMask &mask, idx_t idx, void *dataptr) {
		auto data = reinterpret_cast<VectorTryCastData *>(dataptr);
		RESULT_TYPE output;
		if (DUCKDB_LIKELY(
		        OP::template Operation<INPUT_TYPE, RESULT_TYPE>(input, output, data->result, data->parameters))) {
			return output;
		}
		return HandleVectorCastError::Operation<RESULT_TYPE>(CastExceptionText<INPUT_TYPE, RESULT_TYPE>(input), mask,
		                                                     idx, *data);
	}
};

struct VectorDecimalCastData {
	VectorDecimalCastData(Vector &result, CastParameters &parameters, uint8_t width_p, uint8_t scale_p)
	    : vector_cast_data(result, parameters), width(width_p), scale(scale_p) {
	}

	VectorTryCastData vector_cast_data;
	uint8_t width;
	uint8_t scale;
};

template <class OP>
struct VectorDecimalCastOperator {
	template <class INPUT_TYPE, class RESULT_TYPE>
	static RESULT_TYPE Operation(INPUT_TYPE input, ValidityMask &mask, idx_t idx, void *dataptr) {
		auto data = reinterpret_cast<VectorDecimalCastData *>(dataptr);
		RESULT_TYPE result_value;
		if (!OP::template Operation<INPUT_TYPE, RESULT_TYPE>(input, result_value, data->vector_cast_data.parameters,
		                                                     data->width, data->scale)) {
			return HandleVectorCastError::Operation<RESULT_TYPE>("Failed to cast decimal value", mask, idx,
			                                                     data->vector_cast_data);
		}
		return result_value;
	}
};

struct VectorCastHelpers {
	template <class SRC, class DST, class OP>
	static bool TemplatedCastLoop(Vector &source, Vector &result, idx_t count, CastParameters &parameters) {
		UnaryExecutor::Execute<SRC, DST, OP>(source, result, count);
		return true;
	}

	template <class SRC, class DST, class OP>
	static bool TemplatedTryCastLoop(Vector &source, Vector &result, idx_t count, CastParameters &parameters) {
		VectorTryCastData input(result, parameters);
		UnaryExecutor::GenericExecute<SRC, DST, OP>(source, result, count, &input, parameters.error_message);
		return input.all_converted;
	}

	template <class SRC, class DST, class OP>
	static bool TryCastLoop(Vector &source, Vector &result, idx_t count, CastParameters &parameters) {
		return TemplatedTryCastLoop<SRC, DST, VectorTryCastOperator<OP>>(source, result, count, parameters);
	}

	template <class SRC, class DST, class OP>
	static bool TryCastStrictLoop(Vector &source, Vector &result, idx_t count, CastParameters &parameters) {
		return TemplatedTryCastLoop<SRC, DST, VectorTryCastStrictOperator<OP>>(source, result, count, parameters);
	}

	template <class SRC, class DST, class OP>
	static bool TryCastErrorLoop(Vector &source, Vector &result, idx_t count, CastParameters &parameters) {
		return TemplatedTryCastLoop<SRC, DST, VectorTryCastErrorOperator<OP>>(source, result, count, parameters);
	}

	template <class SRC, class DST, class OP>
	static bool TryCastStringLoop(Vector &source, Vector &result, idx_t count, CastParameters &parameters) {
		return TemplatedTryCastLoop<SRC, DST, VectorTryCastStringOperator<OP>>(source, result, count, parameters);
	}

	template <class SRC, class OP>
	static bool StringCast(Vector &source, Vector &result, idx_t count, CastParameters &parameters) {
		D_ASSERT(result.GetType().InternalType() == PhysicalType::VARCHAR);
		UnaryExecutor::GenericExecute<SRC, string_t, VectorStringCastOperator<OP>>(source, result, count,
		                                                                           (void *)&result);
		return true;
	}

	template <class SRC, class T, class OP>
	static bool TemplatedDecimalCast(Vector &source, Vector &result, idx_t count, CastParameters &parameters,
	                                 uint8_t width, uint8_t scale) {
		VectorDecimalCastData input(result, parameters, width, scale);
		UnaryExecutor::GenericExecute<SRC, T, VectorDecimalCastOperator<OP>>(source, result, count, (void *)&input,
		                                                                     parameters.error_message);
		return input.vector_cast_data.all_converted;
	}

	template <class T>
	static bool ToDecimalCast(Vector &source, Vector &result, idx_t count, CastParameters &parameters) {
		auto &result_type = result.GetType();
		auto width = DecimalType::GetWidth(result_type);
		auto scale = DecimalType::GetScale(result_type);
		switch (result_type.InternalType()) {
		case PhysicalType::INT16:
			return TemplatedDecimalCast<T, int16_t, TryCastToDecimal>(source, result, count, parameters, width, scale);
		case PhysicalType::INT32:
			return TemplatedDecimalCast<T, int32_t, TryCastToDecimal>(source, result, count, parameters, width, scale);
		case PhysicalType::INT64:
			return TemplatedDecimalCast<T, int64_t, TryCastToDecimal>(source, result, count, parameters, width, scale);
		case PhysicalType::INT128:
			return TemplatedDecimalCast<T, hugeint_t, TryCastToDecimal>(source, result, count, parameters, width,
			                                                            scale);
		default:
			throw InternalException("Unimplemented internal type for decimal");
		}
	}

	static idx_t CalculateEscapedStringLength(const string_t &string, const char *special_chars,
	                                          idx_t special_chars_length) {
		auto base_length = string.GetSize();
		idx_t length = base_length;
		auto string_data = string.GetData();

		if (base_length >= 1) {
			if (isspace(string_data[0])) {
				length++;
			}
			if (isspace(string_data[base_length - 1])) {
				length++;
			}
		}

		for (idx_t i = 0; i < base_length; i++) {
			auto special_character = memchr(special_chars, string_data[i], special_chars_length);
			length += (special_character != nullptr);
		}
		return length;
	}

	static idx_t CalculateStringLength(const string_t &string, const char *special_chars, idx_t special_chars_length) {
		return string.GetSize();
	}

	static idx_t WriteEscapedString(void *dest, const string_t &string, const char *special_chars,
	                                idx_t special_chars_length) {
		auto base_length = string.GetSize();

		auto string_start = string.GetData();
		const auto string_end = reinterpret_cast<const void *>(string_start + base_length);
		auto string_data = string_start;

		idx_t dest_offset = 0;
		if (base_length >= 1) {
			if (isspace(string_data[0])) {
				memset(reinterpret_cast<char *>(dest) + dest_offset, '\\', 1);
				dest_offset++;
			}
		}

		while (string_data < string_end) {
			const void *write_end = nullptr;
			for (idx_t j = 0; j < special_chars_length; j++) {
				auto res = memchr(string_data, special_chars[j],
				                  UnsafeNumericCast<size_t>(reinterpret_cast<const char *>(string_end) - string_data));
				if (res && (!write_end || res < write_end)) {
					write_end = res;
				}
			}
			if (!write_end) {
				write_end = string_end;
				auto length = UnsafeNumericCast<size_t>(reinterpret_cast<const char *>(write_end) - string_data);
				memcpy(reinterpret_cast<char *>(dest) + dest_offset, string_data, length);
				dest_offset += length;
				break;
			}

			auto length = UnsafeNumericCast<size_t>(reinterpret_cast<const char *>(write_end) - string_data);
			memcpy(reinterpret_cast<char *>(dest) + dest_offset, string_data, length);
			memset(reinterpret_cast<char *>(dest) + dest_offset + length, '\\', 1);
			memcpy(reinterpret_cast<char *>(dest) + dest_offset + length + 1, write_end, 1);
			dest_offset += length + 2;
			string_data = reinterpret_cast<const char *>(write_end) + 1;
		}

		if (base_length >= 1) {
			//! Replace ' ' with '\ '
			if (isspace(string_start[base_length - 1])) {
				auto character = reinterpret_cast<const unsigned char *>(dest)[dest_offset - 1];
				memset(reinterpret_cast<char *>(dest) + dest_offset - 1, '\\', 1);
				memset(reinterpret_cast<char *>(dest) + dest_offset, character, 1);
				dest_offset++;
			}
		}

		return dest_offset;
	}

	static idx_t WriteString(void *dest, const string_t &string, const char *special_chars,
	                         idx_t special_chars_length) {
		auto len = string.GetSize();
		memcpy(dest, string.GetData(), len);
		return len;
	}
};

struct VectorStringToList {
	static idx_t CountPartsList(const string_t &input);
	static bool SplitStringList(const string_t &input, string_t *child_data, idx_t &child_start, Vector &child);
	static bool StringToNestedTypeCastLoop(const string_t *source_data, ValidityMask &source_mask, Vector &result,
	                                       ValidityMask &result_mask, idx_t count, CastParameters &parameters,
	                                       const SelectionVector *sel);
};

struct VectorStringToArray {
	static bool StringToNestedTypeCastLoop(const string_t *source_data, ValidityMask &source_mask, Vector &result,
	                                       ValidityMask &result_mask, idx_t count, CastParameters &parameters,
	                                       const SelectionVector *sel);
};

struct VectorStringToStruct {
	static bool SplitStruct(const string_t &input, vector<unique_ptr<Vector>> &varchar_vectors, idx_t &row_idx,
	                        string_map_t<idx_t> &child_names, vector<reference<ValidityMask>> &child_masks);
	static bool StringToNestedTypeCastLoop(const string_t *source_data, ValidityMask &source_mask, Vector &result,
	                                       ValidityMask &result_mask, idx_t count, CastParameters &parameters,
	                                       const SelectionVector *sel);
};

struct VectorStringToMap {
	static idx_t CountPartsMap(const string_t &input);
	static bool SplitStringMap(const string_t &input, string_t *child_key_data, string_t *child_val_data,
	                           idx_t &child_start, Vector &varchar_key, Vector &varchar_val);
	static bool StringToNestedTypeCastLoop(const string_t *source_data, ValidityMask &source_mask, Vector &result,
	                                       ValidityMask &result_mask, idx_t count, CastParameters &parameters,
	                                       const SelectionVector *sel);
};

} // namespace duckdb
