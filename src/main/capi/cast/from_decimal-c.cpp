#include "duckdb/main/capi/cast/from_decimal.hpp"
#include "duckdb/common/types/decimal.hpp"

namespace duckdb {

//! DECIMAL -> VARCHAR
template <>
bool CastDecimalCInternal(duckdb_result *source, duckdb_string &result, idx_t col, idx_t row) {
	auto result_data = (duckdb::DuckDBResultData *)source->internal_data;
	auto &query_result = result_data->result;
	auto &source_type = query_result->types[col];
	auto width = duckdb::DecimalType::GetWidth(source_type);
	auto scale = duckdb::DecimalType::GetScale(source_type);
	duckdb::Vector result_vec(duckdb::LogicalType::VARCHAR, false, false);
	duckdb::string_t result_string;
	void *source_address = UnsafeFetchPtr<hugeint_t>(source, col, row);
	switch (source_type.InternalType()) {
	case duckdb::PhysicalType::INT16:
		result_string = duckdb::StringCastFromDecimal::Operation<int16_t>(UnsafeFetchFromPtr<int16_t>(source_address),
		                                                                  width, scale, result_vec);
		break;
	case duckdb::PhysicalType::INT32:
		result_string = duckdb::StringCastFromDecimal::Operation<int32_t>(UnsafeFetchFromPtr<int32_t>(source_address),
		                                                                  width, scale, result_vec);
		break;
	case duckdb::PhysicalType::INT64:
		result_string = duckdb::StringCastFromDecimal::Operation<int64_t>(UnsafeFetchFromPtr<int64_t>(source_address),
		                                                                  width, scale, result_vec);
		break;
	case duckdb::PhysicalType::INT128:
		result_string = duckdb::StringCastFromDecimal::Operation<hugeint_t>(
		    UnsafeFetchFromPtr<hugeint_t>(source_address), width, scale, result_vec);
		break;
	default:
		throw duckdb::InternalException("Unimplemented internal type for decimal");
	}
	result.data = reinterpret_cast<char *>(duckdb_malloc(sizeof(char) * (result_string.GetSize() + 1)));
	memcpy(result.data, result_string.GetData(), result_string.GetSize());
	result.data[result_string.GetSize()] = '\0';
	result.size = result_string.GetSize();
	return true;
}

template <class INTERNAL_TYPE>
duckdb_hugeint FetchInternals(void *source_address) {
	throw duckdb::NotImplementedException("FetchInternals not implemented for internal type");
}

template <>
duckdb_hugeint FetchInternals<int16_t>(void *source_address) {
	duckdb_hugeint result;
	int16_t intermediate_result;

	if (!TryCast::Operation<int16_t, int16_t>(UnsafeFetchFromPtr<int16_t>(source_address), intermediate_result)) {
		intermediate_result = FetchDefaultValue::Operation<int16_t>();
	}
	hugeint_t hugeint_result = Hugeint::Cast<int16_t>(intermediate_result);
	result.lower = hugeint_result.lower;
	result.upper = hugeint_result.upper;
	return result;
}
template <>
duckdb_hugeint FetchInternals<int32_t>(void *source_address) {
	duckdb_hugeint result;
	int32_t intermediate_result;

	if (!TryCast::Operation<int32_t, int32_t>(UnsafeFetchFromPtr<int32_t>(source_address), intermediate_result)) {
		intermediate_result = FetchDefaultValue::Operation<int32_t>();
	}
	hugeint_t hugeint_result = Hugeint::Cast<int32_t>(intermediate_result);
	result.lower = hugeint_result.lower;
	result.upper = hugeint_result.upper;
	return result;
}
template <>
duckdb_hugeint FetchInternals<int64_t>(void *source_address) {
	duckdb_hugeint result;
	int64_t intermediate_result;

	if (!TryCast::Operation<int64_t, int64_t>(UnsafeFetchFromPtr<int64_t>(source_address), intermediate_result)) {
		intermediate_result = FetchDefaultValue::Operation<int64_t>();
	}
	hugeint_t hugeint_result = Hugeint::Cast<int64_t>(intermediate_result);
	result.lower = hugeint_result.lower;
	result.upper = hugeint_result.upper;
	return result;
}
template <>
duckdb_hugeint FetchInternals<hugeint_t>(void *source_address) {
	duckdb_hugeint result;
	hugeint_t intermediate_result;

	if (!TryCast::Operation<hugeint_t, hugeint_t>(UnsafeFetchFromPtr<hugeint_t>(source_address), intermediate_result)) {
		intermediate_result = FetchDefaultValue::Operation<hugeint_t>();
	}
	result.lower = intermediate_result.lower;
	result.upper = intermediate_result.upper;
	return result;
}

//! DECIMAL -> DECIMAL (internal fetch)
template <>
bool CastDecimalCInternal(duckdb_result *source, duckdb_decimal &result, idx_t col, idx_t row) {
	auto result_data = (duckdb::DuckDBResultData *)source->internal_data;
	result_data->result->types[col].GetDecimalProperties(result.width, result.scale);
	auto source_address = UnsafeFetchPtr<hugeint_t>(source, col, row);

	if (result.width > duckdb::Decimal::MAX_WIDTH_INT64) {
		result.value = FetchInternals<hugeint_t>(source_address);
	} else if (result.width > duckdb::Decimal::MAX_WIDTH_INT32) {
		result.value = FetchInternals<int64_t>(source_address);
	} else if (result.width > duckdb::Decimal::MAX_WIDTH_INT16) {
		result.value = FetchInternals<int32_t>(source_address);
	} else {
		result.value = FetchInternals<int16_t>(source_address);
	}
	return true;
}

} // namespace duckdb
