#include "parameter_wrapper.hpp"
#include "duckdb_odbc.hpp"
#include "duckdb/common/types/decimal.hpp"
#include "duckdb/common/string_util.hpp"

using duckdb::Decimal;
using duckdb::idx_t;
using duckdb::ParameterDescriptor;
using duckdb::ParameterWrapper;

ParameterWrapper::~ParameterWrapper() {
}

void ParameterWrapper::Clear() {
	param_descriptors.clear();
	cur_paramset_idx = 0;
}

void ParameterWrapper::ResetParamSetIndex() {
	cur_paramset_idx = 0;
}

SQLRETURN ParameterWrapper::GetValues(std::vector<Value> &values) {
	values.clear();
	if (param_descriptors.empty()) {
		return SQL_NO_DATA;
	}
	D_ASSERT(cur_paramset_idx < paramset_size);
	// Fill values
	for (idx_t desc_idx = 0; desc_idx < param_descriptors.size(); ++desc_idx) {
		// set a proper parameter value
		auto ret = param_descriptors[desc_idx].SetValue(cur_paramset_idx);
		if (param_status_ptr) {
			param_status_ptr[cur_paramset_idx] = ret;
		}
		if (ret != SQL_PARAM_SUCCESS) {
			auto msg = duckdb::StringUtil::Format(
			    "Error setting parameter value: ParameterSet '%ld', ParameterIndex '%ld'", cur_paramset_idx, desc_idx);
			error_messages->emplace_back(msg);
			if (!param_status_ptr) {
				return SQL_ERROR;
			}
		}
		values.emplace_back(param_descriptors[desc_idx].values[cur_paramset_idx]);
	}
	++cur_paramset_idx;
	if (cur_paramset_idx == paramset_size) {
		return SQL_NO_DATA;
	}
	return SQL_SUCCESS;
}

SQLRETURN ParameterDescriptor::ValidateNumeric(int precision, int scale) {
	if (precision < 1 || precision > Decimal::MAX_WIDTH_DECIMAL || scale < 0 || scale > Decimal::MAX_WIDTH_DECIMAL ||
	    scale > precision) {
		// TODO we should use SQLGetDiagField to register the error message
		auto msg = StringUtil::Format("Numeric precision %d and scale %d not supported", precision, scale);
		return SQL_ERROR;
	}
	return SQL_SUCCESS;
}

void ParameterDescriptor::SetValue(Value &value, idx_t val_idx) {
	if (val_idx >= values.size()) {
		values.emplace_back(value);
		return;
	}
	// replacing value, i.e., reuse paramente in the prepared stmt
	values[val_idx] = value;
}

SQLRETURN ParameterDescriptor::SetValue(idx_t val_idx) {
	if (app_param_desc.param_value_ptr == nullptr || app_param_desc.str_len_or_ind_ptr == nullptr ||
	    app_param_desc.str_len_or_ind_ptr[val_idx] == SQL_NULL_DATA) {
		Value val_null(nullptr);
		SetValue(val_null, val_idx);
		return SQL_SUCCESS;
	}

	duckdb::Value value;
	// TODO need to check it param_value_ptr is an array of parameters
	// and get the right parameter using the index (now it's working for all supported tests)
	duckdb::const_data_ptr_t dataptr = (duckdb::const_data_ptr_t)app_param_desc.param_value_ptr;

	switch (impl_param_desc.param_type) {
	case SQL_CHAR:
	case SQL_VARCHAR:
		value = Value(duckdb::OdbcUtils::ReadString(app_param_desc.param_value_ptr, app_param_desc.buffer_len));
		break;
	case SQL_TINYINT:
		if (app_param_desc.value_type == SQL_C_UTINYINT) {
			value = Value::UTINYINT(Load<uint8_t>(dataptr));
		} else {
			value = Value::TINYINT(Load<int8_t>(dataptr));
		}
		break;
	case SQL_SMALLINT:
		if (app_param_desc.value_type == SQL_C_USHORT) {
			value = Value::USMALLINT(Load<uint16_t>(dataptr));
		} else {
			value = Value::SMALLINT(Load<int16_t>(dataptr));
		}
		break;
	case SQL_INTEGER:
		if (app_param_desc.value_type == SQL_C_ULONG) {
			value = Value::UINTEGER(Load<uint32_t>(dataptr));
		} else {
			value = Value::INTEGER(Load<int32_t>(dataptr));
		}
		break;
	case SQL_BIGINT:
		if (app_param_desc.value_type == SQL_C_UBIGINT) {
			value = Value::UBIGINT(Load<uint64_t>(dataptr));
		} else {
			value = Value::BIGINT(Load<int64_t>(dataptr));
		}
		break;
	case SQL_FLOAT:
		value = Value::FLOAT(Load<float>(dataptr));
		break;
	case SQL_DOUBLE:
		value = Value::DOUBLE(Load<double>(dataptr));
		break;
	case SQL_NUMERIC: {
		auto numeric = (SQL_NUMERIC_STRUCT *)app_param_desc.param_value_ptr;
		dataptr = numeric->val;

		auto precision = impl_param_desc.col_size;
		if (ValidateNumeric(precision, impl_param_desc.dec_digits) == SQL_ERROR) {
			return SQL_ERROR;
		}
		if (impl_param_desc.col_size <= Decimal::MAX_WIDTH_INT64) {
			value = Value::DECIMAL(Load<int64_t>(dataptr), precision, impl_param_desc.dec_digits);
		} else {
			hugeint_t dec_value;
			memcpy(&dec_value.lower, dataptr, sizeof(dec_value.lower));
			memcpy(&dec_value.upper, dataptr + sizeof(dec_value.lower), sizeof(dec_value.upper));
			value = Value::DECIMAL(dec_value, precision, impl_param_desc.dec_digits);
		}
		break;
	}
	case SQL_BINARY: {
	}
	// TODO more types
	default:
		// TODO error message?
		return SQL_PARAM_ERROR;
	}

	SetValue(value, val_idx);
	return SQL_PARAM_SUCCESS;
}
