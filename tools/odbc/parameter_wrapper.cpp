#include "parameter_wrapper.hpp"
#include "duckdb_odbc.hpp"
#include "duckdb/common/types/decimal.hpp"
#include "duckdb/common/string_util.hpp"
#include <iostream>

using duckdb::Decimal;
using duckdb::idx_t;
using duckdb::ParameterDescriptor;
using duckdb::ParameterWrapper;

//! ParameterWrapper methods *****************************************

ParameterWrapper::~ParameterWrapper() {
	Clear();
}

void ParameterWrapper::Clear() {
	param_descriptors.clear();
	pool_allocated_ptr.clear();
	paramset_idx = 0;
	cur_paramset_idx = 0;
	cur_param_idx = 0;
}

void ParameterWrapper::Reset() {
	paramset_idx = 0;
	cur_paramset_idx = 0;
	cur_param_idx = 0;
}

void ParameterWrapper::SetParamProcessedPtr(SQLPOINTER value_ptr) {
	param_processed_ptr = (SQLULEN *)value_ptr;
	if (param_processed_ptr) {
		*param_processed_ptr = 0;
	}
}

SQLRETURN ParameterWrapper::GetNextParam(SQLPOINTER *param) {
	if (cur_param_idx >= param_descriptors.size()) {
		return SQL_NO_DATA;
	}

	auto param_desc = param_descriptors[cur_param_idx];
	*param = param_desc.apd.param_value_ptr;
	if (param_processed_ptr) {
		*param_processed_ptr += 1;
	}
	if (param_desc.apd.str_len_or_ind_ptr[cur_paramset_idx] == SQL_DATA_AT_EXEC) {
		return SQL_NEED_DATA;
	}
	return SQL_SUCCESS;
}

SQLRETURN ParameterWrapper::GetValues(std::vector<Value> &values) {
	values.clear();
	if (param_descriptors.empty()) {
		return SQL_SUCCESS;
	}
	D_ASSERT(paramset_idx < paramset_size);
	// Fill values
	for (idx_t desc_idx = 0; desc_idx < param_descriptors.size(); ++desc_idx) {
		std::cout << "ParameterWrapper::GetValues" << std::endl;
		// set a proper parameter value
		auto ret = param_descriptors[desc_idx].SetValue(paramset_idx);
		if (param_status_ptr) {
			param_status_ptr[paramset_idx] = ret;
		}
		if (ret == SQL_NEED_DATA) {
			return SQL_NEED_DATA;
		}
		if (ret != SQL_PARAM_SUCCESS) {
			auto msg = duckdb::StringUtil::Format(
			    "Error setting parameter value: ParameterSet '%ld', ParameterIndex '%ld'", cur_paramset_idx, desc_idx);
			error_messages->emplace_back(msg);
			if (!param_status_ptr) {
				return SQL_ERROR;
			}
		}
		values.emplace_back(param_descriptors[desc_idx].values[paramset_idx]);
	}
	++paramset_idx;
	if (paramset_idx == paramset_size) {
		return SQL_SUCCESS;
	}
	return SQL_STILL_EXECUTING;
}

SQLRETURN ParameterWrapper::PutData(SQLPOINTER data_ptr, SQLLEN str_len_or_ind_ptr) {
	if (cur_param_idx >= param_descriptors.size()) {
		return SQL_ERROR;
	}

	auto param_desc = &param_descriptors[cur_param_idx];
	if (param_desc->apd.value_type == SQL_C_CHAR || param_desc->apd.value_type == SQL_C_BINARY) {
		return PutCharData(*param_desc, data_ptr, str_len_or_ind_ptr);
	}

	// filling the current param, go to the next one
	++cur_param_idx;
	if (str_len_or_ind_ptr == SQL_NULL_DATA) {
		*param_desc->apd.str_len_or_ind_ptr = SQL_NULL_DATA;
		return SQL_SUCCESS;
	}

	param_desc->apd.param_value_ptr = data_ptr;
	return SQL_SUCCESS;
}

SQLRETURN ParameterWrapper::FillParamCharDataBuffer(ParameterDescriptor &param_desc, SQLPOINTER data_ptr,
                                                    SQLLEN str_len_or_ind_ptr) {
	size_t offset = 0;
	if (*param_desc.apd.str_len_or_ind_ptr == SQL_DATA_AT_EXEC) {
		pool_allocated_ptr.emplace_back(unique_ptr<char[]>(new char[param_desc.ipd.col_size]));
		param_desc.apd.param_value_ptr = pool_allocated_ptr.back().get();
		*param_desc.apd.str_len_or_ind_ptr = 0;
	} else {
		// appending more data into it
		offset = *param_desc.apd.str_len_or_ind_ptr;
	}

	size_t size = str_len_or_ind_ptr;
	if (size >= ((size_t)param_desc.ipd.col_size - offset)) {
		size = (size_t)param_desc.ipd.col_size - offset;
		// filled the current param buffer data, go to the next one
		++cur_param_idx;
	}

	memcpy((char *)param_desc.apd.param_value_ptr + offset, (char *)data_ptr, size);
	*param_desc.apd.str_len_or_ind_ptr += size;

	if (param_status_ptr) {
		param_status_ptr[cur_param_idx] = SQL_PARAM_SUCCESS;
	}

	return SQL_SUCCESS;
}

SQLRETURN ParameterWrapper::FillCurParamCharSet(ParameterDescriptor &param_desc, SQLPOINTER data_ptr,
                                                SQLLEN str_len_or_ind_ptr) {
	auto len_ptr = &param_desc.apd.str_len_or_ind_ptr[cur_paramset_idx];
	auto col_size = (size_t)param_desc.ipd.col_size;

	if (*len_ptr == SQL_DATA_AT_EXEC && !param_desc.ipd.allocated) {
		auto alloc_size = col_size * paramset_size;
		pool_allocated_ptr.emplace_back(unique_ptr<char[]>(new char[alloc_size]));
		param_desc.apd.param_value_ptr = pool_allocated_ptr.back().get();
		param_desc.ipd.allocated = true;
	}

	auto value_ptr = (char *)param_desc.apd.param_value_ptr + (cur_paramset_idx * col_size);

	size_t size = str_len_or_ind_ptr;
	if (size >= ((size_t)param_desc.ipd.col_size)) {
		size = col_size;
	}

	memcpy(value_ptr, (const char *)data_ptr, size);
	*len_ptr = size;

	if (param_status_ptr) {
		param_status_ptr[cur_paramset_idx] = SQL_PARAM_SUCCESS;
	}

	++cur_paramset_idx;
	if (cur_paramset_idx >= paramset_size) {
		// got to the next param set
		cur_paramset_idx = 0;
		++cur_param_idx;
	}

	return SQL_SUCCESS;
}

SQLRETURN ParameterWrapper::PutCharData(ParameterDescriptor &param_desc, SQLPOINTER data_ptr,
                                        SQLLEN str_len_or_ind_ptr) {
	if (paramset_size == 1) {
		return FillParamCharDataBuffer(param_desc, data_ptr, str_len_or_ind_ptr);
	}

	return FillCurParamCharSet(param_desc, data_ptr, str_len_or_ind_ptr);
}

bool ParameterWrapper::HasParamSetToProcess() {
	return (paramset_idx < paramset_size && !param_descriptors.empty());
}

//! ParameterDescriptor methods *****************************************

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
	// replacing value, i.e., reusing parameter in a prepared stmt
	values[val_idx] = value;
}

SQLRETURN ParameterDescriptor::SetValue(idx_t val_idx) {
	if (apd.param_value_ptr == nullptr && apd.str_len_or_ind_ptr == nullptr) {
		return SQL_ERROR;
	}

	if (apd.param_value_ptr == nullptr || apd.str_len_or_ind_ptr == nullptr ||
	    apd.str_len_or_ind_ptr[val_idx] == SQL_NULL_DATA) {
		Value val_null(nullptr);
		SetValue(val_null, val_idx);
		return SQL_SUCCESS;
	}

	if (apd.str_len_or_ind_ptr[val_idx] == SQL_DATA_AT_EXEC ||
	    (SQLULEN)apd.str_len_or_ind_ptr[val_idx] == SQL_LEN_DATA_AT_EXEC(ipd.col_size)) {
		return SQL_NEED_DATA;
	}

	duckdb::Value value;
	// TODO need to check it param_value_ptr is an array of parameters
	// and get the right parameter using the index (now it's working for all supported tests)
	duckdb::const_data_ptr_t dataptr = (duckdb::const_data_ptr_t)apd.param_value_ptr;

	switch (ipd.param_type) {
	case SQL_CHAR:
	case SQL_VARCHAR: {
		auto str_data = (char *)apd.param_value_ptr + (val_idx * ipd.col_size);
		auto str_len = apd.str_len_or_ind_ptr[val_idx];
		value = Value(duckdb::OdbcUtils::ReadString(str_data, str_len));
		break;
	}
	case SQL_WCHAR: {
		auto str_data = (wchar_t *)apd.param_value_ptr + (val_idx * ipd.col_size);
		auto str_len = apd.str_len_or_ind_ptr[val_idx];
		value = Value(duckdb::OdbcUtils::ReadString(str_data, str_len));
		break;
	}
	case SQL_VARBINARY:
	case SQL_BINARY: {
		auto blob_data = (duckdb::const_data_ptr_t)apd.param_value_ptr + (val_idx * ipd.col_size);
		auto blob_len = apd.str_len_or_ind_ptr[val_idx];
		value = Value::BLOB(blob_data, blob_len);
		break;
	}
	case SQL_TINYINT:
		if (apd.value_type == SQL_C_UTINYINT) {
			value = Value::UTINYINT(Load<uint8_t>(dataptr));
		} else {
			value = Value::TINYINT(Load<int8_t>(dataptr));
		}
		break;
	case SQL_SMALLINT:
		if (apd.value_type == SQL_C_USHORT) {
			value = Value::USMALLINT(Load<uint16_t>(dataptr));
		} else {
			value = Value::SMALLINT(Load<int16_t>(dataptr));
		}
		break;
	case SQL_INTEGER:
		if (apd.value_type == SQL_C_ULONG) {
			value = Value::UINTEGER(Load<uint32_t>(dataptr));
		} else {
			value = Value::INTEGER(Load<int32_t>(dataptr));
		}
		break;
	case SQL_BIGINT:
		if (apd.value_type == SQL_C_UBIGINT) {
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
		auto numeric = (SQL_NUMERIC_STRUCT *)apd.param_value_ptr;
		dataptr = numeric->val;

		auto precision = ipd.col_size;
		if (ValidateNumeric(precision, ipd.dec_digits) == SQL_ERROR) {
			return SQL_ERROR;
		}
		if (ipd.col_size <= Decimal::MAX_WIDTH_INT64) {
			value = Value::DECIMAL(Load<int64_t>(dataptr), precision, ipd.dec_digits);
		} else {
			hugeint_t dec_value;
			memcpy(&dec_value.lower, dataptr, sizeof(dec_value.lower));
			memcpy(&dec_value.upper, dataptr + sizeof(dec_value.lower), sizeof(dec_value.upper));
			value = Value::DECIMAL(dec_value, precision, ipd.dec_digits);
		}
		break;
	}
	// TODO more types
	default:
		// TODO error message?
		return SQL_PARAM_ERROR;
	}

	SetValue(value, val_idx);
	return SQL_PARAM_SUCCESS;
}
