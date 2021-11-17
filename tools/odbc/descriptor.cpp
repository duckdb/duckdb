#include "descriptor.hpp"
#include "odbc_interval.hpp"

using duckdb::OdbcHandleDesc;
using duckdb::DescRecord;
using duckdb::OdbcInterval;

//! OdbcHandleDesc fucntions ********************************
duckdb::DescRecord *OdbcHandleDesc::GetDescRecord(duckdb::idx_t param_idx) {
	if (param_idx >= records.size()) {
		records.resize(param_idx + 1);
		header.sql_desc_count = records.size();
	}
	return &records[param_idx];
}


SQLRETURN OdbcHandleDesc::SetDescField(SQLSMALLINT rec_number, SQLSMALLINT field_identifier, SQLPOINTER value_ptr, SQLINTEGER buffer_length) {
	switch (field_identifier)
	{
	case SQL_DESC_CONCISE_TYPE:
		return SQL_SUCCESS;
	case SQL_DESC_ARRAY_STATUS_PTR:
		header.sql_desc_array_status_ptr = (SQLUSMALLINT *) value_ptr;
		return SQL_SUCCESS;
	case SQL_DESC_ROWS_PROCESSED_PTR:
		header.sql_desc_rows_processed_ptr = (SQLULEN *) value_ptr;
		return SQL_SUCCESS;
	case SQL_DESC_ARRAY_SIZE: {
		auto size = *((SQLULEN *)value_ptr);
		if (size <= 0) {
			stmt->error_messages.emplace_back("Invalid attribute/option identifier.");
			return SQL_ERROR;
		}
		header.sql_desc_array_size = size;
		return SQL_SUCCESS;
	}
	default:
		break;
	}
	return SQL_SUCCESS;
}

void OdbcHandleDesc::Clear() {
	// header.Clear();
	Reset();
}

void OdbcHandleDesc::Reset() {
	records.clear();
}


// SQLRETURN OdbcHandleDesc::GetParamValues(std::vector<Value> &values) {
// 	values.clear();
// 	if (records.empty()) {
// 		return SQL_SUCCESS;
// 	}
// 	D_ASSERT(param_ctl.paramset_idx < header.sql_desc_count);
// 	// Fill values
// 	for (idx_t rec_idx = 0; rec_idx < records.size(); ++rec_idx) {
// 		// set a proper parameter value
// 		auto ret = SetValue(param_ctl.paramset_idx);
// 		if (header.sql_desc_array_status_ptr) {
// 			header.sql_desc_array_status_ptr[param_ctl.paramset_idx] = ret;
// 		}
// 		if (ret == SQL_NEED_DATA) {
// 			return SQL_NEED_DATA;
// 		}
// 		if (ret != SQL_PARAM_SUCCESS) {
// 			auto msg = duckdb::StringUtil::Format(
// 			    "Error setting parameter value: ParameterSet '%ld', ParameterIndex '%ld'", param_ctl.cur_paramset_idx, rec_idx);
// 			stmt->error_messages.emplace_back(msg);
// 			if (!header.sql_desc_array_status_ptr) {
// 				return SQL_ERROR;
// 			}
// 		}
// 		values.emplace_back(param_ctl.GetNextValue());
// 	}
// 	return param_ctl.SetParamIndex();
// }

// SQLRETURN OdbcHandleDesc::SetValue(idx_t val_idx) {
// 	if (apd.sql_desc_data_ptr == nullptr && apd.sql_desc_indicator_ptr == nullptr) {
// 		return SQL_ERROR;
// 	}

// 	if (apd.sql_desc_data_ptr == nullptr || apd.sql_desc_indicator_ptr == nullptr ||
// 	    apd.sql_desc_indicator_ptr[val_idx] == SQL_NULL_DATA) {
// 		Value val_null(nullptr);
// 		SetValue(val_null, val_idx);
// 		return SQL_SUCCESS;
// 	}

// 	if (apd.sql_desc_indicator_ptr[val_idx] == SQL_DATA_AT_EXEC ||
// 	    (SQLULEN)apd.sql_desc_indicator_ptr[val_idx] == SQL_LEN_DATA_AT_EXEC(ipd.sql_desc_length)) {
// 		return SQL_NEED_DATA;
// 	}

// 	duckdb::Value value;
// 	// TODO need to check it param_value_ptr is an array of parameters
// 	// and get the right parameter using the index (now it's working for all supported tests)
// 	duckdb::const_data_ptr_t dataptr = (duckdb::const_data_ptr_t)apd.sql_desc_data_ptr;

// 	switch (ipd.sql_desc_type) {
// 	case SQL_CHAR:
// 	case SQL_VARCHAR: {
// 		auto str_data = (char *)apd.sql_desc_data_ptr + (val_idx * ipd.sql_desc_length);
// 		auto str_len = apd.sql_desc_indicator_ptr[val_idx];
// 		value = Value(duckdb::OdbcUtils::ReadString(str_data, str_len));
// 		break;
// 	}
// 	case SQL_WCHAR: {
// 		auto str_data = (wchar_t *)apd.sql_desc_data_ptr + (val_idx * ipd.sql_desc_length);
// 		auto str_len = apd.sql_desc_indicator_ptr[val_idx];
// 		value = Value(duckdb::OdbcUtils::ReadString(str_data, str_len));
// 		break;
// 	}
// 	case SQL_VARBINARY:
// 	case SQL_BINARY: {
// 		auto blob_data = (duckdb::const_data_ptr_t)apd.sql_desc_data_ptr + (val_idx * ipd.sql_desc_length);
// 		auto blob_len = apd.sql_desc_indicator_ptr[val_idx];
// 		value = Value::BLOB(blob_data, blob_len);
// 		break;
// 	}
// 	case SQL_TINYINT:
// 		if (apd.sql_desc_type == SQL_C_UTINYINT) {
// 			value = Value::UTINYINT(Load<uint8_t>(dataptr));
// 		} else {
// 			value = Value::TINYINT(Load<int8_t>(dataptr));
// 		}
// 		break;
// 	case SQL_SMALLINT:
// 		if (apd.sql_desc_type == SQL_C_USHORT) {
// 			value = Value::USMALLINT(Load<uint16_t>(dataptr));
// 		} else {
// 			value = Value::SMALLINT(Load<int16_t>(dataptr));
// 		}
// 		break;
// 	case SQL_INTEGER:
// 		if (apd.sql_desc_type == SQL_C_ULONG) {
// 			value = Value::UINTEGER(Load<uint32_t>(dataptr));
// 		} else {
// 			value = Value::INTEGER(Load<int32_t>(dataptr));
// 		}
// 		break;
// 	case SQL_BIGINT:
// 		if (apd.sql_desc_type == SQL_C_UBIGINT) {
// 			value = Value::UBIGINT(Load<uint64_t>(dataptr));
// 		} else {
// 			value = Value::BIGINT(Load<int64_t>(dataptr));
// 		}
// 		break;
// 	case SQL_FLOAT:
// 		value = Value::FLOAT(Load<float>(dataptr));
// 		break;
// 	case SQL_DOUBLE:
// 		value = Value::DOUBLE(Load<double>(dataptr));
// 		break;
// 	case SQL_NUMERIC: {
// 		auto numeric = (SQL_NUMERIC_STRUCT *)apd.sql_desc_data_ptr;
// 		dataptr = numeric->val;

// 		auto precision = ipd.sql_desc_length;
// 		if (ValidateNumeric(precision, ipd.dec_digits) == SQL_ERROR) {
// 			return SQL_ERROR;
// 		}
// 		if (ipd.sql_desc_length <= Decimal::MAX_WIDTH_INT64) {
// 			value = Value::DECIMAL(Load<int64_t>(dataptr), precision, ipd.dec_digits);
// 		} else {
// 			hugeint_t dec_value;
// 			memcpy(&dec_value.lower, dataptr, sizeof(dec_value.lower));
// 			memcpy(&dec_value.upper, dataptr + sizeof(dec_value.lower), sizeof(dec_value.upper));
// 			value = Value::DECIMAL(dec_value, precision, ipd.dec_digits);
// 		}
// 		break;
// 	}
// 	// TODO more types
// 	default:
// 		// TODO error message?
// 		return SQL_PARAM_ERROR;
// 	}

// 	SetValue(value, val_idx);
// 	return SQL_PARAM_SUCCESS;
// }

// void OdbcHandleDesc::SetValue(Value &value, idx_t val_idx) {
// 	if (val_idx >= param_clt.values.size()) {
// 		param_clt.values.emplace_back(value);
// 		return;
// 	}
// 	// replacing value, i.e., reusing parameter in a prepared stmt
// 	param_clt.values[val_idx] = value;
// }

//! DescRecord fucntions ******************************************************

SQLRETURN DescRecord::SetValueType(SQLSMALLINT value_type) {
	sql_desc_type = value_type;

	if (OdbcInterval::IsIntervalType(value_type)) {
		sql_desc_type = SQL_INTERVAL;
		sql_desc_concise_type = value_type;
		auto interval_code = OdbcInterval::GetIntervalCode(value_type);
		if (interval_code == SQL_ERROR) {
			return SQL_ERROR;
		}
	    sql_desc_datetime_interval_code = interval_code;
	}

	return SQL_SUCCESS;
}

//! ParameterController *******************************************************
// Value ParameterController::GetNextValue() {
// 	return values[paramset_idx];
// }

// SQLRETURN ParameterController::SetParamIndex() {
// 	++paramset_idx;
// 	if (paramset_idx == paramset_size) {
// 		return SQL_SUCCESS;
// 	}
// 	return SQL_STILL_EXECUTING;
// }