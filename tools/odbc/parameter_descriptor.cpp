#include "parameter_descriptor.hpp"
#include "duckdb/common/types/decimal.hpp"

using duckdb::Decimal;
using duckdb::ParameterDescriptor;
using duckdb::Value;
using duckdb::OdbcHandleDesc;

ParameterDescriptor::ParameterDescriptor(OdbcHandleStmt *stmt_ptr)
	: stmt(stmt_ptr), paramset_idx(0), cur_paramset_idx(0), cur_param_idx(0) {

	apd = make_unique<OdbcHandleDesc>(DescType::APD, stmt);
	ipd = make_unique<OdbcHandleDesc>(DescType::IPD, stmt);
}

OdbcHandleDesc *ParameterDescriptor::GetIPD() {
	return ipd.get();
}

OdbcHandleDesc *ParameterDescriptor::GetAPD() {
	return apd.get();
}

void ParameterDescriptor::Clear() {
	ipd->records.clear();
	apd->records.clear();
	Reset();
}

void ParameterDescriptor::Reset() {
	pool_allocated_ptr.clear();
	ipd->header.sql_desc_count = 0;
	apd->header.sql_desc_count = 0;
	paramset_idx = 0;
	cur_paramset_idx = 0;
	cur_param_idx = 0;
}

void ParameterDescriptor::ResetParams(SQLSMALLINT count) {
	pool_allocated_ptr.clear();

	ipd->records.resize(count);
	ipd->header.sql_desc_count = count;

	apd->records.resize(count);
	apd->header.sql_desc_count = count;
}

SQLRETURN ParameterDescriptor::GetParamValues(std::vector<Value> &values) {
	values.clear();
	if (ipd->records.empty()) {
		return SQL_SUCCESS;
	}
	D_ASSERT((SQLULEN)paramset_idx < apd->header.sql_desc_array_size);
	// Fill values
	for (idx_t rec_idx = 0; rec_idx < ipd->records.size(); ++rec_idx) {
		auto ret = SetValue(rec_idx);
		if (ipd->header.sql_desc_array_status_ptr) {
			ipd->header.sql_desc_array_status_ptr[paramset_idx] = ret;
		}
		if (ret == SQL_NEED_DATA) {
			return SQL_NEED_DATA;
		}
		if (ret != SQL_PARAM_SUCCESS) {
			auto msg = duckdb::StringUtil::Format(
			    "Error setting parameter value: ParameterSet '%ld', ParameterIndex '%ld'", cur_paramset_idx, rec_idx);
			stmt->error_messages.emplace_back(msg);
			if (!ipd->header.sql_desc_array_status_ptr) {
				return SQL_ERROR;
			}
		}
		values.emplace_back(GetNextValue());
	}
	return SetParamIndex();
}

void ParameterDescriptor::SetParamProcessedPtr(SQLPOINTER value_ptr) {
	ipd->header.sql_desc_rows_processed_ptr = (SQLULEN *)value_ptr;
	if (ipd->header.sql_desc_rows_processed_ptr) {
		*ipd->header.sql_desc_rows_processed_ptr = 0;
	}
}

SQLRETURN ParameterDescriptor::GetNextParam(SQLPOINTER *param) {
	if ((SQLSMALLINT)cur_param_idx >= apd->header.sql_desc_count) {
		return SQL_NO_DATA;
	}

	auto param_desc = apd->records[cur_param_idx];
	*param = param_desc.sql_desc_data_ptr;
	if (ipd->header.sql_desc_rows_processed_ptr) {
		*ipd->header.sql_desc_rows_processed_ptr += 1;
	}
	if (param_desc.sql_desc_indicator_ptr[cur_paramset_idx] == SQL_DATA_AT_EXEC) {
		return SQL_NEED_DATA;
	}
	return SQL_SUCCESS;
}

SQLRETURN ParameterDescriptor::PutData(SQLPOINTER data_ptr, SQLLEN str_len_or_ind_ptr) {
	if ((SQLSMALLINT)cur_param_idx >= apd->header.sql_desc_count) {
		return SQL_ERROR;
	}

	auto apd_record = &apd->records[cur_param_idx];
	if (apd_record->sql_desc_type == SQL_C_CHAR || apd_record->sql_desc_type == SQL_C_BINARY) {
		auto ipd_record = &ipd->records[cur_param_idx];
		return PutCharData(*apd_record, *ipd_record, data_ptr, str_len_or_ind_ptr);
	}

	// filling the current param, go to the next one
	++cur_param_idx;
	if (str_len_or_ind_ptr == SQL_NULL_DATA) {
		*apd_record->sql_desc_indicator_ptr = SQL_NULL_DATA;
		return SQL_SUCCESS;
	}

	apd_record->sql_desc_data_ptr = data_ptr;
	return SQL_SUCCESS;
}

bool ParameterDescriptor::HasParamSetToProcess() {
	return (paramset_idx < apd->header.sql_desc_array_size && !ipd->records.empty());
}

SQLRETURN ParameterDescriptor::PutCharData(DescRecord &apd_record, DescRecord &ipd_record, SQLPOINTER data_ptr,
                                           SQLLEN str_len_or_ind_ptr) {
	if (apd->header.sql_desc_array_size == 1) {
		return FillParamCharDataBuffer(apd_record, ipd_record, data_ptr, str_len_or_ind_ptr);
	}

	return FillCurParamCharSet(apd_record, ipd_record, data_ptr, str_len_or_ind_ptr);
}

SQLRETURN ParameterDescriptor::FillParamCharDataBuffer(DescRecord &apd_record, DescRecord &ipd_record,
                                                       SQLPOINTER data_ptr, SQLLEN str_len_or_ind_ptr) {
	size_t offset = 0;
	if (*apd_record.sql_desc_indicator_ptr == SQL_DATA_AT_EXEC) {
		pool_allocated_ptr.emplace_back(unique_ptr<char[]>(new char[ipd_record.sql_desc_length]));
		apd_record.sql_desc_data_ptr = pool_allocated_ptr.back().get();
		*apd_record.sql_desc_indicator_ptr = 0;
	} else {
		// appending more data into it
		offset = *apd_record.sql_desc_indicator_ptr;
	}

	size_t size = str_len_or_ind_ptr;
	if (size >= ((size_t)ipd_record.sql_desc_length - offset)) {
		size = (size_t)ipd_record.sql_desc_length - offset;
		// filled the current param buffer data, go to the next one
		++cur_param_idx;
	}

	memcpy((char *)apd_record.sql_desc_data_ptr + offset, (char *)data_ptr, size);
	*apd_record.sql_desc_indicator_ptr += size;

	if (ipd->header.sql_desc_array_status_ptr) {
		ipd->header.sql_desc_array_status_ptr[cur_param_idx] = SQL_PARAM_SUCCESS;
	}

	return SQL_PARAM_SUCCESS;
}

SQLRETURN ParameterDescriptor::FillCurParamCharSet(DescRecord &apd_record, DescRecord &ipd_record, SQLPOINTER data_ptr,
                                                   SQLLEN str_len_or_ind_ptr) {
	auto len_ptr = &apd_record.sql_desc_indicator_ptr[cur_paramset_idx];
	auto col_size = (size_t)ipd_record.sql_desc_length;

	if (*len_ptr == SQL_DATA_AT_EXEC && pool_allocated_ptr.empty()) {
		auto alloc_size = col_size * apd->header.sql_desc_array_size;
		pool_allocated_ptr.emplace_back(unique_ptr<char[]>(new char[alloc_size]));
		apd_record.sql_desc_data_ptr = pool_allocated_ptr.back().get();
	}

	auto value_ptr = (char *)apd_record.sql_desc_data_ptr + (cur_paramset_idx * col_size);

	size_t size = str_len_or_ind_ptr;
	if (size >= ((size_t)ipd_record.sql_desc_length)) {
		size = col_size;
	}

	memcpy(value_ptr, (const char *)data_ptr, size);
	*len_ptr = size;

	if (ipd->header.sql_desc_array_status_ptr) {
		ipd->header.sql_desc_array_status_ptr[cur_paramset_idx] = SQL_PARAM_SUCCESS;
	}

	++cur_paramset_idx;
	if (cur_paramset_idx >= apd->header.sql_desc_array_size) {
		// got to the next param set
		cur_paramset_idx = 0;
		++cur_param_idx;
	}

	return SQL_PARAM_SUCCESS;
}

SQLRETURN ParameterDescriptor::SetValue(idx_t rec_idx) {
	auto val_idx = paramset_idx;

	if (apd->records[rec_idx].sql_desc_data_ptr == nullptr && apd->records[rec_idx].sql_desc_indicator_ptr == nullptr) {
		return SQL_ERROR;
	}

	if (apd->records[rec_idx].sql_desc_data_ptr == nullptr || apd->records[rec_idx].sql_desc_indicator_ptr == nullptr ||
	    apd->records[rec_idx].sql_desc_indicator_ptr[val_idx] == SQL_NULL_DATA) {
		Value val_null(nullptr);
		SetValue(val_null, val_idx);
		return SQL_SUCCESS;
	}

	if (apd->records[rec_idx].sql_desc_indicator_ptr[val_idx] == SQL_DATA_AT_EXEC ||
	    (SQLULEN)apd->records[rec_idx].sql_desc_indicator_ptr[val_idx] ==
	        SQL_LEN_DATA_AT_EXEC(ipd->records[rec_idx].sql_desc_length)) {
		return SQL_NEED_DATA;
	}

	duckdb::Value value;
	// TODO need to check it param_value_ptr is an array of parameters
	// and get the right parameter using the index (now it's working for all supported tests)
	duckdb::const_data_ptr_t dataptr = (duckdb::const_data_ptr_t)apd->records[rec_idx].sql_desc_data_ptr;

	switch (ipd->records[rec_idx].sql_desc_type) {
	case SQL_CHAR:
	case SQL_VARCHAR: {
		auto str_data =
		    (char *)apd->records[rec_idx].sql_desc_data_ptr + (val_idx * ipd->records[rec_idx].sql_desc_length);
		auto str_len = apd->records[rec_idx].sql_desc_indicator_ptr[val_idx];
		value = Value(duckdb::OdbcUtils::ReadString(str_data, str_len));
		break;
	}
	case SQL_WCHAR: {
		auto str_data =
		    (wchar_t *)apd->records[rec_idx].sql_desc_data_ptr + (val_idx * ipd->records[rec_idx].sql_desc_length);
		auto str_len = apd->records[rec_idx].sql_desc_indicator_ptr[val_idx];
		value = Value(duckdb::OdbcUtils::ReadString(str_data, str_len));
		break;
	}
	case SQL_VARBINARY:
	case SQL_BINARY: {
		auto blob_data = (duckdb::const_data_ptr_t)apd->records[rec_idx].sql_desc_data_ptr +
		                 (val_idx * ipd->records[rec_idx].sql_desc_length);
		auto blob_len = apd->records[rec_idx].sql_desc_indicator_ptr[val_idx];
		value = Value::BLOB(blob_data, blob_len);
		break;
	}
	case SQL_TINYINT:
		if (apd->records[rec_idx].sql_desc_type == SQL_C_UTINYINT) {
			value = Value::UTINYINT(Load<uint8_t>(dataptr));
		} else {
			value = Value::TINYINT(Load<int8_t>(dataptr));
		}
		break;
	case SQL_SMALLINT:
		if (apd->records[rec_idx].sql_desc_type == SQL_C_USHORT) {
			value = Value::USMALLINT(Load<uint16_t>(dataptr));
		} else {
			value = Value::SMALLINT(Load<int16_t>(dataptr));
		}
		break;
	case SQL_INTEGER:
		if (apd->records[rec_idx].sql_desc_type == SQL_C_ULONG) {
			value = Value::UINTEGER(Load<uint32_t>(dataptr));
		} else {
			value = Value::INTEGER(Load<int32_t>(dataptr));
		}
		break;
	case SQL_BIGINT:
		if (apd->records[rec_idx].sql_desc_type == SQL_C_UBIGINT) {
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
		auto numeric = (SQL_NUMERIC_STRUCT *)apd->records[rec_idx].sql_desc_data_ptr;
		dataptr = numeric->val;

		auto precision = ipd->records[rec_idx].sql_desc_precision;
		auto scale = ipd->records[rec_idx].sql_desc_scale;
		if (ValidateNumeric(precision, scale) == SQL_ERROR) {
			return SQL_ERROR;
		}
		if (precision <= Decimal::MAX_WIDTH_INT64) {
			value = Value::DECIMAL(Load<int64_t>(dataptr), precision, scale);
		} else {
			hugeint_t dec_value;
			memcpy(&dec_value.lower, dataptr, sizeof(dec_value.lower));
			memcpy(&dec_value.upper, dataptr + sizeof(dec_value.lower), sizeof(dec_value.upper));
			value = Value::DECIMAL(dec_value, precision, scale);
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

void ParameterDescriptor::SetValue(Value &value, idx_t val_idx) {
	if (val_idx >= values.size()) {
		values.emplace_back(value);
		return;
	}
	// replacing value, i.e., reusing parameter in a prepared stmt
	values[val_idx] = value;
}

Value ParameterDescriptor::GetNextValue() {
	return values[paramset_idx];
}

SQLRETURN ParameterDescriptor::SetParamIndex() {
	++paramset_idx;
	if (paramset_idx == apd->header.sql_desc_array_size) {
		return SQL_SUCCESS;
	}
	return SQL_STILL_EXECUTING;
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