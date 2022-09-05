#include "parameter_descriptor.hpp"
#include "odbc_utils.hpp"
#include "duckdb/common/types/decimal.hpp"

using duckdb::Decimal;
using duckdb::OdbcHandleDesc;
using duckdb::ParameterDescriptor;
using duckdb::Value;

ParameterDescriptor::ParameterDescriptor(OdbcHandleStmt *stmt_ptr)
    : stmt(stmt_ptr), paramset_idx(0), cur_paramset_idx(0), cur_param_idx(0) {

	apd = make_unique<OdbcHandleDesc>(stmt->dbc);
	ipd = make_unique<OdbcHandleDesc>(stmt->dbc);

	cur_apd = apd.get();
}

OdbcHandleDesc *ParameterDescriptor::GetIPD() {
	return ipd.get();
}

OdbcHandleDesc *ParameterDescriptor::GetAPD() {
	return cur_apd;
}

void ParameterDescriptor::Clear() {
	ipd->records.clear();
	cur_apd->records.clear();
	Reset();
}

void ParameterDescriptor::SetCurrentAPD(OdbcHandleDesc *new_apd) {
	cur_apd = new_apd;
	cur_apd->header.sql_desc_alloc_type = SQL_DESC_ALLOC_USER;
}

void ParameterDescriptor::Reset() {
	pool_allocated_ptr.clear();
	ipd->header.sql_desc_count = 0;
	cur_apd->header.sql_desc_count = 0;
	paramset_idx = 0;
	cur_paramset_idx = 0;
	cur_param_idx = 0;
}

void ParameterDescriptor::ResetParams(SQLSMALLINT count) {
	pool_allocated_ptr.clear();

	ipd->records.resize(count);
	ipd->header.sql_desc_count = count;

	cur_apd->records.resize(count);
	cur_apd->header.sql_desc_count = count;
}

void ParameterDescriptor::ResetCurrentAPD() {
	cur_apd = apd.get();
}

SQLRETURN ParameterDescriptor::GetParamValues(std::vector<Value> &values) {
	values.clear();
	if (ipd->records.empty()) {
		return SQL_SUCCESS;
	}
	D_ASSERT((SQLULEN)paramset_idx < cur_apd->header.sql_desc_array_size);
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
		values.emplace_back(GetNextValue(rec_idx));
	}
	return SetParamIndex();
}

void ParameterDescriptor::SetParamProcessedPtr(SQLULEN *value_ptr) {
	ipd->header.sql_desc_rows_processed_ptr = value_ptr;
	if (ipd->header.sql_desc_rows_processed_ptr) {
		*ipd->header.sql_desc_rows_processed_ptr = 0;
	}
}

SQLULEN *ParameterDescriptor::GetParamProcessedPtr() {
	return ipd->header.sql_desc_rows_processed_ptr;
}

void ParameterDescriptor::SetArrayStatusPtr(SQLUSMALLINT *value_ptr) {
	ipd->header.sql_desc_array_status_ptr = value_ptr;
}

SQLUSMALLINT *ParameterDescriptor::SetArrayStatusPtr() {
	return ipd->header.sql_desc_array_status_ptr;
}

void ParameterDescriptor::SetBindOffesetPtr(SQLLEN *value_ptr) {
	stmt->param_desc->apd->header.sql_desc_bind_offset_ptr = (SQLLEN *)value_ptr;
}

SQLLEN *ParameterDescriptor::GetBindOffesetPtr() {
	return stmt->param_desc->apd->header.sql_desc_bind_offset_ptr;
}

SQLRETURN ParameterDescriptor::GetNextParam(SQLPOINTER *param) {
	if ((SQLSMALLINT)cur_param_idx >= cur_apd->header.sql_desc_count) {
		return SQL_NO_DATA;
	}

	auto apd_record = &cur_apd->records[cur_param_idx];
	*param = GetSQLDescDataPtr(*apd_record);
	if (ipd->header.sql_desc_rows_processed_ptr) {
		*ipd->header.sql_desc_rows_processed_ptr += 1;
	}
	auto sql_desc_ind_ptr = GetSQLDescIndicatorPtr(*apd_record, cur_paramset_idx);
	if (*sql_desc_ind_ptr == SQL_DATA_AT_EXEC) {
		return SQL_NEED_DATA;
	}
	return SQL_SUCCESS;
}

SQLRETURN ParameterDescriptor::PutData(SQLPOINTER data_ptr, SQLLEN str_len_or_ind_ptr) {
	if ((SQLSMALLINT)cur_param_idx >= cur_apd->header.sql_desc_count) {
		return SQL_ERROR;
	}

	auto apd_record = &cur_apd->records[cur_param_idx];
	if (apd_record->sql_desc_type == SQL_C_CHAR || apd_record->sql_desc_type == SQL_C_BINARY) {
		auto ipd_record = &ipd->records[cur_param_idx];
		return PutCharData(*apd_record, *ipd_record, data_ptr, str_len_or_ind_ptr);
	}

	// filling the current param, go to the next one
	++cur_param_idx;
	if (str_len_or_ind_ptr == SQL_NULL_DATA) {
		SetSQLDescIndicatorPtr(*apd_record, SQL_NULL_DATA);
		return SQL_SUCCESS;
	}

	SetSQLDescDataPtr(*apd_record, data_ptr);
	return SQL_SUCCESS;
}

bool ParameterDescriptor::HasParamSetToProcess() {
	return (paramset_idx < cur_apd->header.sql_desc_array_size && !ipd->records.empty());
}

SQLRETURN ParameterDescriptor::PutCharData(DescRecord &apd_record, DescRecord &ipd_record, SQLPOINTER data_ptr,
                                           SQLLEN str_len_or_ind_ptr) {
	if (cur_apd->header.sql_desc_array_size == 1) {
		return FillParamCharDataBuffer(apd_record, ipd_record, data_ptr, str_len_or_ind_ptr);
	}

	return FillCurParamCharSet(apd_record, ipd_record, data_ptr, str_len_or_ind_ptr);
}

SQLRETURN ParameterDescriptor::FillParamCharDataBuffer(DescRecord &apd_record, DescRecord &ipd_record,
                                                       SQLPOINTER data_ptr, SQLLEN str_len_or_ind_ptr) {
	size_t offset = 0;
	auto sql_ind_ptr = GetSQLDescIndicatorPtr(apd_record);
	if (*sql_ind_ptr == SQL_DATA_AT_EXEC) {
		pool_allocated_ptr.emplace_back(unique_ptr<char[]>(new char[ipd_record.sql_desc_length]));
		SetSQLDescDataPtr(apd_record, pool_allocated_ptr.back().get());
		*sql_ind_ptr = 0;
	} else {
		// appending more data into it
		offset = *sql_ind_ptr;
	}

	size_t size = str_len_or_ind_ptr;
	if (size >= ((size_t)ipd_record.sql_desc_length - offset)) {
		size = (size_t)ipd_record.sql_desc_length - offset;
		// filled the current param buffer data, go to the next one
		++cur_param_idx;
	}

	memcpy((char *)GetSQLDescDataPtr(apd_record) + offset, (char *)data_ptr, size);
	*sql_ind_ptr += size;

	if (ipd->header.sql_desc_array_status_ptr) {
		ipd->header.sql_desc_array_status_ptr[cur_param_idx] = SQL_PARAM_SUCCESS;
	}

	return SQL_PARAM_SUCCESS;
}

SQLRETURN ParameterDescriptor::FillCurParamCharSet(DescRecord &apd_record, DescRecord &ipd_record, SQLPOINTER data_ptr,
                                                   SQLLEN str_len_or_ind_ptr) {
	auto len_ptr = GetSQLDescIndicatorPtr(apd_record, cur_paramset_idx);
	auto col_size = (size_t)ipd_record.sql_desc_length;

	if (*len_ptr == SQL_DATA_AT_EXEC && pool_allocated_ptr.empty()) {
		auto alloc_size = col_size * cur_apd->header.sql_desc_array_size;
		pool_allocated_ptr.emplace_back(unique_ptr<char[]>(new char[alloc_size]));
		SetSQLDescDataPtr(apd_record, pool_allocated_ptr.back().get());
	}

	auto value_ptr = (char *)GetSQLDescDataPtr(apd_record) + (cur_paramset_idx * col_size);

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
	if (cur_paramset_idx >= cur_apd->header.sql_desc_array_size) {
		// got to the next param set
		cur_paramset_idx = 0;
		++cur_param_idx;
	}

	return SQL_PARAM_SUCCESS;
}

SQLRETURN ParameterDescriptor::SetValue(idx_t rec_idx) {
	auto val_idx = paramset_idx;
	auto apd_record = &cur_apd->records[rec_idx];
	auto sql_data_ptr = GetSQLDescDataPtr(*apd_record);
	auto sql_ind_ptr = GetSQLDescIndicatorPtr(*apd_record);

	if (sql_data_ptr == nullptr && sql_ind_ptr == nullptr) {
		return SQL_ERROR;
	}

	auto sql_ind_ptr_val_set = GetSQLDescIndicatorPtr(*apd_record, val_idx);
	if (sql_data_ptr == nullptr || sql_ind_ptr == nullptr || *sql_ind_ptr_val_set == SQL_NULL_DATA) {
		Value val_null(nullptr);
		SetValue(val_null, rec_idx);
		return SQL_SUCCESS;
	}

	if (*sql_ind_ptr_val_set == SQL_DATA_AT_EXEC ||
	    (SQLULEN)*sql_ind_ptr_val_set == SQL_LEN_DATA_AT_EXEC(ipd->records[rec_idx].sql_desc_length)) {
		return SQL_NEED_DATA;
	}

	duckdb::Value value;
	// TODO need to check it param_value_ptr is an array of parameters
	// and get the right parameter using the index (now it's working for all supported tests)
	duckdb::const_data_ptr_t dataptr = (duckdb::const_data_ptr_t)sql_data_ptr;

	switch (ipd->records[rec_idx].sql_desc_type) {
	case SQL_CHAR:
	case SQL_VARCHAR: {
		auto buff_size = duckdb::MaxValue((SQLLEN)ipd->records[rec_idx].sql_desc_length,
		                                  apd->records[rec_idx].sql_desc_octet_length);
		auto str_data = (char *)sql_data_ptr + (val_idx * buff_size);
		if (*sql_ind_ptr_val_set == SQL_NTS) {
			*sql_ind_ptr_val_set = strlen(str_data);
		}
		auto str_len = *sql_ind_ptr_val_set;
		value = Value(duckdb::OdbcUtils::ReadString(str_data, str_len));
		break;
	}
	case SQL_WCHAR: {
		auto buff_size = duckdb::MaxValue((SQLLEN)ipd->records[rec_idx].sql_desc_length,
		                                  apd->records[rec_idx].sql_desc_octet_length);
		auto str_data = (wchar_t *)sql_data_ptr + (val_idx * buff_size);
		if (*sql_ind_ptr_val_set == SQL_NTS) {
			*sql_ind_ptr_val_set = wcslen(str_data);
		}
		auto str_len = *sql_ind_ptr_val_set;
		value = Value(duckdb::OdbcUtils::ReadString(str_data, str_len));
		break;
	}
	case SQL_VARBINARY:
	case SQL_BINARY: {
		auto buff_size = duckdb::MaxValue((SQLLEN)ipd->records[rec_idx].sql_desc_length,
		                                  apd->records[rec_idx].sql_desc_octet_length);
		auto blob_data = (duckdb::const_data_ptr_t)sql_data_ptr + (val_idx * buff_size);
		auto blob_len = *sql_ind_ptr_val_set;
		value = Value::BLOB(blob_data, blob_len);
		break;
	}
	case SQL_TINYINT:
		if (cur_apd->records[rec_idx].sql_desc_type == SQL_C_UTINYINT) {
			value = Value::UTINYINT(Load<uint8_t>(dataptr));
		} else {
			value = Value::TINYINT(Load<int8_t>(dataptr));
		}
		break;
	case SQL_SMALLINT:
		if (cur_apd->records[rec_idx].sql_desc_type == SQL_C_USHORT) {
			value = Value::USMALLINT(Load<uint16_t>(dataptr));
		} else {
			value = Value::SMALLINT(Load<int16_t>(dataptr));
		}
		break;
	case SQL_INTEGER:
		if (cur_apd->records[rec_idx].sql_desc_type == SQL_C_ULONG) {
			value = Value::UINTEGER(Load<uint32_t>(dataptr));
		} else {
			value = Value::INTEGER(Load<int32_t>(dataptr));
		}
		break;
	case SQL_BIGINT:
		if (cur_apd->records[rec_idx].sql_desc_type == SQL_C_UBIGINT) {
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
		auto numeric = (SQL_NUMERIC_STRUCT *)sql_data_ptr;
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

	SetValue(value, rec_idx);
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

Value ParameterDescriptor::GetNextValue(idx_t val_idx) {
	return values[val_idx];
}

SQLRETURN ParameterDescriptor::SetParamIndex() {
	++paramset_idx;
	if (ipd->header.sql_desc_rows_processed_ptr) {
		*ipd->header.sql_desc_rows_processed_ptr = paramset_idx;
	}
	if (paramset_idx == cur_apd->header.sql_desc_array_size) {
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

SQLPOINTER ParameterDescriptor::GetSQLDescDataPtr(DescRecord &apd_record) {
	if (cur_apd->header.sql_desc_bind_offset_ptr) {
		return (uint8_t *)apd_record.sql_desc_data_ptr + *cur_apd->header.sql_desc_bind_offset_ptr;
	}
	return apd_record.sql_desc_data_ptr;
}

void ParameterDescriptor::SetSQLDescDataPtr(DescRecord &apd_record, SQLPOINTER data_ptr) {
	auto sql_data_ptr = &(apd_record.sql_desc_data_ptr);
	if (cur_apd->header.sql_desc_bind_offset_ptr) {
		sql_data_ptr += *cur_apd->header.sql_desc_bind_offset_ptr;
	}
	*sql_data_ptr = data_ptr;
}

SQLLEN *ParameterDescriptor::GetSQLDescIndicatorPtr(DescRecord &apd_record, idx_t set_idx) {
	if (cur_apd->header.sql_desc_bind_offset_ptr) {
		return apd_record.sql_desc_indicator_ptr + set_idx + *cur_apd->header.sql_desc_bind_offset_ptr;
	}
	return apd_record.sql_desc_indicator_ptr + set_idx;
}

void ParameterDescriptor::SetSQLDescIndicatorPtr(DescRecord &apd_record, SQLLEN *ind_ptr) {
	auto sql_ind_ptr = apd_record.sql_desc_indicator_ptr;
	if (cur_apd->header.sql_desc_bind_offset_ptr) {
		sql_ind_ptr += *cur_apd->header.sql_desc_bind_offset_ptr;
	}
	sql_ind_ptr = ind_ptr;
}

void ParameterDescriptor::SetSQLDescIndicatorPtr(DescRecord &apd_record, SQLLEN value) {
	auto sql_ind_ptr = apd_record.sql_desc_indicator_ptr;
	if (cur_apd->header.sql_desc_bind_offset_ptr) {
		sql_ind_ptr += *cur_apd->header.sql_desc_bind_offset_ptr;
	}
	*sql_ind_ptr = value;
}

SQLLEN *ParameterDescriptor::GetSQLDescOctetLengthPtr(DescRecord &apd_record, idx_t set_idx) {
	if (cur_apd->header.sql_desc_bind_offset_ptr) {
		return apd_record.sql_desc_octet_length_ptr + set_idx + *cur_apd->header.sql_desc_bind_offset_ptr;
	}
	return apd_record.sql_desc_octet_length_ptr + set_idx;
}

void ParameterDescriptor::SetSQLDescOctetLengthPtr(DescRecord &apd_record, SQLLEN *len_ptr) {
	auto sql_len_ptr = apd_record.sql_desc_octet_length_ptr;
	if (cur_apd->header.sql_desc_bind_offset_ptr) {
		sql_len_ptr += *cur_apd->header.sql_desc_bind_offset_ptr;
	}
	sql_len_ptr = len_ptr;
}
