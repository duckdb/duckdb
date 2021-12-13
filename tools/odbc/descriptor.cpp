#include "descriptor.hpp"
#include "odbc_interval.hpp"

using duckdb::DescHeader;
using duckdb::DescRecord;
using duckdb::OdbcHandleDesc;
using duckdb::OdbcInterval;

//! OdbcHandleDesc functions ********************************
duckdb::DescRecord *OdbcHandleDesc::GetDescRecord(duckdb::idx_t param_idx) {
	if (param_idx >= records.size()) {
		records.resize(param_idx + 1);
		header.sql_desc_count = records.size();
	}
	return &records[param_idx];
}

SQLRETURN OdbcHandleDesc::SetDescField(SQLSMALLINT rec_number, SQLSMALLINT field_identifier, SQLPOINTER value_ptr,
                                       SQLINTEGER buffer_length) {
	switch (field_identifier) {
	case SQL_DESC_CONCISE_TYPE:
		return SQL_SUCCESS;
	case SQL_DESC_ARRAY_STATUS_PTR:
		header.sql_desc_array_status_ptr = (SQLUSMALLINT *)value_ptr;
		return SQL_SUCCESS;
	case SQL_DESC_ROWS_PROCESSED_PTR:
		header.sql_desc_rows_processed_ptr = (SQLULEN *)value_ptr;
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
	Reset();
}

void OdbcHandleDesc::Reset() {
	records.clear();
}

//! DescRecord functions ******************************************************
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

//! DescHeader functions ******************************************************
DescHeader::DescHeader() {
	Reset();
}

void DescHeader::Reset() {
	sql_desc_array_size = 1;
	sql_desc_array_status_ptr = nullptr;
	sql_desc_count = 0;
	sql_desc_rows_processed_ptr = nullptr;
}
