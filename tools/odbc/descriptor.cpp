#include "descriptor.hpp"
#include "api_info.hpp"
#include "odbc_interval.hpp"
#include "odbc_utils.hpp"
#include "parameter_descriptor.hpp"
#include "row_descriptor.hpp"

using duckdb::DescHeader;
using duckdb::DescRecord;
using duckdb::OdbcHandleDesc;
using duckdb::OdbcInterval;

//! ODBC Descriptor functions ********************************

SQLRETURN SQL_API SQLGetDescField(SQLHDESC descriptor_handle, SQLSMALLINT rec_number, SQLSMALLINT field_identifier,
                                  SQLPOINTER value_ptr, SQLINTEGER buffer_length, SQLINTEGER *string_length_ptr) {
	return duckdb::WithDescriptor(descriptor_handle, [&](duckdb::OdbcHandleDesc *handle_desc) {
		if (duckdb::ApiInfo::IsNumericDescriptorField(field_identifier) && value_ptr == nullptr) {
			handle_desc->error_messages.emplace_back("Invalid null value pointer for descriptor numeric field");
			return SQL_ERROR;
		}

		// descriptor header fields
		switch (field_identifier) {
		case SQL_DESC_ALLOC_TYPE: {
			*(SQLSMALLINT *)value_ptr = handle_desc->header.sql_desc_alloc_type;
			return SQL_SUCCESS;
		}
		case SQL_DESC_ARRAY_SIZE: {
			if (handle_desc->IsID()) {
				handle_desc->error_messages.emplace_back("Invalid descriptor field identifier");
				return SQL_ERROR;
			}
			*(SQLULEN *)value_ptr = handle_desc->header.sql_desc_array_size;
			return SQL_SUCCESS;
		}
		case SQL_DESC_ARRAY_STATUS_PTR: {
			*(SQLUSMALLINT **)value_ptr = handle_desc->header.sql_desc_array_status_ptr;
			return SQL_SUCCESS;
		}
		case SQL_DESC_BIND_OFFSET_PTR: {
			if (handle_desc->IsID()) {
				handle_desc->error_messages.emplace_back("Invalid descriptor field identifier");
				return SQL_ERROR;
			}
			*(SQLLEN **)value_ptr = handle_desc->header.sql_desc_bind_offset_ptr;
			return SQL_SUCCESS;
		}
		case SQL_DESC_BIND_TYPE: {
			if (handle_desc->IsID()) {
				handle_desc->error_messages.emplace_back("Invalid descriptor field identifier");
				return SQL_ERROR;
			}
			*(SQLINTEGER *)value_ptr = handle_desc->header.sql_desc_bind_type;
			return SQL_SUCCESS;
		}
		case SQL_DESC_COUNT: {
			*(SQLSMALLINT *)value_ptr = handle_desc->header.sql_desc_count;
			return SQL_SUCCESS;
		}
		case SQL_DESC_ROWS_PROCESSED_PTR: {
			if (handle_desc->IsAD()) {
				handle_desc->error_messages.emplace_back("Invalid descriptor field identifier");
				return SQL_ERROR;
			}
			*(SQLULEN **)value_ptr = handle_desc->header.sql_desc_rows_processed_ptr;
			return SQL_SUCCESS;
		}
		default:
			break;
		}

		if (rec_number <= 0 || rec_number > (SQLSMALLINT)handle_desc->records.size()) {
			handle_desc->error_messages.emplace_back("Invalid descriptor index");
			return SQL_ERROR;
		}
		duckdb::idx_t rec_idx = rec_number - 1;

		// checking descriptor record fields
		switch (field_identifier) {
		case SQL_DESC_AUTO_UNIQUE_VALUE: {
			// not IRD
			if (!handle_desc->IsIRD()) {
				handle_desc->error_messages.emplace_back("Invalid descriptor field identifier");
				return SQL_ERROR;
			}
			// always false, DuckDB doesn't support auto-incrementing
			*(SQLINTEGER *)value_ptr = SQL_FALSE;
			return SQL_SUCCESS;
		}
		case SQL_DESC_BASE_COLUMN_NAME: {
			// not IRD
			if (!handle_desc->IsIRD()) {
				handle_desc->error_messages.emplace_back("Invalid descriptor field identifier");
				return SQL_ERROR;
			}
			duckdb::OdbcUtils::WriteString(handle_desc->records[rec_idx].sql_desc_base_column_name,
			                               (SQLCHAR *)value_ptr, buffer_length, string_length_ptr);
			return SQL_SUCCESS;
		}
		case SQL_DESC_BASE_TABLE_NAME: {
			// not IRD
			if (!handle_desc->IsIRD()) {
				handle_desc->error_messages.emplace_back("Invalid descriptor field identifier");
				return SQL_ERROR;
			}
			duckdb::OdbcUtils::WriteString(handle_desc->records[rec_idx].sql_desc_base_table_name, (SQLCHAR *)value_ptr,
			                               buffer_length, string_length_ptr);
			return SQL_SUCCESS;
		}
		case SQL_DESC_CASE_SENSITIVE: {
			// not IRD
			if (!handle_desc->IsIRD()) {
				handle_desc->error_messages.emplace_back("Invalid descriptor field identifier");
				return SQL_ERROR;
			}
			*(SQLINTEGER *)value_ptr = handle_desc->records[rec_idx].sql_desc_case_sensitive;
			return SQL_SUCCESS;
		}
		case SQL_DESC_CATALOG_NAME: {
			// not IRD
			if (!handle_desc->IsIRD()) {
				handle_desc->error_messages.emplace_back("Invalid descriptor field identifier");
				return SQL_ERROR;
			}
			duckdb::OdbcUtils::WriteString(handle_desc->records[rec_idx].sql_desc_catalog_name, (SQLCHAR *)value_ptr,
			                               buffer_length, string_length_ptr);
			return SQL_SUCCESS;
		}
		case SQL_DESC_CONCISE_TYPE: {
			*(SQLSMALLINT *)value_ptr = handle_desc->records[rec_idx].sql_desc_concise_type;
			return SQL_SUCCESS;
		}
		case SQL_DESC_DATA_PTR: {
			if (handle_desc->IsID()) {
				handle_desc->error_messages.emplace_back("Invalid descriptor field identifier");
				return SQL_ERROR;
			}
			value_ptr = handle_desc->records[rec_idx].sql_desc_data_ptr;
			return SQL_SUCCESS;
		}
		case SQL_DESC_DATETIME_INTERVAL_CODE: {
			*(SQLSMALLINT *)value_ptr = handle_desc->records[rec_idx].sql_desc_datetime_interval_code;
			return SQL_SUCCESS;
		}
		case SQL_DESC_DATETIME_INTERVAL_PRECISION: {
			*(SQLINTEGER *)value_ptr = handle_desc->records[rec_idx].sql_desc_datetime_interval_precision;
			return SQL_SUCCESS;
		}
		case SQL_DESC_DISPLAY_SIZE: {
			// not IRD
			if (!handle_desc->IsIRD()) {
				handle_desc->error_messages.emplace_back("Invalid descriptor field identifier");
				return SQL_ERROR;
			}
			*(SQLLEN *)value_ptr = handle_desc->records[rec_idx].sql_desc_display_size;
			return SQL_SUCCESS;
		}
		case SQL_DESC_FIXED_PREC_SCALE: {
			// not IRD
			if (!handle_desc->IsIRD()) {
				handle_desc->error_messages.emplace_back("Invalid descriptor field identifier");
				return SQL_ERROR;
			}
			*(SQLSMALLINT *)value_ptr = handle_desc->records[rec_idx].sql_desc_fixed_prec_scale;
			return SQL_SUCCESS;
		}
		case SQL_DESC_INDICATOR_PTR: {
			if (handle_desc->IsID()) {
				handle_desc->error_messages.emplace_back("Invalid descriptor field identifier");
				return SQL_ERROR;
			}
			*(SQLLEN **)value_ptr = handle_desc->records[rec_idx].sql_desc_indicator_ptr;
			return SQL_SUCCESS;
		}
		case SQL_DESC_LABEL: {
			// not IRD
			if (!handle_desc->IsIRD()) {
				handle_desc->error_messages.emplace_back("Invalid descriptor field identifier");
				return SQL_ERROR;
			}
			duckdb::OdbcUtils::WriteString(handle_desc->records[rec_idx].sql_desc_label, (SQLCHAR *)value_ptr,
			                               buffer_length, string_length_ptr);
			return SQL_SUCCESS;
		}
		case SQL_DESC_LENGTH: {
			*(SQLULEN *)value_ptr = handle_desc->records[rec_idx].sql_desc_length;
			return SQL_SUCCESS;
		}
		case SQL_DESC_LITERAL_PREFIX: {
			// not IRD
			if (!handle_desc->IsIRD()) {
				handle_desc->error_messages.emplace_back("Invalid descriptor field identifier");
				return SQL_ERROR;
			}
			duckdb::OdbcUtils::WriteString(handle_desc->records[rec_idx].sql_desc_literal_prefix, (SQLCHAR *)value_ptr,
			                               buffer_length, string_length_ptr);
			return SQL_SUCCESS;
		}
		case SQL_DESC_LITERAL_SUFFIX: {
			// not IRD
			if (!handle_desc->IsIRD()) {
				handle_desc->error_messages.emplace_back("Invalid descriptor field identifier");
				return SQL_ERROR;
			}
			duckdb::OdbcUtils::WriteString(handle_desc->records[rec_idx].sql_desc_literal_suffix, (SQLCHAR *)value_ptr,
			                               buffer_length, string_length_ptr);
			return SQL_SUCCESS;
		}
		case SQL_DESC_LOCAL_TYPE_NAME: {
			// is AD
			if (handle_desc->IsAD()) {
				handle_desc->error_messages.emplace_back("Invalid descriptor field identifier");
				return SQL_ERROR;
			}
			duckdb::OdbcUtils::WriteString(handle_desc->records[rec_idx].sql_desc_local_type_name, (SQLCHAR *)value_ptr,
			                               buffer_length, string_length_ptr);
			return SQL_SUCCESS;
		}
		case SQL_DESC_NAME: {
			// is AD
			if (handle_desc->IsAD()) {
				handle_desc->error_messages.emplace_back("Invalid descriptor field identifier");
				return SQL_ERROR;
			}
			duckdb::OdbcUtils::WriteString(handle_desc->records[rec_idx].sql_desc_name, (SQLCHAR *)value_ptr,
			                               buffer_length, string_length_ptr);
			return SQL_SUCCESS;
		}
		case SQL_DESC_NULLABLE: {
			// is AD
			if (!handle_desc->IsAD()) {
				handle_desc->error_messages.emplace_back("Invalid descriptor field identifier");
				return SQL_ERROR;
			}
			*(SQLSMALLINT *)value_ptr = handle_desc->records[rec_idx].sql_desc_nullable;
			return SQL_SUCCESS;
		}
		case SQL_DESC_NUM_PREC_RADIX: {
			*(SQLINTEGER *)value_ptr = handle_desc->records[rec_idx].sql_desc_num_prec_radix;
			return SQL_SUCCESS;
		}
		case SQL_DESC_OCTET_LENGTH: {
			*(SQLLEN *)value_ptr = handle_desc->records[rec_idx].sql_desc_octet_length;
			return SQL_SUCCESS;
		}
		case SQL_DESC_OCTET_LENGTH_PTR: {
			// is ID
			if (handle_desc->IsID()) {
				handle_desc->error_messages.emplace_back("Invalid descriptor field identifier");
				return SQL_ERROR;
			}
			*(SQLLEN **)value_ptr = handle_desc->records[rec_idx].sql_desc_octet_length_ptr;
			return SQL_SUCCESS;
		}
		case SQL_DESC_PARAMETER_TYPE: {
			// not IPD
			if (!handle_desc->IsIPD()) {
				handle_desc->error_messages.emplace_back("Invalid descriptor field identifier");
				return SQL_ERROR;
			}
			*(SQLSMALLINT *)value_ptr = handle_desc->records[rec_idx].sql_desc_parameter_type;
			return SQL_SUCCESS;
		}
		case SQL_DESC_PRECISION: {
			*(SQLSMALLINT *)value_ptr = handle_desc->records[rec_idx].sql_desc_precision;
			return SQL_SUCCESS;
		}
		case SQL_DESC_ROWVER: {
			// is AD
			if (!handle_desc->IsAD()) {
				handle_desc->error_messages.emplace_back("Invalid descriptor field identifier");
				return SQL_ERROR;
			}
			*(SQLSMALLINT *)value_ptr = handle_desc->records[rec_idx].sql_desc_rowver;
			return SQL_SUCCESS;
		}
		case SQL_DESC_SCALE: {
			*(SQLSMALLINT *)value_ptr = handle_desc->records[rec_idx].sql_desc_scale;
			return SQL_SUCCESS;
		}
		case SQL_DESC_SCHEMA_NAME: {
			// not IRD
			if (!handle_desc->IsIRD()) {
				handle_desc->error_messages.emplace_back("Invalid descriptor field identifier");
				return SQL_ERROR;
			}
			duckdb::OdbcUtils::WriteString(handle_desc->records[rec_idx].sql_desc_schema_name, (SQLCHAR *)value_ptr,
			                               buffer_length, string_length_ptr);
			return SQL_SUCCESS;
		}
		case SQL_DESC_SEARCHABLE: {
			// not IRD
			if (!handle_desc->IsIRD()) {
				handle_desc->error_messages.emplace_back("Invalid descriptor field identifier");
				return SQL_ERROR;
			}
			*(SQLSMALLINT *)value_ptr = handle_desc->records[rec_idx].sql_desc_searchable;
			return SQL_SUCCESS;
		}
		case SQL_DESC_TABLE_NAME: {
			// not IRD
			if (!handle_desc->IsIRD()) {
				handle_desc->error_messages.emplace_back("Invalid descriptor field identifier");
				return SQL_ERROR;
			}
			duckdb::OdbcUtils::WriteString(handle_desc->records[rec_idx].sql_desc_table_name, (SQLCHAR *)value_ptr,
			                               buffer_length, string_length_ptr);
			return SQL_SUCCESS;
		}
		case SQL_DESC_TYPE: {
			*(SQLSMALLINT *)value_ptr = handle_desc->records[rec_idx].sql_desc_type;
			return SQL_SUCCESS;
		}
		case SQL_DESC_TYPE_NAME: {
			// is AD
			if (handle_desc->IsAD()) {
				handle_desc->error_messages.emplace_back("Invalid descriptor field identifier");
				return SQL_ERROR;
			}
			duckdb::OdbcUtils::WriteString(handle_desc->records[rec_idx].sql_desc_type_name, (SQLCHAR *)value_ptr,
			                               buffer_length, string_length_ptr);
			return SQL_SUCCESS;
		}
		case SQL_DESC_UNNAMED: {
			// is AD
			if (!handle_desc->IsAD()) {
				handle_desc->error_messages.emplace_back("Invalid descriptor field identifier");
				return SQL_ERROR;
			}
			*(SQLSMALLINT *)value_ptr = handle_desc->records[rec_idx].sql_desc_unnamed;
			return SQL_SUCCESS;
		}
		case SQL_DESC_UNSIGNED: {
			// is AD
			if (!handle_desc->IsAD()) {
				handle_desc->error_messages.emplace_back("Invalid descriptor field identifier");
				return SQL_ERROR;
			}
			*(SQLSMALLINT *)value_ptr = handle_desc->records[rec_idx].sql_desc_unsigned;
			return SQL_SUCCESS;
		}
		case SQL_DESC_UPDATABLE: {
			// not IRD
			if (!handle_desc->IsIRD()) {
				handle_desc->error_messages.emplace_back("Invalid descriptor field identifier");
				return SQL_ERROR;
			}
			*(SQLSMALLINT *)value_ptr = handle_desc->records[rec_idx].sql_desc_updatable;
			return SQL_SUCCESS;
		}
		default:
			handle_desc->error_messages.emplace_back("Invalid descriptor field identifier.");
			return SQL_ERROR;
		}
		return SQL_ERROR;
	});
}

SQLRETURN SQL_API SQLSetDescField(SQLHDESC descriptor_handle, SQLSMALLINT rec_number, SQLSMALLINT field_identifier,
                                  SQLPOINTER value_ptr, SQLINTEGER buffer_length) {
	return duckdb::WithDescriptor(descriptor_handle, [&](duckdb::OdbcHandleDesc *handle_desc) {
		// descriptor header fields
		switch (field_identifier) {
		case SQL_DESC_ALLOC_TYPE: {
			handle_desc->error_messages.emplace_back("Invalid descriptor field identifier (read-only field)");
			return SQL_ERROR;
		}
		case SQL_DESC_ARRAY_SIZE: {
			if (handle_desc->IsID()) {
				handle_desc->error_messages.emplace_back("Invalid descriptor field identifier");
				return SQL_ERROR;
			}
			handle_desc->header.sql_desc_array_size = *(SQLULEN *)value_ptr;
			return SQL_SUCCESS;
		}
		case SQL_DESC_ARRAY_STATUS_PTR: {
			handle_desc->header.sql_desc_array_status_ptr = (SQLUSMALLINT *)value_ptr;
			return SQL_SUCCESS;
		}
		case SQL_DESC_BIND_OFFSET_PTR: {
			if (handle_desc->IsID()) {
				handle_desc->error_messages.emplace_back("Invalid descriptor field identifier");
				return SQL_ERROR;
			}
			handle_desc->header.sql_desc_bind_offset_ptr = (SQLLEN *)value_ptr;
			return SQL_SUCCESS;
		}
		case SQL_DESC_BIND_TYPE: {
			if (handle_desc->IsID()) {
				handle_desc->error_messages.emplace_back("Invalid descriptor field identifier");
				return SQL_ERROR;
			}
			handle_desc->header.sql_desc_bind_type = *(SQLINTEGER *)value_ptr;
			return SQL_SUCCESS;
		}
		case SQL_DESC_COUNT: {
			if (handle_desc->IsIRD()) {
				handle_desc->error_messages.emplace_back("Invalid descriptor field identifier");
				return SQL_ERROR;
			}
			handle_desc->header.sql_desc_count = *(SQLSMALLINT *)value_ptr;
			return SQL_SUCCESS;
		}
		case SQL_DESC_ROWS_PROCESSED_PTR: {
			if (handle_desc->IsAD()) {
				handle_desc->error_messages.emplace_back("Invalid descriptor field identifier");
				return SQL_ERROR;
			}
			if (*(SQLULEN *)value_ptr < 0) {
				handle_desc->error_messages.emplace_back("Invalid descriptor index");
				return SQL_ERROR;
			}
			handle_desc->header.sql_desc_rows_processed_ptr = (SQLULEN *)value_ptr;
			return SQL_SUCCESS;
		}
		default:
			break;
		}

		if (rec_number <= 0 || rec_number > (SQLSMALLINT)handle_desc->records.size()) {
			handle_desc->error_messages.emplace_back("Invalid descriptor index");
			return SQL_ERROR;
		}
		duckdb::idx_t rec_idx = rec_number - 1;

		// checking descriptor record fields
		switch (field_identifier) {
		case SQL_DESC_AUTO_UNIQUE_VALUE:
		case SQL_DESC_BASE_COLUMN_NAME:
		case SQL_DESC_BASE_TABLE_NAME:
		case SQL_DESC_CASE_SENSITIVE:
		case SQL_DESC_CATALOG_NAME:
		case SQL_DESC_DISPLAY_SIZE:
		case SQL_DESC_FIXED_PREC_SCALE:
		case SQL_DESC_LABEL:
		case SQL_DESC_LITERAL_PREFIX:
		case SQL_DESC_LITERAL_SUFFIX:
		case SQL_DESC_LOCAL_TYPE_NAME:
		case SQL_DESC_NULLABLE:
		case SQL_DESC_ROWVER:
		case SQL_DESC_SCHEMA_NAME:
		case SQL_DESC_SEARCHABLE:
		case SQL_DESC_TABLE_NAME:
		case SQL_DESC_TYPE_NAME:
		case SQL_DESC_UNSIGNED:
		case SQL_DESC_UPDATABLE: {
			handle_desc->error_messages.emplace_back("HY091: Invalid descriptor field identifier (read-only field)");
			return SQL_ERROR;
		}
		case SQL_DESC_CONCISE_TYPE: {
			if (handle_desc->IsIRD()) {
				handle_desc->error_messages.emplace_back(
				    "HY091: Invalid descriptor field identifier (read-only field)");
				return SQL_ERROR;
			}
			handle_desc->records[rec_idx].sql_desc_concise_type = *(SQLSMALLINT *)value_ptr;
			return SQL_SUCCESS;
		}
		case SQL_DESC_DATA_PTR: {
			if (handle_desc->IsID()) {
				handle_desc->error_messages.emplace_back(
				    "HY091: Invalid descriptor field identifier (read-only field)");
				return SQL_ERROR;
			}
			handle_desc->records[rec_idx].sql_desc_data_ptr = value_ptr;
			return SQL_SUCCESS;
		}
		case SQL_DESC_DATETIME_INTERVAL_CODE: {
			if (handle_desc->IsIRD()) {
				handle_desc->error_messages.emplace_back(
				    "HY091: Invalid descriptor field identifier (read-only field)");
				return SQL_ERROR;
			}
			handle_desc->records[rec_idx].sql_desc_datetime_interval_code = *(SQLSMALLINT *)value_ptr;
			return SQL_SUCCESS;
		}
		case SQL_DESC_DATETIME_INTERVAL_PRECISION: {
			if (handle_desc->IsIRD()) {
				handle_desc->error_messages.emplace_back(
				    "HY091: Invalid descriptor field identifier (read-only field)");
				return SQL_ERROR;
			}
			handle_desc->records[rec_idx].sql_desc_datetime_interval_precision = *(SQLINTEGER *)value_ptr;
			return SQL_SUCCESS;
		}
		case SQL_DESC_INDICATOR_PTR: {
			if (handle_desc->IsID()) {
				handle_desc->error_messages.emplace_back(
				    "HY091: Invalid descriptor field identifier (read-only field)");
				return SQL_ERROR;
			}
			handle_desc->records[rec_idx].sql_desc_indicator_ptr = (SQLLEN *)value_ptr;
			return SQL_SUCCESS;
		}
		case SQL_DESC_LENGTH: {
			if (handle_desc->IsIRD()) {
				handle_desc->error_messages.emplace_back(
				    "HY091: Invalid descriptor field identifier (read-only field)");
				return SQL_ERROR;
			}
			handle_desc->records[rec_idx].sql_desc_length = *(SQLULEN *)value_ptr;
			return SQL_SUCCESS;
		}
		case SQL_DESC_NAME: {
			if (!handle_desc->IsIPD()) {
				handle_desc->error_messages.emplace_back(
				    "HY091: Invalid descriptor field identifier (read-only field)");
				return SQL_ERROR;
			}
			handle_desc->records[rec_idx].sql_desc_name = duckdb::OdbcUtils::ReadString(value_ptr, buffer_length);
			return SQL_SUCCESS;
		}
		case SQL_DESC_NUM_PREC_RADIX: {
			if (handle_desc->IsIRD()) {
				handle_desc->error_messages.emplace_back(
				    "HY091: Invalid descriptor field identifier (read-only field)");
				return SQL_ERROR;
			}
			handle_desc->records[rec_idx].sql_desc_num_prec_radix = *(SQLINTEGER *)value_ptr;
			return SQL_SUCCESS;
		}
		case SQL_DESC_OCTET_LENGTH: {
			if (handle_desc->IsIRD()) {
				handle_desc->error_messages.emplace_back(
				    "HY091: Invalid descriptor field identifier (read-only field)");
				return SQL_ERROR;
			}
			handle_desc->records[rec_idx].sql_desc_octet_length = *(SQLLEN *)value_ptr;
			return SQL_SUCCESS;
		}
		case SQL_DESC_OCTET_LENGTH_PTR: {
			if (handle_desc->IsID()) {
				handle_desc->error_messages.emplace_back(
				    "HY091: Invalid descriptor field identifier (read-only field)");
				return SQL_ERROR;
			}
			handle_desc->records[rec_idx].sql_desc_octet_length_ptr = (SQLLEN *)value_ptr;
			return SQL_SUCCESS;
		}
		case SQL_DESC_PARAMETER_TYPE: {
			if (!handle_desc->IsIPD()) {
				handle_desc->error_messages.emplace_back(
				    "HY091: Invalid descriptor field identifier (read-only field)");
				return SQL_ERROR;
			}
			handle_desc->records[rec_idx].sql_desc_parameter_type = *(SQLSMALLINT *)value_ptr;
			return SQL_SUCCESS;
		}
		case SQL_DESC_PRECISION: {
			if (handle_desc->IsIRD()) {
				handle_desc->error_messages.emplace_back(
				    "HY091: Invalid descriptor field identifier (read-only field)");
				return SQL_ERROR;
			}
			handle_desc->records[rec_idx].sql_desc_precision = *(SQLSMALLINT *)value_ptr;
			return SQL_SUCCESS;
		}
		case SQL_DESC_SCALE: {
			if (handle_desc->IsIRD()) {
				handle_desc->error_messages.emplace_back(
				    "HY091: Invalid descriptor field identifier (read-only field)");
				return SQL_ERROR;
			}
			handle_desc->records[rec_idx].sql_desc_scale = *(SQLSMALLINT *)value_ptr;
			return SQL_SUCCESS;
		}
		case SQL_DESC_TYPE: {
			if (handle_desc->IsIRD()) {
				handle_desc->error_messages.emplace_back(
				    "HY091: Invalid descriptor field identifier (read-only field)");
				return SQL_ERROR;
			}
			handle_desc->records[rec_idx].sql_desc_type = *(SQLSMALLINT *)value_ptr;
			return SQL_SUCCESS;
		}
		case SQL_DESC_UNNAMED: {
			if (!handle_desc->IsIPD()) {
				handle_desc->error_messages.emplace_back(
				    "HY091: Invalid descriptor field identifier (read-only field)");
				return SQL_ERROR;
			}
			handle_desc->records[rec_idx].sql_desc_unnamed = *(SQLSMALLINT *)value_ptr;
			return SQL_SUCCESS;
		}
		default:
			handle_desc->error_messages.emplace_back("Invalid descriptor field identifier.");
			return SQL_ERROR;
		}
	});
}

SQLRETURN SQL_API SQLCopyDesc(SQLHDESC source_desc_handle, SQLHDESC target_desc_handle) {
	if (source_desc_handle == nullptr || target_desc_handle == nullptr) {
		return SQL_INVALID_HANDLE;
	}
	auto source_desc = (OdbcHandleDesc *)source_desc_handle;
	auto target_desc = (OdbcHandleDesc *)target_desc_handle;
	// Check if source descriptor is an APD, ARD
	for (auto stmt : source_desc->dbc->vec_stmt_ref) {
		if (target_desc == stmt->row_desc->ird.get()) {
			target_desc->error_messages.emplace_back("Cannot modify an implementation row descriptor.");
			return SQL_ERROR;
		}
		if (source_desc == stmt->row_desc->ird.get()) {
			if (!stmt->IsPrepared()) {
				source_desc->error_messages.emplace_back("Associated statement is not prepared.");
				return SQL_ERROR;
			}
		}
	}

	// descriptors are associated with the same connection
	if (source_desc->dbc == target_desc->dbc) {
		// copy assignment operator
		target_desc = source_desc;
	} else {
		// descriptors are associated with the same enviroment
		if (source_desc->dbc->env == target_desc->dbc->env) {
			target_desc->CopyOnlyOdbcFields(*source_desc);
		} else {
			// descriptor connections belong to separate drivers
			target_desc->CopyFieldByField(*source_desc);
		}
	}
	return SQL_SUCCESS;
}

//! OdbcHandleDesc functions ********************************

OdbcHandleDesc::OdbcHandleDesc(const OdbcHandleDesc &other) : OdbcHandle(other) {
	// calling copy assigment operator
	*this = other;
}

OdbcHandleDesc &OdbcHandleDesc::operator=(const OdbcHandleDesc &other) {
	if (&other != this) {
		OdbcHandle::operator=(other);
		dbc = other.dbc;
		CopyOnlyOdbcFields(other);
	}
	return *this;
}

void OdbcHandleDesc::CopyOnlyOdbcFields(const OdbcHandleDesc &other) {
	header = other.header;
	std::copy(other.records.begin(), other.records.end(), std::back_inserter(records));
}
void OdbcHandleDesc::CopyFieldByField(const OdbcHandleDesc &other) {
	// TODO
}

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
			// TODO throw exception
			error_messages.emplace_back("Invalid attribute/option identifier.");
			return SQL_ERROR;
		}
		header.sql_desc_array_size = size;
		return SQL_SUCCESS;
	}
	default:
		break;
	}
	return SQL_ERROR;
}

void OdbcHandleDesc::Clear() {
	Reset();
}

void OdbcHandleDesc::Reset() {
	records.clear();
}

bool OdbcHandleDesc::IsID() {
	return stmt != nullptr;
}

bool OdbcHandleDesc::IsAD() {
	return stmt == nullptr;
}

bool OdbcHandleDesc::IsIRD() {
	return (IsID() && stmt->row_desc->GetIRD() == this);
}

bool OdbcHandleDesc::IsIPD() {
	return (IsID() && stmt->param_desc->GetIPD() == this);
}

//! DescRecord functions ******************************************************

// Copy constructor
DescRecord::DescRecord(const DescRecord &other) {
	sql_desc_auto_unique_value = other.sql_desc_auto_unique_value;
	sql_desc_base_column_name = other.sql_desc_base_column_name;
	sql_desc_base_table_name = other.sql_desc_base_table_name;
	sql_desc_case_sensitive = other.sql_desc_case_sensitive;
	sql_desc_catalog_name = other.sql_desc_catalog_name;
	sql_desc_concise_type = other.sql_desc_concise_type;
	sql_desc_data_ptr = other.sql_desc_data_ptr;
	sql_desc_datetime_interval_code = other.sql_desc_datetime_interval_code;
	sql_desc_datetime_interval_precision = other.sql_desc_datetime_interval_precision;
	sql_desc_display_size = other.sql_desc_display_size;
	sql_desc_fixed_prec_scale = other.sql_desc_fixed_prec_scale;
	sql_desc_indicator_ptr = other.sql_desc_indicator_ptr;
	sql_desc_label = other.sql_desc_label;
	sql_desc_length = other.sql_desc_length;
	sql_desc_literal_prefix = other.sql_desc_literal_prefix;
	sql_desc_literal_suffix = other.sql_desc_literal_suffix;
	sql_desc_local_type_name = other.sql_desc_local_type_name;
	sql_desc_name = other.sql_desc_name;
	sql_desc_nullable = other.sql_desc_nullable;
	sql_desc_num_prec_radix = other.sql_desc_num_prec_radix;
	sql_desc_octet_length = other.sql_desc_octet_length;
	sql_desc_octet_length_ptr = other.sql_desc_octet_length_ptr;
	sql_desc_parameter_type = other.sql_desc_parameter_type;
	sql_desc_precision = other.sql_desc_precision;
	sql_desc_rowver = other.sql_desc_rowver;
	sql_desc_scale = other.sql_desc_scale;
	sql_desc_schema_name = other.sql_desc_schema_name;
	sql_desc_searchable = other.sql_desc_searchable;
	sql_desc_table_name = other.sql_desc_table_name;
	sql_desc_type = other.sql_desc_type;
	sql_desc_type_name = other.sql_desc_type_name;
	sql_desc_unnamed = other.sql_desc_unnamed;
	sql_desc_unsigned = other.sql_desc_unsigned;
	sql_desc_updatable = other.sql_desc_updatable;
}

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

DescHeader::DescHeader(const DescHeader &other) {
	//	sql_desc_alloc_type = other.sql_desc_alloc_type; this can't be copied
	sql_desc_array_size = other.sql_desc_array_size;
	sql_desc_array_status_ptr = other.sql_desc_array_status_ptr;
	sql_desc_bind_offset_ptr = other.sql_desc_bind_offset_ptr;
	sql_desc_bind_type = other.sql_desc_bind_type;
	sql_desc_count = other.sql_desc_count;
	sql_desc_rows_processed_ptr = other.sql_desc_rows_processed_ptr;
}

void DescHeader::Reset() {
	sql_desc_array_size = 1;
	sql_desc_array_status_ptr = nullptr;
	sql_desc_count = 0;
	sql_desc_rows_processed_ptr = nullptr;
}
