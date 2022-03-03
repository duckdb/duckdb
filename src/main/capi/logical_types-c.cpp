#include "duckdb/main/capi_internal.hpp"

duckdb_logical_type duckdb_create_logical_type(duckdb_type type) {
	return new duckdb::LogicalType(duckdb::ConvertCTypeToCPP(type));
}

duckdb_type duckdb_get_type_id(duckdb_logical_type type) {
	if (!type) {
		return DUCKDB_TYPE_INVALID;
	}
	auto ltype = (duckdb::LogicalType *)type;
	;
	return duckdb::ConvertCPPTypeToC(ltype->id());
}

void duckdb_destroy_logical_type(duckdb_logical_type *type) {
	if (type && *type) {
		auto ltype = (duckdb::LogicalType *)*type;
		;
		delete ltype;
		*type = nullptr;
	}
}
