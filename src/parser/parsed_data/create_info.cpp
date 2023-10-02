#include "duckdb/parser/parsed_data/create_info.hpp"

#include "duckdb/parser/parsed_data/create_index_info.hpp"
#include "duckdb/parser/parsed_data/create_schema_info.hpp"
#include "duckdb/parser/parsed_data/create_table_info.hpp"
#include "duckdb/parser/parsed_data/create_view_info.hpp"
#include "duckdb/parser/parsed_data/create_sequence_info.hpp"
#include "duckdb/parser/parsed_data/create_type_info.hpp"
#include "duckdb/parser/parsed_data/alter_info.hpp"
#include "duckdb/parser/parsed_data/create_macro_info.hpp"

namespace duckdb {

void CreateInfo::CopyProperties(CreateInfo &other) const {
	other.type = type;
	other.catalog = catalog;
	other.schema = schema;
	other.on_conflict = on_conflict;
	other.temporary = temporary;
	other.internal = internal;
	other.sql = sql;
}

unique_ptr<AlterInfo> CreateInfo::GetAlterInfo() const {
	throw NotImplementedException("GetAlterInfo not implemented for this type");
}

} // namespace duckdb
