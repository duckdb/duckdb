#include "duckdb/parser/parsed_data/create_info.hpp"

#include "duckdb/parser/parsed_data/create_index_info.hpp"
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
	other.dependencies = dependencies;
	other.comment = comment;
	other.tags = tags;
}

unique_ptr<AlterInfo> CreateInfo::GetAlterInfo() const {
	throw NotImplementedException("GetAlterInfo not implemented for this type");
}

string CreateInfo::GetCreatePrefix(const string &entry) const {
	string prefix = "CREATE";
	if (on_conflict == OnCreateConflict::REPLACE_ON_CONFLICT) {
		prefix += " OR REPLACE";
	}
	if (temporary) {
		prefix += " TEMP";
	}
	prefix += " " + entry + " ";

	if (on_conflict == OnCreateConflict::IGNORE_ON_CONFLICT) {
		prefix += " IF NOT EXISTS ";
	}
	return prefix;
}

} // namespace duckdb
