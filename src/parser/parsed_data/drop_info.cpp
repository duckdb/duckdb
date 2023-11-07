#include "duckdb/parser/parsed_data/drop_info.hpp"

namespace duckdb {

DropInfo::DropInfo() : ParseInfo(TYPE), catalog(INVALID_CATALOG), schema(INVALID_SCHEMA), cascade(false) {
}

unique_ptr<DropInfo> DropInfo::Copy() const {
	auto result = make_uniq<DropInfo>();
	result->type = type;
	result->catalog = catalog;
	result->schema = schema;
	result->name = name;
	result->if_not_found = if_not_found;
	result->cascade = cascade;
	result->allow_drop_internal = allow_drop_internal;
	return result;
}

} // namespace duckdb
