#include "duckdb/parser/parsed_data/drop_info.hpp"
#include "duckdb/parser/parsed_data/extra_drop_info.hpp"

namespace duckdb {

DropInfo::DropInfo() : ParseInfo(TYPE), catalog(INVALID_CATALOG), schema(INVALID_SCHEMA), cascade(false) {
}

DropInfo::DropInfo(const DropInfo &info)
    : ParseInfo(info.info_type), type(info.type), catalog(info.catalog), schema(info.schema), name(info.name),
      if_not_found(info.if_not_found), cascade(info.cascade), allow_drop_internal(info.allow_drop_internal),
      extra_drop_info(info.extra_drop_info ? info.extra_drop_info->Copy() : nullptr) {
}

unique_ptr<DropInfo> DropInfo::Copy() const {
	return make_uniq<DropInfo>(*this);
}

string DropInfo::ToString() const {
	string result = "";
	if (type == CatalogType::PREPARED_STATEMENT) {
		result += "DEALLOCATE PREPARE ";
		result += KeywordHelper::WriteOptionallyQuoted(name);
	} else {
		result += "DROP";
		result += " " + ParseInfo::TypeToString(type);
		if (if_not_found == OnEntryNotFound::RETURN_NULL) {
			result += " IF EXISTS";
		}
		result += " ";
		result += QualifierToString(catalog, schema, name);
		if (cascade) {
			result += " CASCADE";
		}
	}
	result += ";";
	return result;
}

} // namespace duckdb
