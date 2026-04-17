#include "duckdb/parser/parsed_data/drop_info.hpp"
#include "duckdb/parser/parsed_data/extra_drop_info.hpp"
#include "duckdb/parser/tableref/basetableref.hpp"

namespace duckdb {

DropInfo::DropInfo() : ParseInfo(TYPE), catalog(INVALID_CATALOG), schema(INVALID_SCHEMA), cascade(false) {
}

DropInfo::DropInfo(const DropInfo &info)
    : ParseInfo(info.info_type), type(info.type), catalog(info.catalog), schema(info.schema), name(info.name),
      if_not_found(info.if_not_found), cascade(info.cascade), allow_drop_internal(info.allow_drop_internal),
      func_parameters(info.func_parameters), has_func_args(info.has_func_args), is_procedure(info.is_procedure),
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
		if (has_func_args) {
			result += "(";
			for (idx_t i = 0; i < func_parameters.size(); i++) {
				if (i > 0) {
					result += ", ";
				}
				result += func_parameters[i].ToString();
			}
			result += ")";
		}
		if (type == CatalogType::TRIGGER_ENTRY && extra_drop_info) {
			auto &trigger_info = extra_drop_info->Cast<ExtraDropTriggerInfo>();
			if (trigger_info.base_table) {
				result += " ON ";
				result += trigger_info.base_table->Cast<BaseTableRef>().ToString();
			}
		}
		if (cascade) {
			result += " CASCADE";
		}
	}
	result += ";";
	return result;
}

} // namespace duckdb
