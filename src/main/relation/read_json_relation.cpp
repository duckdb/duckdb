#include "duckdb/main/relation/read_json_relation.hpp"
#include "duckdb/parser/column_definition.hpp"
namespace duckdb {

ReadJSONRelation::ReadJSONRelation(const shared_ptr<ClientContext> &context, string json_file_p,
                                   named_parameter_map_t options, bool auto_detect, string alias_p)
    : TableFunctionRelation(context, auto_detect ? "read_json_auto" : "read_json", {Value(json_file_p)},
                            std::move(options)),
      json_file(std::move(json_file_p)), alias(std::move(alias_p)) {

	if (alias.empty()) {
		alias = StringUtil::Split(json_file, ".")[0];
	}
}

string ReadJSONRelation::GetAlias() {
	return alias;
}

} // namespace duckdb
