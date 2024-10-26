#include "duckdb/main/relation/read_json_relation.hpp"
#include "duckdb/parser/column_definition.hpp"
#include "duckdb/common/multi_file_reader.hpp"

namespace duckdb {

void ReadJSONRelation::InitializeAlias(const vector<string> &input) {
	D_ASSERT(!input.empty());
	const auto &first_file = input[0];
	alias = StringUtil::Split(first_file, ".")[0];
}

ReadJSONRelation::ReadJSONRelation(const shared_ptr<ClientContext> &context, vector<string> &input,
                                   named_parameter_map_t options, bool auto_detect, string alias_p)
    : TableFunctionRelation(context, auto_detect ? "read_json_auto" : "read_json",
                            {MultiFileReader::CreateValueFromFileList(input)}, std::move(options)),
      alias(std::move(alias_p)) {

	InitializeAlias(input);
}

ReadJSONRelation::ReadJSONRelation(const shared_ptr<ClientContext> &context, string json_file_p,
                                   named_parameter_map_t options, bool auto_detect, string alias_p)
    : TableFunctionRelation(context, auto_detect ? "read_json_auto" : "read_json", {Value(json_file_p)},
                            std::move(options)),
      json_file(std::move(json_file_p)), alias(std::move(alias_p)) {

	if (alias.empty()) {
		alias = StringUtil::Split(json_file, ".")[0];
	}
}

ReadJSONRelation::~ReadJSONRelation() {
}

string ReadJSONRelation::GetAlias() {
	return alias;
}

} // namespace duckdb
