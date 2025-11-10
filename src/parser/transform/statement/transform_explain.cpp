#include "duckdb/parser/statement/explain_statement.hpp"
#include "duckdb/parser/transformer.hpp"
#include "duckdb/parser/expression/constant_expression.hpp"

namespace duckdb {

ExplainFormat ParseFormat(const Value &val) {
	if (val.type().id() != LogicalTypeId::VARCHAR) {
		throw InvalidInputException("Expected a string as argument to FORMAT");
	}
	auto format_val = val.GetValue<string>();
	case_insensitive_map_t<ExplainFormat> format_mapping {
	    {"default", ExplainFormat::DEFAULT}, {"text", ExplainFormat::TEXT},         {"json", ExplainFormat::JSON},
	    {"html", ExplainFormat::HTML},       {"graphviz", ExplainFormat::GRAPHVIZ}, {"yaml", ExplainFormat::YAML},
	    {"mermaid", ExplainFormat::MERMAID}};
	auto it = format_mapping.find(format_val);
	if (it != format_mapping.end()) {
		return it->second;
	}
	vector<string> options_list;
	for (auto &it : format_mapping) {
		options_list.push_back(it.first);
	}
	auto allowed_options = StringUtil::Join(options_list, ", ");
	throw InvalidInputException("\"%s\" is not a valid FORMAT argument, valid options are: %s", format_val,
	                            allowed_options);
}

unique_ptr<ExplainStatement> Transformer::TransformExplain(duckdb_libpgquery::PGExplainStmt &stmt) {
	auto explain_type = ExplainType::EXPLAIN_STANDARD;
	auto explain_format = ExplainFormat::DEFAULT;
	bool format_is_set = false;
	if (stmt.options) {
		for (auto n = stmt.options->head; n; n = n->next) {
			auto def_elem = PGPointerCast<duckdb_libpgquery::PGDefElem>(n->data.ptr_value);
			auto def_name = def_elem->defname;
			auto elem = StringUtil::Lower(def_name);
			if (elem == "analyze") {
				explain_type = ExplainType::EXPLAIN_ANALYZE;
			} else if (elem == "format") {
				if (def_elem->arg) {
					if (format_is_set) {
						throw InvalidInputException("FORMAT can not be provided more than once");
					}
					auto val = TransformValue(*PGPointerCast<duckdb_libpgquery::PGValue>(def_elem->arg))->value;
					format_is_set = true;
					explain_format = ParseFormat(val);
				}
			} else {
				throw NotImplementedException("Unimplemented explain type: %s", elem);
			}
		}
	}
	return make_uniq<ExplainStatement>(TransformStatement(*stmt.query), explain_type, explain_format);
}

} // namespace duckdb
