#include "duckdb/parser/peg/transformer/peg_transformer.hpp"
#include "duckdb/parser/statement/explain_statement.hpp"
#include "duckdb/parser/expression/columnref_expression.hpp"
#include "duckdb/parser/expression/constant_expression.hpp"

namespace duckdb {

ExplainFormat ParseExplainFormat(const Value &val) {
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
	for (auto &format : format_mapping) {
		options_list.push_back(format.first);
	}
	auto allowed_options = StringUtil::Join(options_list, ", ");
	throw InvalidInputException("\"%s\" is not a valid FORMAT argument, valid options are: %s", format_val,
	                            allowed_options);
}

unique_ptr<SQLStatement>
PEGTransformerFactory::TransformExplainStatement(PEGTransformer &transformer, const bool &explain_analyze,
                                                 const vector<GenericCopyOption> &explain_option_list,
                                                 unique_ptr<SQLStatement> explainable_statements) {
	auto explain_type = explain_analyze ? ExplainType::EXPLAIN_ANALYZE : ExplainType::EXPLAIN_STANDARD;
	bool format_is_set = false;
	auto explain_format = ExplainFormat::DEFAULT;
	if (!explain_option_list.empty()) {
		for (auto option : explain_option_list) {
			auto option_name = StringUtil::Lower(option.name.GetIdentifierName());
			if (option_name == "format") {
				if (format_is_set) {
					throw InvalidInputException("FORMAT can not be provided more than once");
				}
				explain_format = ParseExplainFormat(option.children[0]);
				format_is_set = true;
			} else if (option_name == "analyze") {
				explain_type = ExplainType::EXPLAIN_ANALYZE;
			} else {
				throw NotImplementedException("Unimplemented explain type: %s", option_name);
			}
		}
	}
	auto statement = std::move(explainable_statements);
	return make_uniq<ExplainStatement>(std::move(statement), explain_type, explain_format);
}

bool PEGTransformerFactory::TransformExplainAnalyze(PEGTransformer &transformer) {
	return true;
}

unique_ptr<SQLStatement>
PEGTransformerFactory::TransformExplainSelectStatement(PEGTransformer &transformer,
                                                       unique_ptr<SelectStatement> select_statement_internal) {
	return std::move(select_statement_internal);
}

vector<GenericCopyOption>
PEGTransformerFactory::TransformExplainOptionList(PEGTransformer &transformer,
                                                  const vector<GenericCopyOption> &explain_option) {
	return explain_option;
}

GenericCopyOption PEGTransformerFactory::TransformExplainOption(PEGTransformer &transformer,
                                                                const Identifier &explain_option_name,
                                                                unique_ptr<ParsedExpression> expression) {
	GenericCopyOption copy_option;
	copy_option.name = Identifier(StringUtil::Lower(explain_option_name.GetIdentifierName()));
	if (!expression) {
		return copy_option;
	}
	if (expression->GetExpressionType() == ExpressionType::VALUE_CONSTANT) {
		copy_option.children.push_back(Value(expression->Cast<ConstantExpression>().GetValue()));
	} else if (expression->GetExpressionType() == ExpressionType::COLUMN_REF) {
		copy_option.children.push_back(Value(expression->Cast<ColumnRefExpression>().GetColumnName()));
	} else {
		copy_option.expression = std::move(expression);
	}
	return copy_option;
}

} // namespace duckdb
