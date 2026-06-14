#include "duckdb/parser/peg/transformer/peg_transformer.hpp"
#include "duckdb/parser/statement/explain_statement.hpp"
#include "duckdb/parser/expression/columnref_expression.hpp"
#include "duckdb/parser/expression/constant_expression.hpp"

namespace duckdb {

ProfilerPrintFormat ParseProfilerPrintFormat(const Value &val) {
	if (val.type().id() != LogicalTypeId::VARCHAR) {
		throw InvalidInputException("Expected a string as argument to FORMAT");
	}
	// resolve the format name through the shared explain format registry (see main/profiler/profiler_print_format.hpp)
	return ProfilerPrintFormat::FromString(val.GetValue<string>());
}

unique_ptr<SQLStatement>
PEGTransformerFactory::TransformExplainStatement(PEGTransformer &transformer, const bool &explain_analyze,
                                                 const vector<GenericCopyOption> &explain_option_list,
                                                 unique_ptr<SQLStatement> explainable_statements) {
	auto explain_type = explain_analyze ? ExplainType::EXPLAIN_ANALYZE : ExplainType::EXPLAIN_STANDARD;
	bool format_is_set = false;
	auto format = ProfilerPrintFormat::Default();
	if (!explain_option_list.empty()) {
		for (auto option : explain_option_list) {
			auto option_name = StringUtil::Lower(option.name.GetIdentifierName());
			if (option_name == "format") {
				if (format_is_set) {
					throw InvalidInputException("FORMAT can not be provided more than once");
				}
				format = ParseProfilerPrintFormat(option.children[0]);
				format_is_set = true;
			} else if (option_name == "analyze") {
				explain_type = ExplainType::EXPLAIN_ANALYZE;
			} else {
				throw NotImplementedException("Unimplemented explain type: %s", option_name);
			}
		}
	}
	auto statement = std::move(explainable_statements);
	return make_uniq<ExplainStatement>(std::move(statement), explain_type, format);
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
