#include "duckdb/parser/statement/execute_statement.hpp"
#include "transformer/peg_transformer.hpp"

namespace duckdb {

unique_ptr<SQLStatement> PEGTransformerFactory::TransformExecuteStatement(PEGTransformer &transformer,
                                                                          optional_ptr<ParseResult> parse_result) {
	auto &list_pr = parse_result->Cast<ListParseResult>();
	auto result = make_uniq<ExecuteStatement>();
	result->name = list_pr.Child<IdentifierParseResult>(1).identifier;
	auto table_function_opt = list_pr.Child<OptionalParseResult>(2);
	if (table_function_opt.HasResult()) {
		idx_t param_idx = 0;
		auto table_function_arguments =
		    transformer.Transform<vector<unique_ptr<ParsedExpression>>>(table_function_opt.optional_result);
		for (idx_t i = 0; i < table_function_arguments.size(); i++) {
			auto &expr = table_function_arguments[i];
			if (!table_function_arguments[i]->IsScalar()) {
				throw InvalidInputException("Only scalar parameters, named parameters or NULL supported for EXECUTE");
			}
			if (!table_function_arguments[i]->GetAlias().empty() && param_idx != 0) {
				throw NotImplementedException("Mixing named parameters and positional parameters is not supported yet");
			}
			auto param_name = expr->GetAlias();
			if (table_function_arguments[i]->GetAlias().empty()) {
				param_name = std::to_string(param_idx + 1);
				if (param_idx != i) {
					throw NotImplementedException(
					    "Mixing named parameters and positional parameters is not supported yet");
				}
				param_idx++;
			}
			expr->ClearAlias();
			result->named_values[param_name] = std::move(expr);
		}
	}
	return std::move(result);
}
} // namespace duckdb
