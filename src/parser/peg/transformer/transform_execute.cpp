#include "duckdb/parser/statement/execute_statement.hpp"
#include "duckdb/parser/peg/transformer/peg_transformer.hpp"

namespace duckdb {

unique_ptr<SQLStatement>
PEGTransformerFactory::TransformExecuteStatement(PEGTransformer &transformer, const string &identifier,
                                                 vector<unique_ptr<ParsedExpression>> table_function_arguments) {
	auto result = make_uniq<ExecuteStatement>();
	result->name = identifier;
	if (table_function_arguments.empty()) {
		return std::move(result);
	}
	idx_t param_idx = 0;
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
				throw NotImplementedException("Mixing named parameters and positional parameters is not supported yet");
			}
			param_idx++;
		}
		expr->ClearAlias();
		result->named_values[param_name] = std::move(expr);
	}
	return std::move(result);
}
} // namespace duckdb
