#include "duckdb/parser/statement/execute_statement.hpp"
#include "duckdb/parser/peg/transformer/peg_transformer.hpp"

namespace duckdb {

unique_ptr<SQLStatement>
PEGTransformerFactory::TransformExecuteStatement(PEGTransformer &transformer, const Identifier &identifier,
                                                 vector<FunctionArgument> table_function_arguments) {
	auto result = make_uniq<ExecuteStatement>();
	result->name = identifier;
	if (table_function_arguments.empty()) {
		return std::move(result);
	}
	idx_t param_idx = 0;
	for (idx_t i = 0; i < table_function_arguments.size(); i++) {
		auto &arg = table_function_arguments[i];
		if (!table_function_arguments[i].GetExpression().IsScalar()) {
			throw InvalidInputException("Only scalar parameters, named parameters or NULL supported for EXECUTE");
		}
		if (!table_function_arguments[i].GetName().empty() && param_idx != 0) {
			throw NotImplementedException("Mixing named parameters and positional parameters is not supported yet");
		}
		auto param_name = arg.GetName();
		if (table_function_arguments[i].GetName().empty()) {
			param_name = Identifier(std::to_string(param_idx + 1));
			if (param_idx != i) {
				throw NotImplementedException("Mixing named parameters and positional parameters is not supported yet");
			}
			param_idx++;
		}
		arg.GetExpressionMutable()->ClearAlias();
		result->named_values[param_name] = std::move(arg.GetExpressionMutable());
	}
	return std::move(result);
}
} // namespace duckdb
