#include "duckdb/parser/statement/execute_statement.hpp"
#include "duckdb/parser/peg/transformer/peg_transformer.hpp"

namespace duckdb {

unique_ptr<SQLStatement>
PEGTransformerFactory::TransformExecuteStatement(PEGTransformer &transformer, const Identifier &identifier,
                                                 optional<vector<FunctionArgument>> table_function_arguments) {
	auto result = make_uniq<ExecuteStatement>();
	result->name = identifier;
	if (!table_function_arguments) {
		return std::move(result);
	}
	idx_t param_idx = 0;
	auto &arguments = *table_function_arguments;
	for (idx_t i = 0; i < arguments.size(); i++) {
		auto &arg = arguments[i];
		if (!arguments[i].GetExpression().IsScalar()) {
			throw InvalidInputException("Only scalar parameters, named parameters or NULL supported for EXECUTE");
		}
		if (!arguments[i].GetName().empty() && param_idx != 0) {
			throw NotImplementedException("Mixing named parameters and positional parameters is not supported yet");
		}
		auto param_name = arg.GetName();
		if (arguments[i].GetName().empty()) {
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

void PEGTransformerFactory::InitializeExecuteStatementTrampoline(PEGTransformer &transformer, TransformStack &stack,
                                                                 TransformStackFrame &frame) {
	frame.ReserveChildSlots(0);
}

unique_ptr<TransformResultValue> PEGTransformerFactory::FinalizeExecuteStatementTrampoline(PEGTransformer &transformer,
                                                                                           TransformStack &stack,
                                                                                           TransformStackFrame &frame) {
	auto &list_pr = frame.parse_result.Cast<ListParseResult>();
	auto &table_function_arguments_opt = list_pr.GetChild(2).Cast<OptionalParseResult>();
	if (table_function_arguments_opt.HasResult()) {
		throw NotImplementedException("EXECUTE parameters are not supported by the trampoline transformer yet");
	}
	auto identifier = list_pr.GetChild(1).Cast<IdentifierParseResult>().identifier;
	auto result = TransformExecuteStatement(transformer, identifier, optional<vector<FunctionArgument>>());
	return make_uniq<TypedTransformResult<unique_ptr<SQLStatement>>>(std::move(result));
}
} // namespace duckdb
