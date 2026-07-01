#include "duckdb/parser/statement/prepare_statement.hpp"
#include "duckdb/parser/peg/transformer/peg_transformer.hpp"

namespace duckdb {

bool IsPrepareableStatement(StatementType type) {
	switch (type) {
	case StatementType::SELECT_STATEMENT:
	case StatementType::INSERT_STATEMENT:
	case StatementType::UPDATE_STATEMENT:
	case StatementType::COPY_STATEMENT:
	case StatementType::DELETE_STATEMENT:
		return true;
	default:
		return false;
	}
}

unique_ptr<SQLStatement>
PEGTransformerFactory::TransformPrepareStatement(PEGTransformer &transformer, const Identifier &identifier,
                                                 const optional<vector<LogicalType>> &type_list,
                                                 unique_ptr<SQLStatement> statement) {
	auto result = make_uniq<PrepareStatement>();
	result->name = identifier;
	if (!IsPrepareableStatement(statement->type)) {
		throw ParserException("%s is not a preparable statement", EnumUtil::ToString(statement->type));
	}
	result->statement = std::move(statement);
	transformer.ClearParameters();
	return std::move(result);
}

vector<LogicalType> PEGTransformerFactory::TransformTypeList(PEGTransformer &transformer,
                                                             const vector<LogicalType> &type) {
	throw NotImplementedException("TypeList for prepared statement has not been implemented.");
}

void PEGTransformerFactory::InitializePrepareStatementTrampoline(PEGTransformer &transformer, TransformStack &stack,
                                                                 TransformStackFrame &frame) {
	frame.ReserveChildSlots(0);
}

unique_ptr<TransformResultValue> PEGTransformerFactory::FinalizePrepareStatementTrampoline(PEGTransformer &transformer,
                                                                                           TransformStack &stack,
                                                                                           TransformStackFrame &frame) {
	auto &list_pr = frame.parse_result.Cast<ListParseResult>();
	auto &type_list_opt = list_pr.GetChild(2).Cast<OptionalParseResult>();
	if (type_list_opt.HasResult()) {
		throw NotImplementedException("TypeList for prepared statement has not been implemented.");
	}
	auto identifier = list_pr.GetChild(1).Cast<IdentifierParseResult>().identifier;
	auto statement = TransformStatementTrampoline(transformer, list_pr.GetChild(4));
	auto result =
	    PEGTransformerFactory::TransformPrepareStatement(transformer, identifier, optional<vector<LogicalType>>(),
	                                                     std::move(statement));
	return make_uniq<TypedTransformResult<unique_ptr<SQLStatement>>>(std::move(result));
}

} // namespace duckdb
