#include "duckdb/parser/statement/prepare_statement.hpp"
#include "transformer/peg_transformer.hpp"

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

unique_ptr<SQLStatement> PEGTransformerFactory::TransformPrepareStatement(PEGTransformer &transformer,
                                                                          ParseResult &parse_result) {
	auto &list_pr = parse_result.Cast<ListParseResult>();
	auto result = make_uniq<PrepareStatement>();
	result->name = list_pr.Child<IdentifierParseResult>(1).identifier;
	auto &type_list_opt = list_pr.Child<OptionalParseResult>(2);
	if (type_list_opt.HasResult()) {
		throw NotImplementedException("TypeList for prepared statement has not been implemented.");
	}
	auto stmt = transformer.Transform<unique_ptr<SQLStatement>>(list_pr.Child<ListParseResult>(4));
	if (!IsPrepareableStatement(stmt->type)) {
		throw ParserException("%s is not a preparable statement", EnumUtil::ToString(stmt->type));
	}
	result->statement = std::move(stmt);
	transformer.ClearParameters();
	return std::move(result);
}

} // namespace duckdb
