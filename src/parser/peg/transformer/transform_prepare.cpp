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

unique_ptr<SQLStatement> PEGTransformerFactory::TransformPrepareStatement(PEGTransformer &transformer,
                                                                          const Identifier &identifier,
                                                                          const vector<LogicalType> &type_list,
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

} // namespace duckdb
