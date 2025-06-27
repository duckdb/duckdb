#include "duckdb/parser/statement/merge_into_statement.hpp"

namespace duckdb {

MergeIntoStatement::MergeIntoStatement() : SQLStatement(StatementType::MERGE_INTO_STATEMENT) {
}

MergeIntoStatement::MergeIntoStatement(const MergeIntoStatement &other) : SQLStatement(other) {
	throw InternalException("FIXME Copy");
}

string MergeIntoStatement::ToString() const {
	throw InternalException("FIXME ToString");
}

unique_ptr<SQLStatement> MergeIntoStatement::Copy() const {
	return unique_ptr<MergeIntoStatement>(new MergeIntoStatement(*this));
}

} // namespace duckdb
