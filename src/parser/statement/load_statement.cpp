#include "duckdb/parser/statement/load_statement.hpp"

namespace duckdb {

LoadStatement::LoadStatement(std::string file_p) : SQLStatement(StatementType::LOAD_STATEMENT), file(move(file_p)) {
}

unique_ptr<SQLStatement> LoadStatement::Copy() const {
	return make_unique<LoadStatement>(file);
}

} // namespace duckdb
