#include "duckdb/parser/statement/passthrough_statement.hpp"

#include "duckdb/common/string_util.hpp"

namespace duckdb {

PassthroughStatement::PassthroughStatement(string target_p, string payload_p)
    : SQLStatement(StatementType::PASSTHROUGH_STATEMENT), target(std::move(target_p)), payload(std::move(payload_p)) {
}

PassthroughStatement::PassthroughStatement(unique_ptr<SQLStatement> local_statement_p)
    : SQLStatement(StatementType::PASSTHROUGH_STATEMENT), target_is_local(true),
      local_statement(std::move(local_statement_p)) {
}

PassthroughStatement::PassthroughStatement(const PassthroughStatement &other)
    : SQLStatement(other), target(other.target), payload(other.payload), target_is_local(other.target_is_local),
      local_statement(other.local_statement ? other.local_statement->Copy() : nullptr) {
}

unique_ptr<SQLStatement> PassthroughStatement::Copy() const {
	return unique_ptr<SQLStatement>(new PassthroughStatement(*this));
}

string PassthroughStatement::ToString() const {
	if (target_is_local) {
		return "CONNECT LOCAL EXECUTE " + (local_statement ? local_statement->ToString() : string());
	}
	return StringUtil::Format("CONNECT %s EXECUTE %s", target, payload);
}

} // namespace duckdb
