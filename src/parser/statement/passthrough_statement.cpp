#include "duckdb/parser/statement/passthrough_statement.hpp"

#include "duckdb/common/string_util.hpp"

namespace duckdb {

PassthroughStatement::PassthroughStatement(string target_p, string payload_p)
    : SQLStatement(StatementType::PASSTHROUGH_STATEMENT), target(std::move(target_p)), payload(std::move(payload_p)) {
}

PassthroughStatement::PassthroughStatement(const PassthroughStatement &other)
    : SQLStatement(other), target(other.target), payload(other.payload) {
}

unique_ptr<SQLStatement> PassthroughStatement::Copy() const {
	return unique_ptr<SQLStatement>(new PassthroughStatement(*this));
}

string PassthroughStatement::ToString() const {
	// Renders back to the surface syntax users would have typed (CONNECT <target> EXECUTE <payload>),
	// including the synthetic case where the extract layer rewrote a RAW-while-bound chunk.
	return StringUtil::Format("CONNECT %s EXECUTE %s", target, payload);
}

} // namespace duckdb
