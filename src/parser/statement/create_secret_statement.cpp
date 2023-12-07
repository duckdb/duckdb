
#include "duckdb/parser/statement/create_secret_statement.hpp"

namespace duckdb {

CreateSecretStatement::CreateSecretStatement(OnCreateConflict on_conflict, SecretPersistMode persist_mode)
    : SQLStatement(StatementType::CREATE_SECRET_STATEMENT) {
	info = make_uniq<CreateSecretInfo>(on_conflict, persist_mode);
}

CreateSecretStatement::CreateSecretStatement(const CreateSecretStatement &other)
    : SQLStatement(other), info(other.info->Copy()) {
}

unique_ptr<SQLStatement> CreateSecretStatement::Copy() const {
	return unique_ptr<CreateSecretStatement>(new CreateSecretStatement(*this));
}

} // namespace duckdb
