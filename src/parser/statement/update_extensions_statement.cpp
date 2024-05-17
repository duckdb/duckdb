#include "duckdb/parser/statement/update_extensions_statement.hpp"

namespace duckdb {

UpdateExtensionsStatement::UpdateExtensionsStatement() : SQLStatement(StatementType::UPDATE_EXTENSIONS_STATEMENT) {
}

UpdateExtensionsStatement::UpdateExtensionsStatement(const UpdateExtensionsStatement &other)
    : SQLStatement(other), info(other.info->Copy()) {
}

string UpdateExtensionsStatement::ToString() const {
	string result;
	result += "UPDATE EXTENSIONS";

	if (!info->extensions_to_update.empty()) {
		result += "(";
		for (idx_t i = 0; i < info->extensions_to_update.size(); i++) {
			if (i > 0) {
				result += ", ";
			}
			result += info->extensions_to_update[i];
		}
		result += ")";
	}

	return result;
}

unique_ptr<SQLStatement> UpdateExtensionsStatement::Copy() const {
	return unique_ptr<UpdateExtensionsStatement>(new UpdateExtensionsStatement(*this));
}

} // namespace duckdb
