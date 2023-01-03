#include "duckdb/parser/statement/extension_statement.hpp"

namespace duckdb {

ExtensionStatement::ExtensionStatement(ParserExtension extension_p, unique_ptr<ParserExtensionParseData> parse_data_p)
    : SQLStatement(StatementType::EXTENSION_STATEMENT), extension(Move(extension_p)), parse_data(Move(parse_data_p)) {
}

unique_ptr<SQLStatement> ExtensionStatement::Copy() const {
	return make_unique<ExtensionStatement>(extension, parse_data->Copy());
}

} // namespace duckdb
