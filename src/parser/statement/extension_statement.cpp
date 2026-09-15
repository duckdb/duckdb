#include "duckdb/parser/statement/extension_statement.hpp"

namespace duckdb {

ExtensionStatement::ExtensionStatement(ParserExtension extension_p, unique_ptr<ParserExtensionParseData> parse_data_p)
    : SQLStatement(StatementType::EXTENSION_STATEMENT), extension(std::move(extension_p)),
      parse_data(std::move(parse_data_p)) {
}

ExtensionStatement::ExtensionStatement(const ExtensionStatement &other)
    : SQLStatement(other), extension(other.extension), parse_data(other.parse_data->Copy()) {
}

unique_ptr<SQLStatement> ExtensionStatement::Copy() const {
	return unique_ptr<ExtensionStatement>(new ExtensionStatement(*this));
}

string ExtensionStatement::ToString() const {
	return parse_data->ToString();
}

} // namespace duckdb
