#include "duckdb/parser/peg/transformer/peg_transformer.hpp"
#include "duckdb/parser/sql_statement.hpp"

namespace duckdb {

// UseStatement <- 'USE' UseTarget
unique_ptr<SQLStatement> PEGTransformerFactory::TransformUseStatement(const QualifiedName &use_target) {
	string value_str;
	if (IsInvalidSchema(use_target.schema)) {
		value_str = SQLIdentifier::ToString(use_target.name);
	} else {
		value_str = SQLIdentifier(use_target.schema) + "." + SQLIdentifier(use_target.name);
	}

	auto value_expr = make_uniq<ConstantExpression>(Value(value_str));
	return make_uniq<SetVariableStatement>("schema", std::move(value_expr), SetScope::AUTOMATIC);
}

// UseTarget <- UseTargetCatalogSchema / SchemaName / CatalogName
QualifiedName PEGTransformerFactory::TransformUseTarget(PEGTransformer &transformer, ParseResult &pr) {
	if (pr.type == ParseResultType::IDENTIFIER) {
		QualifiedName result;
		result.name = pr.Cast<IdentifierParseResult>().identifier;
		return result;
	}
	return transformer.Transform<QualifiedName>(pr);
}

// UseTargetCatalogSchema <- CatalogName '.' ReservedSchemaName DotIdentifier*
QualifiedName PEGTransformerFactory::TransformUseTargetCatalogSchema(const string &catalog_name,
                                                                     const string &reserved_schema_name,
                                                                     const vector<string> &dot_identifier) {
	if (!dot_identifier.empty()) {
		throw ParserException("Expected \"USE database\" or \"USE database.schema\"");
	}
	QualifiedName result;
	result.catalog = INVALID_CATALOG;
	result.schema = catalog_name;
	result.name = reserved_schema_name;
	return result;
}

string PEGTransformerFactory::TransformDotIdentifier(const string &identifier) {
	return identifier;
}
} // namespace duckdb
