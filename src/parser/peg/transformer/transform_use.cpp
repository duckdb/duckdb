#include "duckdb/parser/peg/transformer/peg_transformer.hpp"
#include "duckdb/parser/sql_statement.hpp"

namespace duckdb {

// UseStatement <- 'USE' UseTarget
unique_ptr<SQLStatement> PEGTransformerFactory::TransformUseStatement(PEGTransformer &transformer,
                                                                      const QualifiedName &use_target) {
	string value_str;
	if (IsInvalidSchema(use_target.schema)) {
		value_str = SQLIdentifier::ToString(use_target.name.GetIdentifierName());
	} else {
		value_str = SQLIdentifier(use_target.schema) + "." + SQLIdentifier(use_target.name);
	}

	auto value_expr = make_uniq<ConstantExpression>(Value(value_str));
	return make_uniq<SetVariableStatement>("schema", std::move(value_expr), SetScope::AUTOMATIC);
}

// UseTarget <- UseTargetCatalogSchema / SchemaName / CatalogName
QualifiedName PEGTransformerFactory::TransformUseTarget(PEGTransformer &transformer, ParseResult &choice_result) {
	if (choice_result.type == ParseResultType::IDENTIFIER) {
		QualifiedName result;
		result.name = choice_result.Cast<IdentifierParseResult>().identifier;
		return result;
	}
	return transformer.Transform<QualifiedName>(choice_result);
}

// UseTargetCatalogSchema <- CatalogName '.' ReservedSchemaName DotIdentifier*
QualifiedName PEGTransformerFactory::TransformUseTargetCatalogSchema(PEGTransformer &transformer,
                                                                     const Identifier &catalog_name,
                                                                     const Identifier &reserved_schema_name,
                                                                     const vector<Identifier> &dot_identifier) {
	if (!dot_identifier.empty()) {
		throw ParserException("Expected \"USE database\" or \"USE database.schema\"");
	}
	QualifiedName result;
	result.catalog = Identifier::InvalidCatalog();
	result.schema = catalog_name;
	result.name = reserved_schema_name;
	return result;
}

Identifier PEGTransformerFactory::TransformDotIdentifier(PEGTransformer &transformer, const Identifier &identifier) {
	return identifier;
}
} // namespace duckdb
