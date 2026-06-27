#include "duckdb/parser/peg/transformer/peg_transformer.hpp"
#include "duckdb/parser/sql_statement.hpp"

namespace duckdb {

// UseStatement <- 'USE' UseTarget
unique_ptr<SQLStatement> PEGTransformerFactory::TransformUseStatement(PEGTransformer &transformer,
                                                                      const QualifiedName &use_target) {
	string value_str;
	if (IsInvalidSchema(use_target.Schema())) {
		value_str = SQLIdentifier::ToString(use_target.Name().GetIdentifierName());
	} else {
		value_str = SQLIdentifier(use_target.Schema()) + "." + SQLIdentifier(use_target.Name());
	}

	auto value_expr = make_uniq<ConstantExpression>(Value(value_str));
	return make_uniq<SetVariableStatement>("schema", std::move(value_expr), SetScope::AUTOMATIC);
}

// UseTarget <- UseTargetCatalogSchema / SchemaName / CatalogName
QualifiedName PEGTransformerFactory::TransformSchemaNameAsUseTarget(PEGTransformer &transformer,
                                                                    const Identifier &schema_name) {
	QualifiedName result;
	result = QualifiedName(schema_name);
	return result;
}

QualifiedName PEGTransformerFactory::TransformCatalogNameAsUseTarget(PEGTransformer &transformer,
                                                                     const Identifier &catalog_name) {
	QualifiedName result;
	result = QualifiedName(catalog_name);
	return result;
}

// UseTargetCatalogSchema <- CatalogName '.' ReservedSchemaName DotIdentifier*
QualifiedName
PEGTransformerFactory::TransformUseTargetCatalogSchema(PEGTransformer &transformer, const Identifier &catalog_name,
                                                       const Identifier &reserved_schema_name,
                                                       const optional<vector<Identifier>> &dot_identifier) {
	if (dot_identifier && !dot_identifier->empty()) {
		throw ParserException("Expected \"USE database\" or \"USE database.schema\"");
	}
	QualifiedName result({catalog_name}, reserved_schema_name);
	return result;
}

Identifier PEGTransformerFactory::TransformDotIdentifier(PEGTransformer &transformer, const Identifier &identifier) {
	return identifier;
}
} // namespace duckdb
