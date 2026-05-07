#include "duckdb/parser/peg/transformer/peg_transformer.hpp"
#include "duckdb/parser/sql_statement.hpp"

namespace duckdb {

// UseStatement <- 'USE' UseTarget
unique_ptr<SQLStatement> PEGTransformerFactory::TransformUseStatement(QualifiedName use_target) {
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

// UseTargetCatalogSchema <- CatalogName '.' ReservedSchemaName ('.' Identifier)*
QualifiedName PEGTransformerFactory::TransformUseTargetCatalogSchema(PEGTransformer &transformer,
                                                                     ParseResult &parse_result) {
	auto &list_pr = parse_result.Cast<ListParseResult>();
	auto catalog = list_pr.Child<IdentifierParseResult>(0).identifier;
	auto schema = list_pr.Child<IdentifierParseResult>(2).identifier;
	auto &extra_opt = list_pr.Child<OptionalParseResult>(3);
	if (extra_opt.HasResult()) {
		throw ParserException("Expected \"USE database\" or \"USE database.schema\"");
	}
	QualifiedName result;
	result.catalog = INVALID_CATALOG;
	result.schema = catalog;
	result.name = schema;
	return result;
}
} // namespace duckdb
