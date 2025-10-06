#include "transformer/peg_transformer.hpp"
#include "duckdb/parser/sql_statement.hpp"

namespace duckdb {

// UseStatement <- 'USE' UseTarget
unique_ptr<SQLStatement> PEGTransformerFactory::TransformUseStatement(PEGTransformer &transformer,
                                                                      optional_ptr<ParseResult> parse_result) {
	auto &list_pr = parse_result->Cast<ListParseResult>();
	auto qn = transformer.Transform<QualifiedName>(list_pr, 1);

	string value_str;
	if (qn.schema.empty()) {
		value_str = KeywordHelper::WriteOptionallyQuoted(qn.name, '"');
	} else {
		value_str = KeywordHelper::WriteOptionallyQuoted(qn.schema, '"') + "." +
		            KeywordHelper::WriteOptionallyQuoted(qn.name, '"');
	}

	auto value_expr = make_uniq<ConstantExpression>(Value(value_str));
	return make_uniq<SetVariableStatement>("schema", std::move(value_expr), SetScope::AUTOMATIC);
}

// UseTarget <- (CatalogName '.' ReservedSchemaName) / SchemaName / CatalogName
QualifiedName PEGTransformerFactory::TransformUseTarget(PEGTransformer &transformer,
                                                        optional_ptr<ParseResult> parse_result) {
	auto &list_pr = parse_result->Cast<ListParseResult>();
	auto &choice_pr = list_pr.Child<ChoiceParseResult>(0);
	string qualified_name;
	if (choice_pr.result->type == ParseResultType::LIST) {
		auto use_target_children = choice_pr.result->Cast<ListParseResult>();
		for (auto &child : use_target_children.children) {
			if (child->type == ParseResultType::IDENTIFIER) {
				qualified_name += child->Cast<IdentifierParseResult>().identifier;
			} else if (child->type == ParseResultType::KEYWORD) {
				qualified_name += child->Cast<KeywordParseResult>().keyword;
			}
		}
	} else if (choice_pr.result->type == ParseResultType::IDENTIFIER) {
		qualified_name = choice_pr.result->Cast<IdentifierParseResult>().identifier;
	} else {
		throw InternalException("Unexpected parse result type encountered in UseTarget");
	}
	auto result = QualifiedName::Parse(qualified_name);
	return result;
}
} // namespace duckdb
