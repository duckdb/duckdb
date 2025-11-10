#include "transformer/peg_transformer.hpp"
#include "duckdb/parser/sql_statement.hpp"

namespace duckdb {

// UseStatement <- 'USE' UseTarget
unique_ptr<SQLStatement> PEGTransformerFactory::TransformUseStatement(PEGTransformer &transformer,
                                                                      optional_ptr<ParseResult> parse_result) {
	auto &list_pr = parse_result->Cast<ListParseResult>();
	auto qn = transformer.Transform<QualifiedName>(list_pr, 1);

	string value_str;
	if (IsInvalidSchema(qn.schema)) {
		value_str = KeywordHelper::WriteOptionallyQuoted(qn.name);
	} else {
		value_str =
		    KeywordHelper::WriteOptionallyQuoted(qn.schema) + "." + KeywordHelper::WriteOptionallyQuoted(qn.name);
	}

	auto value_expr = make_uniq<ConstantExpression>(Value(value_str));
	return make_uniq<SetVariableStatement>("schema", std::move(value_expr), SetScope::AUTOMATIC);
}

// UseTarget <- (CatalogName '.' ReservedSchemaName) / SchemaName / CatalogName
QualifiedName PEGTransformerFactory::TransformUseTarget(PEGTransformer &transformer,
                                                        optional_ptr<ParseResult> parse_result) {
	auto &list_pr = parse_result->Cast<ListParseResult>();
	auto &choice_pr = list_pr.Child<ChoiceParseResult>(0);
	QualifiedName result;
	if (choice_pr.result->type == ParseResultType::LIST) {
		vector<string> entries;
		auto use_target_children = choice_pr.result->Cast<ListParseResult>();
		for (auto &child : use_target_children.GetChildren()) {
			if (child->type == ParseResultType::IDENTIFIER) {
				entries.push_back(child->Cast<IdentifierParseResult>().identifier);
			}
		}
		if (entries.size() == 2) {
			result.catalog = INVALID_CATALOG;
			result.schema = entries[0];
			result.name = entries[1];
		} else {
			throw InternalException("Invalid amount of entries for use statement");
		}
	} else if (choice_pr.result->type == ParseResultType::IDENTIFIER) {
		result.name = choice_pr.result->Cast<IdentifierParseResult>().identifier;
	} else {
		throw InternalException("Unexpected parse result type encountered in UseTarget");
	}
	return result;
}
} // namespace duckdb
