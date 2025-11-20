#include "duckdb/parser/parsed_data/comment_on_column_info.hpp"
#include "duckdb/parser/statement/alter_statement.hpp"
#include "transformer/peg_transformer.hpp"

namespace duckdb {

unique_ptr<SQLStatement> PEGTransformerFactory::TransformCommentStatement(PEGTransformer &transformer,
                                                                          optional_ptr<ParseResult> parse_result) {
	auto &list_pr = parse_result->Cast<ListParseResult>();
	auto comment_on_type = transformer.Transform<CatalogType>(list_pr.Child<ListParseResult>(2));
	auto dotted_identifier = transformer.Transform<vector<string>>(list_pr.Child<ListParseResult>(3));
	auto comment_value = transformer.Transform<Value>(list_pr.Child<ListParseResult>(5));

	auto result = make_uniq<AlterStatement>();
	unique_ptr<AlterInfo> info;

	string column_name;
	if (comment_on_type == CatalogType::INVALID) {
		// Column type returned
		column_name = dotted_identifier.back();
		dotted_identifier.pop_back();
		auto qualified_name = StringToQualifiedName(dotted_identifier);
		info = make_uniq<SetColumnCommentInfo>(qualified_name.catalog, qualified_name.schema, qualified_name.name,
		                                       column_name, comment_value, OnEntryNotFound::THROW_EXCEPTION);
	} else if (comment_on_type == CatalogType::DATABASE_ENTRY) {
		throw NotImplementedException("Adding comments to databases is not implemented");
	} else if (comment_on_type == CatalogType::SCHEMA_ENTRY) {
		throw NotImplementedException("Adding comments to schemas is not implemented");
	} else {
		auto qualified_name = StringToQualifiedName(dotted_identifier);
		info = make_uniq<SetCommentInfo>(comment_on_type, qualified_name.catalog, qualified_name.schema,
		                                 qualified_name.name, comment_value, OnEntryNotFound::THROW_EXCEPTION);
	}
	if (!info) {
		throw NotImplementedException("Cannot comment on this type");
	}
	result->info = std::move(info);
	return std::move(result);
}

CatalogType PEGTransformerFactory::TransformCommentOnType(PEGTransformer &transformer,
                                                          optional_ptr<ParseResult> parse_result) {
	auto list_pr = parse_result->Cast<ListParseResult>();
	return transformer.TransformEnum<CatalogType>(list_pr.Child<ChoiceParseResult>(0).result);
}

Value PEGTransformerFactory::TransformCommentValue(PEGTransformer &transformer,
                                                   optional_ptr<ParseResult> parse_result) {
	// CommentValue <- 'NULL'i / StringLiteral
	auto &list_pr = parse_result->Cast<ListParseResult>();
	auto choice_pr = list_pr.Child<ChoiceParseResult>(0);
	if (choice_pr.result->type == ParseResultType::STRING) {
		return Value(choice_pr.result->Cast<StringLiteralParseResult>().result);
	}
	return Value();
}

} // namespace duckdb
