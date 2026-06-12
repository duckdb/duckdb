#include "duckdb/parser/parsed_data/comment_on_column_info.hpp"
#include "duckdb/parser/statement/alter_statement.hpp"
#include "duckdb/parser/peg/transformer/peg_transformer.hpp"

namespace duckdb {

unique_ptr<SQLStatement> PEGTransformerFactory::TransformCommentStatement(PEGTransformer &transformer,
                                                                          const CatalogType &comment_on_type,
                                                                          const vector<string> &dotted_identifier,
                                                                          const Value &comment_value) {
	auto result = make_uniq<AlterStatement>();
	unique_ptr<AlterInfo> info;

	Identifier column_name;
	if (comment_on_type == CatalogType::INVALID) {
		// Column type returned
		auto identifier = dotted_identifier;
		column_name = Identifier(identifier.back());
		identifier.pop_back();
		if (identifier.empty()) {
			throw ParserException("Invalid column reference: '%s'", column_name.GetIdentifierName());
		}
		auto qualified_name = StringToQualifiedName(identifier);
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

CatalogType PEGTransformerFactory::TransformCommentTable(PEGTransformer &transformer) {
	return CatalogType::TABLE_ENTRY;
}

CatalogType PEGTransformerFactory::TransformCommentSequence(PEGTransformer &transformer) {
	return CatalogType::SEQUENCE_ENTRY;
}

CatalogType PEGTransformerFactory::TransformCommentFunction(PEGTransformer &transformer) {
	return CatalogType::MACRO_ENTRY;
}

CatalogType PEGTransformerFactory::TransformCommentMacroTable(PEGTransformer &transformer) {
	return CatalogType::TABLE_MACRO_ENTRY;
}

CatalogType PEGTransformerFactory::TransformCommentMacro(PEGTransformer &transformer) {
	return CatalogType::MACRO_ENTRY;
}

CatalogType PEGTransformerFactory::TransformCommentView(PEGTransformer &transformer) {
	return CatalogType::VIEW_ENTRY;
}

CatalogType PEGTransformerFactory::TransformCommentDatabase(PEGTransformer &transformer) {
	return CatalogType::DATABASE_ENTRY;
}

CatalogType PEGTransformerFactory::TransformCommentIndex(PEGTransformer &transformer) {
	return CatalogType::INDEX_ENTRY;
}

CatalogType PEGTransformerFactory::TransformCommentSchema(PEGTransformer &transformer) {
	return CatalogType::SCHEMA_ENTRY;
}

CatalogType PEGTransformerFactory::TransformCommentType(PEGTransformer &transformer) {
	return CatalogType::TYPE_ENTRY;
}

CatalogType PEGTransformerFactory::TransformCommentColumn(PEGTransformer &transformer) {
	return CatalogType::INVALID;
}

Value PEGTransformerFactory::TransformCommentValue(PEGTransformer &transformer, ParseResult &choice_result) {
	// CommentValue <- NullLiteral / StringLiteral
	auto &list_pr = choice_result.Cast<ListParseResult>();
	auto &choice_pr = list_pr.Child<ChoiceParseResult>(0);
	if (choice_pr.GetResult().type == ParseResultType::STRING) {
		return Value(choice_pr.GetResult().Cast<StringLiteralParseResult>().result);
	}
	return transformer.Transform<Value>(choice_pr.GetResult());
}

} // namespace duckdb
