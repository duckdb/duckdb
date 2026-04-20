#include "duckdb/common/vector/flat_vector.hpp"
#include "duckdb/common/vector/string_vector.hpp"
#include "transformer/peg_transformer.hpp"
#include "duckdb/parser/parsed_data/create_type_info.hpp"

namespace duckdb {

unique_ptr<CreateStatement> PEGTransformerFactory::TransformCreateTypeStmt(PEGTransformer &transformer,
                                                                           ParseResult &parse_result) {
	auto &list_pr = parse_result.Cast<ListParseResult>();
	auto result = make_uniq<CreateStatement>();
	auto if_not_exists = list_pr.Child<OptionalParseResult>(1).HasResult();
	auto qualified_name = transformer.Transform<QualifiedName>(list_pr.Child<ListParseResult>(2));
	auto create_type_info = transformer.Transform<unique_ptr<CreateTypeInfo>>(list_pr.Child<ListParseResult>(4));
	create_type_info->catalog = qualified_name.catalog;
	create_type_info->schema = qualified_name.schema;
	create_type_info->name = qualified_name.name;
	create_type_info->on_conflict =
	    if_not_exists ? OnCreateConflict::IGNORE_ON_CONFLICT : OnCreateConflict::ERROR_ON_CONFLICT;
	result->info = std::move(create_type_info);
	return result;
}

unique_ptr<CreateTypeInfo> PEGTransformerFactory::TransformCreateType(PEGTransformer &transformer,
                                                                      ParseResult &parse_result) {
	auto result = make_uniq<CreateTypeInfo>();
	auto &list_pr = parse_result.Cast<ListParseResult>();
	auto &choice_pr = list_pr.Child<ChoiceParseResult>(0);
	if (choice_pr.GetResult().name == "EnumSelectType") {
		result->query = transformer.Transform<unique_ptr<SelectStatement>>(choice_pr.GetResult());
		result->type = LogicalType::INVALID;
	} else {
		result->type = transformer.Transform<LogicalType>(choice_pr.GetResult());
	}
	return result;
}

unique_ptr<SelectStatement> PEGTransformerFactory::TransformEnumSelectType(PEGTransformer &transformer,
                                                                           ParseResult &parse_result) {
	auto &list_pr = parse_result.Cast<ListParseResult>();
	auto &extract_parens = ExtractResultFromParens(list_pr.Child<ListParseResult>(1));
	return transformer.Transform<unique_ptr<SelectStatement>>(extract_parens);
}

LogicalType PEGTransformerFactory::TransformEnumStringLiteralList(PEGTransformer &transformer,
                                                                  ParseResult &parse_result) {
	auto &list_pr = parse_result.Cast<ListParseResult>();
	auto &extract_parens = ExtractResultFromParens(list_pr.Child<ListParseResult>(1));
	auto &string_list_opt = extract_parens.Cast<OptionalParseResult>();
	if (!string_list_opt.HasResult()) {
		Vector enum_vector(LogicalType::VARCHAR, (idx_t)0);
		return LogicalType::ENUM(enum_vector, 0);
	}
	auto string_literal_list = ExtractParseResultsFromList(string_list_opt.GetResult());

	Vector enum_vector(LogicalType::VARCHAR, string_literal_list.size());
	auto string_data = FlatVector::Writer<string_t>(enum_vector, string_literal_list.size());
	for (auto string_literal : string_literal_list) {
		string_data.PushValue(string_t(string_literal.get().Cast<StringLiteralParseResult>().result));
	}
	return LogicalType::ENUM(enum_vector, string_literal_list.size());
}

} // namespace duckdb
