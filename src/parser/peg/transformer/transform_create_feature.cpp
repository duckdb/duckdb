#include "duckdb/parser/peg/transformer/peg_transformer.hpp"
#include "duckdb/parser/parsed_data/create_feature_info.hpp"
#include "duckdb/parser/qualified_name.hpp"
#include "duckdb/parser/expression/function_expression.hpp"
#include "duckdb/parser/expression/constant_expression.hpp"
#include "duckdb/parser/statement/call_statement.hpp"

namespace duckdb {

unique_ptr<CreateStatement> PEGTransformerFactory::TransformCreateFeatureStmt(PEGTransformer &transformer,
                                                                              ParseResult &parse_result) {
	// CreateFeatureStmt <- 'FEATURE' IfNotExists? IdentifierOrStringLiteral 'ON' IdentifierOrStringLiteral
	//                      'ENTITY' IdentifierOrStringLiteral 'TIMESTAMP' IdentifierOrStringLiteral
	//                      'GRANULARITY' FeatureGranularity 'WINDOW' NumberLiteral
	//                      'REFRESH' FeatureRefreshMode 'RETAIN' NumberLiteral
	//                      'AS' Parens(SelectStatementInternal)
	auto &list_pr = parse_result.Cast<ListParseResult>();

	// index 0: 'FEATURE' keyword
	auto if_not_exists = list_pr.Child<OptionalParseResult>(1).HasResult();
	auto feature_name = transformer.Transform<QualifiedName>(list_pr.Child<ListParseResult>(2)).name;
	// index 3: 'ON' keyword
	auto source_table = transformer.Transform<QualifiedName>(list_pr.Child<ListParseResult>(4)).name;
	// index 5: 'ENTITY' keyword
	auto entity_column = transformer.Transform<QualifiedName>(list_pr.Child<ListParseResult>(6)).name;
	// index 7: 'TIMESTAMP' keyword
	auto timestamp_column = transformer.Transform<QualifiedName>(list_pr.Child<ListParseResult>(8)).name;
	// index 9: 'GRANULARITY' keyword
	auto granularity = transformer.Transform<FeatureGranularity>(list_pr.Child<ListParseResult>(10));
	// index 11: 'WINDOW' keyword
	auto &window_num = list_pr.Child<NumberParseResult>(12);
	int64_t window_size = std::stoll(window_num.number);
	// index 13: 'REFRESH' keyword
	auto refresh_mode = transformer.Transform<FeatureRefreshMode>(list_pr.Child<ListParseResult>(14));
	// index 15: 'RETAIN' keyword
	auto &retain_num = list_pr.Child<NumberParseResult>(16);
	int64_t retain_versions = std::stoll(retain_num.number);
	// index 17: 'AS' keyword
	auto &select_parens = ExtractResultFromParens(list_pr.Child<ListParseResult>(18));
	auto query = transformer.Transform<unique_ptr<SelectStatement>>(select_parens);

	auto result = make_uniq<CreateStatement>();
	auto info = make_uniq<CreateFeatureInfo>();
	info->on_conflict = if_not_exists ? OnCreateConflict::IGNORE_ON_CONFLICT : OnCreateConflict::ERROR_ON_CONFLICT;
	info->feature_name = feature_name;
	info->source_table = source_table;
	info->entity_column = entity_column;
	info->timestamp_column = timestamp_column;
	info->granularity = granularity;
	info->window_size = window_size;
	info->refresh_mode = refresh_mode;
	info->retain_versions = retain_versions;
	info->query = std::move(query);
	result->info = std::move(info);
	return result;
}

FeatureGranularity PEGTransformerFactory::TransformFeatureGranularity(PEGTransformer &transformer,
                                                                      ParseResult &parse_result) {
	auto &list_pr = parse_result.Cast<ListParseResult>();
	return transformer.TransformEnum<FeatureGranularity>(list_pr.Child<ChoiceParseResult>(0).GetResult());
}

FeatureRefreshMode PEGTransformerFactory::TransformFeatureRefreshMode(PEGTransformer &transformer,
                                                                      ParseResult &parse_result) {
	auto &list_pr = parse_result.Cast<ListParseResult>();
	return transformer.TransformEnum<FeatureRefreshMode>(list_pr.Child<ChoiceParseResult>(0).GetResult());
}

unique_ptr<SQLStatement> PEGTransformerFactory::TransformRefreshFeatureStatement(PEGTransformer &transformer,
                                                                                 ParseResult &parse_result) {
	// RefreshFeatureStatement <- 'REFRESH' 'FEATURE' IdentifierOrStringLiteral
	auto &list_pr = parse_result.Cast<ListParseResult>();
	// index 0: 'REFRESH' keyword
	// index 1: 'FEATURE' keyword
	auto feature_name = transformer.Transform<QualifiedName>(list_pr.Child<ListParseResult>(2)).name;

	// Rewrite to: CALL refresh_feature('feature_name')
	auto result = make_uniq<CallStatement>();
	vector<unique_ptr<ParsedExpression>> args;
	args.push_back(make_uniq<ConstantExpression>(Value(feature_name)));
	auto function_expression = make_uniq<FunctionExpression>("refresh_feature", std::move(args));
	result->function = std::move(function_expression);
	return std::move(result);
}

unique_ptr<SQLStatement> PEGTransformerFactory::TransformServeFeatureStatement(PEGTransformer &transformer,
                                                                               ParseResult &parse_result) {
	// ServeFeatureStatement <- 'SERVE' ServeFeatureKw List(IdentifierOrStringLiteral) 'FOR' IdentifierOrStringLiteral
	// ServeFeatureEntity? ServeFeatureAsOf?
	auto &list_pr = parse_result.Cast<ListParseResult>();
	// index 0: 'SERVE' keyword
	// index 1: ServeFeatureKw ('FEATURE' or 'FEATURES')
	// index 2: List(IdentifierOrStringLiteral) - feature names
	auto feature_items = ExtractParseResultsFromList(list_pr.Child<ListParseResult>(2));
	// Build comma-separated feature names
	string feature_names;
	for (auto &item : feature_items) {
		auto name = transformer.Transform<QualifiedName>(item).name;
		if (!feature_names.empty()) {
			feature_names += ",";
		}
		feature_names += name;
	}
	// index 3: 'FOR' keyword
	auto spine_table = transformer.Transform<QualifiedName>(list_pr.Child<ListParseResult>(4)).name;

	// index 5: optional ServeFeatureEntity <- 'ENTITY' IdentifierOrStringLiteral
	string entity_column;
	auto &entity_opt = list_pr.Child<OptionalParseResult>(5);
	if (entity_opt.HasResult()) {
		auto &entity_list = entity_opt.GetResult().Cast<ListParseResult>();
		// index 0: 'ENTITY' keyword, index 1: identifier
		entity_column = transformer.Transform<QualifiedName>(entity_list.Child<ListParseResult>(1)).name;
	}

	// index 6: optional ServeFeatureAsOf <- 'ASOF' IdentifierOrStringLiteral
	string as_of_column;
	auto &asof_opt = list_pr.Child<OptionalParseResult>(6);
	if (asof_opt.HasResult()) {
		auto &asof_list = asof_opt.GetResult().Cast<ListParseResult>();
		// index 0: 'ASOF' keyword, index 1: identifier
		as_of_column = transformer.Transform<QualifiedName>(asof_list.Child<ListParseResult>(1)).name;
	}

	// Rewrite to: CALL serve_feature('feature_names', 'spine_table', 'entity_column', 'as_of_column')
	// Empty strings signal "use default from feature metadata"
	auto result = make_uniq<CallStatement>();
	vector<unique_ptr<ParsedExpression>> args;
	args.push_back(make_uniq<ConstantExpression>(Value(feature_names)));
	args.push_back(make_uniq<ConstantExpression>(Value(spine_table)));
	args.push_back(make_uniq<ConstantExpression>(Value(entity_column)));
	args.push_back(make_uniq<ConstantExpression>(Value(as_of_column)));
	auto function_expression = make_uniq<FunctionExpression>("serve_feature", std::move(args));
	result->function = std::move(function_expression);
	return std::move(result);
}

unique_ptr<SQLStatement> PEGTransformerFactory::TransformFeatureAtVersionStatement(PEGTransformer &transformer,
                                                                                   ParseResult &parse_result) {
	// FeatureAtVersionStatement <- 'FEATURE' IdentifierOrStringLiteral 'AT' 'VERSION' NumberLiteral
	auto &list_pr = parse_result.Cast<ListParseResult>();
	// index 0: 'FEATURE' keyword
	auto feature_name = transformer.Transform<QualifiedName>(list_pr.Child<ListParseResult>(1)).name;
	// index 2: 'AT' keyword
	// index 3: 'VERSION' keyword
	auto &version_num = list_pr.Child<NumberParseResult>(4);
	int64_t version = std::stoll(version_num.number);

	// Rewrite to: CALL feature_at_version('feature_name', version)
	auto result = make_uniq<CallStatement>();
	vector<unique_ptr<ParsedExpression>> args;
	args.push_back(make_uniq<ConstantExpression>(Value(feature_name)));
	args.push_back(make_uniq<ConstantExpression>(Value::BIGINT(version)));
	auto function_expression = make_uniq<FunctionExpression>("feature_at_version", std::move(args));
	result->function = std::move(function_expression);
	return std::move(result);
}

} // namespace duckdb
