#include "duckdb/parser/peg/transformer/peg_transformer.hpp"
#include "duckdb/parser/parsed_data/create_feature_info.hpp"
#include "duckdb/parser/qualified_name.hpp"
#include "duckdb/parser/expression/function_expression.hpp"
#include "duckdb/parser/expression/constant_expression.hpp"
#include "duckdb/parser/statement/call_statement.hpp"
#include "duckdb/common/types/interval.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/string_util.hpp"

namespace duckdb {

static int64_t ParsePositiveIntegerScheduleCount(const string &number) {
	if (number.empty() || number[0] == '-') {
		throw ParserException("Refresh schedule shorthand count must be a positive integer");
	}
	idx_t start = number[0] == '+' ? 1 : 0;
	if (start >= number.size() || number.find_first_not_of("0123456789", start) != string::npos) {
		throw ParserException("Refresh schedule shorthand count must be a positive integer");
	}
	int64_t result;
	try {
		result = std::stoll(number);
	} catch (std::exception &) {
		throw ParserException("Refresh schedule shorthand count is out of range");
	}
	if (result <= 0) {
		throw ParserException("Refresh schedule interval must be positive");
	}
	return result;
}

static interval_t ParseIntervalSchedule(const string &interval_str, const string &context) {
	interval_t schedule_interval {0, 0, 0};
	string error_message;
	if (!Interval::FromCString(interval_str.c_str(), interval_str.size(), schedule_interval, &error_message, false)) {
		throw ParserException("Invalid interval in %s clause: %s", context, error_message);
	}
	if (schedule_interval.months == 0 && schedule_interval.days == 0 && schedule_interval.micros <= 0) {
		throw ParserException("Refresh schedule interval must be positive");
	}
	return schedule_interval;
}

static int32_t ScheduleCountToDays(int64_t count) {
	if (count > NumericLimits<int32_t>::Maximum()) {
		throw ParserException("Refresh schedule shorthand count is out of range");
	}
	return static_cast<int32_t>(count);
}

static int64_t ScheduleCountToMicros(int64_t count, int64_t micros_per_unit) {
	if (count > NumericLimits<int64_t>::Maximum() / micros_per_unit) {
		throw ParserException("Refresh schedule shorthand count is out of range");
	}
	return count * micros_per_unit;
}

unique_ptr<CreateStatement> PEGTransformerFactory::TransformCreateFeatureStmt(PEGTransformer &transformer,
                                                                              ParseResult &parse_result) {
	// CreateFeatureStmt <- 'FEATURE' IfNotExists? IdentifierOrStringLiteral 'ON' IdentifierOrStringLiteral
	//                      'ENTITY' IdentifierOrStringLiteral 'TIMESTAMP' IdentifierOrStringLiteral
	//                      FeatureGranularityClause? 'WINDOW' NumberLiteral
	//                      FeatureRefreshClause? FeatureRetainClause?
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
	// index 9: FeatureGranularityClause? (default: DAY)
	FeatureGranularity granularity = FeatureGranularity::DAY;
	auto &granularity_opt = list_pr.Child<OptionalParseResult>(9);
	if (granularity_opt.HasResult()) {
		auto &clause = granularity_opt.GetResult().Cast<ListParseResult>();
		granularity = transformer.Transform<FeatureGranularity>(clause.Child<ListParseResult>(1));
	}
	// index 10: FeatureWindowClause? (default: 1)
	int64_t window_size = 1;
	auto &window_opt = list_pr.Child<OptionalParseResult>(10);
	if (window_opt.HasResult()) {
		auto &clause = window_opt.GetResult().Cast<ListParseResult>();
		window_size = std::stoll(clause.Child<NumberParseResult>(1).number);
	}
	// index 11: FeatureRefreshClause? (default: INCREMENTAL)
	FeatureRefreshMode refresh_mode = FeatureRefreshMode::INCREMENTAL;
	auto &refresh_opt = list_pr.Child<OptionalParseResult>(11);
	if (refresh_opt.HasResult()) {
		auto &clause = refresh_opt.GetResult().Cast<ListParseResult>();
		refresh_mode = transformer.Transform<FeatureRefreshMode>(clause.Child<ListParseResult>(1));
	}
	// index 12: FeatureScheduleClause? (default: no schedule)
	bool has_schedule = false;
	interval_t schedule_interval {0, 0, 0};
	auto &schedule_opt = list_pr.Child<OptionalParseResult>(12);
	if (schedule_opt.HasResult()) {
		schedule_interval = transformer.Transform<interval_t>(schedule_opt.GetResult());
		has_schedule = true;
	}
	// index 13: FeatureRetainClause? (default: 1)
	int64_t retain_versions = 1;
	auto &retain_opt = list_pr.Child<OptionalParseResult>(13);
	if (retain_opt.HasResult()) {
		auto &clause = retain_opt.GetResult().Cast<ListParseResult>();
		retain_versions = std::stoll(clause.Child<NumberParseResult>(1).number);
	}
	// index 14: 'AS' keyword
	auto &select_parens = ExtractResultFromParens(list_pr.Child<ListParseResult>(15));
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
	info->has_schedule = has_schedule;
	info->schedule_interval = schedule_interval;
	info->schedule_enabled = has_schedule;
	info->query = std::move(query);
	result->info = std::move(info);
	return result;
}

interval_t PEGTransformerFactory::TransformFeatureScheduleClause(PEGTransformer &transformer,
                                                                 ParseResult &parse_result) {
	// FeatureScheduleClause <- FeatureScheduleIntervalClause / FeatureScheduleShorthandClause
	auto &list_pr = parse_result.Cast<ListParseResult>();
	auto &choice = list_pr.Child<ChoiceParseResult>(0).GetResult();
	auto &clause = choice.Cast<ListParseResult>();
	if (StringUtil::CIEquals(choice.name, "FeatureScheduleIntervalClause")) {
		return ParseIntervalSchedule(clause.Child<StringLiteralParseResult>(2).result, "EVERY");
	}

	if (!StringUtil::CIEquals(choice.name, "FeatureScheduleShorthandClause")) {
		throw InternalException("Unsupported feature schedule clause");
	}

	auto count = ParsePositiveIntegerScheduleCount(clause.Child<NumberParseResult>(1).number);
	auto &unit = clause.Child<ListParseResult>(2).Child<ChoiceParseResult>(0).GetResult();
	if (StringUtil::CIEquals(unit.name, "FeatureScheduleUnitDay") ||
	    StringUtil::CIEquals(unit.name, "FeatureScheduleUnitDays")) {
		return interval_t {0, ScheduleCountToDays(count), 0};
	}
	if (StringUtil::CIEquals(unit.name, "FeatureScheduleUnitHour") ||
	    StringUtil::CIEquals(unit.name, "FeatureScheduleUnitHours")) {
		return interval_t {0, 0, ScheduleCountToMicros(count, Interval::MICROS_PER_HOUR)};
	}
	if (StringUtil::CIEquals(unit.name, "FeatureScheduleUnitMinute") ||
	    StringUtil::CIEquals(unit.name, "FeatureScheduleUnitMinutes")) {
		return interval_t {0, 0, ScheduleCountToMicros(count, Interval::MICROS_PER_MINUTE)};
	}
	throw InternalException("Unsupported feature schedule unit");
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
