#include "duckdb/parser/peg/transformer/peg_transformer.hpp"
#include "duckdb/parser/parsed_data/create_feature_info.hpp"
#include "duckdb/parser/qualified_name.hpp"
#include "duckdb/parser/expression/function_expression.hpp"
#include "duckdb/parser/expression/constant_expression.hpp"
#include "duckdb/parser/statement/call_statement.hpp"
#include "duckdb/parser/statement/serve_feature_statement.hpp"
#include "duckdb/common/types/interval.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/string_util.hpp"

namespace duckdb {

static constexpr interval_t DEFAULT_WATERMARK_INTERVAL {0, 0, 0};

static int64_t ParsePositiveIntegerIntervalCount(const string &number, const string &context) {
	if (number.empty() || number[0] == '-') {
		throw ParserException("%s shorthand count must be a positive integer", context);
	}
	idx_t start = number[0] == '+' ? 1 : 0;
	if (start >= number.size() || number.find_first_not_of("0123456789", start) != string::npos) {
		throw ParserException("%s shorthand count must be a positive integer", context);
	}
	int64_t result;
	try {
		result = std::stoll(number);
	} catch (std::exception &) {
		throw ParserException("%s shorthand count is out of range", context);
	}
	if (result <= 0) {
		throw ParserException("%s interval must be positive", context);
	}
	return result;
}

static bool IsPositiveInterval(const interval_t &interval) {
	return interval.months > 0 || interval.days > 0 || interval.micros > 0;
}

static interval_t ParseIntervalSchedule(const string &interval_str, const string &context) {
	interval_t schedule_interval {0, 0, 0};
	string error_message;
	if (!Interval::FromCString(interval_str.c_str(), interval_str.size(), schedule_interval, &error_message, false)) {
		throw ParserException("Invalid interval in %s clause: %s", context, error_message);
	}
	if (!IsPositiveInterval(schedule_interval)) {
		throw ParserException("%s interval must be positive", context);
	}
	return schedule_interval;
}

static int32_t IntervalCountToDays(int64_t count, const string &context) {
	if (count > NumericLimits<int32_t>::Maximum()) {
		throw ParserException("%s shorthand count is out of range", context);
	}
	return static_cast<int32_t>(count);
}

static int64_t IntervalCountToMicros(int64_t count, int64_t micros_per_unit, const string &context) {
	if (count > NumericLimits<int64_t>::Maximum() / micros_per_unit) {
		throw ParserException("%s shorthand count is out of range", context);
	}
	return count * micros_per_unit;
}

static interval_t IntervalFromCountAndUnit(int64_t count, const ParseResult &unit, const string &context) {
	if (StringUtil::CIEquals(unit.name, "FeatureScheduleUnitDay") ||
	    StringUtil::CIEquals(unit.name, "FeatureScheduleUnitDays")) {
		return interval_t {0, IntervalCountToDays(count, context), 0};
	}
	if (StringUtil::CIEquals(unit.name, "FeatureScheduleUnitHour") ||
	    StringUtil::CIEquals(unit.name, "FeatureScheduleUnitHours")) {
		return interval_t {0, 0, IntervalCountToMicros(count, Interval::MICROS_PER_HOUR, context)};
	}
	if (StringUtil::CIEquals(unit.name, "FeatureScheduleUnitMinute") ||
	    StringUtil::CIEquals(unit.name, "FeatureScheduleUnitMinutes")) {
		return interval_t {0, 0, IntervalCountToMicros(count, Interval::MICROS_PER_MINUTE, context)};
	}
	throw InternalException("Unsupported feature interval unit");
}

static interval_t IntervalFromGranularity(int64_t count, FeatureGranularity granularity) {
	switch (granularity) {
	case FeatureGranularity::DAY:
		return interval_t {0, IntervalCountToDays(count, "WINDOW"), 0};
	case FeatureGranularity::HOUR:
		return interval_t {0, 0, IntervalCountToMicros(count, Interval::MICROS_PER_HOUR, "WINDOW")};
	case FeatureGranularity::MINUTE:
		return interval_t {0, 0, IntervalCountToMicros(count, Interval::MICROS_PER_MINUTE, "WINDOW")};
	default:
		throw InternalException("Unsupported feature granularity");
	}
}

static interval_t ParseFeatureInterval(ParseResult &parse_result, const string &context,
                                       FeatureGranularity granularity = FeatureGranularity::DAY,
                                       bool allow_bare = false) {
	// FeatureInterval <- FeatureIntervalString / FeatureIntervalShorthand / FeatureIntervalBare
	auto &list_pr = parse_result.Cast<ListParseResult>();
	auto &choice = list_pr.Child<ChoiceParseResult>(0).GetResult();
	auto &clause = choice.Cast<ListParseResult>();
	if (StringUtil::CIEquals(choice.name, "FeatureIntervalString")) {
		return ParseIntervalSchedule(clause.Child<StringLiteralParseResult>(1).result, context);
	}

	auto count = ParsePositiveIntegerIntervalCount(clause.Child<NumberParseResult>(0).number, context);
	if (StringUtil::CIEquals(choice.name, "FeatureIntervalShorthand")) {
		auto &unit = clause.Child<ListParseResult>(1).Child<ChoiceParseResult>(0).GetResult();
		return IntervalFromCountAndUnit(count, unit, context);
	}
	if (StringUtil::CIEquals(choice.name, "FeatureIntervalBare")) {
		if (!allow_bare) {
			throw ParserException("%s interval requires an explicit unit", context);
		}
		return IntervalFromGranularity(count, granularity);
	}
	throw InternalException("Unsupported feature interval");
}

static interval_t ParseFeatureScheduleInterval(ParseResult &parse_result) {
	// FeatureScheduleClause <- FeatureScheduleIntervalClause / FeatureScheduleShorthandClause
	auto &list_pr = parse_result.Cast<ListParseResult>();
	auto &choice = list_pr.Child<ChoiceParseResult>(0).GetResult();
	auto &clause = choice.Cast<ListParseResult>();
	if (StringUtil::CIEquals(choice.name, "FeatureScheduleIntervalClause")) {
		return ParseIntervalSchedule(clause.Child<StringLiteralParseResult>(2).result, "EVERY");
	}

	if (StringUtil::CIEquals(choice.name, "FeatureScheduleShorthandClause")) {
		auto count = ParsePositiveIntegerIntervalCount(clause.Child<NumberParseResult>(1).number, "EVERY");
		auto &unit = clause.Child<ListParseResult>(2).Child<ChoiceParseResult>(0).GetResult();
		return IntervalFromCountAndUnit(count, unit, "EVERY");
	}
	throw InternalException("Unsupported feature schedule clause");
}

unique_ptr<CreateStatement> PEGTransformerFactory::TransformCreateFeatureStmt(PEGTransformer &transformer,
                                                                              ParseResult &parse_result) {
	// CreateFeatureStmt <- 'FEATURE' IfNotExists? IdentifierOrStringLiteral 'ON' IdentifierOrStringLiteral
	//                      'ENTITY' IdentifierOrStringLiteral 'TIMESTAMP' IdentifierOrStringLiteral
	//                      FeatureGranularityClause? FeatureWindowClause? FeatureWatermarkClause?
	//                      FeatureRefreshClause? FeatureScheduleClause? FeatureRetainClause?
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
	// index 10: FeatureWindowClause? (default: 1 unit of GRANULARITY)
	auto window_interval = IntervalFromGranularity(1, granularity);
	auto &window_opt = list_pr.Child<OptionalParseResult>(10);
	if (window_opt.HasResult()) {
		auto &clause = window_opt.GetResult().Cast<ListParseResult>();
		window_interval = ParseFeatureInterval(clause.Child<ListParseResult>(1), "WINDOW", granularity, true);
	}
	// index 11: FeatureWatermarkClause? (default: zero interval)
	interval_t watermark_interval = DEFAULT_WATERMARK_INTERVAL;
	auto &watermark_opt = list_pr.Child<OptionalParseResult>(11);
	if (watermark_opt.HasResult()) {
		auto &clause = watermark_opt.GetResult().Cast<ListParseResult>();
		watermark_interval = ParseFeatureInterval(clause.Child<ListParseResult>(1), "WATERMARK");
	}
	// index 12: FeatureRefreshClause? (default: INCREMENTAL)
	FeatureRefreshMode refresh_mode = FeatureRefreshMode::INCREMENTAL;
	auto &refresh_opt = list_pr.Child<OptionalParseResult>(12);
	if (refresh_opt.HasResult()) {
		auto &clause = refresh_opt.GetResult().Cast<ListParseResult>();
		refresh_mode = transformer.Transform<FeatureRefreshMode>(clause.Child<ListParseResult>(1));
	}
	// index 13: FeatureScheduleClause? (default: no schedule)
	bool has_schedule = false;
	interval_t schedule_interval {0, 0, 0};
	auto &schedule_opt = list_pr.Child<OptionalParseResult>(13);
	if (schedule_opt.HasResult()) {
		schedule_interval = transformer.Transform<interval_t>(schedule_opt.GetResult());
		has_schedule = true;
	}
	// index 14: FeatureRetainClause? (default: 1)
	int64_t retain_versions = 1;
	auto &retain_opt = list_pr.Child<OptionalParseResult>(14);
	if (retain_opt.HasResult()) {
		auto &clause = retain_opt.GetResult().Cast<ListParseResult>();
		retain_versions = std::stoll(clause.Child<NumberParseResult>(1).number);
	}
	// index 15: 'AS' keyword
	auto &select_parens = ExtractResultFromParens(list_pr.Child<ListParseResult>(16));
	auto query = transformer.Transform<unique_ptr<SelectStatement>>(select_parens);

	auto result = make_uniq<CreateStatement>();
	auto info = make_uniq<CreateFeatureInfo>();
	info->on_conflict = if_not_exists ? OnCreateConflict::IGNORE_ON_CONFLICT : OnCreateConflict::ERROR_ON_CONFLICT;
	info->feature_name = feature_name;
	info->source_table = source_table;
	info->entity_column = entity_column;
	info->timestamp_column = timestamp_column;
	info->granularity = granularity;
	info->window_interval = window_interval;
	info->watermark_interval = watermark_interval;
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
	return ParseFeatureScheduleInterval(parse_result);
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
	vector<string> feature_names;
	for (auto &item : feature_items) {
		auto name = transformer.Transform<QualifiedName>(item).name;
		feature_names.push_back(name);
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

	auto result = make_uniq<ServeFeatureStatement>();
	result->feature_names = std::move(feature_names);
	result->spine_table = std::move(spine_table);
	result->entity_column = std::move(entity_column);
	result->as_of_column = std::move(as_of_column);
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
