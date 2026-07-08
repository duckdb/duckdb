#include "duckdb/parser/peg/transformer/peg_transformer.hpp"
#include "duckdb/parser/parsed_data/create_feature_info.hpp"
#include "duckdb/parser/qualified_name.hpp"
#include "duckdb/parser/expression/function_expression.hpp"
#include "duckdb/parser/expression/constant_expression.hpp"
#include "duckdb/parser/statement/call_statement.hpp"
#include "duckdb/parser/statement/refresh_feature_statement.hpp"
#include "duckdb/common/types/interval.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/string_util.hpp"

namespace duckdb {

static constexpr interval_t ZERO_INTERVAL {0, 0, 0};

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

static interval_t ParseIntervalString(const string &interval_str, const string &context) {
	interval_t result {0, 0, 0};
	string error_message;
	if (!Interval::FromCString(interval_str.c_str(), interval_str.size(), result, &error_message, false)) {
		throw ParserException("Invalid interval in %s clause: %s", context, error_message);
	}
	if (!IsPositiveInterval(result)) {
		throw ParserException("%s interval must be positive", context);
	}
	return result;
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
	if (StringUtil::CIEquals(unit.name, "FeatureIntervalUnitDay") ||
	    StringUtil::CIEquals(unit.name, "FeatureIntervalUnitDays")) {
		return interval_t {0, IntervalCountToDays(count, context), 0};
	}
	if (StringUtil::CIEquals(unit.name, "FeatureIntervalUnitHour") ||
	    StringUtil::CIEquals(unit.name, "FeatureIntervalUnitHours")) {
		return interval_t {0, 0, IntervalCountToMicros(count, Interval::MICROS_PER_HOUR, context)};
	}
	if (StringUtil::CIEquals(unit.name, "FeatureIntervalUnitMinute") ||
	    StringUtil::CIEquals(unit.name, "FeatureIntervalUnitMinutes")) {
		return interval_t {0, 0, IntervalCountToMicros(count, Interval::MICROS_PER_MINUTE, context)};
	}
	if (StringUtil::CIEquals(unit.name, "FeatureIntervalUnitSecond") ||
	    StringUtil::CIEquals(unit.name, "FeatureIntervalUnitSeconds")) {
		return interval_t {0, 0, IntervalCountToMicros(count, Interval::MICROS_PER_SEC, context)};
	}
	throw InternalException("Unsupported feature interval unit");
}

static interval_t ParseFeatureInterval(ParseResult &parse_result, const string &context) {
	// FeatureInterval <- FeatureIntervalString / FeatureIntervalKeywordShorthand / FeatureIntervalShorthand
	auto &list_pr = parse_result.Cast<ListParseResult>();
	auto &choice = list_pr.Child<ChoiceParseResult>(0).GetResult();
	auto &clause = choice.Cast<ListParseResult>();
	if (StringUtil::CIEquals(choice.name, "FeatureIntervalString")) {
		return ParseIntervalString(clause.Child<StringLiteralParseResult>(1).result, context);
	}
	if (StringUtil::CIEquals(choice.name, "FeatureIntervalKeywordShorthand")) {
		auto count = ParsePositiveIntegerIntervalCount(clause.Child<NumberParseResult>(1).number, context);
		auto &unit = clause.Child<ListParseResult>(2).Child<ChoiceParseResult>(0).GetResult();
		return IntervalFromCountAndUnit(count, unit, context);
	}

	auto count = ParsePositiveIntegerIntervalCount(clause.Child<NumberParseResult>(0).number, context);
	if (StringUtil::CIEquals(choice.name, "FeatureIntervalShorthand")) {
		auto &unit = clause.Child<ListParseResult>(1).Child<ChoiceParseResult>(0).GetResult();
		return IntervalFromCountAndUnit(count, unit, context);
	}
	throw InternalException("Unsupported feature interval");
}

static interval_t ParseFeatureScheduleInterval(ParseResult &parse_result) {
	// FeatureScheduleClause <- 'EVERY' FeatureInterval
	auto &list_pr = parse_result.Cast<ListParseResult>();
	return ParseFeatureInterval(list_pr.Child<ListParseResult>(1), "EVERY");
}

unique_ptr<CreateStatement> PEGTransformerFactory::TransformCreateFeatureStmt(PEGTransformer &transformer,
                                                                              ParseResult &parse_result) {
	// CreateFeatureStmt <- 'FEATURE' IfNotExists? IdentifierOrStringLiteral 'ENTITY' IdentifierOrStringLiteral
	//                      ColumnIdList? 'TIMESTAMP' QualifiedName FeatureWindowClause? FeatureTTLClause?
	//                      FeatureScheduleClause? FeatureRetainClause? 'AS' Parens(SelectStatementInternal)
	auto &list_pr = parse_result.Cast<ListParseResult>();

	// index 0: 'FEATURE' keyword
	auto if_not_exists = list_pr.Child<OptionalParseResult>(1).HasResult();
	auto feature_name = transformer.Transform<QualifiedName>(list_pr.Child<ListParseResult>(2)).name;
	// index 3: 'ENTITY' keyword
	auto entity_table = transformer.Transform<QualifiedName>(list_pr.Child<ListParseResult>(4)).name;
	// index 5: ColumnIdList? — optional explicit entity key columns "ENTITY tbl (col, ...)"
	vector<string> user_entity_keys;
	auto &entity_keys_opt = list_pr.Child<OptionalParseResult>(5);
	if (entity_keys_opt.HasResult()) {
		user_entity_keys = transformer.Transform<vector<string>>(entity_keys_opt.GetResult());
	}
	// index 6: 'TIMESTAMP' keyword. The operand is a (possibly-qualified) column reference: an unqualified
	// column keeps schema == INVALID_SCHEMA, while "tbl.col" carries the table qualifier in the schema slot.
	auto timestamp_ref = transformer.Transform<QualifiedName>(list_pr.Child<ListParseResult>(7));
	auto timestamp_column = timestamp_ref.name;
	string timestamp_table;
	if (timestamp_ref.schema != INVALID_SCHEMA) {
		timestamp_table = timestamp_ref.schema;
	}
	// index 8: FeatureWindowClause? (default: 1 day)
	interval_t window_interval {0, 1, 0};
	auto &window_opt = list_pr.Child<OptionalParseResult>(8);
	if (window_opt.HasResult()) {
		auto &clause = window_opt.GetResult().Cast<ListParseResult>();
		window_interval = ParseFeatureInterval(clause.Child<ListParseResult>(1), "WINDOW");
	}
	// index 9: FeatureTTLClause? (default: zero interval = no staleness bound / TTL disabled).
	interval_t ttl_interval = ZERO_INTERVAL;
	auto &ttl_opt = list_pr.Child<OptionalParseResult>(9);
	if (ttl_opt.HasResult()) {
		auto &clause = ttl_opt.GetResult().Cast<ListParseResult>();
		ttl_interval = ParseFeatureInterval(clause.Child<ListParseResult>(1), "TTL");
	}
	// index 10: FeatureScheduleClause? (default: no schedule)
	bool has_schedule = false;
	interval_t schedule_interval {0, 0, 0};
	auto &schedule_opt = list_pr.Child<OptionalParseResult>(10);
	if (schedule_opt.HasResult()) {
		schedule_interval = transformer.Transform<interval_t>(schedule_opt.GetResult());
		has_schedule = true;
	}
	// index 11: FeatureRetainClause? (default: 1)
	int64_t retain_versions = 1;
	auto &retain_opt = list_pr.Child<OptionalParseResult>(11);
	if (retain_opt.HasResult()) {
		auto &clause = retain_opt.GetResult().Cast<ListParseResult>();
		retain_versions = std::stoll(clause.Child<NumberParseResult>(1).number);
	}
	// index 12: 'AS' keyword
	auto &select_parens = ExtractResultFromParens(list_pr.Child<ListParseResult>(13));
	auto query = transformer.Transform<unique_ptr<SelectStatement>>(select_parens);

	auto result = make_uniq<CreateStatement>();
	auto info = make_uniq<CreateFeatureInfo>();
	info->on_conflict = if_not_exists ? OnCreateConflict::IGNORE_ON_CONFLICT : OnCreateConflict::ERROR_ON_CONFLICT;
	info->feature_name = feature_name;
	info->entity_table = entity_table;
	info->user_entity_keys = std::move(user_entity_keys);
	info->timestamp_column = timestamp_column;
	info->timestamp_table = timestamp_table;
	info->window_interval = window_interval;
	info->ttl_interval = ttl_interval;
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

unique_ptr<SQLStatement> PEGTransformerFactory::TransformRefreshFeatureStatement(PEGTransformer &transformer,
                                                                                 ParseResult &parse_result) {
	// RefreshFeatureStatement <- 'REFRESH' 'FEATURE' IdentifierOrStringLiteral FeatureRefreshAtClause?
	// FeatureRefreshAtClause <- 'AT' StringLiteral
	auto &list_pr = parse_result.Cast<ListParseResult>();
	// index 0: 'REFRESH' keyword
	// index 1: 'FEATURE' keyword
	auto feature_name = transformer.Transform<QualifiedName>(list_pr.Child<ListParseResult>(2)).name;

	auto result = make_uniq<RefreshFeatureStatement>();
	result->feature_name = std::move(feature_name);
	// index 3: FeatureRefreshAtClause? (optional AT '<timestamp>')
	auto &at_opt = list_pr.Child<OptionalParseResult>(3);
	if (at_opt.HasResult()) {
		auto &clause = at_opt.GetResult().Cast<ListParseResult>();
		result->at_timestamp = clause.Child<StringLiteralParseResult>(1).result;
	}
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
