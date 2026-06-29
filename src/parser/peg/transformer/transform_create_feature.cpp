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

static optional_ptr<ParseResult> FindParseResultByName(ParseResult &parse_result, const string &name) {
	if (StringUtil::CIEquals(parse_result.name, name)) {
		return parse_result;
	}
	if (parse_result.type == ParseResultType::CHOICE) {
		return FindParseResultByName(parse_result.Cast<ChoiceParseResult>().GetResult(), name);
	}
	if (parse_result.type == ParseResultType::OPTIONAL) {
		auto &optional_pr = parse_result.Cast<OptionalParseResult>();
		return optional_pr.HasResult() ? FindParseResultByName(optional_pr.GetResult(), name) : nullptr;
	}
	if (parse_result.type == ParseResultType::REPEAT) {
		auto &repeat_pr = parse_result.Cast<RepeatParseResult>();
		for (auto &child : repeat_pr.GetChildren()) {
			auto result = FindParseResultByName(child.get(), name);
			if (result) {
				return result;
			}
		}
		return nullptr;
	}
	if (parse_result.type == ParseResultType::LIST) {
		auto &list_pr = parse_result.Cast<ListParseResult>();
		for (auto &child : list_pr.GetChildren()) {
			auto result = FindParseResultByName(child.get(), name);
			if (result) {
				return result;
			}
		}
	}
	return nullptr;
}

static void CollectParseResultsByName(ParseResult &parse_result, const string &name,
                                      vector<reference<ParseResult>> &results) {
	if (StringUtil::CIEquals(parse_result.name, name)) {
		results.push_back(parse_result);
		return;
	}
	if (parse_result.type == ParseResultType::CHOICE) {
		CollectParseResultsByName(parse_result.Cast<ChoiceParseResult>().GetResult(), name, results);
		return;
	}
	if (parse_result.type == ParseResultType::OPTIONAL) {
		auto &optional_pr = parse_result.Cast<OptionalParseResult>();
		if (optional_pr.HasResult()) {
			CollectParseResultsByName(optional_pr.GetResult(), name, results);
		}
		return;
	}
	if (parse_result.type == ParseResultType::REPEAT) {
		auto &repeat_pr = parse_result.Cast<RepeatParseResult>();
		for (auto &child : repeat_pr.GetChildren()) {
			CollectParseResultsByName(child.get(), name, results);
		}
		return;
	}
	if (parse_result.type == ParseResultType::LIST) {
		auto &list_pr = parse_result.Cast<ListParseResult>();
		for (auto &child : list_pr.GetChildren()) {
			CollectParseResultsByName(child.get(), name, results);
		}
	}
}

static optional_ptr<ParseResult> FindFirstIdentifierOrString(ParseResult &parse_result) {
	if (parse_result.type == ParseResultType::IDENTIFIER || parse_result.type == ParseResultType::STRING) {
		return parse_result;
	}
	if (parse_result.type == ParseResultType::CHOICE) {
		return FindFirstIdentifierOrString(parse_result.Cast<ChoiceParseResult>().GetResult());
	}
	if (parse_result.type == ParseResultType::OPTIONAL) {
		auto &optional_pr = parse_result.Cast<OptionalParseResult>();
		return optional_pr.HasResult() ? FindFirstIdentifierOrString(optional_pr.GetResult()) : nullptr;
	}
	if (parse_result.type == ParseResultType::REPEAT) {
		auto &repeat_pr = parse_result.Cast<RepeatParseResult>();
		for (auto &child : repeat_pr.GetChildren()) {
			auto result = FindFirstIdentifierOrString(child.get());
			if (result) {
				return result;
			}
		}
		return nullptr;
	}
	if (parse_result.type == ParseResultType::LIST) {
		auto &list_pr = parse_result.Cast<ListParseResult>();
		for (auto &child : list_pr.GetChildren()) {
			auto result = FindFirstIdentifierOrString(child.get());
			if (result) {
				return result;
			}
		}
	}
	return nullptr;
}

static void CollectIdentifierOrStringNames(ParseResult &parse_result, vector<string> &result) {
	if (parse_result.type == ParseResultType::IDENTIFIER) {
		result.push_back(parse_result.Cast<IdentifierParseResult>().identifier);
		return;
	}
	if (parse_result.type == ParseResultType::STRING) {
		result.push_back(parse_result.Cast<StringLiteralParseResult>().result);
		return;
	}
	if (parse_result.type == ParseResultType::CHOICE) {
		CollectIdentifierOrStringNames(parse_result.Cast<ChoiceParseResult>().GetResult(), result);
		return;
	}
	if (parse_result.type == ParseResultType::OPTIONAL) {
		auto &optional_pr = parse_result.Cast<OptionalParseResult>();
		if (optional_pr.HasResult()) {
			CollectIdentifierOrStringNames(optional_pr.GetResult(), result);
		}
		return;
	}
	if (parse_result.type == ParseResultType::REPEAT) {
		auto &repeat_pr = parse_result.Cast<RepeatParseResult>();
		for (auto &child : repeat_pr.GetChildren()) {
			CollectIdentifierOrStringNames(child.get(), result);
		}
		return;
	}
	if (parse_result.type == ParseResultType::LIST) {
		auto &list_pr = parse_result.Cast<ListParseResult>();
		for (auto &child : list_pr.GetChildren()) {
			CollectIdentifierOrStringNames(child.get(), result);
		}
	}
}

static string ExtractFirstIdentifierName(ParseResult &parse_result, const string &context) {
	auto result = FindFirstIdentifierOrString(parse_result);
	if (!result) {
		throw InternalException("Expected identifier in %s", context);
	}
	if (result->type == ParseResultType::IDENTIFIER) {
		return result->Cast<IdentifierParseResult>().identifier;
	}
	return result->Cast<StringLiteralParseResult>().result;
}

unique_ptr<CreateStatement> PEGTransformerFactory::TransformCreateFeatureStmt(PEGTransformer &transformer,
                                                                              ParseResult &parse_result) {
	// CreateFeatureStmt <- 'FEATURE' IfNotExists? IdentifierOrStringLiteral 'TIMESTAMP' IdentifierOrStringLiteral
	//                      FeatureWindowClause? FeatureWatermarkClause?
	//                      FeatureRefreshClause? FeatureScheduleClause? FeatureRetainClause? 'AS'
	//                      Parens(SelectStatementInternal)
	auto &list_pr = parse_result.Cast<ListParseResult>();

	// index 0: 'FEATURE' keyword
	auto if_not_exists = list_pr.Child<OptionalParseResult>(1).HasResult();
	auto feature_name = transformer.Transform<QualifiedName>(list_pr.Child<ListParseResult>(2)).name;
	// index 3: 'TIMESTAMP' keyword
	auto timestamp_column = transformer.Transform<QualifiedName>(list_pr.Child<ListParseResult>(4)).name;
	// index 5: FeatureWindowClause? (default: 1 day)
	interval_t window_interval {0, 1, 0};
	auto &window_opt = list_pr.Child<OptionalParseResult>(5);
	if (window_opt.HasResult()) {
		auto &clause = window_opt.GetResult().Cast<ListParseResult>();
		window_interval = ParseFeatureInterval(clause.Child<ListParseResult>(1), "WINDOW");
	}
	// index 6: FeatureWatermarkClause? (default: zero interval)
	interval_t watermark_interval = ZERO_INTERVAL;
	auto &watermark_opt = list_pr.Child<OptionalParseResult>(6);
	if (watermark_opt.HasResult()) {
		auto &clause = watermark_opt.GetResult().Cast<ListParseResult>();
		watermark_interval = ParseFeatureInterval(clause.Child<ListParseResult>(1), "WATERMARK");
	}
	// index 7: FeatureRefreshClause? (default: INCREMENTAL)
	FeatureRefreshMode refresh_mode = FeatureRefreshMode::INCREMENTAL;
	auto &refresh_opt = list_pr.Child<OptionalParseResult>(7);
	if (refresh_opt.HasResult()) {
		auto &clause = refresh_opt.GetResult().Cast<ListParseResult>();
		refresh_mode = transformer.Transform<FeatureRefreshMode>(clause.Child<ListParseResult>(1));
	}
	// index 8: FeatureScheduleClause? (default: no schedule)
	bool has_schedule = false;
	interval_t schedule_interval {0, 0, 0};
	auto &schedule_opt = list_pr.Child<OptionalParseResult>(8);
	if (schedule_opt.HasResult()) {
		schedule_interval = transformer.Transform<interval_t>(schedule_opt.GetResult());
		has_schedule = true;
	}
	// index 9: FeatureRetainClause? (default: 1)
	int64_t retain_versions = 1;
	auto &retain_opt = list_pr.Child<OptionalParseResult>(9);
	if (retain_opt.HasResult()) {
		auto &clause = retain_opt.GetResult().Cast<ListParseResult>();
		retain_versions = std::stoll(clause.Child<NumberParseResult>(1).number);
	}
	// index 10: 'AS' keyword
	auto &select_parens = ExtractResultFromParens(list_pr.Child<ListParseResult>(11));
	auto query = transformer.Transform<unique_ptr<SelectStatement>>(select_parens);

	auto result = make_uniq<CreateStatement>();
	auto info = make_uniq<CreateFeatureInfo>();
	info->on_conflict = if_not_exists ? OnCreateConflict::IGNORE_ON_CONFLICT : OnCreateConflict::ERROR_ON_CONFLICT;
	info->feature_name = feature_name;
	info->timestamp_column = timestamp_column;
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
	// ServeFeatureStatement <- 'SERVE' ServeFeatureKw List(ServeFeatureItem) 'FOR' IdentifierOrStringLiteral
	// ServeFeatureEntity? ServeFeatureAsOf?
	auto &list_pr = parse_result.Cast<ListParseResult>();
	// index 0: 'SERVE' keyword
	// index 1: ServeFeatureKw ('FEATURE' or 'FEATURES')
	// index 2: List(ServeFeatureItem) - feature names and optional entity mappings
	auto feature_items = ExtractParseResultsFromList(list_pr.Child<ListParseResult>(2));
	vector<string> feature_names;
	vector<vector<FeatureServeEntityMapping>> entity_mappings;
	for (auto &item : feature_items) {
		auto &feature_item = item.get().Cast<ListParseResult>();
		auto name = ExtractFirstIdentifierName(feature_item.Child<ListParseResult>(0), "SERVE FEATURE item");
		feature_names.push_back(name);

		vector<FeatureServeEntityMapping> mappings;
		auto &mapping_opt = feature_item.Child<OptionalParseResult>(1);
		if (mapping_opt.HasResult()) {
			auto &mapping_clause = mapping_opt.GetResult().Cast<ListParseResult>();
			auto mapping_list_result = FindParseResultByName(mapping_clause, "ServeFeatureEntityList");
			if (mapping_list_result) {
				vector<reference<ParseResult>> mapping_items;
				CollectParseResultsByName(*mapping_list_result, "ServeFeatureEntityMapping", mapping_items);
				for (auto &mapping_item_ref : mapping_items) {
					auto &mapping_item = mapping_item_ref.get();
					auto mapping_pair = FindParseResultByName(mapping_item, "ServeFeatureEntityMappingPair");
					auto &mapping_source = mapping_pair ? *mapping_pair : mapping_item;
					vector<string> identifiers;
					CollectIdentifierOrStringNames(mapping_source, identifiers);
					if (identifiers.empty()) {
						throw InternalException("Expected identifier in SERVE FEATURE entity mapping");
					}
					FeatureServeEntityMapping mapping;
					mapping.feature_column = identifiers[0];
					mapping.spine_column = mapping.feature_column;
					if (identifiers.size() > 1) {
						mapping.spine_column = identifiers[1];
					}
					mappings.push_back(std::move(mapping));
				}
			} else {
				FeatureServeEntityMapping mapping;
				mapping.spine_column = ExtractFirstIdentifierName(mapping_clause, "SERVE FEATURE entity mapping");
				mappings.push_back(std::move(mapping));
			}
		}
		entity_mappings.push_back(std::move(mappings));
	}
	// index 3: 'FOR' keyword
	auto spine_table = ExtractFirstIdentifierName(list_pr.Child<ListParseResult>(4), "SERVE FEATURE spine table");

	// index 5: optional ServeFeatureEntity <- 'ENTITY' IdentifierOrStringLiteral
	string entity_column;
	auto &entity_opt = list_pr.Child<OptionalParseResult>(5);
	if (entity_opt.HasResult()) {
		auto &entity_list = entity_opt.GetResult().Cast<ListParseResult>();
		// index 0: 'ENTITY' keyword, index 1: identifier
		entity_column = ExtractFirstIdentifierName(entity_list, "SERVE FEATURE ENTITY clause");
	}

	// index 6: optional ServeFeatureAsOf <- 'ASOF' IdentifierOrStringLiteral
	string as_of_column;
	auto &asof_opt = list_pr.Child<OptionalParseResult>(6);
	if (asof_opt.HasResult()) {
		auto &asof_list = asof_opt.GetResult().Cast<ListParseResult>();
		// index 0: 'ASOF' keyword, index 1: identifier
		as_of_column = ExtractFirstIdentifierName(asof_list, "SERVE FEATURE ASOF clause");
	}

	auto result = make_uniq<ServeFeatureStatement>();
	result->feature_names = std::move(feature_names);
	result->entity_mappings = std::move(entity_mappings);
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
