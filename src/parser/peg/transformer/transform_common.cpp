#include "duckdb/common/extra_type_info.hpp"
#include "duckdb/common/enums/date_part_specifier.hpp"
#include "duckdb/common/operator/cast_operators.hpp"
#include "duckdb/common/types/decimal.hpp"
#include "duckdb/parser/peg/transformer/peg_transformer.hpp"
#include "duckdb/common/types/hugeint.hpp"
#include "duckdb/common/limits.hpp"
#include "duckdb/common/operator/negate.hpp"
#include "duckdb/parser/expression/type_expression.hpp"
#include "duckdb/common/types/bignum.hpp"

namespace duckdb {

string PEGTransformerFactory::TransformIdentifierOrKeyword(PEGTransformer &transformer, ParseResult &parse_result) {
	if (parse_result.type == ParseResultType::IDENTIFIER) {
		return parse_result.Cast<IdentifierParseResult>().identifier.GetIdentifierName();
	}
	if (parse_result.type == ParseResultType::KEYWORD) {
		return parse_result.Cast<KeywordParseResult>().keyword;
	}
	if (parse_result.type == ParseResultType::CHOICE) {
		auto &choice_pr = parse_result.Cast<ChoiceParseResult>();
		return transformer.Transform<string>(choice_pr.GetResult());
	}
	if (parse_result.type == ParseResultType::LIST) {
		auto &list_pr = parse_result.Cast<ListParseResult>();
		for (auto child : list_pr.GetChildren()) {
			if (child.get().type == ParseResultType::LIST &&
			    child.get().Cast<ListParseResult>().GetChildren().empty()) {
				continue;
			}
			if (child.get().type == ParseResultType::CHOICE) {
				auto &choice_result = child.get().Cast<ChoiceParseResult>().GetResult();
				if (choice_result.type == ParseResultType::IDENTIFIER) {
					return choice_result.Cast<IdentifierParseResult>().identifier.GetIdentifierName();
				}
				if (choice_result.type == ParseResultType::KEYWORD) {
					return choice_result.Cast<KeywordParseResult>().keyword;
				}
				return transformer.Transform<string>(choice_result);
			}
			if (child.get().type == ParseResultType::IDENTIFIER) {
				return child.get().Cast<IdentifierParseResult>().identifier.GetIdentifierName();
			}
			throw InternalException("Unexpected IdentifierOrKeyword type encountered %s.",
			                        ParseResultToString(child.get().type));
		}
	}
	throw ParserException("Unexpected ParseResult type in identifier transformation.");
}

LogicalType PEGTransformerFactory::TransformType(PEGTransformer &transformer,
                                                 unique_ptr<ParsedExpression> type_variations,
                                                 const optional<vector<int64_t>> &array_bounds) {
	auto type = std::move(type_variations);
	if (array_bounds) {
		for (auto array_size : *array_bounds) {
			vector<unique_ptr<ParsedExpression>> children_types;
			children_types.push_back(std::move(type));

			if (array_size < 0) {
				type = make_uniq<TypeExpression>(Identifier("list"), std::move(children_types));
			} else {
				children_types.push_back(make_uniq<ConstantExpression>(Value::BIGINT(array_size)));
				type = make_uniq<TypeExpression>(Identifier("array"), std::move(children_types));
			}
		}
	}
	return LogicalType::UNBOUND(std::move(type));
}

int64_t PEGTransformerFactory::TransformArrayKeyword(PEGTransformer &transformer) {
	return -1;
}

int64_t PEGTransformerFactory::TransformSquareBracketsArray(PEGTransformer &transformer,
                                                            optional<unique_ptr<ParsedExpression>> expression) {
	if (!expression) {
		// Empty array so we return -1 to signify it's a list
		return -1;
	}
	auto &array_size = *expression;
	if (array_size->GetExpressionClass() != ExpressionClass::CONSTANT) {
		throw ParserException("Expected a constant number as array size");
	}
	auto &const_number = array_size->Cast<ConstantExpression>();
	if (!const_number.GetValue().type().IsIntegral()) {
		throw BinderException("Expected an integer as array bound instead of %s", const_number.GetValue().ToString());
	}
	auto number_val = const_number.GetValue().GetValue<int64_t>();
	if (number_val < 0) {
		throw ParserException("Array size must be greater than 0");
	}
	return number_val;
}

unique_ptr<ParsedExpression>
PEGTransformerFactory::TransformTimeType(PEGTransformer &transformer, const LogicalTypeId &time_or_timestamp,
                                         optional<vector<unique_ptr<ParsedExpression>>> type_modifiers,
                                         const optional<bool> &time_zone) {
	auto type = time_or_timestamp;
	vector<unique_ptr<ParsedExpression>> modifiers;
	if (type_modifiers) {
		modifiers = std::move(*type_modifiers);
	}
	auto with_timezone = time_zone && *time_zone;
	if (type == LogicalTypeId::TIME) {
		if (!modifiers.empty()) {
			throw ParserException("Type TIME does not allow any modifiers");
		}
		if (with_timezone) {
			return make_uniq<TypeExpression>(Identifier(EnumUtil::ToString(LogicalType::TIME_TZ)),
			                                 vector<unique_ptr<ParsedExpression>> {});
		}
		return make_uniq<TypeExpression>(Identifier(EnumUtil::ToString(LogicalType::TIME)),
		                                 vector<unique_ptr<ParsedExpression>> {});
	}
	if (type == LogicalTypeId::TIMESTAMP) {
		if (modifiers.empty()) {
			if (with_timezone) {
				return make_uniq<TypeExpression>(Identifier(EnumUtil::ToString(LogicalType::TIMESTAMP_TZ)),
				                                 vector<unique_ptr<ParsedExpression>> {});
			}
			return make_uniq<TypeExpression>(Identifier(EnumUtil::ToString(LogicalType::TIMESTAMP)),
			                                 vector<unique_ptr<ParsedExpression>> {});
		}
		if (modifiers.size() > 1) {
			throw ParserException("TIMESTAMP only supports a single modifier");
		}
		if (modifiers[0]->GetExpressionClass() != ExpressionClass::CONSTANT) {
			throw ParserException("Expected a constant expression for timestamp precision");
		}
		auto timestamp_precision = modifiers[0]->Cast<ConstantExpression>().GetValue().GetValue<int64_t>();
		if (timestamp_precision > 10) {
			throw ParserException("TIMESTAMP only supports until nano-second precision (9)");
		}
		if (timestamp_precision < 0) {
			throw ParserException("TIMESTAMP precision should be between 0 and 10 (inclusive)");
		}
		if (timestamp_precision == 0) {
			return make_uniq<TypeExpression>(Identifier(EnumUtil::ToString(LogicalType::TIMESTAMP_S)),
			                                 vector<unique_ptr<ParsedExpression>> {});
		}
		if (timestamp_precision <= 3) {
			return make_uniq<TypeExpression>(Identifier(EnumUtil::ToString(LogicalType::TIMESTAMP_MS)),
			                                 vector<unique_ptr<ParsedExpression>> {});
		}
		if (timestamp_precision <= 6) {
			// Corresponds to microseconds, which is the default TIMESTAMP
			return make_uniq<TypeExpression>(Identifier(EnumUtil::ToString(LogicalType::TIMESTAMP)),
			                                 vector<unique_ptr<ParsedExpression>> {});
		}
		return make_uniq<TypeExpression>(Identifier(EnumUtil::ToString(LogicalType::TIMESTAMP_NS)),
		                                 vector<unique_ptr<ParsedExpression>> {});
	}
	throw ParserException("Unexpected time type encountered");
}

bool PEGTransformerFactory::TransformTimeZone(PEGTransformer &transformer, const bool &with_or_without) {
	return with_or_without;
}

bool PEGTransformerFactory::TransformWithRule(PEGTransformer &transformer) {
	return true;
}

bool PEGTransformerFactory::TransformWithoutRule(PEGTransformer &transformer) {
	return false;
}

LogicalTypeId PEGTransformerFactory::TransformTimeTypeId(PEGTransformer &transformer) {
	return LogicalTypeId::TIME;
}

LogicalTypeId PEGTransformerFactory::TransformTimestampTypeId(PEGTransformer &transformer) {
	return LogicalTypeId::TIMESTAMP;
}

unique_ptr<ParsedExpression> PEGTransformerFactory::TransformSimpleNumericType(PEGTransformer &transformer,
                                                                               const string &child) {
	return make_uniq<TypeExpression>(Identifier(child), vector<unique_ptr<ParsedExpression>> {});
}

string PEGTransformerFactory::TransformIntType(PEGTransformer &transformer) {
	return LogicalTypeIdToString(LogicalTypeId::INTEGER);
}

string PEGTransformerFactory::TransformIntegerType(PEGTransformer &transformer) {
	return LogicalTypeIdToString(LogicalTypeId::INTEGER);
}

string PEGTransformerFactory::TransformSmallintType(PEGTransformer &transformer) {
	return LogicalTypeIdToString(LogicalTypeId::SMALLINT);
}

string PEGTransformerFactory::TransformBigintType(PEGTransformer &transformer) {
	return LogicalTypeIdToString(LogicalTypeId::BIGINT);
}

string PEGTransformerFactory::TransformRealType(PEGTransformer &transformer) {
	return LogicalTypeIdToString(LogicalTypeId::FLOAT);
}

string PEGTransformerFactory::TransformBooleanType(PEGTransformer &transformer) {
	return LogicalTypeIdToString(LogicalTypeId::BOOLEAN);
}

string PEGTransformerFactory::TransformDoubleType(PEGTransformer &transformer) {
	return LogicalTypeIdToString(LogicalTypeId::DOUBLE);
}

unique_ptr<ParsedExpression>
PEGTransformerFactory::TransformFloatType(PEGTransformer &transformer,
                                          optional<unique_ptr<ParsedExpression>> number_literal) {
	return make_uniq<TypeExpression>(Identifier("FLOAT"), vector<unique_ptr<ParsedExpression>> {});
}

unique_ptr<ParsedExpression>
PEGTransformerFactory::TransformDecimalType(PEGTransformer &transformer,
                                            optional<vector<unique_ptr<ParsedExpression>>> type_modifiers) {
	vector<unique_ptr<ParsedExpression>> modifiers;
	if (type_modifiers) {
		modifiers = std::move(*type_modifiers);
	}
	return make_uniq<TypeExpression>(Identifier("DECIMAL"), std::move(modifiers));
}

unique_ptr<ParsedExpression>
PEGTransformerFactory::TransformDecType(PEGTransformer &transformer,
                                        optional<vector<unique_ptr<ParsedExpression>>> type_modifiers) {
	return TransformDecimalType(transformer, std::move(type_modifiers));
}

unique_ptr<ParsedExpression>
PEGTransformerFactory::TransformNumericModType(PEGTransformer &transformer,
                                               optional<vector<unique_ptr<ParsedExpression>>> type_modifiers) {
	return TransformDecimalType(transformer, std::move(type_modifiers));
}

vector<unique_ptr<ParsedExpression>>
PEGTransformerFactory::TransformTypeModifiers(PEGTransformer &transformer,
                                              optional<vector<unique_ptr<ParsedExpression>>> expression) {
	if (!expression) {
		return vector<unique_ptr<ParsedExpression>> {};
	}
	for (auto &expr : *expression) {
		if (expr->GetExpressionClass() != ExpressionClass::CONSTANT) {
			throw ParserException("Expected a constant as type modifier");
		}
	}
	return std::move(*expression);
}

unique_ptr<ParsedExpression>
PEGTransformerFactory::TransformCharacterSimpleType(PEGTransformer &transformer,
                                                    optional<vector<unique_ptr<ParsedExpression>>> type_modifiers) {
	vector<unique_ptr<ParsedExpression>> modifiers;
	if (type_modifiers) {
		modifiers = std::move(*type_modifiers);
	}
	return make_uniq<TypeExpression>(Identifier("VARCHAR"), std::move(modifiers));
}

unique_ptr<ParsedExpression>
PEGTransformerFactory::TransformQualifiedSimpleType(PEGTransformer &transformer,
                                                    const QualifiedName &qualified_type_name,
                                                    optional<vector<unique_ptr<ParsedExpression>>> type_modifiers) {
	vector<unique_ptr<ParsedExpression>> modifiers;
	if (type_modifiers) {
		modifiers = std::move(*type_modifiers);
	}
	return make_uniq<TypeExpression>(qualified_type_name, std::move(modifiers));
}

QualifiedName PEGTransformerFactory::TransformTypeNameAsQualifiedName(PEGTransformer &transformer,
                                                                      const Identifier &type_name) {
	QualifiedName result(type_name);
	return result;
}

QualifiedName PEGTransformerFactory::TransformSchemaReservedTypeName(PEGTransformer &transformer,
                                                                     const Identifier &schema_qualification,
                                                                     const Identifier &reserved_type_name) {
	QualifiedName result({schema_qualification}, reserved_type_name);
	return result;
}

QualifiedName PEGTransformerFactory::TransformCatalogReservedSchemaTypeName(
    PEGTransformer &transformer, const Identifier &catalog_qualification,
    const Identifier &reserved_schema_qualification, const Identifier &reserved_type_name) {
	QualifiedName result(catalog_qualification, reserved_schema_qualification, reserved_type_name);
	return result;
}

unique_ptr<ParsedExpression> PEGTransformerFactory::TransformMapType(PEGTransformer &transformer,
                                                                     const vector<LogicalType> &type) {
	if (type.size() != 2) {
		throw ParserException("Map type needs exactly two entries, key and value type.");
	}
	vector<unique_ptr<ParsedExpression>> map_children;
	map_children.push_back(UnboundType::GetTypeExpression(type[0])->Copy());
	map_children.push_back(UnboundType::GetTypeExpression(type[1])->Copy());
	return make_uniq<TypeExpression>(Identifier("MAP"), std::move(map_children));
}

unique_ptr<ParsedExpression> PEGTransformerFactory::TransformTupleType(PEGTransformer &transformer,
                                                                       const vector<LogicalType> &type) {
	vector<unique_ptr<ParsedExpression>> tuple_children;
	for (auto &child : type) {
		tuple_children.push_back(UnboundType::GetTypeExpression(child)->Copy());
	}
	return make_uniq<TypeExpression>(Identifier("TUPLE"), std::move(tuple_children));
}

unique_ptr<ParsedExpression>
PEGTransformerFactory::TransformRowType(PEGTransformer &transformer,
                                        const optional<child_list_t<LogicalType>> &col_id_type_list) {
	vector<unique_ptr<ParsedExpression>> struct_children;
	if (col_id_type_list) {
		for (auto &child : *col_id_type_list) {
			auto &type_expr = UnboundType::GetTypeExpression(child.second);
			auto new_type_expr = type_expr->Copy();
			new_type_expr->SetAlias(child.first);
			struct_children.push_back(std::move(new_type_expr));
		}
	}
	return make_uniq<TypeExpression>(Identifier("STRUCT"), std::move(struct_children));
}

unique_ptr<ParsedExpression>
PEGTransformerFactory::TransformGeometryType(PEGTransformer &transformer,
                                             optional<unique_ptr<ParsedExpression>> expression) {
	if (!expression) {
		return make_uniq<TypeExpression>(Identifier("GEOMETRY"), vector<unique_ptr<ParsedExpression>> {});
	}
	auto geo_modifier = std::move(*expression);
	vector<unique_ptr<ParsedExpression>> geo_children;
	if (geo_modifier->GetExpressionClass() != ExpressionClass::CONSTANT) {
		throw ParserException("Expected a constant as type modifier");
	}
	geo_children.push_back(std::move(geo_modifier));
	return make_uniq<TypeExpression>(Identifier("GEOMETRY"), std::move(geo_children));
}

unique_ptr<ParsedExpression> PEGTransformerFactory::TransformVariantType(PEGTransformer &transformer) {
	return make_uniq<TypeExpression>(Identifier("VARIANT"), vector<unique_ptr<ParsedExpression>> {});
}

unique_ptr<ParsedExpression>
PEGTransformerFactory::TransformUnionType(PEGTransformer &transformer,
                                          const child_list_t<LogicalType> &col_id_type_list) {
	identifier_set_t union_names;
	vector<unique_ptr<ParsedExpression>> union_children;
	for (auto &colid : col_id_type_list) {
		union_names.insert(colid.first);
		auto &type_expr = UnboundType::GetTypeExpression(colid.second);
		auto new_type_expr = type_expr->Copy();
		new_type_expr->SetAlias(colid.first);
		union_children.push_back(std::move(new_type_expr));
	}
	return make_uniq<TypeExpression>(Identifier("UNION"), std::move(union_children));
}

child_list_t<LogicalType>
PEGTransformerFactory::TransformColIdTypeList(PEGTransformer &transformer,
                                              const vector<pair<Identifier, LogicalType>> &col_id_type) {
	return col_id_type;
}

pair<Identifier, LogicalType> PEGTransformerFactory::TransformColIdType(PEGTransformer &transformer,
                                                                        const Identifier &col_id,
                                                                        const LogicalType &type) {
	return make_pair(Identifier(col_id), type);
}

unique_ptr<ParsedExpression> PEGTransformerFactory::TransformBitType(
    PEGTransformer &transformer, const bool &has_result,
    optional<vector<unique_ptr<ParsedExpression>>> expression) { // NOLINT(performance-unnecessary-value-param)
	return make_uniq<TypeExpression>(Identifier("BIT"), vector<unique_ptr<ParsedExpression>> {});
}

unique_ptr<ParsedExpression> PEGTransformerFactory::TransformIntervalWithoutSpecifier(PEGTransformer &transformer) {
	return make_uniq<TypeExpression>(Identifier("INTERVAL"), vector<unique_ptr<ParsedExpression>> {});
}

DatePartSpecifier PEGTransformerFactory::TransformIntervalToIntervalAsType(PEGTransformer &transformer,
                                                                           ParseResult &parse_result) {
	return DatePartSpecifier::INVALID;
}

unique_ptr<ParsedExpression>
PEGTransformerFactory::TransformIntervalWithRangeSpecifier(PEGTransformer &transformer,
                                                           const DatePartSpecifier &interval_to_interval_as_type) {
	return make_uniq<TypeExpression>(Identifier("INTERVAL"), vector<unique_ptr<ParsedExpression>> {});
}

unique_ptr<ParsedExpression>
PEGTransformerFactory::TransformIntervalWithSimpleSpecifier(PEGTransformer &transformer,
                                                            const DatePartSpecifier &interval) {
	return make_uniq<TypeExpression>(Identifier("INTERVAL"), vector<unique_ptr<ParsedExpression>> {});
}

DatePartSpecifier PEGTransformerFactory::TransformYearKeyword(PEGTransformer &transformer) {
	return DatePartSpecifier::YEAR;
}

DatePartSpecifier PEGTransformerFactory::TransformMonthKeyword(PEGTransformer &transformer) {
	return DatePartSpecifier::MONTH;
}

DatePartSpecifier PEGTransformerFactory::TransformDayKeyword(PEGTransformer &transformer) {
	return DatePartSpecifier::DAY;
}

DatePartSpecifier PEGTransformerFactory::TransformHourKeyword(PEGTransformer &transformer) {
	return DatePartSpecifier::HOUR;
}

DatePartSpecifier PEGTransformerFactory::TransformMinuteKeyword(PEGTransformer &transformer) {
	return DatePartSpecifier::MINUTE;
}

DatePartSpecifier PEGTransformerFactory::TransformSecondKeyword(PEGTransformer &transformer) {
	return DatePartSpecifier::SECOND;
}

DatePartSpecifier PEGTransformerFactory::TransformMillisecondKeyword(PEGTransformer &transformer) {
	return DatePartSpecifier::MILLISECONDS;
}

DatePartSpecifier PEGTransformerFactory::TransformMicrosecondKeyword(PEGTransformer &transformer) {
	return DatePartSpecifier::MICROSECONDS;
}

DatePartSpecifier PEGTransformerFactory::TransformWeekKeyword(PEGTransformer &transformer) {
	return DatePartSpecifier::WEEK;
}

DatePartSpecifier PEGTransformerFactory::TransformQuarterKeyword(PEGTransformer &transformer) {
	return DatePartSpecifier::QUARTER;
}

DatePartSpecifier PEGTransformerFactory::TransformDecadeKeyword(PEGTransformer &transformer) {
	return DatePartSpecifier::DECADE;
}

DatePartSpecifier PEGTransformerFactory::TransformCenturyKeyword(PEGTransformer &transformer) {
	return DatePartSpecifier::CENTURY;
}

DatePartSpecifier PEGTransformerFactory::TransformMillenniumKeyword(PEGTransformer &transformer) {
	return DatePartSpecifier::MILLENNIUM;
}

static DatePartSpecifier UnsupportedIntervalRange(DatePartSpecifier first_interval, DatePartSpecifier second_interval) {
	throw ParserException("%s TO %s is not supported", EnumUtil::ToString(first_interval),
	                      EnumUtil::ToString(second_interval));
}

DatePartSpecifier PEGTransformerFactory::TransformYearToMonth(PEGTransformer &transformer,
                                                              const DatePartSpecifier &year_keyword,
                                                              const DatePartSpecifier &month_keyword) {
	return UnsupportedIntervalRange(year_keyword, month_keyword);
}

DatePartSpecifier PEGTransformerFactory::TransformDayToHour(PEGTransformer &transformer,
                                                            const DatePartSpecifier &day_keyword,
                                                            const DatePartSpecifier &hour_keyword) {
	return UnsupportedIntervalRange(day_keyword, hour_keyword);
}

DatePartSpecifier PEGTransformerFactory::TransformDayToMinute(PEGTransformer &transformer,
                                                              const DatePartSpecifier &day_keyword,
                                                              const DatePartSpecifier &minute_keyword) {
	return UnsupportedIntervalRange(day_keyword, minute_keyword);
}

DatePartSpecifier PEGTransformerFactory::TransformDayToSecond(PEGTransformer &transformer,
                                                              const DatePartSpecifier &day_keyword,
                                                              const DatePartSpecifier &second_keyword) {
	return UnsupportedIntervalRange(day_keyword, second_keyword);
}

DatePartSpecifier PEGTransformerFactory::TransformHourToMinute(PEGTransformer &transformer,
                                                               const DatePartSpecifier &hour_keyword,
                                                               const DatePartSpecifier &minute_keyword) {
	return UnsupportedIntervalRange(hour_keyword, minute_keyword);
}

DatePartSpecifier PEGTransformerFactory::TransformHourToSecond(PEGTransformer &transformer,
                                                               const DatePartSpecifier &hour_keyword,
                                                               const DatePartSpecifier &second_keyword) {
	return UnsupportedIntervalRange(hour_keyword, second_keyword);
}

DatePartSpecifier PEGTransformerFactory::TransformMinuteToSecond(PEGTransformer &transformer,
                                                                 const DatePartSpecifier &minute_keyword,
                                                                 const DatePartSpecifier &second_keyword) {
	return UnsupportedIntervalRange(minute_keyword, second_keyword);
}

unique_ptr<ParsedExpression> PEGTransformerFactory::TryNegateValue(const ConstantExpression &expr) {
	auto &val = expr.GetValue();

	switch (val.type().id()) {
	case LogicalTypeId::INTEGER: {
		auto raw = val.GetValue<int32_t>();
		if (!NegateOperator::CanNegate<int32_t>(raw)) {
			return make_uniq<ConstantExpression>(Value::BIGINT(-static_cast<int64_t>(raw)));
		}
		return make_uniq<ConstantExpression>(Value::INTEGER(-raw));
	}
	case LogicalTypeId::BIGINT: {
		auto raw = val.GetValue<int64_t>();
		if (!NegateOperator::CanNegate<int64_t>(raw)) {
			return make_uniq<ConstantExpression>(Value::HUGEINT(-static_cast<hugeint_t>(raw)));
		}
		return make_uniq<ConstantExpression>(Value::BIGINT(-raw));
	}
	case LogicalTypeId::HUGEINT: {
		auto raw = val.GetValue<hugeint_t>();
		if (!NegateOperator::CanNegate<hugeint_t>(raw)) {
			return nullptr;
		}
		return make_uniq<ConstantExpression>(Value::HUGEINT(-raw));
	}
	case LogicalTypeId::UHUGEINT: {
		auto uval = val.GetValue<uhugeint_t>();
		uhugeint_t abs_min_hugeint = static_cast<uhugeint_t>(NumericLimits<hugeint_t>::Maximum()) + 1;

		if (uval == abs_min_hugeint) {
			return make_uniq<ConstantExpression>(Value::HUGEINT(NumericLimits<hugeint_t>::Minimum()));
		}
		if (uval < abs_min_hugeint) {
			return make_uniq<ConstantExpression>(Value::HUGEINT(-static_cast<hugeint_t>(uval)));
		}
		return nullptr;
	}
	case LogicalTypeId::DOUBLE:
		return make_uniq<ConstantExpression>(Value::DOUBLE(-val.GetValue<double>()));
	default:
		return nullptr;
	}
}

unique_ptr<ParsedExpression> PEGTransformerFactory::ConvertNumberToValue(string val) {
	string_t str_val(val);
	bool try_cast_as_integer = true;
	bool try_cast_as_decimal = true;
	optional_idx decimal_position = optional_idx::Invalid();
	idx_t num_underscores = 0;
	idx_t num_integer_underscores = 0;
	for (idx_t i = 0; i < str_val.GetSize(); i++) {
		if (val[i] == '.') {
			// decimal point: cast as either decimal or double
			try_cast_as_integer = false;
			decimal_position = i;
		}
		if (val[i] == 'e' || val[i] == 'E') {
			// found exponent, cast as double
			try_cast_as_integer = false;
			try_cast_as_decimal = false;
		}
		if (val[i] == '_') {
			num_underscores++;
			if (!decimal_position.IsValid()) {
				num_integer_underscores++;
			}
		}
	}
	if (try_cast_as_integer) {
		int32_t int_value;
		if (TryCast::Operation<string_t, int32_t>(str_val, int_value)) {
			return make_uniq<ConstantExpression>(Value::INTEGER(int_value));
		}
		int64_t bigint_value;
		// try to cast as bigint first
		if (TryCast::Operation<string_t, int64_t>(str_val, bigint_value)) {
			// successfully cast to bigint: bigint value
			return make_uniq<ConstantExpression>(Value::BIGINT(bigint_value));
		}
		hugeint_t hugeint_value;
		// if that is not successful; try to cast as hugeint
		if (TryCast::Operation<string_t, hugeint_t>(str_val, hugeint_value)) {
			// successfully cast to bigint: bigint value
			return make_uniq<ConstantExpression>(Value::HUGEINT(hugeint_value));
		}
		uhugeint_t uhugeint_value;
		// if that is not successful; try to cast as uhugeint
		if (TryCast::Operation<string_t, uhugeint_t>(str_val, uhugeint_value)) {
			// successfully cast to bigint: bigint value
			return make_uniq<ConstantExpression>(Value::UHUGEINT(uhugeint_value));
		}
		// if that is not successful; try to cast as bignum for very large integers
		// this preserves precision for integers that exceed uhugeint limits
		try {
			auto bignum_str = Bignum::VarcharToBignum(str_val);
			return make_uniq<ConstantExpression>(Value::BIGNUM(bignum_str));
		} catch (const ConversionException &) {
			// if bignum parsing fails (e.g., invalid format), continue to decimal or double fallback
		}
	}
	idx_t decimal_offset = val[0] == '-' ? 3 : 2;
	if (try_cast_as_decimal && decimal_position.IsValid() &&
	    str_val.GetSize() - num_underscores < Decimal::MAX_WIDTH_DECIMAL + decimal_offset) {
		// figure out the width/scale based on the decimal position
		auto width = NumericCast<uint8_t>(str_val.GetSize() - 1 - num_underscores);
		auto scale = NumericCast<uint8_t>(width - decimal_position.GetIndex() + num_integer_underscores);
		if (val[0] == '-') {
			width--;
		}
		if (width <= Decimal::MAX_WIDTH_DECIMAL) {
			// we can cast the value as a decimal
			Value val_width = Value(str_val).DefaultCastAs(LogicalType::DECIMAL(width, scale));
			return make_uniq<ConstantExpression>(std::move(val_width));
		}
	}
	// if there is a decimal or the value is too big to cast as either hugeint or bigint
	double dbl_value = Cast::Operation<string_t, double>(str_val);
	return make_uniq<ConstantExpression>(Value::DOUBLE(dbl_value));
}

// NumberLiteral <- < [+-]?[0-9]*([.][0-9]*)? >
unique_ptr<ParsedExpression> PEGTransformerFactory::TransformNumberLiteral(PEGTransformer &transformer,
                                                                           ParseResult &parse_result) {
	auto &literal_pr = parse_result.Cast<NumberParseResult>();
	return ConvertNumberToValue(literal_pr.number);
}

unique_ptr<ParsedExpression> PEGTransformerFactory::TransformSetofType(PEGTransformer &transformer,
                                                                       const LogicalType &type) {
	return UnboundType::GetTypeExpression(type)->Copy();
}

// StringLiteral <- '\'' [^\']* '\''
string PEGTransformerFactory::TransformStringLiteral(PEGTransformer &transformer, ParseResult &parse_result) {
	auto &string_literal_pr = parse_result.Cast<StringLiteralParseResult>();
	return string_literal_pr.result;
}

Identifier PEGTransformerFactory::TransformConstraintName(PEGTransformer &transformer,
                                                          const Identifier &col_id_or_string) {
	return Identifier(col_id_or_string);
}
} // namespace duckdb
