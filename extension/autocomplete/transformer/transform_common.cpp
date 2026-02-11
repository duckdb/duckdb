#include "duckdb/common/extra_type_info.hpp"
#include "duckdb/common/operator/cast_operators.hpp"
#include "duckdb/common/types/decimal.hpp"
#include "transformer/peg_transformer.hpp"
#include "duckdb/common/types/hugeint.hpp"
#include "duckdb/common/limits.hpp"
#include "duckdb/common/operator/negate.hpp"
#include "duckdb/parser/expression/type_expression.hpp"

namespace duckdb {

string PEGTransformerFactory::TransformIdentifierOrKeyword(PEGTransformer &transformer,
                                                           optional_ptr<ParseResult> parse_result) {
	if (parse_result->type == ParseResultType::IDENTIFIER) {
		return parse_result->Cast<IdentifierParseResult>().identifier;
	}
	if (parse_result->type == ParseResultType::KEYWORD) {
		return parse_result->Cast<KeywordParseResult>().keyword;
	}
	if (parse_result->type == ParseResultType::CHOICE) {
		auto &choice_pr = parse_result->Cast<ChoiceParseResult>();
		return transformer.Transform<string>(choice_pr.result);
	}
	if (parse_result->type == ParseResultType::LIST) {
		auto &list_pr = parse_result->Cast<ListParseResult>();
		for (auto &child : list_pr.GetChildren()) {
			if (child->type == ParseResultType::LIST && child->Cast<ListParseResult>().GetChildren().empty()) {
				continue;
			}
			if (child->type == ParseResultType::CHOICE) {
				auto &choice_pr = child->Cast<ChoiceParseResult>();
				if (choice_pr.result->type == ParseResultType::IDENTIFIER) {
					return choice_pr.result->Cast<IdentifierParseResult>().identifier;
				}
				if (choice_pr.result->type == ParseResultType::KEYWORD) {
					return choice_pr.result->Cast<KeywordParseResult>().keyword;
				}
				return transformer.Transform<string>(choice_pr.result);
			}
			if (child->type == ParseResultType::IDENTIFIER) {
				return child->Cast<IdentifierParseResult>().identifier;
			}
			throw InternalException("Unexpected IdentifierOrKeyword type encountered %s.",
			                        ParseResultToString(child->type));
		}
	}
	throw ParserException("Unexpected ParseResult type in identifier transformation.");
}

LogicalType PEGTransformerFactory::TransformType(PEGTransformer &transformer, optional_ptr<ParseResult> parse_result) {
	auto &list_pr = parse_result->Cast<ListParseResult>();
	auto &type_pr = list_pr.Child<ListParseResult>(0);
	auto type = transformer.Transform<unique_ptr<ParsedExpression>>(type_pr.Child<ChoiceParseResult>(0).result);
	auto opt_array_bounds_pr = list_pr.Child<OptionalParseResult>(1);
	if (!opt_array_bounds_pr.HasResult()) {
		return LogicalType::UNBOUND(std::move(type));
	}
	auto array_bounds_repeat = opt_array_bounds_pr.optional_result->Cast<RepeatParseResult>();
	for (auto &array_bound : array_bounds_repeat.children) {
		vector<unique_ptr<ParsedExpression>> children_types;
		children_types.push_back(std::move(type));

		auto array_size = transformer.Transform<int64_t>(array_bound);
		if (array_size < 0) {
			type = make_uniq<TypeExpression>("list", std::move(children_types));
		} else {
			children_types.push_back(make_uniq<ConstantExpression>(Value::BIGINT(array_size)));
			type = make_uniq<TypeExpression>("array", std::move(children_types));
		}
	}
	return LogicalType::UNBOUND(std::move(type));
}

int64_t PEGTransformerFactory::TransformArrayBounds(PEGTransformer &transformer,
                                                    optional_ptr<ParseResult> parse_result) {
	auto &list_pr = parse_result->Cast<ListParseResult>();
	auto choice_pr = list_pr.Child<ChoiceParseResult>(0).result;
	if (choice_pr->name.empty()) {
		return -1;
	}
	return transformer.Transform<int64_t>(list_pr.Child<ChoiceParseResult>(0).result);
}

int64_t PEGTransformerFactory::TransformArrayKeyword(PEGTransformer &transformer,
                                                     optional_ptr<ParseResult> parse_result) {
	// ArrayKeyword <- 'ARRAY'i
	// Empty array so we return -1 to signify it's a list
	return -1;
}

int64_t PEGTransformerFactory::TransformSquareBracketsArray(PEGTransformer &transformer,
                                                            optional_ptr<ParseResult> parse_result) {
	auto &list_pr = parse_result->Cast<ListParseResult>();
	auto opt_array_size = list_pr.Child<OptionalParseResult>(1);
	if (opt_array_size.HasResult()) {
		auto number = transformer.Transform<unique_ptr<ParsedExpression>>(opt_array_size.optional_result);
		if (number->GetExpressionClass() == ExpressionClass::CONSTANT) {
			auto &const_number = number->Cast<ConstantExpression>();
			if (!const_number.value.type().IsIntegral()) {
				throw BinderException("Expected an integer as array bound instead of %s",
				                      const_number.value.ToString());
			}
			return const_number.value.GetValue<int64_t>();
		}
	}
	return -1;
}

unique_ptr<ParsedExpression> PEGTransformerFactory::TransformTimeType(PEGTransformer &transformer,
                                                                      optional_ptr<ParseResult> parse_result) {
	auto &list_pr = parse_result->Cast<ListParseResult>();
	LogicalTypeId type = transformer.Transform<LogicalTypeId>(list_pr.Child<ListParseResult>(0));
	auto opt_type_modifiers = list_pr.Child<OptionalParseResult>(1);
	vector<unique_ptr<ParsedExpression>> modifiers;
	if (opt_type_modifiers.HasResult()) {
		modifiers = transformer.Transform<vector<unique_ptr<ParsedExpression>>>(opt_type_modifiers.optional_result);
	}
	auto opt_timezone = list_pr.Child<OptionalParseResult>(2);
	const bool with_timezone =
	    opt_timezone.HasResult() ? transformer.Transform<bool>(opt_timezone.optional_result) : false;
	if (type == LogicalTypeId::TIME) {
		if (!modifiers.empty()) {
			throw ParserException("Type TIME does not allow any modifiers");
		}
		if (with_timezone) {
			return make_uniq<TypeExpression>(EnumUtil::ToString(LogicalType::TIME_TZ),
			                                 vector<unique_ptr<ParsedExpression>> {});
		}
		return make_uniq<TypeExpression>(EnumUtil::ToString(LogicalType::TIME),
		                                 vector<unique_ptr<ParsedExpression>> {});
	}
	if (type == LogicalTypeId::TIMESTAMP) {
		if (modifiers.empty()) {
			if (with_timezone) {
				return make_uniq<TypeExpression>(EnumUtil::ToString(LogicalType::TIMESTAMP_TZ),
				                                 vector<unique_ptr<ParsedExpression>> {});
			}
			return make_uniq<TypeExpression>(EnumUtil::ToString(LogicalType::TIMESTAMP),
			                                 vector<unique_ptr<ParsedExpression>> {});
		}
		if (modifiers.size() > 1) {
			throw ParserException("TIMESTAMP only supports a single modifier");
		}
		if (modifiers[0]->GetExpressionClass() != ExpressionClass::CONSTANT) {
			throw ParserException("Expected a constant expression for timestamp precision");
		}
		auto timestamp_precision = modifiers[0]->Cast<ConstantExpression>().value.GetValue<int64_t>();
		if (timestamp_precision > 10) {
			throw ParserException("TIMESTAMP only supports until nano-second precision (9)");
		}
		if (timestamp_precision < 0) {
			throw ParserException("TIMESTAMP precision should be between 0 and 10 (inclusive)");
		}
		if (timestamp_precision == 0) {
			return make_uniq<TypeExpression>(EnumUtil::ToString(LogicalType::TIMESTAMP_S),
			                                 vector<unique_ptr<ParsedExpression>> {});
		}
		if (timestamp_precision <= 3) {
			return make_uniq<TypeExpression>(EnumUtil::ToString(LogicalType::TIMESTAMP_MS),
			                                 vector<unique_ptr<ParsedExpression>> {});
		}
		if (timestamp_precision <= 6) {
			// Corresponds to microseconds, which is the default TIMESTAMP
			return make_uniq<TypeExpression>(EnumUtil::ToString(LogicalType::TIMESTAMP),
			                                 vector<unique_ptr<ParsedExpression>> {});
		}
		return make_uniq<TypeExpression>(EnumUtil::ToString(LogicalType::TIMESTAMP_NS),
		                                 vector<unique_ptr<ParsedExpression>> {});
	}
	throw ParserException("Unexpected time type encountered");
}

bool PEGTransformerFactory::TransformTimeZone(PEGTransformer &transformer, optional_ptr<ParseResult> parse_result) {
	auto &list_pr = parse_result->Cast<ListParseResult>();
	return transformer.Transform<bool>(list_pr.Child<ListParseResult>(0));
}

bool PEGTransformerFactory::TransformWithOrWithout(PEGTransformer &transformer,
                                                   optional_ptr<ParseResult> parse_result) {
	auto &list_pr = parse_result->Cast<ListParseResult>();
	return transformer.TransformEnum<bool>(list_pr.Child<ChoiceParseResult>(0));
}

LogicalTypeId PEGTransformerFactory::TransformTimeOrTimestamp(PEGTransformer &transformer,
                                                              optional_ptr<ParseResult> parse_result) {
	auto &list_pr = parse_result->Cast<ListParseResult>();
	auto time_or_timestamp = list_pr.Child<ChoiceParseResult>(0).result;
	return transformer.TransformEnum<LogicalTypeId>(time_or_timestamp);
}

unique_ptr<ParsedExpression> PEGTransformerFactory::TransformNumericType(PEGTransformer &transformer,
                                                                         optional_ptr<ParseResult> parse_result) {
	auto &list_pr = parse_result->Cast<ListParseResult>();
	return transformer.Transform<unique_ptr<ParsedExpression>>(list_pr.Child<ChoiceParseResult>(0).result);
}

unique_ptr<ParsedExpression> PEGTransformerFactory::TransformSimpleNumericType(PEGTransformer &transformer,
                                                                               optional_ptr<ParseResult> parse_result) {
	auto &list_pr = parse_result->Cast<ListParseResult>();
	auto numeric = transformer.TransformEnum<string>(list_pr.Child<ChoiceParseResult>(0).result);
	return make_uniq<TypeExpression>(numeric, vector<unique_ptr<ParsedExpression>> {});
}

unique_ptr<ParsedExpression>
PEGTransformerFactory::TransformDecimalNumericType(PEGTransformer &transformer,
                                                   optional_ptr<ParseResult> parse_result) {
	auto &list_pr = parse_result->Cast<ListParseResult>();
	return transformer.Transform<unique_ptr<ParsedExpression>>(list_pr.Child<ChoiceParseResult>(0).result);
}

unique_ptr<ParsedExpression> PEGTransformerFactory::TransformFloatType(PEGTransformer &transformer,
                                                                       optional_ptr<ParseResult> parse_result) {
	return make_uniq<TypeExpression>("FLOAT", vector<unique_ptr<ParsedExpression>> {});
}

unique_ptr<ParsedExpression> PEGTransformerFactory::TransformDecimalType(PEGTransformer &transformer,
                                                                         optional_ptr<ParseResult> parse_result) {
	auto &list_pr = parse_result->Cast<ListParseResult>();
	auto opt_type_modifier = list_pr.Child<OptionalParseResult>(1);
	if (!opt_type_modifier.HasResult()) {
		return make_uniq<TypeExpression>("DECIMAL", vector<unique_ptr<ParsedExpression>> {});
	}
	auto type_modifiers =
	    transformer.Transform<vector<unique_ptr<ParsedExpression>>>(opt_type_modifier.optional_result);
	return make_uniq<TypeExpression>("DECIMAL", std::move(type_modifiers));
}

vector<unique_ptr<ParsedExpression>>
PEGTransformerFactory::TransformTypeModifiers(PEGTransformer &transformer, optional_ptr<ParseResult> parse_result) {
	auto &list_pr = parse_result->Cast<ListParseResult>();
	vector<unique_ptr<ParsedExpression>> result;
	auto extract_list = ExtractResultFromParens(list_pr.Child<ListParseResult>(0))->Cast<OptionalParseResult>();
	if (extract_list.HasResult()) {
		auto expressions = ExtractParseResultsFromList(extract_list.optional_result);
		for (auto expression : expressions) {
			auto expr = transformer.Transform<unique_ptr<ParsedExpression>>(expression);
			if (expr->GetExpressionClass() != ExpressionClass::CONSTANT) {
				// TODO (Dtenwolde/maxxen) remove this in the future
				throw ParserException("Expected a constant as type modifier");
			}
			result.push_back(std::move(expr));
		}
	}

	return result;
}

unique_ptr<ParsedExpression> PEGTransformerFactory::TransformSimpleType(PEGTransformer &transformer,
                                                                        optional_ptr<ParseResult> parse_result) {
	auto &list_pr = parse_result->Cast<ListParseResult>();
	auto opt_modifiers = list_pr.Child<OptionalParseResult>(1);
	vector<unique_ptr<ParsedExpression>> children;
	if (opt_modifiers.HasResult()) {
		children = transformer.Transform<vector<unique_ptr<ParsedExpression>>>(opt_modifiers.optional_result);
	}
	auto qualified_type_or_character = list_pr.Child<ListParseResult>(0);
	auto type_or_character_pr = qualified_type_or_character.Child<ChoiceParseResult>(0).result;
	if (type_or_character_pr->name == "QualifiedTypeName") {
		auto qualified_type_name = transformer.Transform<QualifiedName>(type_or_character_pr);
		if (qualified_type_name.schema.empty()) {
			qualified_type_name.schema = qualified_type_name.catalog;
			qualified_type_name.catalog = INVALID_CATALOG;
		}
		return make_uniq<TypeExpression>(qualified_type_name.catalog, qualified_type_name.schema,
		                                 qualified_type_name.name, std::move(children));
	}
	if (type_or_character_pr->name == "CharacterType") {
		return transformer.Transform<unique_ptr<ParsedExpression>>(type_or_character_pr);
	}
	throw InternalException("Unexpected rule %s encountered in SimpleType", type_or_character_pr->name);
}

QualifiedName PEGTransformerFactory::TransformQualifiedTypeName(PEGTransformer &transformer,
                                                                optional_ptr<ParseResult> parse_result) {
	auto &list_pr = parse_result->Cast<ListParseResult>();
	vector<string> qualified_typename;
	auto opt_identifiers = list_pr.Child<OptionalParseResult>(0);
	if (opt_identifiers.HasResult()) {
		auto repeat_identifiers = opt_identifiers.optional_result->Cast<RepeatParseResult>();
		for (auto &child : repeat_identifiers.children) {
			auto repeat_list = child->Cast<ListParseResult>();
			qualified_typename.push_back(repeat_list.Child<IdentifierParseResult>(0).identifier);
		}
	}

	if (list_pr.GetChild(1)->type == ParseResultType::IDENTIFIER) {
		qualified_typename.push_back(list_pr.Child<IdentifierParseResult>(1).identifier);
	} else {
		qualified_typename.push_back(transformer.Transform<string>(list_pr.Child<ListParseResult>(2)));
	}
	return StringToQualifiedName(qualified_typename);
}

unique_ptr<ParsedExpression> PEGTransformerFactory::TransformCharacterType(PEGTransformer &transformer,
                                                                           optional_ptr<ParseResult> parse_result) {
	return make_uniq<TypeExpression>("VARCHAR", vector<unique_ptr<ParsedExpression>> {});
}

unique_ptr<ParsedExpression> PEGTransformerFactory::TransformMapType(PEGTransformer &transformer,
                                                                     optional_ptr<ParseResult> parse_result) {
	auto &list_pr = parse_result->Cast<ListParseResult>();
	auto extract_parens = ExtractResultFromParens(list_pr.Child<ListParseResult>(1));
	auto type_list = ExtractParseResultsFromList(extract_parens);
	if (type_list.size() != 2) {
		throw ParserException("Map type needs exactly two entries, key and value type.");
	}
	auto key_type = transformer.Transform<LogicalType>(type_list[0]);
	auto value_type = transformer.Transform<LogicalType>(type_list[1]);
	vector<unique_ptr<ParsedExpression>> map_children;
	map_children.push_back(UnboundType::GetTypeExpression(key_type)->Copy());
	map_children.push_back(UnboundType::GetTypeExpression(value_type)->Copy());
	return make_uniq<TypeExpression>("MAP", std::move(map_children));
}

unique_ptr<ParsedExpression> PEGTransformerFactory::TransformRowType(PEGTransformer &transformer,
                                                                     optional_ptr<ParseResult> parse_result) {
	auto &list_pr = parse_result->Cast<ListParseResult>();
	auto colid_list = transformer.Transform<child_list_t<LogicalType>>(list_pr.Child<ListParseResult>(1));
	vector<unique_ptr<ParsedExpression>> struct_children;
	for (auto &child : colid_list) {
		auto &type_expr = UnboundType::GetTypeExpression(child.second);
		auto new_type_expr = type_expr->Copy();
		new_type_expr->alias = child.first;
		struct_children.push_back(std::move(new_type_expr));
	}
	return make_uniq<TypeExpression>("STRUCT", std::move(struct_children));
}

unique_ptr<ParsedExpression> PEGTransformerFactory::TransformGeometryType(PEGTransformer &transformer,
                                                                          optional_ptr<ParseResult> parse_result) {
	auto &list_pr = parse_result->Cast<ListParseResult>();
	auto crs_opt = list_pr.Child<OptionalParseResult>(1);
	if (!crs_opt.HasResult()) {
		return make_uniq<TypeExpression>("GEOMETRY", vector<unique_ptr<ParsedExpression>> {});
	}
	auto extract_parens = ExtractResultFromParens(crs_opt.optional_result);
	auto crs = transformer.Transform<unique_ptr<ParsedExpression>>(extract_parens);
	vector<unique_ptr<ParsedExpression>> geo_children;
	if (crs->GetExpressionClass() != ExpressionClass::CONSTANT) {
		throw ParserException("Expected a constant as type modifier");
	}
	geo_children.push_back(std::move(crs));
	return make_uniq<TypeExpression>("GEOMETRY", std::move(geo_children));
}

unique_ptr<ParsedExpression> PEGTransformerFactory::TransformVariantType(PEGTransformer &transformer,
                                                                         optional_ptr<ParseResult> parse_result) {
	return make_uniq<TypeExpression>("VARIANT", vector<unique_ptr<ParsedExpression>> {});
}

unique_ptr<ParsedExpression> PEGTransformerFactory::TransformUnionType(PEGTransformer &transformer,
                                                                       optional_ptr<ParseResult> parse_result) {
	auto &list_pr = parse_result->Cast<ListParseResult>();
	auto colid_list = transformer.Transform<child_list_t<LogicalType>>(list_pr.Child<ListParseResult>(1));
	case_insensitive_string_set_t union_names;
	vector<unique_ptr<ParsedExpression>> union_children;
	for (auto &colid : colid_list) {
		union_names.insert(colid.first);
		auto &type_expr = UnboundType::GetTypeExpression(colid.second);
		auto new_type_expr = type_expr->Copy();
		new_type_expr->alias = colid.first;
		union_children.push_back(std::move(new_type_expr));
	}
	return make_uniq<TypeExpression>("UNION", std::move(union_children));
}

child_list_t<LogicalType> PEGTransformerFactory::TransformColIdTypeList(PEGTransformer &transformer,
                                                                        optional_ptr<ParseResult> parse_result) {
	auto &list_pr = parse_result->Cast<ListParseResult>();
	auto extract_list = ExtractResultFromParens(list_pr.Child<ListParseResult>(0));
	auto colid_type_list = ExtractParseResultsFromList(extract_list);

	child_list_t<LogicalType> result;
	for (auto colid_type : colid_type_list) {
		result.push_back(transformer.Transform<pair<string, LogicalType>>(colid_type));
	}
	return result;
}

pair<string, LogicalType> PEGTransformerFactory::TransformColIdType(PEGTransformer &transformer,
                                                                    optional_ptr<ParseResult> parse_result) {
	auto &list_pr = parse_result->Cast<ListParseResult>();
	auto colid = transformer.Transform<string>(list_pr.Child<ListParseResult>(0));
	auto type = transformer.Transform<LogicalType>(list_pr.Child<ListParseResult>(1));
	return make_pair(colid, type);
}

unique_ptr<ParsedExpression> PEGTransformerFactory::TransformBitType(PEGTransformer &transformer,
                                                                     optional_ptr<ParseResult> parse_result) {
	return make_uniq<TypeExpression>("BIT", vector<unique_ptr<ParsedExpression>> {});
}

unique_ptr<ParsedExpression> PEGTransformerFactory::TransformIntervalType(PEGTransformer &transformer,
                                                                          optional_ptr<ParseResult> parse_result) {
	return make_uniq<TypeExpression>("INTERVAL", vector<unique_ptr<ParsedExpression>> {});
}

unique_ptr<ParsedExpression> PEGTransformerFactory::TransformIntervalInterval(PEGTransformer &transformer,
                                                                              optional_ptr<ParseResult> parse_result) {
	auto &list_pr = parse_result->Cast<ListParseResult>();
	auto opt_interval = list_pr.Child<OptionalParseResult>(1);
	if (opt_interval.HasResult()) {
		auto logical_type = transformer.Transform<LogicalType>(opt_interval.optional_result);
		return make_uniq<TypeExpression>(LogicalTypeIdToString(logical_type.id()),
		                                 vector<unique_ptr<ParsedExpression>> {});
	}
	return make_uniq<TypeExpression>("INTERVAL", vector<unique_ptr<ParsedExpression>> {});
}

DatePartSpecifier PEGTransformerFactory::TransformInterval(PEGTransformer &transformer,
                                                           optional_ptr<ParseResult> parse_result) {
	auto &list_pr = parse_result->Cast<ListParseResult>();
	auto choice_pr = list_pr.Child<ChoiceParseResult>(0);
	return transformer.TransformEnum<DatePartSpecifier>(choice_pr);
}

bool PEGTransformerFactory::TryNegateValue(Value &val) {
	switch (val.type().id()) {
	case LogicalTypeId::INTEGER: {
		auto raw = val.GetValue<int32_t>();
		if (!NegateOperator::CanNegate<int32_t>(raw)) {
			val = Value::BIGINT(-static_cast<int64_t>(raw));
		} else {
			val = Value::INTEGER(-raw);
		}
		return true;
	}
	case LogicalTypeId::BIGINT: {
		auto raw = val.GetValue<int64_t>();
		if (!NegateOperator::CanNegate<int64_t>(raw)) {
			val = Value::HUGEINT(-static_cast<hugeint_t>(raw));
		} else {
			val = Value::BIGINT(-raw);
		}
		return true;
	}
	case LogicalTypeId::HUGEINT: {
		auto raw = val.GetValue<hugeint_t>();
		if (!NegateOperator::CanNegate<hugeint_t>(raw)) {
			return false;
		}
		val = Value::HUGEINT(-raw);
		return true;
	}
	case LogicalTypeId::UHUGEINT: {
		auto uval = val.GetValue<uhugeint_t>();
		uhugeint_t abs_min_hugeint = static_cast<uhugeint_t>(NumericLimits<hugeint_t>::Maximum()) + 1;

		if (uval == abs_min_hugeint) {
			val = Value::HUGEINT(NumericLimits<hugeint_t>::Minimum());
			return true;
		}
		if (uval < abs_min_hugeint) {
			val = Value::HUGEINT(-static_cast<hugeint_t>(uval));
			return true;
		}

		return false;
	}
	case LogicalTypeId::DOUBLE:
		val = Value::DOUBLE(-val.GetValue<double>());
		return true;
	default:
		return false;
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
			Value val = Value(str_val);
			val = val.DefaultCastAs(LogicalType::DECIMAL(width, scale));
			return make_uniq<ConstantExpression>(std::move(val));
		}
	}
	// if there is a decimal or the value is too big to cast as either hugeint or bigint
	double dbl_value = Cast::Operation<string_t, double>(str_val);
	return make_uniq<ConstantExpression>(Value::DOUBLE(dbl_value));
}

// NumberLiteral <- < [+-]?[0-9]*([.][0-9]*)? >
unique_ptr<ParsedExpression> PEGTransformerFactory::TransformNumberLiteral(PEGTransformer &transformer,
                                                                           optional_ptr<ParseResult> parse_result) {
	auto literal_pr = parse_result->Cast<NumberParseResult>();
	return ConvertNumberToValue(literal_pr.number);
}

unique_ptr<ParsedExpression> PEGTransformerFactory::TransformSetofType(PEGTransformer &transformer,
                                                                       optional_ptr<ParseResult> parse_result) {
	auto &list_pr = parse_result->Cast<ListParseResult>();
	auto setof_type = transformer.Transform<LogicalType>(list_pr.Child<ListParseResult>(1));
	return UnboundType::GetTypeExpression(setof_type)->Copy();
}

// StringLiteral <- '\'' [^\']* '\''
string PEGTransformerFactory::TransformStringLiteral(PEGTransformer &transformer,
                                                     optional_ptr<ParseResult> parse_result) {
	auto &string_literal_pr = parse_result->Cast<StringLiteralParseResult>();
	return string_literal_pr.result;
}
} // namespace duckdb
