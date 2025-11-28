#include "duckdb/common/extra_type_info.hpp"
#include "duckdb/common/operator/cast_operators.hpp"
#include "duckdb/common/types/decimal.hpp"
#include "transformer/peg_transformer.hpp"
#include "duckdb/common/extra_type_info.hpp"

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
	auto type = transformer.Transform<LogicalType>(type_pr.Child<ChoiceParseResult>(0).result);
	auto opt_array_bounds_pr = list_pr.Child<OptionalParseResult>(1);
	if (opt_array_bounds_pr.HasResult()) {
		auto array_bounds_repeat = opt_array_bounds_pr.optional_result->Cast<RepeatParseResult>();
		for (auto &array_bound : array_bounds_repeat.children) {
			auto array_size = transformer.Transform<int64_t>(array_bound);
			if (array_size < 0) {
				type = LogicalType::LIST(type);
			} else if (array_size == 0) {
				// Empty arrays are not supported
				throw ParserException("Arrays must have a size of at least 1");
			} else if (array_size > static_cast<int64_t>(ArrayType::MAX_ARRAY_SIZE)) {
				throw ParserException("Arrays must have a size of at most %d", ArrayType::MAX_ARRAY_SIZE);
			} else {
				type = LogicalType::ARRAY(type, NumericCast<idx_t>(array_size));
			}
		}
	}
	return type;
}

int64_t PEGTransformerFactory::TransformArrayBounds(PEGTransformer &transformer,
                                                    optional_ptr<ParseResult> parse_result) {
	auto &list_pr = parse_result->Cast<ListParseResult>();
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
		return std::stoi(opt_array_size.optional_result->Cast<NumberParseResult>().number);
	}
	return -1;
}

LogicalType PEGTransformerFactory::TransformTimeType(PEGTransformer &transformer,
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
		if (modifiers.size() > 0) {
			throw ParserException("Type TIME does not allow any modifiers");
		}
		return with_timezone ? LogicalType::TIME_TZ : LogicalType::TIME;
	}
	if (type == LogicalTypeId::TIMESTAMP) {
		if (modifiers.size() == 0) {
			return with_timezone ? LogicalType::TIMESTAMP_TZ : LogicalType::TIMESTAMP;
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
			return LogicalType::TIMESTAMP_S;
		} else if (timestamp_precision <= 3) {
			return LogicalType::TIMESTAMP_MS;
		} else if (timestamp_precision <= 6) {
			// Corresponds to microseconds, which is the default TIMESTAMP
			return LogicalType::TIMESTAMP;
		} else {
			return LogicalType::TIMESTAMP_NS;
		}
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

LogicalType PEGTransformerFactory::TransformNumericType(PEGTransformer &transformer,
                                                        optional_ptr<ParseResult> parse_result) {
	auto &list_pr = parse_result->Cast<ListParseResult>();
	return transformer.Transform<LogicalType>(list_pr.Child<ChoiceParseResult>(0).result);
}

LogicalType PEGTransformerFactory::TransformSimpleNumericType(PEGTransformer &transformer,
                                                              optional_ptr<ParseResult> parse_result) {
	auto &list_pr = parse_result->Cast<ListParseResult>();
	return transformer.TransformEnum<LogicalType>(list_pr.Child<ChoiceParseResult>(0).result);
}

LogicalType PEGTransformerFactory::TransformDecimalNumericType(PEGTransformer &transformer,
                                                               optional_ptr<ParseResult> parse_result) {
	auto &list_pr = parse_result->Cast<ListParseResult>();
	return transformer.Transform<LogicalType>(list_pr.Child<ChoiceParseResult>(0).result);
}

LogicalType PEGTransformerFactory::TransformFloatType(PEGTransformer &transformer,
                                                      optional_ptr<ParseResult> parse_result) {
	// TODO(Dtenwolde) Unknown if anything should be done with the Parens(Numberliteral)? part.
	return LogicalType(LogicalTypeId::FLOAT);
}

LogicalType PEGTransformerFactory::TransformDecimalType(PEGTransformer &transformer,
                                                        optional_ptr<ParseResult> parse_result) {
	auto &list_pr = parse_result->Cast<ListParseResult>();
	auto opt_type_modifier = list_pr.Child<OptionalParseResult>(1);
	uint8_t width = 18;
	uint8_t scale = 3;
	if (opt_type_modifier.HasResult()) {
		auto type_modifiers =
		    transformer.Transform<vector<unique_ptr<ParsedExpression>>>(opt_type_modifier.optional_result);
		if (type_modifiers.size() > 2) {
			throw ParserException("A maximum of two modifiers is supported");
		}
		for (const auto &modifier : type_modifiers) {
			if (modifier->GetExpressionClass() != ExpressionClass::CONSTANT) {
				throw ParserException("Type modifiers must be a constant expression");
			}
		}
		switch (type_modifiers.size()) {
		case 2:
			scale = type_modifiers[1]->Cast<ConstantExpression>().value.GetValue<uint8_t>();
			// Intentional fallthrough to also parse width
		case 1:
			width = type_modifiers[0]->Cast<ConstantExpression>().value.GetValue<uint8_t>();
			break;
		case 0:
		default:
			// No modifiers provided, so we just stick to the default values for width and scale.
			break;
		}
	}
	auto type_info = make_shared_ptr<DecimalTypeInfo>(width, scale);
	return LogicalType(LogicalTypeId::DECIMAL, type_info);
}

vector<unique_ptr<ParsedExpression>>
PEGTransformerFactory::TransformTypeModifiers(PEGTransformer &transformer, optional_ptr<ParseResult> parse_result) {
	auto &list_pr = parse_result->Cast<ListParseResult>();
	vector<unique_ptr<ParsedExpression>> result;
	auto extract_list = ExtractResultFromParens(list_pr.Child<ListParseResult>(0))->Cast<OptionalParseResult>();
	if (extract_list.HasResult()) {
		auto expressions = ExtractParseResultsFromList(extract_list.optional_result);
		for (auto expression : expressions) {
			result.push_back(transformer.Transform<unique_ptr<ParsedExpression>>(expression));
		}
	}

	return result;
}

LogicalType PEGTransformerFactory::TransformSimpleType(PEGTransformer &transformer,
                                                       optional_ptr<ParseResult> parse_result) {
	auto &list_pr = parse_result->Cast<ListParseResult>();
	auto qualified_type_or_character = list_pr.Child<ListParseResult>(0);
	auto type_or_character_pr = qualified_type_or_character.Child<ChoiceParseResult>(0).result;
	LogicalType result;
	if (type_or_character_pr->name == "QualifiedTypeName") {
		auto qualified_type_name = transformer.Transform<QualifiedName>(type_or_character_pr);
		result = LogicalType(TransformStringToLogicalTypeId(qualified_type_name.name));
		if (result.id() == LogicalTypeId::USER) {
			vector<Value> modifiers;
			if (qualified_type_name.schema.empty()) {
				qualified_type_name.schema = qualified_type_name.catalog;
				qualified_type_name.catalog = INVALID_CATALOG;
			}
			result = LogicalType::USER(qualified_type_name.catalog, qualified_type_name.schema,
			                           qualified_type_name.name, std::move(modifiers));
		}
	} else if (type_or_character_pr->name == "CharacterType") {
		result = transformer.Transform<LogicalType>(type_or_character_pr);
	} else {
		throw InternalException("Unexpected rule %s encountered in SimpleType", type_or_character_pr->name);
	}
	auto opt_modifiers = list_pr.Child<OptionalParseResult>(1);
	vector<unique_ptr<ParsedExpression>> modifiers;
	if (opt_modifiers.HasResult()) {
		modifiers = transformer.Transform<vector<unique_ptr<ParsedExpression>>>(opt_modifiers.optional_result);
		for (const auto &modifier : modifiers) {
			if (modifier->GetExpressionClass() == ExpressionClass::CONSTANT) {
				continue;
			}
			throw ParserException("Expected a constant as type modifier");
		}
		if (modifiers.size() > 9) {
			throw ParserException("'%s': a maximum of 9 type modifiers is allowed", result.GetAlias());
		}
	}
	// TODO(Dtenwolde) add modifiers
	return result;
}

QualifiedName PEGTransformerFactory::TransformQualifiedTypeName(PEGTransformer &transformer,
                                                                optional_ptr<ParseResult> parse_result) {
	// TODO(Dtenwolde) figure out what to do with qualified names
	auto &list_pr = parse_result->Cast<ListParseResult>();
	QualifiedName result;
	auto catalog_pr = list_pr.Child<OptionalParseResult>(0);
	if (catalog_pr.HasResult()) {
		result.catalog = transformer.Transform<string>(catalog_pr.optional_result);
	}
	auto schema_pr = list_pr.Child<OptionalParseResult>(1);
	if (schema_pr.HasResult()) {
		result.schema = transformer.Transform<string>(schema_pr.optional_result);
	}

	if (list_pr.GetChild(2)->type == ParseResultType::IDENTIFIER) {
		result.name = list_pr.Child<IdentifierParseResult>(2).identifier;
	} else {
		result.name = transformer.Transform<string>(list_pr.Child<ListParseResult>(2));
	}
	return result;
}

LogicalType PEGTransformerFactory::TransformCharacterType(PEGTransformer &transformer,
                                                          optional_ptr<ParseResult> parse_result) {
	return LogicalType(LogicalTypeId::VARCHAR);
}

LogicalType PEGTransformerFactory::TransformMapType(PEGTransformer &transformer,
                                                    optional_ptr<ParseResult> parse_result) {
	auto &list_pr = parse_result->Cast<ListParseResult>();
	auto extract_parens = ExtractResultFromParens(list_pr.Child<ListParseResult>(1));
	auto type_list = ExtractParseResultsFromList(extract_parens);
	if (type_list.size() != 2) {
		throw ParserException("Map type needs exactly two entries, key and value type.");
	}
	auto key_type = transformer.Transform<LogicalType>(type_list[0]);
	auto value_type = transformer.Transform<LogicalType>(type_list[1]);
	return LogicalType::MAP(key_type, value_type);
}

LogicalType PEGTransformerFactory::TransformRowType(PEGTransformer &transformer,
                                                    optional_ptr<ParseResult> parse_result) {
	auto &list_pr = parse_result->Cast<ListParseResult>();
	auto colid_list = transformer.Transform<child_list_t<LogicalType>>(list_pr.Child<ListParseResult>(1));
	return LogicalType::STRUCT(colid_list);
}

LogicalType PEGTransformerFactory::TransformUnionType(PEGTransformer &transformer,
                                                      optional_ptr<ParseResult> parse_result) {
	auto &list_pr = parse_result->Cast<ListParseResult>();
	auto colid_list = transformer.Transform<child_list_t<LogicalType>>(list_pr.Child<ListParseResult>(1));
	return LogicalType::UNION(colid_list);
}

child_list_t<LogicalType> PEGTransformerFactory::TransformColIdTypeList(PEGTransformer &transformer,
                                                                        optional_ptr<ParseResult> parse_result) {
	auto &list_pr = parse_result->Cast<ListParseResult>();
	auto extract_list = ExtractResultFromParens(list_pr.Child<ListParseResult>(0));
	auto colid_type_list = ExtractParseResultsFromList(extract_list);

	child_list_t<LogicalType> result;
	for (auto colid_type : colid_type_list) {
		result.push_back(transformer.Transform<std::pair<std::string, LogicalType>>(colid_type));
	}
	return result;
}

std::pair<std::string, LogicalType> PEGTransformerFactory::TransformColIdType(PEGTransformer &transformer,
                                                                              optional_ptr<ParseResult> parse_result) {
	auto &list_pr = parse_result->Cast<ListParseResult>();
	auto colid = transformer.Transform<string>(list_pr.Child<ListParseResult>(0));
	auto type = transformer.Transform<LogicalType>(list_pr.Child<ListParseResult>(1));
	return std::make_pair(colid, type);
}

LogicalType PEGTransformerFactory::TransformBitType(PEGTransformer &transformer,
                                                    optional_ptr<ParseResult> parse_result) {
	auto &list_pr = parse_result->Cast<ListParseResult>();
	auto &opt_varying = list_pr.Child<OptionalParseResult>(1);
	if (opt_varying.HasResult()) {
		throw ParserException("Type with name varbit does not exist.");
	}
	auto &opt_modifiers = list_pr.Child<OptionalParseResult>(2);
	if (opt_modifiers.HasResult()) {
		throw ParserException("Type BIT does not support any modifiers.");
	}
	return LogicalType(LogicalTypeId::BIT);
}

LogicalType PEGTransformerFactory::TransformIntervalType(PEGTransformer &transformer,
                                                         optional_ptr<ParseResult> parse_result) {
	return LogicalType(LogicalTypeId::INTERVAL);
}

LogicalType PEGTransformerFactory::TransformIntervalInterval(PEGTransformer &transformer,
                                                             optional_ptr<ParseResult> parse_result) {
	auto &list_pr = parse_result->Cast<ListParseResult>();
	auto opt_interval = list_pr.Child<OptionalParseResult>(1);
	if (opt_interval.HasResult()) {
		return transformer.Transform<LogicalType>(opt_interval.optional_result);
	} else {
		return LogicalType(LogicalTypeId::INTERVAL);
	}
}

DatePartSpecifier PEGTransformerFactory::TransformInterval(PEGTransformer &transformer,
                                                           optional_ptr<ParseResult> parse_result) {
	auto &list_pr = parse_result->Cast<ListParseResult>();
	auto choice_pr = list_pr.Child<ChoiceParseResult>(0);
	if (choice_pr.name == "DateTimeToDateTime") {
		throw ParserException("Interval TO is not supported");
	}
	return transformer.TransformEnum<DatePartSpecifier>(choice_pr);
}

// NumberLiteral <- < [+-]?[0-9]*([.][0-9]*)? >
unique_ptr<ParsedExpression> PEGTransformerFactory::TransformNumberLiteral(PEGTransformer &transformer,
                                                                           optional_ptr<ParseResult> parse_result) {
	auto literal_pr = parse_result->Cast<NumberParseResult>();
	string_t str_val(literal_pr.number);
	bool try_cast_as_integer = true;
	bool try_cast_as_decimal = true;
	optional_idx decimal_position = optional_idx::Invalid();
	idx_t num_underscores = 0;
	idx_t num_integer_underscores = 0;
	for (idx_t i = 0; i < str_val.GetSize(); i++) {
		if (literal_pr.number[i] == '.') {
			// decimal point: cast as either decimal or double
			try_cast_as_integer = false;
			decimal_position = i;
		}
		if (literal_pr.number[i] == 'e' || literal_pr.number[i] == 'E') {
			// found exponent, cast as double
			try_cast_as_integer = false;
			try_cast_as_decimal = false;
		}
		if (literal_pr.number[i] == '_') {
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
	idx_t decimal_offset = literal_pr.number[0] == '-' ? 3 : 2;
	if (try_cast_as_decimal && decimal_position.IsValid() &&
	    str_val.GetSize() - num_underscores < Decimal::MAX_WIDTH_DECIMAL + decimal_offset) {
		// figure out the width/scale based on the decimal position
		auto width = NumericCast<uint8_t>(str_val.GetSize() - 1 - num_underscores);
		auto scale = NumericCast<uint8_t>(width - decimal_position.GetIndex() + num_integer_underscores);
		if (literal_pr.number[0] == '-') {
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

LogicalType PEGTransformerFactory::TransformSetofType(PEGTransformer &transformer,
                                                      optional_ptr<ParseResult> parse_result) {
	auto &list_pr = parse_result->Cast<ListParseResult>();
	return transformer.Transform<LogicalType>(list_pr.Child<ListParseResult>(1));
}

// StringLiteral <- '\'' [^\']* '\''
string PEGTransformerFactory::TransformStringLiteral(PEGTransformer &transformer,
                                                     optional_ptr<ParseResult> parse_result) {
	auto &string_literal_pr = parse_result->Cast<StringLiteralParseResult>();
	return string_literal_pr.result;
}
} // namespace duckdb
