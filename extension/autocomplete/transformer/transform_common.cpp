#include "duckdb/common/operator/cast_operators.hpp"
#include "duckdb/common/types/decimal.hpp"
#include "transformer/peg_transformer.hpp"

namespace duckdb {

// // ArrayBounds <- ('[' NumberLiteral? ']') / 'ARRAY'
// unique_ptr<SQLStatement> PEGTransformerFactory::TransformArrayBounds(PEGTransformer &transformer,
//                                                                      optional_ptr<ParseResult> parse_result) {
// 	auto &list_pr = parse_result->Cast<ListParseResult>();
// 	throw NotImplementedException("Rule 'ArrayBounds' has not been implemented yet");
// }
//
// // BitType <- 'BIT' 'VARYING'? Parens(List(Expression))?
// unique_ptr<SQLStatement> PEGTransformerFactory::TransformBitType(PEGTransformer &transformer,
//                                                                  optional_ptr<ParseResult> parse_result) {
// 	auto &list_pr = parse_result->Cast<ListParseResult>();
// 	throw NotImplementedException("Rule 'BitType' has not been implemented yet");
// }
//
// // CatalogName <- Identifier
// unique_ptr<SQLStatement> PEGTransformerFactory::TransformCatalogName(PEGTransformer &transformer,
//                                                                      optional_ptr<ParseResult> parse_result) {
// 	auto &list_pr = parse_result->Cast<ListParseResult>();
// 	throw NotImplementedException("Rule 'CatalogName' has not been implemented yet");
// }
//
// // CenturyKeyword <- 'CENTURY' / 'CENTURIES'
// unique_ptr<SQLStatement> PEGTransformerFactory::TransformCenturyKeyword(PEGTransformer &transformer,
//                                                                         optional_ptr<ParseResult> parse_result) {
// 	auto &list_pr = parse_result->Cast<ListParseResult>();
// 	throw NotImplementedException("Rule 'CenturyKeyword' has not been implemented yet");
// }
//
// // CharacterType <- ('CHARACTER' 'VARYING'?) /
// // ('CHAR' 'VARYING'?) /
// // ('NATIONAL' 'CHARACTER' 'VARYING'?) /
// // ('NATIONAL' 'CHAR' 'VARYING'?) /
// // ('NCHAR' 'VARYING'?) /
// // 'VARCHAR'
// unique_ptr<SQLStatement> PEGTransformerFactory::TransformCharacterType(PEGTransformer &transformer,
//                                                                        optional_ptr<ParseResult> parse_result) {
// 	auto &list_pr = parse_result->Cast<ListParseResult>();
// 	throw NotImplementedException("Rule 'CharacterType' has not been implemented yet");
// }
//
// // ColIdType <- ColId Type
// unique_ptr<SQLStatement> PEGTransformerFactory::TransformColIdType(PEGTransformer &transformer,
//                                                                    optional_ptr<ParseResult> parse_result) {
// 	auto &list_pr = parse_result->Cast<ListParseResult>();
// 	throw NotImplementedException("Rule 'ColIdType' has not been implemented yet");
// }
//
// // CollationName <- Identifier
// unique_ptr<SQLStatement> PEGTransformerFactory::TransformCollationName(PEGTransformer &transformer,
//                                                                        optional_ptr<ParseResult> parse_result) {
// 	auto &list_pr = parse_result->Cast<ListParseResult>();
// 	throw NotImplementedException("Rule 'CollationName' has not been implemented yet");
// }
//
// // ColumnName <- Identifier
// unique_ptr<SQLStatement> PEGTransformerFactory::TransformColumnName(PEGTransformer &transformer,
//                                                                     optional_ptr<ParseResult> parse_result) {
// 	auto &list_pr = parse_result->Cast<ListParseResult>();
// 	throw NotImplementedException("Rule 'ColumnName' has not been implemented yet");
// }
//
// // ConstraintName <- ColIdOrString
// unique_ptr<SQLStatement> PEGTransformerFactory::TransformConstraintName(PEGTransformer &transformer,
//                                                                         optional_ptr<ParseResult> parse_result) {
// 	auto &list_pr = parse_result->Cast<ListParseResult>();
// 	throw NotImplementedException("Rule 'ConstraintName' has not been implemented yet");
// }
//
// // CopyOptionName <- ColLabel
// unique_ptr<SQLStatement> PEGTransformerFactory::TransformCopyOptionName(PEGTransformer &transformer,
//                                                                         optional_ptr<ParseResult> parse_result) {
// 	auto &list_pr = parse_result->Cast<ListParseResult>();
// 	throw NotImplementedException("Rule 'CopyOptionName' has not been implemented yet");
// }
//
// // DayKeyword <- 'DAY' / 'DAYS'
// unique_ptr<SQLStatement> PEGTransformerFactory::TransformDayKeyword(PEGTransformer &transformer,
//                                                                     optional_ptr<ParseResult> parse_result) {
// 	auto &list_pr = parse_result->Cast<ListParseResult>();
// 	throw NotImplementedException("Rule 'DayKeyword' has not been implemented yet");
// }
//
// // DecadeKeyword <- 'DECADE' / 'DECADES'
// unique_ptr<SQLStatement> PEGTransformerFactory::TransformDecadeKeyword(PEGTransformer &transformer,
//                                                                        optional_ptr<ParseResult> parse_result) {
// 	auto &list_pr = parse_result->Cast<ListParseResult>();
// 	throw NotImplementedException("Rule 'DecadeKeyword' has not been implemented yet");
// }
//
// // FunctionName <- Identifier
// unique_ptr<SQLStatement> PEGTransformerFactory::TransformFunctionName(PEGTransformer &transformer,
//                                                                       optional_ptr<ParseResult> parse_result) {
// 	auto &list_pr = parse_result->Cast<ListParseResult>();
// 	throw NotImplementedException("Rule 'FunctionName' has not been implemented yet");
// }
//
// // HourKeyword <- 'HOUR' / 'HOURS'
// unique_ptr<SQLStatement> PEGTransformerFactory::TransformHourKeyword(PEGTransformer &transformer,
//                                                                      optional_ptr<ParseResult> parse_result) {
// 	auto &list_pr = parse_result->Cast<ListParseResult>();
// 	throw NotImplementedException("Rule 'HourKeyword' has not been implemented yet");
// }
//
// // IndexName <- Identifier
// unique_ptr<SQLStatement> PEGTransformerFactory::TransformIndexName(PEGTransformer &transformer,
//                                                                    optional_ptr<ParseResult> parse_result) {
// 	auto &list_pr = parse_result->Cast<ListParseResult>();
// 	throw NotImplementedException("Rule 'IndexName' has not been implemented yet");
// }
//
// // Interval <- YearKeyword /
// // MonthKeyword /
// // DayKeyword /
// // HourKeyword /
// // MinuteKeyword /
// // SecondKeyword /
// // MillisecondKeyword /
// // MicrosecondKeyword /
// // WeekKeyword /
// // QuarterKeyword /
// // DecadeKeyword /
// // CenturyKeyword /
// // MillenniumKeyword /
// // (YearKeyword 'TO' MonthKeyword) /
// // (DayKeyword 'TO' HourKeyword) /
// // (DayKeyword 'TO' MinuteKeyword) /
// // (DayKeyword 'TO' SecondKeyword) /
// // (HourKeyword 'TO' MinuteKeyword) /
// // (HourKeyword 'TO' SecondKeyword) /
// // (MinuteKeyword 'TO' SecondKeyword)
// unique_ptr<SQLStatement> PEGTransformerFactory::TransformInterval(PEGTransformer &transformer,
//                                                                   optional_ptr<ParseResult> parse_result) {
// 	auto &list_pr = parse_result->Cast<ListParseResult>();
// 	throw NotImplementedException("Rule 'Interval' has not been implemented yet");
// }
//
// // IntervalType <- ('INTERVAL' Interval?) / ('INTERVAL' Parens(NumberLiteral))
// unique_ptr<SQLStatement> PEGTransformerFactory::TransformIntervalType(PEGTransformer &transformer,
//                                                                       optional_ptr<ParseResult> parse_result) {
// 	auto &list_pr = parse_result->Cast<ListParseResult>();
// 	throw NotImplementedException("Rule 'IntervalType' has not been implemented yet");
// }
//
// // MapType <- 'MAP' Parens(List(Type))
// unique_ptr<SQLStatement> PEGTransformerFactory::TransformMapType(PEGTransformer &transformer,
//                                                                  optional_ptr<ParseResult> parse_result) {
// 	auto &list_pr = parse_result->Cast<ListParseResult>();
// 	throw NotImplementedException("Rule 'MapType' has not been implemented yet");
// }
//
// // MicrosecondKeyword <- 'MICROSECOND' / 'MICROSECONDS'
// unique_ptr<SQLStatement> PEGTransformerFactory::TransformMicrosecondKeyword(PEGTransformer &transformer,
//                                                                             optional_ptr<ParseResult> parse_result) {
// 	auto &list_pr = parse_result->Cast<ListParseResult>();
// 	throw NotImplementedException("Rule 'MicrosecondKeyword' has not been implemented yet");
// }
//
// // MillenniumKeyword <- 'MILLENNIUM' / 'MILLENNIA'
// unique_ptr<SQLStatement> PEGTransformerFactory::TransformMillenniumKeyword(PEGTransformer &transformer,
//                                                                            optional_ptr<ParseResult> parse_result) {
// 	auto &list_pr = parse_result->Cast<ListParseResult>();
// 	throw NotImplementedException("Rule 'MillenniumKeyword' has not been implemented yet");
// }
//
// // MillisecondKeyword <- 'MILLISECOND' / 'MILLISECONDS'
// unique_ptr<SQLStatement> PEGTransformerFactory::TransformMillisecondKeyword(PEGTransformer &transformer,
//                                                                             optional_ptr<ParseResult> parse_result) {
// 	auto &list_pr = parse_result->Cast<ListParseResult>();
// 	throw NotImplementedException("Rule 'MillisecondKeyword' has not been implemented yet");
// }
//
// // MinuteKeyword <- 'MINUTE' / 'MINUTES'
// unique_ptr<SQLStatement> PEGTransformerFactory::TransformMinuteKeyword(PEGTransformer &transformer,
//                                                                        optional_ptr<ParseResult> parse_result) {
// 	auto &list_pr = parse_result->Cast<ListParseResult>();
// 	throw NotImplementedException("Rule 'MinuteKeyword' has not been implemented yet");
// }
//
// // MonthKeyword <- 'MONTH' / 'MONTHS'
// unique_ptr<SQLStatement> PEGTransformerFactory::TransformMonthKeyword(PEGTransformer &transformer,
//                                                                       optional_ptr<ParseResult> parse_result) {
// 	auto &list_pr = parse_result->Cast<ListParseResult>();
// 	throw NotImplementedException("Rule 'MonthKeyword' has not been implemented yet");
// }

// NumberLiteral <- < [+-]?[0-9]*([.][0-9]*)? >
unique_ptr<ParsedExpression> PEGTransformerFactory::TransformNumberLiteral(PEGTransformer &transformer, optional_ptr<ParseResult> parse_result) {
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

// // NumericType <- 'INT' /
// // 'INTEGER' /
// // 'SMALLINT' /
// // 'BIGINT' /
// // 'REAL' /
// // 'BOOLEAN' /
// // ('FLOAT' Parens(NumberLiteral)?) /
// // ('DOUBLE' 'PRECISION') /
// // ('DECIMAL' TypeModifiers?) /
// // ('DEC' TypeModifiers?) /
// // ('NUMERIC' TypeModifiers?)
// unique_ptr<SQLStatement> PEGTransformerFactory::TransformNumericType(PEGTransformer &transformer,
//                                                                      optional_ptr<ParseResult> parse_result) {
// 	auto &list_pr = parse_result->Cast<ListParseResult>();
// 	throw NotImplementedException("Rule 'NumericType' has not been implemented yet");
// }
//
// // PragmaName <- Identifier
// unique_ptr<SQLStatement> PEGTransformerFactory::TransformPragmaName(PEGTransformer &transformer,
//                                                                     optional_ptr<ParseResult> parse_result) {
// 	auto &list_pr = parse_result->Cast<ListParseResult>();
// 	throw NotImplementedException("Rule 'PragmaName' has not been implemented yet");
// }
//
// // QualifiedTypeName <- CatalogQualification? SchemaQualification? TypeName
// unique_ptr<SQLStatement> PEGTransformerFactory::TransformQualifiedTypeName(PEGTransformer &transformer,
//                                                                            optional_ptr<ParseResult> parse_result) {
// 	auto &list_pr = parse_result->Cast<ListParseResult>();
// 	throw NotImplementedException("Rule 'QualifiedTypeName' has not been implemented yet");
// }
//
// // QuarterKeyword <- 'QUARTER' / 'QUARTERS'
// unique_ptr<SQLStatement> PEGTransformerFactory::TransformQuarterKeyword(PEGTransformer &transformer,
//                                                                         optional_ptr<ParseResult> parse_result) {
// 	auto &list_pr = parse_result->Cast<ListParseResult>();
// 	throw NotImplementedException("Rule 'QuarterKeyword' has not been implemented yet");
// }
//
// // ReservedColumnName <- Identifier
// unique_ptr<SQLStatement> PEGTransformerFactory::TransformReservedColumnName(PEGTransformer &transformer,
//                                                                             optional_ptr<ParseResult> parse_result) {
// 	auto &list_pr = parse_result->Cast<ListParseResult>();
// 	throw NotImplementedException("Rule 'ReservedColumnName' has not been implemented yet");
// }
//
// // ReservedFunctionName <- Identifier
// unique_ptr<SQLStatement> PEGTransformerFactory::TransformReservedFunctionName(PEGTransformer &transformer,
//                                                                               optional_ptr<ParseResult> parse_result) {
// 	auto &list_pr = parse_result->Cast<ListParseResult>();
// 	throw NotImplementedException("Rule 'ReservedFunctionName' has not been implemented yet");
// }
//
// // ReservedIdentifier <- Identifier
// unique_ptr<SQLStatement> PEGTransformerFactory::TransformReservedIdentifier(PEGTransformer &transformer,
//                                                                             optional_ptr<ParseResult> parse_result) {
// 	auto &list_pr = parse_result->Cast<ListParseResult>();
// 	throw NotImplementedException("Rule 'ReservedIdentifier' has not been implemented yet");
// }
//
// // ReservedSchemaName <- Identifier
// unique_ptr<SQLStatement> PEGTransformerFactory::TransformReservedSchemaName(PEGTransformer &transformer,
//                                                                             optional_ptr<ParseResult> parse_result) {
// 	auto &list_pr = parse_result->Cast<ListParseResult>();
// 	throw NotImplementedException("Rule 'ReservedSchemaName' has not been implemented yet");
// }
//
// // ReservedTableName <- Identifier
// unique_ptr<SQLStatement> PEGTransformerFactory::TransformReservedTableName(PEGTransformer &transformer,
//                                                                            optional_ptr<ParseResult> parse_result) {
// 	auto &list_pr = parse_result->Cast<ListParseResult>();
// 	throw NotImplementedException("Rule 'ReservedTableName' has not been implemented yet");
// }
//
// // RowOrStruct <- 'ROW' / 'STRUCT'
// //
// // # internal definitions
// // %whitespace <- [ \t\n\r]*
// // List(D) <- D (',' D)* ','?
// // Parens(D) <- '(' D ')'
// unique_ptr<SQLStatement> PEGTransformerFactory::TransformRowOrStruct(PEGTransformer &transformer,
//                                                                      optional_ptr<ParseResult> parse_result) {
// 	auto &list_pr = parse_result->Cast<ListParseResult>();
// 	throw NotImplementedException("Rule 'RowOrStruct' has not been implemented yet");
// }
//
// // RowType <- RowOrStruct Parens(List(ColIdType))
// unique_ptr<SQLStatement> PEGTransformerFactory::TransformRowType(PEGTransformer &transformer,
//                                                                  optional_ptr<ParseResult> parse_result) {
// 	auto &list_pr = parse_result->Cast<ListParseResult>();
// 	throw NotImplementedException("Rule 'RowType' has not been implemented yet");
// }
//
// // SchemaName <- Identifier
// unique_ptr<SQLStatement> PEGTransformerFactory::TransformSchemaName(PEGTransformer &transformer,
//                                                                     optional_ptr<ParseResult> parse_result) {
// 	auto &list_pr = parse_result->Cast<ListParseResult>();
// 	throw NotImplementedException("Rule 'SchemaName' has not been implemented yet");
// }
//
// // SecondKeyword <- 'SECOND' / 'SECONDS'
// unique_ptr<SQLStatement> PEGTransformerFactory::TransformSecondKeyword(PEGTransformer &transformer,
//                                                                        optional_ptr<ParseResult> parse_result) {
// 	auto &list_pr = parse_result->Cast<ListParseResult>();
// 	throw NotImplementedException("Rule 'SecondKeyword' has not been implemented yet");
// }
//
// // SecretName <- ColId
// unique_ptr<SQLStatement> PEGTransformerFactory::TransformSecretName(PEGTransformer &transformer,
//                                                                     optional_ptr<ParseResult> parse_result) {
// 	auto &list_pr = parse_result->Cast<ListParseResult>();
// 	throw NotImplementedException("Rule 'SecretName' has not been implemented yet");
// }
//
// // SequenceName <- Identifier
// unique_ptr<SQLStatement> PEGTransformerFactory::TransformSequenceName(PEGTransformer &transformer,
//                                                                       optional_ptr<ParseResult> parse_result) {
// 	auto &list_pr = parse_result->Cast<ListParseResult>();
// 	throw NotImplementedException("Rule 'SequenceName' has not been implemented yet");
// }
//
// // SetofType <- 'SETOF' Type
// unique_ptr<SQLStatement> PEGTransformerFactory::TransformSetofType(PEGTransformer &transformer,
//                                                                    optional_ptr<ParseResult> parse_result) {
// 	auto &list_pr = parse_result->Cast<ListParseResult>();
// 	throw NotImplementedException("Rule 'SetofType' has not been implemented yet");
// }
//
// // SettingName <- Identifier
// unique_ptr<SQLStatement> PEGTransformerFactory::TransformSettingName(PEGTransformer &transformer,
//                                                                      optional_ptr<ParseResult> parse_result) {
// 	auto &list_pr = parse_result->Cast<ListParseResult>();
// 	throw NotImplementedException("Rule 'SettingName' has not been implemented yet");
// }
//
// // SimpleType <- (QualifiedTypeName / CharacterType) TypeModifiers?
// unique_ptr<SQLStatement> PEGTransformerFactory::TransformSimpleType(PEGTransformer &transformer,
//                                                                     optional_ptr<ParseResult> parse_result) {
// 	auto &list_pr = parse_result->Cast<ListParseResult>();
// 	throw NotImplementedException("Rule 'SimpleType' has not been implemented yet");
// }
//
// // StringLiteral <- '\'' [^\']* '\''
// unique_ptr<SQLStatement> PEGTransformerFactory::TransformStringLiteral(PEGTransformer &transformer,
//                                                                        optional_ptr<ParseResult> parse_result) {
// 	auto &list_pr = parse_result->Cast<ListParseResult>();
// 	throw NotImplementedException("Rule 'StringLiteral' has not been implemented yet");
// }
//
// // TableFunctionName <- Identifier
// unique_ptr<SQLStatement> PEGTransformerFactory::TransformTableFunctionName(PEGTransformer &transformer,
//                                                                            optional_ptr<ParseResult> parse_result) {
// 	auto &list_pr = parse_result->Cast<ListParseResult>();
// 	throw NotImplementedException("Rule 'TableFunctionName' has not been implemented yet");
// }
//
// // TableName <- Identifier
// unique_ptr<SQLStatement> PEGTransformerFactory::TransformTableName(PEGTransformer &transformer,
//                                                                    optional_ptr<ParseResult> parse_result) {
// 	auto &list_pr = parse_result->Cast<ListParseResult>();
// 	throw NotImplementedException("Rule 'TableName' has not been implemented yet");
// }
//
// // TimeOrTimestamp <- 'TIME' / 'TIMESTAMP'
// unique_ptr<SQLStatement> PEGTransformerFactory::TransformTimeOrTimestamp(PEGTransformer &transformer,
//                                                                          optional_ptr<ParseResult> parse_result) {
// 	auto &list_pr = parse_result->Cast<ListParseResult>();
// 	throw NotImplementedException("Rule 'TimeOrTimestamp' has not been implemented yet");
// }
//
// // TimeType <- TimeOrTimestamp TypeModifiers? TimeZone?
// unique_ptr<SQLStatement> PEGTransformerFactory::TransformTimeType(PEGTransformer &transformer,
//                                                                   optional_ptr<ParseResult> parse_result) {
// 	auto &list_pr = parse_result->Cast<ListParseResult>();
// 	throw NotImplementedException("Rule 'TimeType' has not been implemented yet");
// }
//
// // TimeZone <- WithOrWithout 'TIME' 'ZONE'
// unique_ptr<SQLStatement> PEGTransformerFactory::TransformTimeZone(PEGTransformer &transformer,
//                                                                   optional_ptr<ParseResult> parse_result) {
// 	auto &list_pr = parse_result->Cast<ListParseResult>();
// 	throw NotImplementedException("Rule 'TimeZone' has not been implemented yet");
// }
//
// // Type <- (TimeType / IntervalType / BitType / RowType / MapType / UnionType / NumericType / SetofType / SimpleType)
// // ArrayBounds*
// unique_ptr<SQLStatement> PEGTransformerFactory::TransformType(PEGTransformer &transformer,
//                                                               optional_ptr<ParseResult> parse_result) {
// 	auto &list_pr = parse_result->Cast<ListParseResult>();
// 	throw NotImplementedException("Rule 'Type' has not been implemented yet");
// }
//
// // TypeModifiers <- Parens(List(Expression)?)
// unique_ptr<SQLStatement> PEGTransformerFactory::TransformTypeModifiers(PEGTransformer &transformer,
//                                                                        optional_ptr<ParseResult> parse_result) {
// 	auto &list_pr = parse_result->Cast<ListParseResult>();
// 	throw NotImplementedException("Rule 'TypeModifiers' has not been implemented yet");
// }
//
// // UnionType <- 'UNION' Parens(List(ColIdType))
// unique_ptr<SQLStatement> PEGTransformerFactory::TransformUnionType(PEGTransformer &transformer,
//                                                                    optional_ptr<ParseResult> parse_result) {
// 	auto &list_pr = parse_result->Cast<ListParseResult>();
// 	throw NotImplementedException("Rule 'UnionType' has not been implemented yet");
// }
//
// // WeekKeyword <- 'WEEK' / 'WEEKS'
// unique_ptr<SQLStatement> PEGTransformerFactory::TransformWeekKeyword(PEGTransformer &transformer,
//                                                                      optional_ptr<ParseResult> parse_result) {
// 	auto &list_pr = parse_result->Cast<ListParseResult>();
// 	throw NotImplementedException("Rule 'WeekKeyword' has not been implemented yet");
// }
//
// // WithOrWithout <- 'WITH' / 'WITHOUT'
// unique_ptr<SQLStatement> PEGTransformerFactory::TransformWithOrWithout(PEGTransformer &transformer,
//                                                                        optional_ptr<ParseResult> parse_result) {
// 	auto &list_pr = parse_result->Cast<ListParseResult>();
// 	throw NotImplementedException("Rule 'WithOrWithout' has not been implemented yet");
// }
//
// // YearKeyword <- 'YEAR' / 'YEARS'
// unique_ptr<SQLStatement> PEGTransformerFactory::TransformYearKeyword(PEGTransformer &transformer,
//                                                                      optional_ptr<ParseResult> parse_result) {
// 	auto &list_pr = parse_result->Cast<ListParseResult>();
// 	throw NotImplementedException("Rule 'YearKeyword' has not been implemented yet");
// }
} // namespace duckdb
