#include "duckdb/parser/peg/transformer/peg_transformer.hpp"
#include "duckdb/parser/expression/cast_expression.hpp"
#include "duckdb/parser/expression/default_expression.hpp"
#include "duckdb/parser/expression/columnref_expression.hpp"
#include "duckdb/parser/expression/constant_expression.hpp"
#include "duckdb/common/string_util.hpp"

namespace duckdb {

namespace {

// Extract the matched isolation level string from an IsolationLevel parse result.
// Grammar: IsolationLevel <- ('READ' 'COMMITTED') / ('READ' 'UNCOMMITTED') /
//                            ('REPEATABLE' 'READ') / 'SERIALIZABLE'
// Returns a value compatible with TransactionIsolationLevel's enum string form
// (e.g. "read committed", "repeatable read").
string ExtractIsolationLevelString(ParseResult &parse_result) {
	// IsolationLevel is a top-level rule whose body is a Choice -> the matcher
	// wraps a single ChoiceMatcher in the rule's outer ListMatcher.
	auto &outer = parse_result.Cast<ListParseResult>();
	auto &choice_pr = outer.Child<ChoiceParseResult>(0);
	auto &inner = choice_pr.GetResult();
	if (inner.type == ParseResultType::KEYWORD) {
		// 'SERIALIZABLE'
		auto &keyword = inner.Cast<KeywordParseResult>().keyword;
		return StringUtil::Lower(keyword);
	}
	// Sequence of two keywords: ('READ' 'COMMITTED'), ('READ' 'UNCOMMITTED'),
	// or ('REPEATABLE' 'READ').
	auto &inner_list = inner.Cast<ListParseResult>();
	auto &first = inner_list.Child<KeywordParseResult>(0).keyword;
	auto &second = inner_list.Child<KeywordParseResult>(1).keyword;
	return StringUtil::Lower(first) + " " + StringUtil::Lower(second);
}

// PG-compat for serenedb: SET search_path = a, "b,c", cat.s  -> normalize to
// one comma-joined PG-quoted string so ParseList(...) treats each entry as one
// atomic name. Mirrors the original libpg_query path.
unique_ptr<SetStatement> TransformSetSearchPath(const string &name, SetScope scope,
                                                vector<unique_ptr<ParsedExpression>> values) {
	auto make_set = [&](string value) {
		return make_uniq<SetVariableStatement>(name, make_uniq<ConstantExpression>(Value(std::move(value))), scope);
	};
	auto serialize = [&](ParsedExpression &expr) -> string {
		if (expr.GetExpressionType() == ExpressionType::COLUMN_REF) {
			// ColumnRefExpression::ToString applies PG quoting so names with
			// commas/dots survive ParseList as one atomic entry.
			return expr.ToString();
		}
		if (expr.GetExpressionType() == ExpressionType::VALUE_CONSTANT) {
			return expr.Cast<ConstantExpression>().GetValue().ToString();
		}
		throw ParserException("SET search_path: expected identifier or string literal");
	};
	if (values.empty()) {
		return make_set("");
	}
	if (values.size() == 1) {
		auto &expr = *values[0];
		if (expr.GetExpressionType() == ExpressionType::VALUE_DEFAULT) {
			return make_uniq<ResetVariableStatement>(name, scope);
		}
		// Single string literal: wrap in double quotes so commas in the literal
		// are not treated as separators by ParseList. Empty literal stays empty.
		if (expr.GetExpressionType() == ExpressionType::VALUE_CONSTANT) {
			auto &const_expr = expr.Cast<ConstantExpression>();
			auto val = const_expr.GetValue();
			if (val.type().id() == LogicalTypeId::VARCHAR) {
				string raw = val.GetValue<string>();
				if (raw.empty()) {
					return make_set(raw);
				}
				string wrapped = "\"" + StringUtil::Replace(raw, "\"", "\"\"") + "\"";
				return make_set(std::move(wrapped));
			}
		}
		return make_set(serialize(expr));
	}
	// Multi-arg: comma-join each PG-quoted element.
	string joined;
	for (auto &value : values) {
		if (!joined.empty()) {
			joined += ",";
		}
		joined += serialize(*value);
	}
	return make_set(std::move(joined));
}

} // namespace

// ResetStatement <- 'RESET' (ResetAll / SetVariable / SetSetting)
unique_ptr<SQLStatement> PEGTransformerFactory::TransformResetStatement(PEGTransformer &transformer,
                                                                        ParseResult &parse_result) {
	auto &list_pr = parse_result.Cast<ListParseResult>();
	auto &child_pr = list_pr.Child<ListParseResult>(1);
	auto &choice_pr = child_pr.Child<ChoiceParseResult>(0);

	SettingInfo setting_info = transformer.Transform<SettingInfo>(choice_pr.GetResult());
	// PG-compat: RESET LOCAL is handled by PhysicalReset at execution time
	// (rejected outside a transaction with the canonical PG error). The
	// upstream PEG transformer's NotImplemented throw is removed to keep
	// parity with the v2026.05.18 libpg_query path.
	return make_uniq<ResetVariableStatement>(setting_info.name, setting_info.scope);
}

// ResetAliasedSetting <- ResetSettingAlias
// ResetSettingAlias <- ('TRANSACTION' 'ISOLATION' 'LEVEL') / ('SESSION' 'AUTHORIZATION') / ('TIME' 'ZONE')
// PG-compat: RESET takes the same multi-word aliases as SHOW. Map each to
// its underlying GUC name; the case-insensitive setting lookup picks up the
// canonical-cased registration (TimeZone) on its own.
SettingInfo PEGTransformerFactory::TransformResetAliasedSetting(PEGTransformer &transformer,
                                                                ParseResult &parse_result) {
	auto &outer = parse_result.Cast<ListParseResult>();
	auto &alias = outer.Child<ListParseResult>(0);
	auto &choice = alias.Child<ChoiceParseResult>(0);
	auto &alts = choice.GetResult().Cast<ListParseResult>();
	auto &first_kw = alts.Child<KeywordParseResult>(0).keyword;

	SettingInfo info;
	if (StringUtil::CIEquals(first_kw, "TRANSACTION")) {
		info.name = "transaction_isolation";
	} else if (StringUtil::CIEquals(first_kw, "SESSION")) {
		info.name = "session_authorization";
	} else {
		info.name = "timezone";
	}
	return info;
}

// ResetAll <- ('LOCAL' 'ALL') / 'ALL'
// PhysicalReset::GetDataInternal dispatches to ResetAll(...) when the
// target name is empty, so emit a SettingInfo with an empty name. The
// LOCAL variant flags scope=LOCAL so PhysicalReset can transaction-bound it.
SettingInfo PEGTransformerFactory::TransformResetAll(PEGTransformer &transformer, ParseResult &parse_result) {
	SettingInfo result;
	result.name = "";
	auto &list_pr = parse_result.Cast<ListParseResult>();
	auto &choice = list_pr.Child<ChoiceParseResult>(0);
	if (choice.GetResult().type == ParseResultType::LIST) {
		// First alternative: 'LOCAL' 'ALL' keywords.
		result.scope = SetScope::LOCAL;
	}
	return result;
}

// SetTransactionIsolation <- 'TRANSACTION' 'ISOLATION' 'LEVEL' IsolationLevel
// Maps to PG's SET TRANSACTION ISOLATION LEVEL ...; we forward the parsed level
// into serenedb's existing "transaction_isolation" client setting, whose
// SetLocal callback enforces "must be inside a transaction".
unique_ptr<SetStatement> PEGTransformerFactory::TransformSetTransactionIsolation(PEGTransformer &transformer,
                                                                                 ParseResult &parse_result) {
	auto &list_pr = parse_result.Cast<ListParseResult>();
	// children: 'TRANSACTION', 'ISOLATION', 'LEVEL', IsolationLevel
	auto level = ExtractIsolationLevelString(list_pr.Child<ListParseResult>(3));
	return make_uniq<SetVariableStatement>("transaction_isolation", make_uniq<ConstantExpression>(Value(level)),
	                                       SetScope::AUTOMATIC);
}

// SetSessionCharacteristics <- 'SESSION' 'CHARACTERISTICS' 'AS' 'TRANSACTION' 'ISOLATION' 'LEVEL' IsolationLevel
// Maps to PG's SET SESSION CHARACTERISTICS AS TRANSACTION ISOLATION LEVEL ...;
// forwarded into serenedb's "default_transaction_isolation" setting, which is
// what BEGIN reads as the default for new transactions.
unique_ptr<SetStatement> PEGTransformerFactory::TransformSetSessionCharacteristics(PEGTransformer &transformer,
                                                                                   ParseResult &parse_result) {
	auto &list_pr = parse_result.Cast<ListParseResult>();
	// children: 'SESSION', 'CHARACTERISTICS', 'AS', 'TRANSACTION', 'ISOLATION', 'LEVEL', IsolationLevel
	auto level = ExtractIsolationLevelString(list_pr.Child<ListParseResult>(6));
	return make_uniq<SetVariableStatement>("default_transaction_isolation", make_uniq<ConstantExpression>(Value(level)),
	                                       SetScope::AUTOMATIC);
}

// SetAssignment <- VariableAssign VariableList
vector<unique_ptr<ParsedExpression>> PEGTransformerFactory::TransformSetAssignment(PEGTransformer &transformer,
                                                                                   ParseResult &parse_result) {
	auto &list_pr = parse_result.Cast<ListParseResult>();
	return transformer.Transform<vector<unique_ptr<ParsedExpression>>>(list_pr, 1);
}

// SetSetting <- SettingScope? SettingName
SettingInfo PEGTransformerFactory::TransformSetSetting(PEGTransformer &transformer, ParseResult &parse_result) {
	auto &list_pr = parse_result.Cast<ListParseResult>();
	auto &optional_scope_pr = list_pr.Child<OptionalParseResult>(0);

	SettingInfo result;
	result.name = list_pr.Child<IdentifierParseResult>(1).identifier;
	if (optional_scope_pr.HasResult()) {
		auto &setting_scope = optional_scope_pr.GetResult().Cast<ListParseResult>();
		auto &scope_value = setting_scope.Child<ChoiceParseResult>(0);
		result.scope = transformer.TransformEnum<SetScope>(scope_value);
	}
	return result;
}

// SetStatement <- 'SET' (StandardAssignment / SetTimeZone)
unique_ptr<SQLStatement> PEGTransformerFactory::TransformSetStatement(PEGTransformer &transformer,
                                                                      ParseResult &parse_result) {
	auto &list_pr = parse_result.Cast<ListParseResult>();
	auto &child_pr = list_pr.Child<ListParseResult>(1);
	return transformer.Transform<unique_ptr<SetStatement>>(child_pr.Child<ChoiceParseResult>(0).GetResult());
}

// ZoneIntervalWithInterval <- 'INTERVAL' StringLiteral Interval?
unique_ptr<ParsedExpression> PEGTransformerFactory::TransformZoneIntervalWithInterval(PEGTransformer &transformer,
                                                                                      ParseResult &parse_result) {
	auto &list_pr = parse_result.Cast<ListParseResult>();
	// child 0 = 'INTERVAL' keyword, child 1 = StringLiteral, child 2 = Interval?
	auto &str_pr = list_pr.Child<StringLiteralParseResult>(1);
	auto expr = make_uniq<ConstantExpression>(Value(str_pr.result));
	return make_uniq<CastExpression>(LogicalType::INTERVAL, std::move(expr));
}

// ZoneIntervalWithPrecision <- 'INTERVAL' Parens(NumberLiteral) StringLiteral
unique_ptr<ParsedExpression> PEGTransformerFactory::TransformZoneIntervalWithPrecision(PEGTransformer &transformer,
                                                                                       ParseResult &parse_result) {
	auto &list_pr = parse_result.Cast<ListParseResult>();
	// child 0 = 'INTERVAL' keyword, child 1 = Parens(NumberLiteral), child 2 = StringLiteral
	auto &str_pr = list_pr.Child<StringLiteralParseResult>(2);
	auto expr = make_uniq<ConstantExpression>(Value(str_pr.result));
	return make_uniq<CastExpression>(LogicalType::INTERVAL, std::move(expr));
}

// ZoneValue <- ZoneIntervalWithPrecision / ZoneIntervalWithInterval / StringLiteral / Identifier / NumberLiteral /
// 'DEFAULT' / 'LOCAL'
unique_ptr<ParsedExpression> PEGTransformerFactory::TransformZoneValue(PEGTransformer &transformer,
                                                                       ParseResult &parse_result) {
	auto &list_pr = parse_result.Cast<ListParseResult>();
	auto &choice_pr = list_pr.Child<ChoiceParseResult>(0);
	if (choice_pr.GetResult().type == ParseResultType::STRING) {
		return make_uniq<ConstantExpression>(Value(choice_pr.GetResult().Cast<StringLiteralParseResult>().result));
	}
	if (choice_pr.GetResult().type == ParseResultType::IDENTIFIER) {
		return make_uniq<ConstantExpression>(Value(choice_pr.GetResult().Cast<IdentifierParseResult>().identifier));
	}
	const auto &name = choice_pr.GetResult().name;
	if (name == "ZoneIntervalWithPrecision" || name == "ZoneIntervalWithInterval" || name == "NumberLiteral") {
		return transformer.Transform<unique_ptr<ParsedExpression>>(choice_pr.GetResult());
	}
	return make_uniq<DefaultExpression>();
}

// SetTimeZone <- 'TIME' 'ZONE' ZoneValue
unique_ptr<SetStatement> PEGTransformerFactory::TransformSetTimeZone(PEGTransformer &transformer,
                                                                     ParseResult &parse_result) {
	auto &list_pr = parse_result.Cast<ListParseResult>();
	auto expr = transformer.Transform<unique_ptr<ParsedExpression>>(list_pr.Child<ListParseResult>(2));
	if (expr->GetExpressionClass() == ExpressionClass::DEFAULT) {
		return make_uniq<ResetVariableStatement>("timezone", SetScope::AUTOMATIC);
	}
	return make_uniq<SetVariableStatement>("timezone", std::move(expr), SetScope::AUTOMATIC);
}

// RoleSpec <- 'DEFAULT' / StringLiteral / Identifier
// Returns a DefaultExpression for the DEFAULT keyword and a string-typed
// ConstantExpression for the identifier / string-literal cases. Shared
// by SetRole and SetSessionAuthorization.
unique_ptr<ParsedExpression> PEGTransformerFactory::TransformRoleSpec(PEGTransformer &transformer,
                                                                      ParseResult &parse_result) {
	auto &list_pr = parse_result.Cast<ListParseResult>();
	auto &choice_pr = list_pr.Child<ChoiceParseResult>(0);
	auto &result = choice_pr.GetResult();
	if (result.type == ParseResultType::STRING) {
		return make_uniq<ConstantExpression>(Value(result.Cast<StringLiteralParseResult>().result));
	}
	if (result.type == ParseResultType::IDENTIFIER) {
		return make_uniq<ConstantExpression>(Value(result.Cast<IdentifierParseResult>().identifier));
	}
	return make_uniq<DefaultExpression>();
}

// SetRole <- 'ROLE' RoleSpec
// Routes to the `role` extension setting (registered in serenedb's
// server/query/config_variables.cpp with a NoOverwrite callback so the
// value flows into session state but doesn't perform real role-switch).
unique_ptr<SetStatement> PEGTransformerFactory::TransformSetRole(PEGTransformer &transformer,
                                                                 ParseResult &parse_result) {
	auto &list_pr = parse_result.Cast<ListParseResult>();
	auto expr = transformer.Transform<unique_ptr<ParsedExpression>>(list_pr.Child<ListParseResult>(1));
	if (expr->GetExpressionClass() == ExpressionClass::DEFAULT) {
		return make_uniq<ResetVariableStatement>("role", SetScope::AUTOMATIC);
	}
	return make_uniq<SetVariableStatement>("role", std::move(expr), SetScope::AUTOMATIC);
}

// SetSessionAuthorization <- 'SESSION' 'AUTHORIZATION' RoleSpec
// Routes to the `session_authorization` setting. Same NoOverwrite shape as
// SET ROLE.
unique_ptr<SetStatement> PEGTransformerFactory::TransformSetSessionAuthorization(PEGTransformer &transformer,
                                                                                 ParseResult &parse_result) {
	auto &list_pr = parse_result.Cast<ListParseResult>();
	auto expr = transformer.Transform<unique_ptr<ParsedExpression>>(list_pr.Child<ListParseResult>(2));
	if (expr->GetExpressionClass() == ExpressionClass::DEFAULT) {
		return make_uniq<ResetVariableStatement>("session_authorization", SetScope::AUTOMATIC);
	}
	return make_uniq<SetVariableStatement>("session_authorization", std::move(expr), SetScope::AUTOMATIC);
}

// SetVariable <- VariableScope Identifier
SettingInfo PEGTransformerFactory::TransformSetVariable(PEGTransformer &transformer, ParseResult &parse_result) {
	auto &list_pr = parse_result.Cast<ListParseResult>();

	SettingInfo result;
	result.scope = transformer.TransformEnum<SetScope>(list_pr.Child<ListParseResult>(0));
	result.name = list_pr.Child<IdentifierParseResult>(1).identifier;
	return result;
}

// StandardAssignment <- (SetVariable / SetSetting) SetAssignment
unique_ptr<SetStatement> PEGTransformerFactory::TransformStandardAssignment(PEGTransformer &transformer,
                                                                            ParseResult &parse_result) {
	auto &list_pr = parse_result.Cast<ListParseResult>();
	auto &first_sub_rule = list_pr.Child<ListParseResult>(0);

	auto &setting_or_var_pr = first_sub_rule.Child<ChoiceParseResult>(0);
	SettingInfo setting_info = transformer.Transform<SettingInfo>(setting_or_var_pr.GetResult());
	// PG-compat: SET LOCAL is enforced at PhysicalSet::SetVariable
	// (transaction-bound). Don't reject at parse time -- that's a regression
	// from the upstream PEG transformer; v2026.05.18's libpg_query path
	// passed the scope through.
	auto &set_assignment_pr = list_pr.Child<ListParseResult>(1);
	auto values = transformer.Transform<vector<unique_ptr<ParsedExpression>>>(set_assignment_pr);
	// PG-compat for serenedb: SET search_path accepts comma-separated lists
	// and unquoted/string-literal/DEFAULT shapes. Normalize into a single
	// already-PG-quoted string before producing the SetVariableStatement.
	if (StringUtil::CIEquals(setting_info.name, "search_path")) {
		return TransformSetSearchPath(setting_info.name, setting_info.scope, std::move(values));
	}
	if (values.size() > 1) {
		throw ParserException("SET can only contain a single value");
	}
	auto value = std::move(values[0]);
	if (value->GetExpressionClass() == ExpressionClass::COLUMN_REF) {
		// SET value cannot be a column reference
		auto &col_ref = value->Cast<ColumnRefExpression>();
		value = make_uniq<ConstantExpression>(col_ref.GetColumnName());
	} else if (value->GetExpressionClass() == ExpressionClass::DEFAULT) {
		return make_uniq<ResetVariableStatement>(setting_info.name, setting_info.scope);
	}
	return make_uniq<SetVariableStatement>(setting_info.name, std::move(value), setting_info.scope);
}

// VariableList <- List(Expression)
vector<unique_ptr<ParsedExpression>> PEGTransformerFactory::TransformVariableList(PEGTransformer &transformer,
                                                                                  ParseResult &parse_result) {
	auto &list_pr = parse_result.Cast<ListParseResult>();
	auto expr_list = ExtractParseResultsFromList(list_pr.Child<ListParseResult>(0));
	vector<unique_ptr<ParsedExpression>> expressions;
	for (auto &expr : expr_list) {
		expressions.push_back(transformer.Transform<unique_ptr<ParsedExpression>>(expr));
	}
	return expressions;
}
} // namespace duckdb
