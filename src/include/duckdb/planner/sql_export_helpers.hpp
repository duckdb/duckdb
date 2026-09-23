#pragma once

#include "duckdb/common/identifier.hpp"
#include "duckdb/parser/expression/cast_expression.hpp"
#include "duckdb/parser/expression/collate_expression.hpp"
#include "duckdb/common/type_visitor.hpp"
#include "duckdb/planner/logical_plan_verification_result.hpp"

namespace duckdb {
namespace SQLExportHelpers {

inline LogicalPlanVerificationPath
ChildPath(const LogicalPlanVerificationPath &path, idx_t ordinal,
          LogicalPlanVerificationPathComponentType type = LogicalPlanVerificationPathComponentType::EXPRESSION_CHILD) {
	auto result = path;
	result.components.push_back({type, ordinal});
	return result;
}

inline LogicalPlanVerificationIssue MakeIssue(LogicalPlanVerificationIssueCode code, LogicalPlanVerificationPhase phase,
                                              optional<LogicalPlanVerificationPath> path,
                                              optional<LogicalPlanVerificationConstructIdentity> construct,
                                              string message) {
	LogicalPlanVerificationIssue issue;
	issue.code = code;
	issue.phase = phase;
	issue.path = std::move(path);
	issue.construct = std::move(construct);
	issue.message = std::move(message);
	return issue;
}

inline unique_ptr<ParsedExpression> OrderExpression(const LogicalType &type, unique_ptr<ParsedExpression> expression) {
	if (type.id() == LogicalTypeId::VARCHAR) {
		if (type.HasAlias()) {
			expression = make_uniq<CastExpression>(LogicalType::VARCHAR, std::move(expression));
		}
		return make_uniq<CollateExpression>("C", std::move(expression));
	}
	return expression;
}

inline bool IsValidIdentifier(const Identifier &identifier) {
	auto &name = identifier.GetIdentifierName();
	return !name.empty() && name.find('\0') == string::npos && Value::StringIsValid(name);
}

inline bool IsSQLExportType(LogicalTypeId id) {
	return TypeExpression::IsSQLType(id);
}

inline bool IsSQLRepresentableType(const LogicalType &type) {
	return TypeExpression::CanRepresent(type);
}

inline bool IsSQLValueType(const LogicalType &type) {
	return type.IsComplete() &&
	       !TypeVisitor::Contains(type, [](const LogicalType &child) { return !IsSQLExportType(child.id()); });
}

inline string TypeCollationSignature(const LogicalType &type) {
	string result;
	TypeVisitor::Contains(type, [&](const LogicalType &child) {
		if (child.id() == LogicalTypeId::VARCHAR) {
			const auto collation = StringType::GetCollation(child);
			result += std::to_string(collation.size()) + ":" + collation + ";";
		}
		return false;
	});
	return result;
}

} // namespace SQLExportHelpers
} // namespace duckdb
