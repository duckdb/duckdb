#pragma once

#include "duckdb/common/identifier.hpp"
#include "duckdb/common/type_visitor.hpp"
#include "duckdb/common/unordered_set.hpp"
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

inline bool IsValidIdentifier(const Identifier &identifier) {
	auto &name = identifier.GetIdentifierName();
	return !name.empty() && name.find('\0') == string::npos && Value::StringIsValid(name);
}

inline bool IsSQLExportType(LogicalTypeId id) {
	static const auto admitted_ids = [] {
		// SQL export follows AllTypes' value-type coverage, with SQLNULL included and TUPLE excluded.
		unordered_set<LogicalTypeId> ids {LogicalTypeId::SQLNULL};
		for (auto &type : LogicalType::AllTypes()) {
			if (type.id() != LogicalTypeId::TUPLE) {
				ids.insert(type.id());
			}
		}
		return ids;
	}();
	return admitted_ids.count(id) != 0;
}

inline bool IsSQLRepresentableType(const LogicalType &type) {
	return type.IsComplete() &&
	       !TypeVisitor::Contains(type, [](const LogicalType &child) { return !IsSQLExportType(child.id()); });
}

} // namespace SQLExportHelpers
} // namespace duckdb
