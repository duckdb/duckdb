//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/planner/logical_plan_compiler_result.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/common.hpp"
#include "duckdb/common/enums/expression_type.hpp"
#include "duckdb/common/enums/logical_operator_type.hpp"
#include "duckdb/common/optional.hpp"
#include "duckdb/common/types/value.hpp"

namespace duckdb {

enum class LogicalPlanCompilerPathRoot : uint8_t { LOGICAL_PLAN, STANDALONE_EXPRESSION };

enum class LogicalPlanCompilerPathComponentType : uint8_t { OPERATOR_CHILD, OPERATOR_EXPRESSION, EXPRESSION_CHILD };

struct LogicalPlanCompilerPathComponent {
	LogicalPlanCompilerPathComponentType type;
	idx_t ordinal;

	DUCKDB_API bool operator==(const LogicalPlanCompilerPathComponent &other) const;
};

struct LogicalPlanCompilerPath {
	LogicalPlanCompilerPathRoot root = LogicalPlanCompilerPathRoot::LOGICAL_PLAN;
	vector<LogicalPlanCompilerPathComponent> components;

	DUCKDB_API bool IsValid() const;
	DUCKDB_API bool operator==(const LogicalPlanCompilerPath &other) const;
	DUCKDB_API bool operator<(const LogicalPlanCompilerPath &other) const;
};

enum class LogicalPlanCompilerConstructType : uint8_t {
	LOGICAL_OPERATOR,
	EXPRESSION,
	FUNCTION,
	SOURCE_FUNCTION,
	LOGICAL_TYPE,
	BINDING_TYPE_MISMATCH,
	EXTENSION,
	EXPORT_FEATURE
};

struct LogicalPlanCompilerFunctionIdentity {
	string catalog;
	string schema;
	string name;
	vector<LogicalType> arguments;
	LogicalType return_type;

	DUCKDB_API bool IsValid() const;
	DUCKDB_API bool operator==(const LogicalPlanCompilerFunctionIdentity &other) const;
};

struct LogicalPlanCompilerTypeMismatch {
	LogicalType expected_type;
	LogicalType actual_type;

	DUCKDB_API bool IsValid() const;
	DUCKDB_API bool operator==(const LogicalPlanCompilerTypeMismatch &other) const;
};

struct LogicalPlanCompilerConstructIdentity {
	LogicalPlanCompilerConstructType type = LogicalPlanCompilerConstructType::LOGICAL_OPERATOR;
	optional<LogicalOperatorType> logical_operator;
	optional<ExpressionClass> expression;
	optional<LogicalPlanCompilerFunctionIdentity> function;
	optional<LogicalType> logical_type;
	optional<LogicalPlanCompilerTypeMismatch> type_mismatch;
	optional<string> identifier;

	DUCKDB_API static LogicalPlanCompilerConstructIdentity LogicalOperator(LogicalOperatorType type);
	DUCKDB_API static LogicalPlanCompilerConstructIdentity Expression(ExpressionClass expression_class);
	DUCKDB_API static LogicalPlanCompilerConstructIdentity Function(LogicalPlanCompilerFunctionIdentity identity);
	DUCKDB_API static LogicalPlanCompilerConstructIdentity SourceFunction(LogicalPlanCompilerFunctionIdentity identity);
	DUCKDB_API static LogicalPlanCompilerConstructIdentity LogicalTypeValue(LogicalType type);
	DUCKDB_API static LogicalPlanCompilerConstructIdentity BindingTypeMismatch(LogicalType expected_type,
	                                                                           LogicalType actual_type);
	DUCKDB_API static LogicalPlanCompilerConstructIdentity Extension(string identifier);
	DUCKDB_API static LogicalPlanCompilerConstructIdentity ExportFeature(string identifier);

	DUCKDB_API bool IsValid() const;
	DUCKDB_API bool operator==(const LogicalPlanCompilerConstructIdentity &other) const;
	DUCKDB_API bool operator<(const LogicalPlanCompilerConstructIdentity &other) const;
};

enum class LogicalPlanCompilerIssueCode : uint8_t {
	INVALID_BINDING,
	TYPE_MISMATCH,
	UNSUPPORTED_OPERATOR,
	UNSUPPORTED_EXPRESSION,
	UNSUPPORTED_FUNCTION,
	UNSUPPORTED_SOURCE,
	UNSUPPORTED_EXTENSION,
	MALFORMED_EXTENSION_RESULT,
	UNSUPPORTED_EXPORT_FEATURE,
	INTERNAL_INVARIANT
};

enum class LogicalPlanCompilerPhase : uint8_t { VERIFY, EXPRESSION_EXPORT, PLAN_EXPORT };

struct LogicalPlanCompilerIssue {
	LogicalPlanCompilerIssueCode code = LogicalPlanCompilerIssueCode::INTERNAL_INVARIANT;
	LogicalPlanCompilerPhase phase = LogicalPlanCompilerPhase::VERIFY;
	optional<LogicalPlanCompilerPath> path;
	optional<LogicalPlanCompilerConstructIdentity> construct;
	vector<pair<string, Value>> facts;
	string message;

	DUCKDB_API bool IsValid() const;
	DUCKDB_API void Normalize();
	DUCKDB_API bool operator==(const LogicalPlanCompilerIssue &other) const;
	DUCKDB_API bool operator<(const LogicalPlanCompilerIssue &other) const;
};

struct LogicalPlanVerificationSuccess {
	bool operator==(const LogicalPlanVerificationSuccess &other) const {
		return true;
	}
};

template <class T>
class LogicalPlanCompilerResult {
public:
	static LogicalPlanCompilerResult Success(T value) {
		return LogicalPlanCompilerResult(optional<T>(std::move(value)), {});
	}

	static LogicalPlanCompilerResult Failure(vector<LogicalPlanCompilerIssue> issues) {
		for (auto &issue : issues) {
			issue.Normalize();
		}
		std::sort(issues.begin(), issues.end());
		issues.erase(std::unique(issues.begin(), issues.end()), issues.end());
		return LogicalPlanCompilerResult(optional<T>(), std::move(issues));
	}

	bool IsSuccess() const {
		return value.has_value();
	}

	bool HasError() const {
		return !IsSuccess();
	}

	T &GetValue() {
		D_ASSERT(IsSuccess());
		return *value;
	}

	const T &GetValue() const {
		D_ASSERT(IsSuccess());
		return *value;
	}

	const vector<LogicalPlanCompilerIssue> &GetIssues() const {
		return issues;
	}

	bool IsValid() const {
		if (IsSuccess()) {
			return issues.empty();
		}
		if (issues.empty()) {
			return false;
		}
		for (auto &issue : issues) {
			if (!issue.IsValid()) {
				return false;
			}
		}
		return true;
	}

private:
	LogicalPlanCompilerResult(optional<T> value_p, vector<LogicalPlanCompilerIssue> issues_p)
	    : value(std::move(value_p)), issues(std::move(issues_p)) {
	}

private:
	optional<T> value;
	vector<LogicalPlanCompilerIssue> issues;
};

} // namespace duckdb
