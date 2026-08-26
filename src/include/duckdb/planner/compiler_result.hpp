//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/planner/compiler_result.hpp
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

enum class CompilerPathRoot : uint8_t { LOGICAL_PLAN, STANDALONE_EXPRESSION };

enum class CompilerPathComponentType : uint8_t { OPERATOR_CHILD, OPERATOR_EXPRESSION, EXPRESSION_CHILD };

struct CompilerPathComponent {
	CompilerPathComponentType type;
	idx_t ordinal;

	DUCKDB_API bool operator==(const CompilerPathComponent &other) const;
};

struct CompilerPath {
	CompilerPathRoot root = CompilerPathRoot::LOGICAL_PLAN;
	vector<CompilerPathComponent> components;

	DUCKDB_API bool IsValid() const;
	DUCKDB_API bool operator==(const CompilerPath &other) const;
	DUCKDB_API bool operator<(const CompilerPath &other) const;
};

enum class CompilerConstructType : uint8_t {
	LOGICAL_OPERATOR,
	EXPRESSION,
	FUNCTION,
	SOURCE_FUNCTION,
	LOGICAL_TYPE,
	BINDING_TYPE_MISMATCH,
	EXTENSION,
	EXPORT_FEATURE
};

struct CompilerFunctionIdentity {
	string catalog;
	string schema;
	string name;
	vector<LogicalType> arguments;
	LogicalType return_type;

	DUCKDB_API bool IsValid() const;
	DUCKDB_API bool operator==(const CompilerFunctionIdentity &other) const;
};

struct CompilerTypeMismatch {
	LogicalType expected_type;
	LogicalType actual_type;

	DUCKDB_API bool IsValid() const;
	DUCKDB_API bool operator==(const CompilerTypeMismatch &other) const;
};

struct CompilerConstructIdentity {
	CompilerConstructType type = CompilerConstructType::LOGICAL_OPERATOR;
	optional<LogicalOperatorType> logical_operator;
	optional<ExpressionClass> expression;
	optional<CompilerFunctionIdentity> function;
	optional<LogicalType> logical_type;
	optional<CompilerTypeMismatch> type_mismatch;
	optional<string> identifier;

	DUCKDB_API static CompilerConstructIdentity LogicalOperator(LogicalOperatorType type);
	DUCKDB_API static CompilerConstructIdentity Expression(ExpressionClass expression_class);
	DUCKDB_API static CompilerConstructIdentity Function(CompilerFunctionIdentity identity);
	DUCKDB_API static CompilerConstructIdentity SourceFunction(CompilerFunctionIdentity identity);
	DUCKDB_API static CompilerConstructIdentity LogicalTypeValue(LogicalType type);
	DUCKDB_API static CompilerConstructIdentity BindingTypeMismatch(LogicalType expected_type, LogicalType actual_type);
	DUCKDB_API static CompilerConstructIdentity Extension(string identifier);
	DUCKDB_API static CompilerConstructIdentity ExportFeature(string identifier);

	DUCKDB_API bool IsValid() const;
	DUCKDB_API bool operator==(const CompilerConstructIdentity &other) const;
};

enum class CompilerIssueCode : uint8_t {
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

enum class CompilerPhase : uint8_t { VERIFY, EXPRESSION_EXPORT, PLAN_EXPORT };

struct CompilerIssue {
	CompilerIssueCode code = CompilerIssueCode::INTERNAL_INVARIANT;
	CompilerPhase phase = CompilerPhase::VERIFY;
	optional<CompilerPath> path;
	optional<CompilerConstructIdentity> construct;
	vector<pair<string, Value>> facts;
	string message;

	DUCKDB_API bool IsValid() const;
	DUCKDB_API bool operator==(const CompilerIssue &other) const;
	DUCKDB_API bool operator<(const CompilerIssue &other) const;
};

struct VerificationSuccess {
	bool operator==(const VerificationSuccess &other) const {
		return true;
	}
};

template <class T>
class CompilerResult {
public:
	static CompilerResult Success(T value) {
		return CompilerResult(optional<T>(std::move(value)), {});
	}

	static CompilerResult Failure(vector<CompilerIssue> issues) {
		return CompilerResult(optional<T>(), std::move(issues));
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

	const vector<CompilerIssue> &GetIssues() const {
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
	CompilerResult(optional<T> value_p, vector<CompilerIssue> issues_p)
	    : value(std::move(value_p)), issues(std::move(issues_p)) {
	}

private:
	optional<T> value;
	vector<CompilerIssue> issues;
};

} // namespace duckdb
