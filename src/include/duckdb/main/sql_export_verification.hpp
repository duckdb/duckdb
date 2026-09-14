//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/main/sql_export_verification.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/enums/debug_sql_export_verification.hpp"
#include "duckdb/common/enums/statement_type.hpp"
#include "duckdb/main/client_context_state.hpp"
#include "duckdb/planner/logical_plan_verification_result.hpp"

namespace duckdb {

class Planner;
class LogicalOperator;

enum class SQLExportOutcome : uint8_t {
	NOT_APPLICABLE,
	STRUCTURALLY_VALIDATED,
	UNSUPPORTED_EXPRESSION,
	UNSUPPORTED_OPERATOR,
	UNSUPPORTED_SOURCE,
	UNSUPPORTED_EXTENSION,
	UNSUPPORTED_EXPORT_FEATURE,
	UNSUPPORTED_INPUT_PROFILE,
	EXPORT_ERROR,
	SERIALIZE_ERROR,
	REPARSE_ERROR,
	REBIND_ERROR,
	REOPTIMIZE_ERROR,
	OUTPUT_SCHEMA_MISMATCH
};

enum class SQLExportExecutionRoute : uint8_t { NONE, GENERATED, ORIGINAL_NOT_APPLICABLE, ORIGINAL_FALLBACK };

enum class SQLExportComparability : uint8_t { NOT_APPLICABLE, COMPARABLE, NON_REPEATABLE, UNKNOWN };

enum class SQLExportExecutionStatus : uint8_t { NOT_RUN, SUCCEEDED, ERRORED };

struct SQLExportInventoryEntry {
	string kind;
	string construct;
	LogicalPlanVerificationPath path;
};

//! Owned observations only; no planning objects or callbacks survive publication.
struct SQLExportVerificationRecord {
	DebugSQLExportVerification mode = DebugSQLExportVerification::OFF;
	SQLExportOutcome outcome = SQLExportOutcome::NOT_APPLICABLE;
	SQLExportExecutionRoute route = SQLExportExecutionRoute::NONE;
	SQLExportComparability comparability = SQLExportComparability::NOT_APPLICABLE;
	SQLExportExecutionStatus execution = SQLExportExecutionStatus::NOT_RUN;
	string code = "ORIGINAL_PLANNING";
	string phase = "ORIGINAL_PLANNING";
	optional<LogicalPlanVerificationPath> path;
	vector<LogicalPlanVerificationIssue> issues;
	vector<SQLExportInventoryEntry> inventory;
	idx_t statement_index = 0;
	idx_t export_count = 0;
	bool query_error = false;
	bool eligible = false;
	bool generated = false;
	bool strict_failure = false;
	bool propagated_error = false;
	string generated_sql;
};

//! Opt-in collector. Callers attach it before a query and drain it after materialization.
class SQLExportVerificationState : public ClientContextState {
public:
	DUCKDB_API static shared_ptr<SQLExportVerificationState> Get(ClientContext &context);
	DUCKDB_API static shared_ptr<SQLExportVerificationState> GetOrCreate(ClientContext &context);
	DUCKDB_API static void Remove(ClientContext &context);
	DUCKDB_API vector<SQLExportVerificationRecord> TakeRecords();
	DUCKDB_API void Publish(SQLExportVerificationRecord record);
	void QueryBegin(ClientContext &context) override;
	void QueryEnd(ClientContext &context, optional_ptr<ErrorData> error) override;
	idx_t StatementCount() const {
		return statement_count;
	}
	bool retain_failure_sql = false;

private:
	idx_t statement_count = 0;
	vector<SQLExportVerificationRecord> records;
	optional<SQLExportVerificationRecord> active_record;
};

//! One execution-planning invocation, including extension-requested retries.
class SQLExportVerification {
public:
	SQLExportVerification(ClientContext &context, DebugSQLExportVerification mode);
	~SQLExportVerification();
	void BeginPlanningAttempt();
	void Verify(Planner &planner, StatementType type, bool has_parameters);
	void Publish(bool planning_succeeded);
	bool HasVerifierException() const;
	string ErrorQuery() const;

private:
	void Failure(SQLExportOutcome outcome, const string &code);
	void Inventory(LogicalOperator &root);
	void RoundTrip(Planner &planner);

private:
	ClientContext &context;
	SQLExportVerificationRecord record;
	shared_ptr<SQLExportVerificationState> observer;
	//! The generated binder remains alive through physical planning.
	unique_ptr<Planner> generated_planner;
	bool verifier_exception = false;
	optional<LogicalPlanVerificationPath> input_profile_path;
};

} // namespace duckdb
