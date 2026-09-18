#pragma once

#include "catch.hpp"
#include "test_helpers.hpp"
#include "duckdb/main/sql_export_verification.hpp"
#include "sql_export_test_helpers.hpp"
#include "duckdb/planner/planner_extension.hpp"

namespace sql_export_verification_test {

using namespace duckdb;

SQLExportVerificationRecord TakeSQLExportRecord(SQLExportVerificationState &observer);

void SetSQLExportMode(Connection &con, SQLExportVerificationState &observer, const string &mode);

void RegisterSQLExportOpaqueSource(DuckDB &db, Connection &con);

struct SQLExportBindHook : public PlannerExtensionInfo {
	idx_t calls = 0;
	bool throw_on_second = false;
};

void SQLExportChangeGeneratedSchema(PlannerExtensionInput &input, BoundStatement &statement);

} // namespace sql_export_verification_test
