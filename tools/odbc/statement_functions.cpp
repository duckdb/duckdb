#include "statement_functions.hpp"

SQLRETURN duckdb::PrepareStmt(SQLHSTMT statement_handle, SQLCHAR *statement_text, SQLINTEGER text_length) {
	return duckdb::WithStatement(statement_handle, [&](duckdb::OdbcHandleStmt *stmt) {
		if (stmt->stmt) {
			stmt->stmt.reset();
		}
		if (stmt->res) {
			stmt->res.reset();
		}
		if (stmt->chunk) {
			stmt->chunk.reset();
		}
		// we should not clear the parameters because of SQLExecDirect may reuse them
		// stmt->params.resize(0);
		stmt->bound_cols.resize(0);

		auto query = duckdb::OdbcUtils::ReadString(statement_text, text_length);
		stmt->stmt = stmt->dbc->conn->Prepare(query);
		if (!stmt->stmt->success) {
			return SQL_ERROR;
		}
		stmt->params.resize(stmt->stmt->n_param);
		stmt->bound_cols.resize(stmt->stmt->ColumnCount());
		return SQL_SUCCESS;
	});
}

SQLRETURN duckdb::ExecuteStmt(SQLHSTMT statement_handle) {
	return duckdb::WithStatement(statement_handle, [&](duckdb::OdbcHandleStmt *stmt) {
		if (stmt->res) {
			stmt->res.reset();
		}
		if (stmt->chunk) {
			stmt->chunk.reset();
		}

		stmt->chunk_row = -1;
		stmt->open = false;
		if (stmt->rows_fetched_ptr) {
			*stmt->rows_fetched_ptr = 0;
		}
		stmt->res = stmt->stmt->Execute(stmt->params);
		if (!stmt->res->success) {
			return SQL_ERROR;
		}
		stmt->open = true;
		return SQL_SUCCESS;
	});
}
