//===----------------------------------------------------------------------===//
//                         DuckDB
//
// sqllogic_command.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb.hpp"

namespace duckdb {
class SQLLogicTestRunner;

enum class SortStyle : uint8_t { NO_SORT, ROW_SORT, VALUE_SORT };
enum class ExpectedResult : uint8_t { RESULT_SUCCESS, RESULT_ERROR, RESULT_UNKNOWN };

struct LoopDefinition {
	string loop_iterator_name;
	int loop_idx;
	int loop_start;
	int loop_end;
	bool is_parallel;
	vector<string> tokens;
};

struct ExecuteContext {
	ExecuteContext() : con(nullptr), is_parallel(false) {
	}
	ExecuteContext(Connection *con, vector<LoopDefinition> running_loops_p)
	    : con(con), running_loops(std::move(running_loops_p)), is_parallel(true) {
	}

	Connection *con;
	vector<LoopDefinition> running_loops;
	bool is_parallel;
	string sql_query;
	string error_file;
	int error_line;
};

class Command {
public:
	Command(SQLLogicTestRunner &runner);
	virtual ~Command();

	SQLLogicTestRunner &runner;
	string connection_name;
	int query_line;
	string base_sql_query;
	string file_name;

public:
	Connection *CommandConnection(ExecuteContext &context) const;

	duckdb::unique_ptr<MaterializedQueryResult> ExecuteQuery(ExecuteContext &context, Connection *connection,
	                                                         string file_name, idx_t query_line) const;

	virtual void ExecuteInternal(ExecuteContext &context) const = 0;
	void Execute(ExecuteContext &context) const;

private:
	void RestartDatabase(ExecuteContext &context, Connection *&connection, string sql_query) const;
};

class Statement : public Command {
public:
	Statement(SQLLogicTestRunner &runner);

	ExpectedResult expected_result;
	string expected_error;

public:
	void ExecuteInternal(ExecuteContext &context) const override;
};

class Query : public Command {
public:
	Query(SQLLogicTestRunner &runner);

	idx_t expected_column_count = 0;
	SortStyle sort_style = SortStyle::NO_SORT;
	vector<string> values;
	bool query_has_label = false;
	string query_label;

public:
	void ExecuteInternal(ExecuteContext &context) const override;
};

class RestartCommand : public Command {
public:
	RestartCommand(SQLLogicTestRunner &runner);

public:
	void ExecuteInternal(ExecuteContext &context) const override;
};

class ReconnectCommand : public Command {
public:
	ReconnectCommand(SQLLogicTestRunner &runner);

public:
	void ExecuteInternal(ExecuteContext &context) const override;
};

class LoopCommand : public Command {
public:
	LoopCommand(SQLLogicTestRunner &runner, LoopDefinition definition_p);

public:
	LoopDefinition definition;
	vector<duckdb::unique_ptr<Command>> loop_commands;

	void ExecuteInternal(ExecuteContext &context) const override;
};

} // namespace duckdb
