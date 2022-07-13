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

struct LoopDefinition {
	string loop_iterator_name;
	int loop_idx;
	int loop_start;
	int loop_end;
	vector<string> tokens;
};

class Command {
public:
	Command(SQLLogicTestRunner &runner);
	virtual ~Command();

	SQLLogicTestRunner &runner;
	string connection_name;
	int query_line;
	string sql_query;
	string file_name;

public:
	Connection *CommandConnection();

	unique_ptr<MaterializedQueryResult> ExecuteQuery(Connection *connection, string file_name, idx_t query_line,
	                                                 string sql_query);

	virtual void ExecuteInternal() = 0;
	void Execute();

private:
	void RestartDatabase(Connection *&connection, string sql_query);
};

class Statement : public Command {
public:
	Statement(SQLLogicTestRunner &runner);

	bool expect_ok;

public:
	void ExecuteInternal() override;
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
	void ExecuteInternal() override;
};

class RestartCommand : public Command {
public:
	RestartCommand(SQLLogicTestRunner &runner);
	void ExecuteInternal() override;
};

class LoopCommand : public Command {
public:
	LoopCommand(SQLLogicTestRunner &runner, LoopDefinition definition_p);

	LoopDefinition definition;
	vector<unique_ptr<Command>> loop_commands;

	void ExecuteInternal();
};

} // namespace duckdb
