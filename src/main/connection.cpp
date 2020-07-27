#include "duckdb/main/connection.hpp"

#include "duckdb/main/client_context.hpp"
#include "duckdb/main/connection_manager.hpp"
#include "duckdb/main/database.hpp"
#include "duckdb/main/appender.hpp"
#include "duckdb/main/relation/read_csv_relation.hpp"
#include "duckdb/main/relation/table_relation.hpp"
#include "duckdb/main/relation/table_function_relation.hpp"
#include "duckdb/main/relation/value_relation.hpp"
#include "duckdb/main/relation/view_relation.hpp"
#include "duckdb/parser/parser.hpp"

using namespace duckdb;
using namespace std;

Connection::Connection(DuckDB &database) : db(database), context(make_unique<ClientContext>(database)) {
	db.connection_manager->AddConnection(this);
#ifdef DEBUG
	EnableProfiling();
#endif
}

Connection::~Connection() {
	if (record_writer) {
		record_writer->Sync();
	}
	if (!context->is_invalidated) {
		context->Cleanup();
		db.connection_manager->RemoveConnection(this);
	}
}

string Connection::GetProfilingInformation(ProfilerPrintFormat format) {
	if (context->is_invalidated) {
		return "Context is invalidated.";
	}
	if (format == ProfilerPrintFormat::JSON) {
		return context->profiler.ToJSON();
	} else {
		return context->profiler.ToString();
	}
}

void Connection::Interrupt() {
	context->Interrupt();
}

void Connection::EnableProfiling() {
	context->EnableProfiling();
}

void Connection::DisableProfiling() {
	context->DisableProfiling();
}

static string ParseGroupFromPath(string file) {
	string extension = "";
	if (file.find(".test_slow") != std::string::npos) {
		// "slow" in the name indicates a slow test (i.e. only run as part of allunit)
		extension = "[.]";
	}
	// move backwards to the last slash
	int group_begin = -1, group_end = -1;
	for(idx_t i = file.size(); i > 0; i--) {
		if (file[i - 1] == '/' || file[i - 1] == '\\') {
			if (group_end == -1) {
				group_end = i - 1;
			} else {
				group_begin = i;
				return "[" + file.substr(group_begin, group_end - group_begin) + "]";
			}
		}
	}
	if (group_end == -1) {
		return "[" + file + "]";
	}
	return "[" + file.substr(0, group_end) + "]";
}

void Connection::Record(string file, string description, vector<string> extensions) {
	string target = StringUtil::Replace(file, ".cpp", ".test");
	record_writer = make_unique<BufferedFileWriter>(context->db.GetFileSystem(), target.c_str());
	string path = "# name: " + StringUtil::Replace(target, "/Users/myth/Programs/duckdb/", "") + "\n";
	record_writer->WriteData((const_data_ptr_t) path.c_str(), path.size());
	if (!description.empty()) {
		description = "# description: " + description + "\n";
		record_writer->WriteData((const_data_ptr_t) description.c_str(), description.size());
	}
	string group_name = "# group: " + ParseGroupFromPath(file) + "\n";
	record_writer->WriteData((const_data_ptr_t) group_name.c_str(), group_name.size());
	for(auto extension : extensions) {
		string ext = "# extension: " + extension + "\n";
		record_writer->WriteData((const_data_ptr_t) ext.c_str(), ext.size());
	}
	record_writer->WriteData((const_data_ptr_t) "\n", 1);
}

void Connection::EnableQueryVerification() {
	if (record_writer) {
		string query = "statement ok\nPRAGMA enable_verification\n\n";
		record_writer->WriteData((const_data_ptr_t) query.c_str(), query.size());
	}
	context->query_verification_enabled = true;
}

void Connection::DisableQueryVerification() {
	context->query_verification_enabled = false;
}

void Connection::ForceParallelism() {
	if (record_writer) {
		string query = "statement ok\nPRAGMA force_parallelism\n\n";
		record_writer->WriteData((const_data_ptr_t) query.c_str(), query.size());
	}
	context->force_parallelism = true;
}

unique_ptr<QueryResult> Connection::SendQuery(string query) {
	return context->Query(query, true);
}

unique_ptr<MaterializedQueryResult> Connection::Query(string query) {
	auto result = context->Query(query, false);
	if (record_writer) {
		auto &materialized = (MaterializedQueryResult &) *result;
		string q;
		if (result->success) {
			if (result->statement_type == StatementType::SELECT_STATEMENT && materialized.collection.count > 0) {
				// record the answer
				q = "query ";
				for(idx_t i = 0; i < materialized.sql_types.size(); i++) {
					switch(materialized.sql_types[i].id) {
					case SQLTypeId::TINYINT:
					case SQLTypeId::SMALLINT:
					case SQLTypeId::INTEGER:
					case SQLTypeId::BIGINT:
						q += "I";
						break;
					case SQLTypeId::DECIMAL:
					case SQLTypeId::FLOAT:
					case SQLTypeId::DOUBLE:
						q += "R";
						break;
					default:
						q += "T";
						break;
					}
				}
				q += "\n";
				q += query + "\n";
				q += "----\n";
				for(idx_t r = 0; r < materialized.collection.count; r++) {
					for(idx_t c = 0; c < materialized.sql_types.size(); c++) {
						auto val = materialized.collection.GetValue(c, r);
						if (c != 0) {
							q += "\n";
						}
						if (val.is_null) {
							q += "NULL";
						} else {
							switch (materialized.sql_types[c].id) {
							case SQLTypeId::BOOLEAN:
								q += val.value_.boolean ? "1" : "0";
								break;
							default:
								q += val.ToString().size() == 0 ? "(empty)" : val.ToString();
								break;
							}
						}
					}
					q += "\n";
				}
				q += "\n";
			} else {
				q = "statement ok\n"+ query + "\n\n";
			}
		} else {
			q = "statement error\n"+ query + "\n\n";
		}
		record_writer->WriteData((const_data_ptr_t) q.c_str(), q.size());
	}
	assert(result->type == QueryResultType::MATERIALIZED_RESULT);
	return unique_ptr_cast<QueryResult, MaterializedQueryResult>(move(result));
}

unique_ptr<PreparedStatement> Connection::Prepare(string query) {
	return context->Prepare(query);
}

unique_ptr<QueryResult> Connection::QueryParamsRecursive(string query, vector<Value> &values) {
	auto statement = Prepare(query);
	if (!statement->success) {
		return make_unique<MaterializedQueryResult>(statement->error);
	}
	return statement->Execute(values);
}

unique_ptr<TableDescription> Connection::TableInfo(string table_name) {
	return TableInfo(DEFAULT_SCHEMA, table_name);
}

unique_ptr<TableDescription> Connection::TableInfo(string schema_name, string table_name) {
	return context->TableInfo(schema_name, table_name);
}

vector<unique_ptr<SQLStatement>> Connection::ExtractStatements(string query) {
	Parser parser;
	parser.ParseQuery(query);
	return move(parser.statements);
}

void Connection::Append(TableDescription &description, DataChunk &chunk) {
	context->Append(description, chunk);
}

shared_ptr<Relation> Connection::Table(string table_name) {
	return Table(DEFAULT_SCHEMA, move(table_name));
}

shared_ptr<Relation> Connection::Table(string schema_name, string table_name) {
	auto table_info = TableInfo(schema_name, table_name);
	if (!table_info) {
		throw Exception("Table does not exist!");
	}
	return make_shared<TableRelation>(*context, move(table_info));
}

shared_ptr<Relation> Connection::View(string tname) {
	return View(DEFAULT_SCHEMA, move(tname));
}

shared_ptr<Relation> Connection::View(string schema_name, string table_name) {
	return make_shared<ViewRelation>(*context, move(schema_name), move(table_name));
}

shared_ptr<Relation> Connection::TableFunction(string fname) {
	vector<Value> values;
	return TableFunction(move(fname), move(values));
}

shared_ptr<Relation> Connection::TableFunction(string fname, vector<Value> values) {
	return make_shared<TableFunctionRelation>(*context, move(fname), move(values));
}

shared_ptr<Relation> Connection::Values(vector<vector<Value>> values) {
	vector<string> column_names;
	return Values(move(values), move(column_names));
}

shared_ptr<Relation> Connection::Values(vector<vector<Value>> values, vector<string> column_names, string alias) {
	return make_shared<ValueRelation>(*context, move(values), move(column_names), alias);
}

shared_ptr<Relation> Connection::Values(string values) {
	vector<string> column_names;
	return Values(move(values), move(column_names));
}

shared_ptr<Relation> Connection::Values(string values, vector<string> column_names, string alias) {
	return make_shared<ValueRelation>(*context, move(values), move(column_names), alias);
}

shared_ptr<Relation> Connection::ReadCSV(string csv_file, vector<string> columns) {
	// parse columns
	vector<ColumnDefinition> column_list;
	for (auto &column : columns) {
		auto col_list = Parser::ParseColumnList(column);
		if (col_list.size() != 1) {
			throw ParserException("Expected a singlec olumn definition");
		}
		column_list.push_back(move(col_list[0]));
	}
	return make_shared<ReadCSVRelation>(*context, csv_file, move(column_list));
}

void Connection::BeginTransaction() {
	auto result = Query("BEGIN TRANSACTION");
	if (!result->success) {
		throw Exception(result->error);
	}
}

void Connection::Commit() {
	auto result = Query("COMMIT");
	if (!result->success) {
		throw Exception(result->error);
	}
}

void Connection::Rollback() {
	auto result = Query("ROLLBACK");
	if (!result->success) {
		throw Exception(result->error);
	}
}
