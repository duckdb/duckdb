#include <stdint.h>
#include "duckdb.hpp"
#include "duckdb/parser/parser.hpp"
#include "duckdb/parser/expression/cast_expression.hpp"
#include "duckdb/parser/parsed_data/create_table_function_info.hpp"
#include "duckdb/common/types/date.hpp"
#include "duckdb/common/types/timestamp.hpp"

#include <cmath>

namespace duckdb {

struct AttachFunctionData : public TableFunctionData {
	AttachFunctionData() {
	}

	bool finished = false;
	bool overwrite = false;
	string file_name = "";
};

static unique_ptr<FunctionData> AttachBind(ClientContext &context, TableFunctionBindInput &input,
                                           vector<LogicalType> &return_types, vector<string> &names) {

	auto result = make_unique<AttachFunctionData>();
	result->file_name = input.inputs[0].GetValue<string>();

	for (auto &kv : input.named_parameters) {
		if (kv.first == "overwrite") {
			result->overwrite = BooleanValue::Get(kv.second);
		}
	}

	return_types.push_back(LogicalType::BOOLEAN);
	names.emplace_back("Success");
	return move(result);
}

static void AttachFunction(ClientContext &context, TableFunctionInput &data_p, DataChunk &output) {
	auto &data = (AttachFunctionData &)*data_p.bind_data;
	if (data.finished) {
		return;
	}

	DuckDB db {data.file_name};
        auto econn = Connection(db);
	auto dconn = Connection(context.db->GetDatabase(context));

	{

	        auto q = econn.Query("select table_name from duckdb_tables();");
		for(auto &row : *q) {
			auto table_name = row.GetValue<string>(0);

			auto table = econn.Table(table_name)->Execute();

			vector<vector<Value>> table_values;
			unique_ptr<DataChunk> trow = nullptr;
			while((trow = table->Fetch()) != nullptr) {
				for(idx_t row_idx = 0; row_idx < trow->size(); row_idx++) {
					vector<Value> row_values;
					for(idx_t col = 0; col < table->names.size(); col++) {
						row_values.push_back(trow->GetValue(col, row_idx));
					}
					table_values.push_back(row_values);
				}
			}

			dconn.Values(table_values, table->names, table_name)
			    ->CreateView(table_name, data.overwrite, true);
		}
	}
	// todo copy views
	// {
	// 	check_ok(sqlite3_prepare_v2(db, "SELECT sql FROM sqlite_master WHERE type='view'", -1, &res, nullptr), db);

	// 	while (sqlite3_step(res) == SQLITE_ROW) {
	// 		auto view_sql = string((const char *)sqlite3_column_text(res, 0));
	// 		dconn.Query(view_sql);
	// 	}
	// 	check_ok(sqlite3_finalize(res), db);
	// }
	data.finished = true;
}

static void RegisterAttachFunction(BuiltinFunctions &set) {
	TableFunction attach_duckdb("attach_external", {LogicalType::VARCHAR}, AttachFunction, AttachBind);
	attach_duckdb.named_parameters["overwrite"] = LogicalType::BOOLEAN;
	set.AddFunction(attach_duckdb);
}

void BuiltinFunctions::RegisterAttachFunctions() {
	RegisterAttachFunction(*this);
}
};
