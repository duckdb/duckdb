#include "duckdb/function/table/system_functions.hpp"

#include "duckdb/common/exception.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/parser/parser.hpp"
#include "duckdb/main/client_data.hpp"
#include "duckdb/main/prepared_statement_data.hpp"

namespace duckdb {

struct DuckDBPreparedStatementsData : public GlobalTableFunctionState {
	DuckDBPreparedStatementsData() : offset(0) {
	}

	vector<std::pair<string, shared_ptr<PreparedStatementData>>> entries;
	idx_t offset;
};

static unique_ptr<FunctionData> DuckDBPreparedStatementsBind(ClientContext &context, TableFunctionBindInput &input,
                                                             vector<LogicalType> &return_types, vector<string> &names) {
	names.emplace_back("name");
	return_types.emplace_back(LogicalType::VARCHAR);

	names.emplace_back("statement");
	return_types.emplace_back(LogicalType::VARCHAR);

	names.emplace_back("parameter_types");
	return_types.emplace_back(LogicalType::LIST(LogicalType::VARCHAR));

	names.emplace_back("result_types");
	return_types.emplace_back(LogicalType::LIST(LogicalType::VARCHAR));
	return nullptr;
}

unique_ptr<GlobalTableFunctionState> DuckDBPreparedStatementsInit(ClientContext &context,
                                                                  TableFunctionInitInput &input) {
	auto result = make_uniq<DuckDBPreparedStatementsData>();
	auto &prepared_statements = context.client_data->prepared_statements;
	for (auto &it : prepared_statements) {
		result->entries.emplace_back(it.first, it.second);
	}
	return std::move(result);
}

void DuckDBPreparedStatementsFunction(ClientContext &context, TableFunctionInput &data_p, DataChunk &output) {
	auto &data = data_p.global_state->Cast<DuckDBPreparedStatementsData>();
	if (data.offset >= data.entries.size()) {
		// finished returning values
		return;
	}
	// start returning values
	// either fill up the chunk or return all the remaining columns
	idx_t count = 0;
	while (data.offset < data.entries.size() && count < STANDARD_VECTOR_SIZE) {
		auto &entry = data.entries[data.offset++];
		auto &name = entry.first;
		auto &prepared_statement = *entry.second;

		// name, VARCHAR
		output.SetValue(0, count, Value(name));
		// statement, VARCHAR
		output.SetValue(1, count, Value(prepared_statement.unbound_statement->ToString()));
		// parameter_types, VARCHAR[]
		auto &named_parameter_map = prepared_statement.unbound_statement->named_param_map;
		if (named_parameter_map.empty()) {
			output.SetValue(2, count, Value(LogicalType::LIST(LogicalType::VARCHAR)));
		} else {
			vector<Value> parameter_types;
			for (idx_t i = 0; i < prepared_statement.properties.parameter_count; i++) {
				parameter_types.push_back(LogicalType(LogicalTypeId::UNKNOWN).ToString());
			}
			output.SetValue(2, count, Value::LIST(std::move(parameter_types)));
		}
		// result_types, VARCHAR[]
		switch (prepared_statement.properties.return_type) {
		case StatementReturnType::QUERY_RESULT: {
			if (prepared_statement.physical_plan) {
				auto plan_types = prepared_statement.physical_plan->Root().GetTypes();
				vector<Value> return_types;
				for (auto &type : plan_types) {
					return_types.push_back(type.ToString());
				}
				output.SetValue(3, count, Value::LIST(return_types));
			} else {
				output.SetValue(3, count, Value(LogicalType::LIST(LogicalType::VARCHAR)));
			}
			break;
		}
		case StatementReturnType::CHANGED_ROWS: {
			output.SetValue(3, count, Value::LIST({"BIGINT"}));
			break;
		}
		case StatementReturnType::NOTHING:
		default: {
			output.SetValue(3, count, Value(LogicalType::LIST(LogicalType::VARCHAR)));
			break;
		}
		}
		count++;
	}
	output.SetCardinality(count);
}

void DuckDBPreparedStatementsFun::RegisterFunction(BuiltinFunctions &set) {
	set.AddFunction(TableFunction("duckdb_prepared_statements", {}, DuckDBPreparedStatementsFunction,
	                              DuckDBPreparedStatementsBind, DuckDBPreparedStatementsInit));
}

} // namespace duckdb
