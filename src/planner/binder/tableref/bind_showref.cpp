#include "duckdb/planner/binder.hpp"
#include "duckdb/parser/tableref/showref.hpp"
#include "duckdb/planner/tableref/bound_table_function.hpp"
#include "duckdb/planner/operator/logical_column_data_get.hpp"
#include "duckdb/function/pragma/pragma_functions.hpp"
#include "duckdb/parser/parsed_data/create_view_info.hpp"
#include "duckdb/parser/tableref/subqueryref.hpp"

namespace duckdb {

unique_ptr<BoundTableRef> Binder::BindShowQuery(ShowRef &ref) {
	// bind the child plan of the DESCRIBE statement
	auto child_binder = Binder::CreateBinder(context, this);
	auto plan = child_binder->Bind(*ref.query);

	// construct a column data collection with the result
	vector<string> return_names = {"column_name", "column_type", "null", "key", "default", "extra"};
	vector<LogicalType> return_types = {LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::VARCHAR,
	                                    LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::VARCHAR};
	DataChunk output;
	output.Initialize(Allocator::Get(context), return_types);

	auto collection = make_uniq<ColumnDataCollection>(context, return_types);
	ColumnDataAppendState append_state;
	collection->InitializeAppend(append_state);
	for (idx_t column_idx = 0; column_idx < plan.types.size(); column_idx++) {
		auto type = plan.types[column_idx];
		auto &name = plan.names[column_idx];

		// "name", TypeId::VARCHAR
		output.SetValue(0, output.size(), Value(name));
		// "type", TypeId::VARCHAR
		output.SetValue(1, output.size(), Value(type.ToString()));
		// "null", TypeId::VARCHAR
		output.SetValue(2, output.size(), Value("YES"));
		// "pk", TypeId::BOOL
		output.SetValue(3, output.size(), Value());
		// "dflt_value", TypeId::VARCHAR
		output.SetValue(4, output.size(), Value());
		// "extra", TypeId::VARCHAR
		output.SetValue(5, output.size(), Value());

		output.SetCardinality(output.size() + 1);
		if (output.size() == STANDARD_VECTOR_SIZE) {
			collection->Append(append_state, output);
			output.Reset();
		}
	}
	collection->Append(append_state, output);

	auto show = make_uniq<LogicalColumnDataGet>(GenerateTableIndex(), return_types, std::move(collection));
	bind_context.AddGenericBinding(show->table_index, "__show_select", return_names, return_types);
	return make_uniq<BoundTableFunction>(std::move(show));
}

unique_ptr<BoundTableRef> Binder::BindShowTable(ShowRef &ref) {
	auto lname = StringUtil::Lower(ref.table_name);

	string sql;
	if (lname == "\"databases\"") {
		sql = PragmaShowDatabases();
	} else if (lname == "\"tables\"") {
		sql = PragmaShowTables();
	} else if (lname == "__show_tables_expanded") {
		sql = PragmaShowTablesExpanded();
	} else {
		sql = PragmaShow(ref.table_name);
	}
	auto select = CreateViewInfo::ParseSelect(sql);
	auto subquery = make_uniq<SubqueryRef>(std::move(select));
	return Bind(*subquery);
}

unique_ptr<BoundTableRef> Binder::Bind(ShowRef &ref) {
	if (ref.show_type == ShowType::SUMMARY) {
		return BindSummarize(ref);
	}
	if (ref.query) {
		return BindShowQuery(ref);
	} else {
		return BindShowTable(ref);
	}
}

} // namespace duckdb
