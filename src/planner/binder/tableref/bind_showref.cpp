#include "duckdb/function/pragma/pragma_functions.hpp"
#include "duckdb/function/table/system_functions.hpp"
#include "duckdb/parser/parsed_data/create_view_info.hpp"
#include "duckdb/parser/tableref/showref.hpp"
#include "duckdb/parser/tableref/subqueryref.hpp"
#include "duckdb/planner/binder.hpp"
#include "duckdb/planner/operator/logical_column_data_get.hpp"
#include "duckdb/planner/tableref/bound_table_function.hpp"
#include "duckdb/planner/operator/logical_get.hpp"
#include "duckdb/planner/operator/logical_projection.hpp"
#include "duckdb/catalog/catalog_entry/table_catalog_entry.hpp"

namespace duckdb {

struct BaseTableColumnInfo {
	optional_ptr<TableCatalogEntry> table;
	optional_ptr<const ColumnDefinition> column;
};

BaseTableColumnInfo FindBaseTableColumn(LogicalOperator &op, ColumnBinding binding) {
	BaseTableColumnInfo result;
	switch (op.type) {
	case LogicalOperatorType::LOGICAL_GET: {
		auto &get = op.Cast<LogicalGet>();
		if (get.table_index != binding.table_index) {
			return result;
		}
		auto table = get.GetTable();
		if (!table) {
			break;
		}
		if (!get.projection_ids.empty()) {
			throw InternalException("Projection ids should not exist here");
		}
		result.table = table;
		auto base_column_id = get.GetColumnIds()[binding.column_index];
		result.column = &table->GetColumn(LogicalIndex(base_column_id.GetPrimaryIndex()));
		return result;
	}
	case LogicalOperatorType::LOGICAL_PROJECTION: {
		auto &projection = op.Cast<LogicalProjection>();
		if (binding.table_index != projection.table_index) {
			break;
		}
		auto &expr = projection.expressions[binding.column_index];
		if (expr->type == ExpressionType::BOUND_COLUMN_REF) {
			// if the projection at this index only has a column reference we can directly trace it to the base table
			auto &bound_colref = expr->Cast<BoundColumnRefExpression>();
			return FindBaseTableColumn(*projection.children[0], bound_colref.binding);
		}
		break;
	}
	case LogicalOperatorType::LOGICAL_LIMIT:
	case LogicalOperatorType::LOGICAL_ORDER_BY:
	case LogicalOperatorType::LOGICAL_TOP_N:
	case LogicalOperatorType::LOGICAL_SAMPLE:
	case LogicalOperatorType::LOGICAL_DISTINCT:
	case LogicalOperatorType::LOGICAL_FILTER:
	case LogicalOperatorType::LOGICAL_COMPARISON_JOIN:
	case LogicalOperatorType::LOGICAL_JOIN:
	case LogicalOperatorType::LOGICAL_ANY_JOIN:
	case LogicalOperatorType::LOGICAL_ASOF_JOIN:
	case LogicalOperatorType::LOGICAL_CROSS_PRODUCT:
		// for any "pass-through" operators - search in children directly
		for (auto &child : op.children) {
			result = FindBaseTableColumn(*child, binding);
			if (result.table) {
				return result;
			}
		}
		break;
	default:
		// unsupported operator
		break;
	}
	return result;
}

BaseTableColumnInfo FindBaseTableColumn(LogicalOperator &op, idx_t column_index) {
	auto bindings = op.GetColumnBindings();
	return FindBaseTableColumn(op, bindings[column_index]);
}

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
		// check if we can trace the column to a base table so that we can figure out constraint information
		auto result = FindBaseTableColumn(*plan.plan, column_idx);
		if (result.table) {
			// we can! emit the information from the base table directly
			PragmaTableInfo::GetColumnInfo(*result.table, *result.column, output, output.size());
		} else {
			// we cannot - read the type/name from the plan instead
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
		}

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
	} else if (lname == "\"variables\"") {
		sql = PragmaShowVariables();
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
