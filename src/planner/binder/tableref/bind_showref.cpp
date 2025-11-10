#include "duckdb/function/pragma/pragma_functions.hpp"
#include "duckdb/function/table/system_functions.hpp"
#include "duckdb/parser/parsed_data/create_view_info.hpp"
#include "duckdb/parser/tableref/showref.hpp"
#include "duckdb/parser/tableref/subqueryref.hpp"
#include "duckdb/planner/binder.hpp"
#include "duckdb/planner/operator/logical_column_data_get.hpp"
#include "duckdb/planner/operator/logical_get.hpp"
#include "duckdb/planner/operator/logical_projection.hpp"
#include "duckdb/catalog/catalog_entry/table_catalog_entry.hpp"
#include "duckdb/catalog/catalog.hpp"
#include "duckdb/main/client_data.hpp"
#include "duckdb/main/client_context.hpp"

namespace duckdb {

struct BaseTableColumnInfo {
	optional_ptr<TableCatalogEntry> table = nullptr;
	optional_ptr<const ColumnDefinition> column = nullptr;
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
		auto base_column_id = get.GetColumnIds()[binding.column_index];
		if (base_column_id.IsVirtualColumn()) {
			//! Virtual column (like ROW_ID) does not have a ColumnDefinition entry in the TableCatalogEntry
			return result;
		}
		result.table = table;
		result.column = &table->GetColumn(LogicalIndex(base_column_id.GetPrimaryIndex()));
		return result;
	}
	case LogicalOperatorType::LOGICAL_PROJECTION: {
		auto &projection = op.Cast<LogicalProjection>();
		if (binding.table_index != projection.table_index) {
			break;
		}
		auto &expr = projection.expressions[binding.column_index];
		if (expr->GetExpressionType() == ExpressionType::BOUND_COLUMN_REF) {
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

BoundStatement Binder::BindShowQuery(ShowRef &ref) {
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
		idx_t row_index = output.size();
		auto &alias = plan.names[column_idx];
		if (result.table) {
			// we can! emit the information from the base table directly
			PragmaTableInfo::GetColumnInfo(*result.table, *result.column, output, row_index);
			// Override the base column name with the alias if one is specified.
			if (alias != result.column->Name()) {
				output.SetValue(0, row_index, Value(alias));
			}
		} else {
			// we cannot - read the type/name from the plan instead
			auto type = plan.types[column_idx];

			// "name", TypeId::VARCHAR
			output.SetValue(0, row_index, Value(alias));
			// "type", TypeId::VARCHAR
			output.SetValue(1, row_index, Value(type.ToString()));
			// "null", TypeId::VARCHAR
			output.SetValue(2, row_index, Value("YES"));
			// "pk", TypeId::BOOL
			output.SetValue(3, row_index, Value());
			// "dflt_value", TypeId::VARCHAR
			output.SetValue(4, row_index, Value());
			// "extra", TypeId::VARCHAR
			output.SetValue(5, row_index, Value());
		}

		output.SetCardinality(output.size() + 1);
		if (output.size() == STANDARD_VECTOR_SIZE) {
			collection->Append(append_state, output);
			output.Reset();
		}
	}
	collection->Append(append_state, output);

	auto table_index = GenerateTableIndex();

	BoundStatement result;
	result.names = return_names;
	result.types = return_types;
	result.plan = make_uniq<LogicalColumnDataGet>(table_index, return_types, std::move(collection));
	bind_context.AddGenericBinding(table_index, "__show_select", return_names, return_types);
	return result;
}

BoundStatement Binder::BindShowTable(ShowRef &ref) {
	auto lname = StringUtil::Lower(ref.table_name);

	string sql;
	if (lname == "\"databases\"") {
		sql = PragmaShowDatabases();
	} else if (lname == "\"tables\"") {
		sql = PragmaShowTables();
	} else if (ref.show_type == ShowType::SHOW_FROM) {
		auto catalog_name = ref.catalog_name;
		auto schema_name = ref.schema_name;

		// Check for unqualified name, promote schema to catalog if unambiguous, and set schema_name to empty if so
		Binder::BindSchemaOrCatalog(catalog_name, schema_name);

		// If fully qualified, check if the schema exists
		if (!catalog_name.empty() && !schema_name.empty()) {
			auto schema_entry = Catalog::GetSchema(context, catalog_name, schema_name, OnEntryNotFound::RETURN_NULL);
			if (!schema_entry) {
				throw CatalogException("SHOW TABLES FROM: No catalog + schema named \"%s.%s\" found.", catalog_name,
				                       schema_name);
			}
		} else if (catalog_name.empty() && !schema_name.empty()) {
			// We have a schema name, use default catalog
			auto &client_data = ClientData::Get(context);
			auto &default_entry = client_data.catalog_search_path->GetDefault();
			catalog_name = default_entry.catalog;
			auto schema_entry = Catalog::GetSchema(context, catalog_name, schema_name, OnEntryNotFound::RETURN_NULL);
			if (!schema_entry) {
				throw CatalogException("SHOW TABLES FROM: No catalog + schema named \"%s.%s\" found.", catalog_name,
				                       schema_name);
			}
		}
		sql = PragmaShowTables(catalog_name, schema_name);
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

BoundStatement Binder::Bind(ShowRef &ref) {
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
