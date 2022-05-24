#include "duckdb/catalog/catalog_entry/matview_catalog_entry.hpp"
#include "duckdb/catalog/catalog_entry/view_catalog_entry.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/function/table/table_scan.hpp"
#include "duckdb/main/config.hpp"
#include "duckdb/parser/query_node/select_node.hpp"
#include "duckdb/parser/statement/select_statement.hpp"
#include "duckdb/parser/tableref/basetableref.hpp"
#include "duckdb/parser/tableref/subqueryref.hpp"
#include "duckdb/parser/tableref/table_function_ref.hpp"
#include "duckdb/planner/binder.hpp"
#include "duckdb/planner/operator/logical_get.hpp"
#include "duckdb/planner/tableref/bound_basetableref.hpp"
#include "duckdb/planner/tableref/bound_cteref.hpp"
#include "duckdb/planner/tableref/bound_dummytableref.hpp"
#include "duckdb/planner/tableref/bound_subqueryref.hpp"

namespace duckdb {

unique_ptr<BoundTableRef> Binder::Bind(BaseTableRef &ref) {
	QueryErrorContext error_context(root_statement, ref.query_location);
	// CTEs and views are also referred to using BaseTableRefs, hence need to distinguish here
	// check if the table name refers to a CTE
	auto cte = FindCTE(ref.table_name, ref.table_name == alias);
	if (cte) {
		// Check if there is a CTE binding in the BindContext
		auto ctebinding = bind_context.GetCTEBinding(ref.table_name);
		if (!ctebinding) {
			if (CTEIsAlreadyBound(cte)) {
				throw BinderException("Circular reference to CTE \"%s\", use WITH RECURSIVE to use recursive CTEs",
				                      ref.table_name);
			}
			// Move CTE to subquery and bind recursively
			SubqueryRef subquery(unique_ptr_cast<SQLStatement, SelectStatement>(cte->query->Copy()));
			subquery.alias = ref.alias.empty() ? ref.table_name : ref.alias;
			subquery.column_name_alias = cte->aliases;
			for (idx_t i = 0; i < ref.column_name_alias.size(); i++) {
				if (i < subquery.column_name_alias.size()) {
					subquery.column_name_alias[i] = ref.column_name_alias[i];
				} else {
					subquery.column_name_alias.push_back(ref.column_name_alias[i]);
				}
			}
			return Bind(subquery, cte);
		} else {
			// There is a CTE binding in the BindContext.
			// This can only be the case if there is a recursive CTE present.
			auto index = GenerateTableIndex();
			auto result = make_unique<BoundCTERef>(index, ctebinding->index);
			auto b = ctebinding;
			auto alias = ref.alias.empty() ? ref.table_name : ref.alias;
			auto names = BindContext::AliasColumnNames(alias, b->names, ref.column_name_alias);

			bind_context.AddGenericBinding(index, alias, names, b->types);
			// Update references to CTE
			auto cteref = bind_context.cte_references[ref.table_name];
			(*cteref)++;

			result->types = b->types;
			result->bound_columns = move(names);
			return move(result);
		}
	}
	// not a CTE
	// extract a table or view from the catalog
	auto &catalog = Catalog::GetCatalog(context);
	auto table_or_view =
	    catalog.GetEntry(context, CatalogType::TABLE_ENTRY, ref.schema_name, ref.table_name, true, error_context);
	if (!table_or_view) {
		auto table_name = ref.schema_name.empty() ? ref.table_name : (ref.schema_name + "." + ref.table_name);
		// table could not be found: try to bind a replacement scan
		auto &config = DBConfig::GetConfig(context);
		for (auto &scan : config.replacement_scans) {
			auto replacement_function = scan.function(context, table_name, scan.data.get());
			if (replacement_function) {
				replacement_function->alias = ref.alias.empty() ? ref.table_name : ref.alias;
				replacement_function->column_name_alias = ref.column_name_alias;
				return Bind(*replacement_function);
			}
		}
		// we still didn't find the table
		if (GetBindingMode() == BindingMode::EXTRACT_NAMES) {
			// if we are in EXTRACT_NAMES, we create a dummy table ref
			AddTableName(table_name);

			// add a bind context entry
			auto table_index = GenerateTableIndex();
			auto alias = ref.alias.empty() ? table_name : ref.alias;
			vector<LogicalType> types {LogicalType::INTEGER};
			vector<string> names {"__dummy_col" + to_string(table_index)};
			bind_context.AddGenericBinding(table_index, alias, names, types);
			return make_unique_base<BoundTableRef, BoundEmptyTableRef>(table_index);
		}
		// could not find an alternative: bind again to get the error
		table_or_view = Catalog::GetCatalog(context).GetEntry(context, CatalogType::TABLE_ENTRY, ref.schema_name,
		                                                      ref.table_name, false, error_context);
	}
	switch (table_or_view->type) {
	case CatalogType::TABLE_ENTRY: {
		// base table: create the BoundBaseTableRef node
		auto table_index = GenerateTableIndex();
		auto table = (TableCatalogEntry *)table_or_view;

		auto scan_function = TableScanFunction::GetFunction();
		auto bind_data = make_unique<TableScanBindData>(table);
		auto alias = ref.alias.empty() ? ref.table_name : ref.alias;
		vector<LogicalType> table_types;
		vector<string> table_names;
		for (auto &col : table->columns) {
			table_types.push_back(col.type);
			table_names.push_back(col.name);
		}
		table_names = BindContext::AliasColumnNames(alias, table_names, ref.column_name_alias);

		auto logical_get =
		    make_unique<LogicalGet>(table_index, scan_function, move(bind_data), table_types, table_names);
		bind_context.AddBaseTable(table_index, alias, table_names, table_types, *logical_get);
		return make_unique_base<BoundTableRef, BoundBaseTableRef>(table, move(logical_get));
	}
	case CatalogType::MATVIEW_ENTRY: {
		// base table: create the BoundBaseTableRef node
		auto table_index = GenerateTableIndex();
		auto matView = (MatViewCatalogEntry *)table_or_view;

		auto scan_function = TableScanFunction::GetFunction();
		auto bind_data = make_unique<TableScanBindData>(matView);
		auto alias = ref.alias.empty() ? ref.table_name : ref.alias;
		vector<LogicalType> column_types;
		vector<string> column_names;
		for (auto &col : matView->columns) {
			column_types.push_back(col.type);
			column_names.push_back(col.name);
		}
		column_names = BindContext::AliasColumnNames(alias, column_names, ref.column_name_alias);

		auto logical_get =
		    make_unique<LogicalGet>(table_index, scan_function, move(bind_data), column_types, column_names);
		bind_context.AddBaseTable(table_index, alias, column_names, column_types, *logical_get);
		return make_unique_base<BoundTableRef, BoundBaseTableRef>(matView, move(logical_get));
	}
	case CatalogType::VIEW_ENTRY: {
		// the node is a view: get the query that the view represents
		auto view_catalog_entry = (ViewCatalogEntry *)table_or_view;
		// We need to use a new binder for the view that doesn't reference any CTEs
		// defined for this binder so there are no collisions between the CTEs defined
		// for the view and for the current query
		bool inherit_ctes = false;
		auto view_binder = Binder::CreateBinder(context, this, inherit_ctes);
		view_binder->can_contain_nulls = true;
		SubqueryRef subquery(unique_ptr_cast<SQLStatement, SelectStatement>(view_catalog_entry->query->Copy()));
		subquery.alias = ref.alias.empty() ? ref.table_name : ref.alias;
		subquery.column_name_alias =
		    BindContext::AliasColumnNames(subquery.alias, view_catalog_entry->aliases, ref.column_name_alias);
		// bind the child subquery
		view_binder->AddBoundView(view_catalog_entry);
		auto bound_child = view_binder->Bind(subquery);

		D_ASSERT(bound_child->type == TableReferenceType::SUBQUERY);
		// verify that the types and names match up with the expected types and names
		auto &bound_subquery = (BoundSubqueryRef &)*bound_child;
		if (bound_subquery.subquery->types != view_catalog_entry->types) {
			throw BinderException("Contents of view were altered: types don't match!");
		}
		bind_context.AddSubquery(bound_subquery.subquery->GetRootIndex(), subquery.alias, subquery,
		                         *bound_subquery.subquery);
		return bound_child;
	}
	default:
		throw InternalException("Catalog entry type");
	}
}
} // namespace duckdb
