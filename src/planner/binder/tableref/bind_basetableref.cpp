#include "duckdb/catalog/catalog_entry/view_catalog_entry.hpp"
#include "duckdb/parser/tableref/basetableref.hpp"
#include "duckdb/parser/tableref/subqueryref.hpp"
#include "duckdb/planner/binder.hpp"
#include "duckdb/planner/tableref/bound_basetableref.hpp"
#include "duckdb/planner/tableref/bound_subqueryref.hpp"
#include "duckdb/planner/tableref/bound_cteref.hpp"
#include "duckdb/planner/operator/logical_get.hpp"
#include "duckdb/parser/statement/select_statement.hpp"
#include "duckdb/function/table/table_scan.hpp"

namespace duckdb {
using namespace std;

unique_ptr<BoundTableRef> Binder::Bind(BaseTableRef &ref) {
	// CTEs and views are also referred to using BaseTableRefs, hence need to distinguish here
	// check if the table name refers to a CTE
	auto cte = FindCTE(ref.table_name);
	if (cte) {
		// Check if there is a CTE binding in the BindContext
		auto ctebinding = bind_context.GetCTEBinding(ref.table_name);
		if (!ctebinding) {
			// Move CTE to subquery and bind recursively
			SubqueryRef subquery(cte->query->Copy());
			subquery.alias = ref.alias.empty() ? ref.table_name : ref.alias;
			subquery.column_name_alias = cte->aliases;
			return Bind(subquery);
		} else {
			// There is a CTE binding in the BindContext.
			// This can only be the case if there is a recursive CTE present.
			auto index = GenerateTableIndex();
			auto result = make_unique<BoundCTERef>(index, ctebinding->index);
			auto b = ctebinding;

			bind_context.AddGenericBinding(index, ref.alias.empty() ? ref.table_name : ref.alias, b->names, b->types);
			// Update references to CTE
			auto cteref = bind_context.cte_references[ref.table_name];
			(*cteref)++;

			result->types = b->types;
			result->bound_columns = b->names;
			return move(result);
		}
	}
	// not a CTE
	// extract a table or view from the catalog
	auto table_or_view =
	    Catalog::GetCatalog(context).GetEntry(context, CatalogType::TABLE_ENTRY, ref.schema_name, ref.table_name);
	switch (table_or_view->type) {
	case CatalogType::TABLE_ENTRY: {
		// base table: create the BoundBaseTableRef node
		auto table_index = GenerateTableIndex();
		auto table = (TableCatalogEntry *)table_or_view;

		auto scan_function = TableScanFunction::GetFunction();
		auto bind_data = make_unique<TableScanBindData>(table);
		vector<LogicalType> table_types;
		vector<string> table_names;
		for (auto &col : table->columns) {
			table_types.push_back(col.type);
			table_names.push_back(col.name);
		}

		auto logical_get =
		    make_unique<LogicalGet>(table_index, scan_function, move(bind_data), table_types, table_names);
		auto alias = ref.alias.empty() ? ref.table_name : ref.alias;
		bind_context.AddBaseTable(table_index, alias, move(table_names), move(table_types), table->name_map,
		                          *logical_get);
		return make_unique_base<BoundTableRef, BoundBaseTableRef>(table, move(logical_get));
	}
	case CatalogType::VIEW_ENTRY: {
		// the node is a view: get the query that the view represents
		auto view_catalog_entry = (ViewCatalogEntry *)table_or_view;
		// first we visit the set of CTEs and add them to the bind context
		Binder view_binder(context, this);
		view_binder.CTE_bindings.clear();
		for (auto &cte_it : view_catalog_entry->query->cte_map) {
			view_binder.AddCTE(cte_it.first, cte_it.second.get());
		}
		SubqueryRef subquery(view_catalog_entry->query->node->Copy());
		subquery.alias = ref.alias.empty() ? ref.table_name : ref.alias;
		subquery.column_name_alias = view_catalog_entry->aliases;
		// bind the child subquery
		auto bound_child = view_binder.Bind(subquery);
		assert(bound_child->type == TableReferenceType::SUBQUERY);
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
		throw NotImplementedException("Catalog entry type");
	}
}
} // namespace duckdb
