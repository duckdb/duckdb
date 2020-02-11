#include "duckdb/catalog/catalog_entry/view_catalog_entry.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/main/database.hpp"
#include "duckdb/parser/tableref/basetableref.hpp"
#include "duckdb/parser/tableref/subqueryref.hpp"
#include "duckdb/planner/binder.hpp"
#include "duckdb/planner/tableref/bound_basetableref.hpp"
#include "duckdb/planner/tableref/bound_subqueryref.hpp"
#include "duckdb/planner/tableref/bound_cteref.hpp"

using namespace duckdb;
using namespace std;

unique_ptr<BoundTableRef> Binder::Bind(BaseTableRef &expr) {
	// CTEs and views are also referred to using BaseTableRefs, hence need to distinguish here
	// check if the table name refers to a CTE
	auto cte = FindCTE(expr.table_name);
	if (cte) {
	    // Check if there is a CTE binding in the BindContext
        auto ctebinding = bind_context.GetCTEBinding(expr.table_name);
        if(ctebinding == nullptr) {
            // Move CTE to subquery and bind recursively
            SubqueryRef subquery(move(cte));
            subquery.alias = expr.alias.empty() ? expr.table_name : expr.alias;
            return Bind(subquery);
        } else {
            // There is a CTE binding in the BindContext.
            // This can only be the case if there is a recursive CTE present.
            auto index = GenerateTableIndex();
            auto result = make_unique<BoundCTERef>(index, ctebinding->index);
            auto b = (GenericBinding *)ctebinding;

            bind_context.AddGenericBinding(index, expr.alias.empty() ? expr.table_name : expr.alias, b->names, b->types);
            // Update references to CTE
            auto cteref = bind_context.cte_references[expr.table_name];
            (*cteref)++;

            result->types = b->types;
            result->bound_columns = b->names;
            return move(result);
        }
	}
	// not a CTE
	// extract a table or view from the catalog
	auto table_or_view = context.catalog.GetTableOrView(context, expr.schema_name, expr.table_name);
	switch (table_or_view->type) {
	case CatalogType::TABLE: {
		// base table: create the BoundBaseTableRef node
		auto table = (TableCatalogEntry *)table_or_view;
		auto table_index = GenerateTableIndex();
		auto result = make_unique<BoundBaseTableRef>(table, table_index);
		bind_context.AddBaseTable(result.get(), expr.alias.empty() ? expr.table_name : expr.alias);
		return move(result);
	}
	case CatalogType::VIEW: {
		// the node is a view: get the query that the view represents
		auto view_catalog_entry = (ViewCatalogEntry *)table_or_view;
		SubqueryRef subquery(view_catalog_entry->query->Copy());
		subquery.alias = expr.alias.empty() ? expr.table_name : expr.alias;
		subquery.column_name_alias = view_catalog_entry->aliases;
		// bind the child subquery
		return Bind(subquery);
	}
	default:
		throw NotImplementedException("Catalog entry type");
	}
}
