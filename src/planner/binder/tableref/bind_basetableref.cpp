#include "duckdb/catalog/catalog_entry/view_catalog_entry.hpp"
#include "duckdb/parser/tableref/basetableref.hpp"
#include "duckdb/parser/tableref/subqueryref.hpp"
#include "duckdb/planner/binder.hpp"
#include "duckdb/planner/operator/logical_get.hpp"

using namespace std;

namespace duckdb {

unique_ptr<LogicalOperator> Binder::BindTable(TableCatalogEntry &table, BaseTableRef &ref) {
	// base table: create the BoundBaseTableRef node
	auto table_index = GenerateTableIndex();

	auto logical_get = make_unique<LogicalGet>(&table, table_index);
	auto alias = ref.alias.empty() ? ref.table_name : ref.alias;
	bind_context.AddBaseTable(table_index, alias, table, *logical_get);
	return move(logical_get);
}

unique_ptr<LogicalOperator> Binder::BindView(ViewCatalogEntry &view, BaseTableRef &ref) {
	// the node is a view: get the query that the view represents
	SubqueryRef subquery(view.query->Copy());
	subquery.alias = ref.alias.empty() ? ref.table_name : ref.alias;
	subquery.column_name_alias = view.aliases;
	// bind the child subquery
	return Bind(subquery);
}

unique_ptr<LogicalOperator> Binder::BindTableOrView(BaseTableRef &ref) {
	auto table_or_view =
	    Catalog::GetCatalog(context).GetEntry(context, CatalogType::TABLE, ref.schema_name, ref.table_name);
	switch (table_or_view->type) {
	case CatalogType::TABLE:
		return BindTable((TableCatalogEntry&) *table_or_view, ref);
	case CatalogType::VIEW:
		return BindView((ViewCatalogEntry&) *table_or_view, ref);
	default:
		throw NotImplementedException("Catalog entry type");
	}
}

unique_ptr<LogicalOperator> Binder::Bind(BaseTableRef &ref) {
	// CTEs and views are also referred to using BaseTableRefs, hence need to distinguish here
	// check if the table name refers to a CTE
	auto cte = FindCTE(ref.table_name);
	if (cte) {
		// Check if there is a CTE binding in the BindContext
		auto ctebinding = bind_context.GetCTEBinding(ref.table_name);
		if (ctebinding == nullptr) {
			// Move CTE to subquery and bind recursively
			SubqueryRef subquery(move(cte));
			subquery.alias = ref.alias.empty() ? ref.table_name : ref.alias;
			return Bind(subquery);
		} else {
			// There is a CTE binding in the BindContext.
			// This can only be the case if there is a recursive CTE present.
			// auto index = GenerateTableIndex();
			// auto result = make_unique<BoundCTERef>(index, ctebinding->index);
			// auto b = (GenericBinding *)ctebinding;

			// bind_context.AddGenericBinding(index, ref.alias.empty() ? ref.table_name : ref.alias, b->names,
			//                                b->types);
			// // Update references to CTE
			// auto cteref = bind_context.cte_references[ref.table_name];
			// (*cteref)++;

			// result->types = b->types;
			// result->bound_columns = b->names;
			// return move(result);
			throw NotImplementedException("FIXME bind recursive CTE");
		}
	}
	// not a CTE
	// extract a table or view from the catalog
	return BindTableOrView(ref);
}

}
