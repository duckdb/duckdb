//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/optimizer/remote_pushdown_optimizer.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/catalog/catalog_search_path.hpp"
#include "duckdb/common/case_insensitive_map.hpp"
#include "duckdb/common/optional_ptr.hpp"
#include "duckdb/parser/tokens.hpp"

namespace duckdb {
class Binder;
class Catalog;
class CatalogEntry;
class ExpressionListRef;
class FunctionExpression;
class JoinRef;
class SubqueryRef;
class TableFunctionRef;
class TableRef;
class QueryNode;
class RecursiveCTENode;
class SetOperationNode;
class InsertQueryNode;
class DeleteQueryNode;
class UpdateQueryNode;

enum class CatalogReferenceType { NO_CATALOG_REFERENCED, SINGLE_REMOTE_CATALOG, UNKNOWN_CATALOG_REFERENCE };

struct CatalogPushdownResult {
	CatalogReferenceType reference_type = CatalogReferenceType::UNKNOWN_CATALOG_REFERENCE;
	optional_ptr<Catalog> catalog;
};

class RemotePushdownOptimizer {
public:
	explicit RemotePushdownOptimizer(Binder &binder);
	explicit RemotePushdownOptimizer(RemotePushdownOptimizer &parent);

	void Rewrite(unique_ptr<SQLStatement> &statement);

private:
	void FindRemoteCatalogsInSearchPath();
	CatalogPushdownResult Rewrite(QueryNode &node);
	CatalogPushdownResult Rewrite(SelectNode &node);
	CatalogPushdownResult Rewrite(SetOperationNode &node);
	CatalogPushdownResult Rewrite(InsertQueryNode &node);
	CatalogPushdownResult Rewrite(DeleteQueryNode &node);
	CatalogPushdownResult Rewrite(UpdateQueryNode &node);
	CatalogPushdownResult Rewrite(unique_ptr<TableRef> &ref);
	CatalogPushdownResult Rewrite(ExpressionListRef &ref);
	CatalogPushdownResult Rewrite(RecursiveCTENode &node);
	CatalogPushdownResult Rewrite(JoinRef &ref);
	CatalogPushdownResult Rewrite(SubqueryRef &ref);
	CatalogPushdownResult Rewrite(TableFunctionRef &ref);
	CatalogPushdownResult Rewrite(BaseTableRef &ref);
	CatalogPushdownResult Rewrite(ParsedExpression &expr);

	//! Records a BaseTableRef's name, alias and columns as local for correlated subquery detection
	void TrackLocalTable(const BaseTableRef &ref);
	//! Returns true if the function is defined as a macro in a local (non-remote) catalog
	bool IsLocalMacro(const FunctionExpression &func);

	void FinishPushdown(unique_ptr<SQLStatement> &statement, CatalogPushdownResult result);
	void FinishPushdown(unique_ptr<QueryNode> &node, CatalogPushdownResult result);
	//! Push a single BaseTableRef child to remote. Returns the catalog name if successful, empty string otherwise.
	string PushJoinChild(unique_ptr<TableRef> &ref, CatalogPushdownResult result);

	static CatalogPushdownResult Merge(CatalogPushdownResult a, CatalogPushdownResult b);
	unique_ptr<TableRef> CreateRemoteFunctionRef(CatalogPushdownResult &result, unique_ptr<QueryNode> node);
	static void StripCatalogName(SQLStatement &statement, const string &catalog_name);
	static void StripCatalogName(QueryNode &node, const string &catalog_name);
	static void StripCatalogName(TableRef &ref, const string &catalog_name);
	//! Strip catalog prefix from expression column refs. When strip_subquery_bodies=false, leaves subquery
	//! bodies untouched (used for partial pushdown where inner subqueries are not being pushed).
	static void StripCatalogName(ParsedExpression &expr, const string &catalog_name, bool strip_subquery_bodies = true);
	//! Strip catalog-qualified column refs in ORDER BY expressions of a set operation to just the column name.
	//! In a UNION/INTERSECT/EXCEPT output there are no table associations, so even a 2-part qualifier fails.
	static void StripSetOpOrderByExpr(ParsedExpression &expr, const string &catalog_name);
	//! After StripCatalogName reduces "catalog.table.col" to "table.col", rename "table.col" to "alias.col"
	//! when the pushed table has an explicit alias that differs from its table name.
	static void RenameTableInExpr(ParsedExpression &expr, const string &old_table, const string &new_alias);
	bool RefersToLocalTable(ColumnRefExpression &col_ref) const;

	bool RefersToCTE(const string &cte_name, CatalogPushdownResult &result) const;

private:
	Binder &binder;
	optional_ptr<RemotePushdownOptimizer> parent;
	bool search_path_initialized = false;
	vector<reference<Catalog>> remote_catalogs_in_search_path;
	vector<CatalogSearchEntry> local_catalogs_in_search_path;
	//! Entry pushed to pending_outer_strip_catalogs: catalog + optional table→alias rename info.
	//! After StripCatalogName reduces "catalog.table.col" to "table.col", RenameTableInExpr renames
	//! "table.col" to "alias.col" when the pushed table carries an explicit alias differing from its name.
	struct PendingStripEntry {
		string catalog_name;
		string old_table_name; // table name before push (non-empty when entry is valid)
		string new_alias;      // alias assigned after push (may equal old_table_name)
	};

	//! Names/aliases of non-remote tables seen in the current FROM scope, used to detect correlated subqueries
	case_insensitive_set_t local_table_names;
	//! CTE name → catalog pushdown result, populated as CTEs are analyzed (inner scopes restore on exit)
	case_insensitive_map_t<CatalogPushdownResult> cte_results;
	//! Catalogs individually pushed during FROM processing; Rewrite(SelectNode) reads this to strip outer expressions
	vector<PendingStripEntry> pending_outer_strip_catalogs;
};
} // namespace duckdb
