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
class MergeIntoStatement;

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
	//! Returns true if expr/ref/node contains a column reference to a local table (for correlated subquery detection)
	bool HasLocalTableReference(ParsedExpression &expr);
	bool HasLocalTableReference(TableRef &ref);
	bool HasLocalTableReference(QueryNode &node);

	void FinishPushdown(unique_ptr<SQLStatement> &statement, CatalogPushdownResult result);
	void FinishPushdown(unique_ptr<QueryNode> &node, CatalogPushdownResult result);
	void FinishPushdown(unique_ptr<TableRef> &ref, CatalogPushdownResult result);

	static CatalogPushdownResult Merge(CatalogPushdownResult a, CatalogPushdownResult b);
	unique_ptr<TableRef> CreateRemoteFunctionRef(CatalogPushdownResult &result, unique_ptr<QueryNode> node);
	static void StripCatalogName(SQLStatement &statement, const string &catalog_name);
	static void StripCatalogName(QueryNode &node, const string &catalog_name);
	static void StripCatalogName(TableRef &ref, const string &catalog_name);
	static void StripCatalogName(ParsedExpression &expr, const string &catalog_name, bool strip_subquery_bodies = true);
	bool RefersToLocalTable(ColumnRefExpression &col_ref) const;

	bool RefersToCTE(const string &cte_name, CatalogPushdownResult &result) const;

private:
	Binder &binder;
	optional_ptr<RemotePushdownOptimizer> parent;
	bool search_path_initialized = false;
	vector<reference<Catalog>> remote_catalogs_in_search_path;
	vector<CatalogSearchEntry> local_catalogs_in_search_path;
	//! Names/aliases of non-remote tables seen in the current FROM scope, used to detect correlated subqueries
	case_insensitive_set_t local_table_names;
	//! CTE name → catalog pushdown result, populated as CTEs are analyzed (inner scopes restore on exit)
	case_insensitive_map_t<CatalogPushdownResult> cte_results;
	//! Catalog names of remote tables individually pushed during the current SelectNode's from_table processing.
	//! Populated by Rewrite(JoinRef&) when individual JoinRef children are pushed. Consumed and cleared by
	//! Rewrite(SelectNode&) to strip those catalog names from outer SELECT/WHERE/HAVING/GROUP BY/ORDER BY
	//! expressions so that catalog-qualified refs like "rpc.t1.i" remain resolvable after "rpc.t1" becomes
	//! "quack_query_by_name(...) AS t1".
	vector<string> from_pushed_catalog_names;
	//! Aliases of remote tables individually pushed by Rewrite(JoinRef&). Consumed and cleared by
	//! Rewrite(SelectNode&) to temporarily register them in local_table_names so HasLocalTableReference
	//! detects correlated WHERE subqueries that reference a pushed alias (e.g. "t1.col" after "rpc.t1 AS t1"
	//! was pushed to "quack_query_by_name(...) AS t1").
	vector<string> from_pushed_table_aliases;
};
} // namespace duckdb
