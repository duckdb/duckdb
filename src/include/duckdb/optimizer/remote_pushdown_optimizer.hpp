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
	//! The schema within the remote catalog (after schema-as-catalog disambiguation)
	string schema_name;
};

class RemotePushdownOptimizer {
public:
	explicit RemotePushdownOptimizer(Binder &binder);

	void Rewrite(unique_ptr<SQLStatement> &statement);

private:
	void FindRemoteCatalogsInSearchPath();
	CatalogPushdownResult Rewrite(QueryNode &node);
	CatalogPushdownResult Rewrite(SelectNode &node);
	CatalogPushdownResult Rewrite(SetOperationNode &node);
	CatalogPushdownResult Rewrite(InsertQueryNode &node);
	CatalogPushdownResult Rewrite(DeleteQueryNode &node);
	CatalogPushdownResult Rewrite(UpdateQueryNode &node);
	CatalogPushdownResult Rewrite(MergeIntoStatement &stmt);
	CatalogPushdownResult Rewrite(unique_ptr<TableRef> &ref);
	CatalogPushdownResult Rewrite(ExpressionListRef &ref);
	CatalogPushdownResult Rewrite(RecursiveCTENode &node);
	CatalogPushdownResult Rewrite(JoinRef &ref);
	CatalogPushdownResult Rewrite(SubqueryRef &ref);
	CatalogPushdownResult Rewrite(TableFunctionRef &ref);
	CatalogPushdownResult Rewrite(BaseTableRef &ref);
	CatalogPushdownResult Rewrite(ParsedExpression &expr);
	void PushdownSubqueries(unique_ptr<ParsedExpression> &expr);

	//! Returns true if expr (or any descendant) contains a qualified column reference to a local table.
	//! Used to detect correlated subqueries that reference outer local tables.
	bool HasLocalTableReference(ParsedExpression &expr);
	bool HasLocalTableReference(QueryNode &node);
	bool HasLocalTableReference(TableRef &ref);
	//! Records a BaseTableRef's name, alias and columns as local for correlated subquery detection
	void TrackLocalTable(const BaseTableRef &ref, optional_ptr<CatalogEntry> entry = nullptr);
	//! Returns true if the function is defined as a macro in a local (non-remote) catalog
	bool IsLocalMacro(const FunctionExpression &func);

	void FinishPushdown(unique_ptr<SQLStatement> &statement, CatalogPushdownResult result);
	void FinishPushdown(unique_ptr<QueryNode> &node, CatalogPushdownResult result);
	void FinishPushdown(unique_ptr<TableRef> &ref, CatalogPushdownResult result);
	//! When DML body is all-remote but RETURNING has local expressions, push the DML with
	//! stripped RETURNING (raw col refs only) and wrap result in an outer SELECT that applies
	//! the original RETURNING expressions locally.
	void TryPushDMLWithLocalReturning(unique_ptr<SQLStatement> &statement, CatalogPushdownResult body_result);

	static CatalogPushdownResult Merge(CatalogPushdownResult a, CatalogPushdownResult b);
	static unique_ptr<TableFunctionRef> CreateRemoteFunctionRef(CatalogPushdownResult &result, string remote_sql);
	static void StripCatalogName(SQLStatement &statement, const string &catalog_name);
	static void StripCatalogName(QueryNode &node, const string &catalog_name);
	static void StripCatalogName(TableRef &ref, const string &catalog_name);
	static void StripCatalogName(ParsedExpression &expr, const string &catalog_name, bool strip_subquery_bodies = true);

private:
	Binder &binder;
	bool search_path_initialized = false;
	vector<reference<Catalog>> remote_catalogs_in_search_path;
	vector<CatalogSearchEntry> local_catalogs_in_search_path;
	//! Names/aliases of non-remote tables seen in the current FROM scope, used to detect correlated subqueries
	case_insensitive_set_t local_table_names;
	//! Column names of all tracked local tables - used to detect unqualified correlated column references
	case_insensitive_set_t local_table_column_names;
	//! CTE name → catalog pushdown result, populated as CTEs are analyzed (inner scopes restore on exit)
	case_insensitive_map_t<CatalogPushdownResult> cte_results;
	//! Catalog names of remote tables individually pushed during the current SelectNode's from_table processing.
	//! Populated by Rewrite(JoinRef&) when individual JoinRef children are pushed. Consumed and cleared by
	//! Rewrite(SelectNode&) to strip those catalog names from outer SELECT/WHERE/HAVING/GROUP BY/ORDER BY
	//! expressions so that catalog-qualified refs like "rpc.t1.i" remain resolvable after "rpc.t1" becomes
	//! "quack_query_by_name(...) AS t1". Saved/restored at subquery boundaries to prevent scope leakage.
	vector<string> from_pushed_catalog_names;
	//! Aliases of remote tables individually pushed by Rewrite(JoinRef&). Consumed and cleared by
	//! Rewrite(SelectNode&) to temporarily register them in local_table_names so HasLocalTableReference
	//! detects correlated WHERE subqueries that reference a pushed alias (e.g. "t1.col" after "rpc.t1 AS t1"
	//! was pushed to "quack_query_by_name(...) AS t1"). Saved/restored at subquery boundaries.
	vector<string> from_pushed_table_aliases;
};
} // namespace duckdb
