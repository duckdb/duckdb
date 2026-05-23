//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/planner/statement_preprocessor.hpp
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
class SetOperationNode;
class InsertQueryNode;
class DeleteQueryNode;
class UpdateQueryNode;

enum class CatalogReferenceType { NO_CATALOG_REFERENCED, SINGLE_REMOTE_CATALOG, UNKNOWN_CATALOG_REFERENCE };

struct CatalogPushdownResult {
	CatalogReferenceType reference_type = CatalogReferenceType::UNKNOWN_CATALOG_REFERENCE;
	optional_ptr<Catalog> catalog;
	//! The schema within the remote catalog (after schema-as-catalog disambiguation)
	string schema_name;
};

class StatementRewriter {
public:
	explicit StatementRewriter(Binder &binder);

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
	//! Records a BaseTableRef's name, alias and columns as local for correlated subquery detection
	void TrackLocalTable(const BaseTableRef &ref, optional_ptr<CatalogEntry> entry = nullptr);
	//! Returns true if the function is defined as a macro in a local (non-remote) catalog
	bool IsLocalMacro(const FunctionExpression &func);

	void FinishPushdown(unique_ptr<SQLStatement> &statement, CatalogPushdownResult result);
	void FinishPushdown(unique_ptr<QueryNode> &node, CatalogPushdownResult result);
	void FinishPushdown(unique_ptr<TableRef> &ref, CatalogPushdownResult result);

	static CatalogPushdownResult Merge(CatalogPushdownResult a, CatalogPushdownResult b);
	static unique_ptr<TableFunctionRef> CreateRemoteFunctionRef(CatalogPushdownResult &result, string remote_sql);
	static void StripCatalogName(SQLStatement &statement, const string &catalog_name);
	static void StripCatalogName(QueryNode &node, const string &catalog_name);
	static void StripCatalogName(TableRef &ref, const string &catalog_name);
	static void StripCatalogName(ParsedExpression &expr, const string &catalog_name);

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
};
} // namespace duckdb
