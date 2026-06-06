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

//! An expression that a remote catalog must support in order to push down the query.
//! Expressions reached through their owning pointer can be constant-folded when the remote
//! system cannot evaluate them as written. For other expressions folding is not possible:
//! cast target type expressions are owned by the (potentially shared) type, and top-level
//! ORDER BY / GROUP BY / DISTINCT ON entries are positional contexts, where a folded integer
//! literal would turn into a positional reference.
struct PushdownExpression {
	explicit PushdownExpression(const ParsedExpression &expr) : expression(&expr) {
	}
	explicit PushdownExpression(unique_ptr<ParsedExpression> &slot) : foldable_slot(&slot) {
	}

	const ParsedExpression &Get() const {
		return foldable_slot ? **foldable_slot : *expression;
	}

	//! the expression (only set when there is no foldable slot)
	optional_ptr<const ParsedExpression> expression;
	//! the owning pointer of the expression, set when constant folding is permitted
	optional_ptr<unique_ptr<ParsedExpression>> foldable_slot;
};

struct CatalogPushdownResult {
	explicit CatalogPushdownResult(
	    CatalogReferenceType reference_type = CatalogReferenceType::UNKNOWN_CATALOG_REFERENCE);
	static CatalogPushdownResult Unknown();
	static CatalogPushdownResult NoCatalogReference();
	static CatalogPushdownResult RemoteReference(Catalog &catalog);

	CatalogReferenceType reference_type = CatalogReferenceType::UNKNOWN_CATALOG_REFERENCE;
	optional_ptr<Catalog> catalog;
	vector<PushdownExpression> used_expressions;
	vector<const_reference<TableRef>> used_table_constructs;
	vector<reference<QueryNode>> used_nodes;
};

struct RemotePushdownState {
	bool search_path_initialized = false;
	vector<reference<Catalog>> remote_catalogs_in_search_path;
	vector<CatalogSearchEntry> local_catalogs_in_search_path;
};

class RemotePushdownOptimizer {
public:
	explicit RemotePushdownOptimizer(Binder &binder);
	explicit RemotePushdownOptimizer(optional_ptr<RemotePushdownOptimizer> parent);

	void Rewrite(unique_ptr<SQLStatement> &statement);

private:
	void FindRemoteCatalogsInSearchPath();
	CatalogPushdownResult Rewrite(QueryNode &node);
	//! The per-type query node handlers are deliberately NOT overloads of Rewrite: calling
	//! Rewrite with a statically-typed node (e.g. *InsertStatement::node, which is an
	//! InsertQueryNode) must dispatch through Rewrite(QueryNode &) so the node-level checks
	//! (CTE handling, catalog support verification) are applied
	CatalogPushdownResult RewriteNode(SelectNode &node);
	CatalogPushdownResult RewriteNode(SetOperationNode &node);
	CatalogPushdownResult RewriteNode(InsertQueryNode &node);
	CatalogPushdownResult RewriteNode(DeleteQueryNode &node);
	CatalogPushdownResult RewriteNode(UpdateQueryNode &node);
	CatalogPushdownResult Rewrite(unique_ptr<TableRef> &ref);
	CatalogPushdownResult Rewrite(ExpressionListRef &ref);
	CatalogPushdownResult RewriteNode(RecursiveCTENode &node);
	CatalogPushdownResult Rewrite(JoinRef &ref);
	CatalogPushdownResult Rewrite(SubqueryRef &ref);
	CatalogPushdownResult Rewrite(TableFunctionRef &ref);
	CatalogPushdownResult Rewrite(BaseTableRef &ref);

	enum class ConstantFoldResult {
		//! The expression is not a foldable constant expression (contains columns, is volatile, ...)
		NOT_FOLDABLE,
		//! The expression was replaced with its locally-evaluated constant result
		FOLDED,
		//! The expression is constant but evaluating it raises an error - the query must be
		//! executed locally so the user sees DuckDB's error message
		FOLD_ERROR
	};
	//! Rewrite an expression through its owning pointer. When the remote catalog cannot
	//! evaluate the expression as written, the constant subtrees within it are folded into
	//! their locally-evaluated literals and the expression is checked again - this makes more
	//! queries eligible for remote pushdown, as the remote system only sees the
	//! (DuckDB-evaluated) literal instead of functions whose remote semantics differ.
	//! "can_fold" must be false for expressions where a bare integer literal has positional
	//! meaning (top-level ORDER BY / GROUP BY / DISTINCT ON entries): folding e.g. "1 + 1"
	//! into "2" would turn it into a positional reference.
	CatalogPushdownResult Rewrite(unique_ptr<ParsedExpression> &expr, bool can_fold = true);
	//! Rewrite an expression that cannot be modified (cast target type expressions)
	CatalogPushdownResult Rewrite(const ParsedExpression &expr);
	//! Shared veto / recording logic of the two Rewrite overloads above
	CatalogPushdownResult FinishExpression(CatalogPushdownResult result, const ParsedExpression &expr,
	                                       optional_ptr<unique_ptr<ParsedExpression>> slot);
	//! Per-expression-class catalog analysis (catalog qualification, subqueries, local tables)
	CatalogPushdownResult AnalyzeExpression(const ParsedExpression &expr);
	CatalogPushdownResult AnalyzeExpression(const SubqueryExpression &expr);
	CatalogPushdownResult AnalyzeExpression(const CastExpression &expr);
	CatalogPushdownResult AnalyzeExpression(const FunctionExpression &expr);
	CatalogPushdownResult AnalyzeExpression(const WindowExpression &expr);
	CatalogPushdownResult AnalyzeExpression(const TypeExpression &expr);
	CatalogPushdownResult AnalyzeExpression(const ColumnRefExpression &expr);
	//! Attempt to constant-fold a column-free, non-volatile expression by binding and evaluating
	//! it locally, replacing it with the resulting constant
	ConstantFoldResult TryConstantFold(unique_ptr<ParsedExpression> &expr);
	//! Fold the largest constant subtrees within an expression (TryConstantFold on the whole
	//! expression first, recursing into the children of unfoldable expressions)
	ConstantFoldResult FoldConstantSubtrees(unique_ptr<ParsedExpression> &expr);
	//! Fold a query node's non-literal LIMIT / OFFSET values, which must be literals to be
	//! pushed down to most remote systems
	bool FoldLimitValues(QueryNode &node);

	CatalogPushdownResult Rewrite(const LogicalType &type);
	CatalogPushdownResult CheckCatalogQualification(const ParsedExpression &expr, const string &catalog_name,
	                                                const string &schema_name);
	CatalogPushdownResult RewriteTableFunctionOnly(TableFunctionRef &ref);

	//! Records a BaseTableRef's name, alias and columns as local for correlated subquery detection
	void TrackLocalTable(const TableRef &ref);
	void TrackLocalTable(const BaseTableRef &ref);
	void TrackLocalTable(const TableFunctionRef &ref);
	void TrackLocalTable(const SubqueryRef &ref);
	//! Returns true if the function is defined as a macro in a local (non-remote) catalog
	bool IsLocalMacro(const FunctionExpression &func);

	void FinishPushdown(unique_ptr<SQLStatement> &statement, CatalogPushdownResult result);
	void FinishPushdown(unique_ptr<QueryNode> &node, CatalogPushdownResult result);

	CatalogPushdownResult Merge(CatalogPushdownResult a, CatalogPushdownResult b);
	unique_ptr<TableRef> CreateRemoteFunctionRef(CatalogPushdownResult &result, unique_ptr<QueryNode> node);
	static void StripCatalogName(SQLStatement &statement, const string &catalog_name);
	static void StripCatalogName(QueryNode &node, const string &catalog_name);
	static void StripCatalogName(TableRef &ref, const string &catalog_name);
	//! Strip catalog prefix from expression column refs. When strip_subquery_bodies=false, leaves subquery
	//! bodies untouched (used for partial pushdown where inner subqueries are not being pushed).
	static void StripCatalogName(ParsedExpression &expr, const string &catalog_name);
	bool RefersToLocalTable(const ColumnRefExpression &col_ref) const;

	bool RefersToCTE(const string &cte_name, CatalogPushdownResult &result) const;

private:
	Binder &binder;
	optional_ptr<RemotePushdownOptimizer> parent;
	unique_ptr<RemotePushdownState> owned_pushdown_state;
	RemotePushdownState &pushdown_state;
	//! Names/aliases of non-remote tables seen in the current FROM scope, used to detect correlated subqueries
	case_insensitive_set_t local_table_names;
	//! CTE name to catalog pushdown result, populated as CTEs are analyzed (inner scopes restore on exit)
	case_insensitive_map_t<CatalogPushdownResult> cte_results;
};
} // namespace duckdb
