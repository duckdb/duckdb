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
	explicit CatalogPushdownResult(
	    CatalogReferenceType reference_type = CatalogReferenceType::UNKNOWN_CATALOG_REFERENCE);
	static CatalogPushdownResult Unknown();
	static CatalogPushdownResult NoCatalogReference();
	static CatalogPushdownResult RemoteReference(Catalog &catalog);

	CatalogReferenceType reference_type = CatalogReferenceType::UNKNOWN_CATALOG_REFERENCE;
	optional_ptr<Catalog> catalog;
	vector<const_reference<ParsedExpression>> used_expressions;
	vector<const_reference<TableRef>> used_table_constructs;
	vector<const_reference<QueryNode>> used_nodes;
};

//! Whether an expression (tree) can be constant-folded
enum class ExpressionFoldability { FOLDABLE, NOT_FOLDABLE };

//! Whether an expression itself may be constant-folded
enum class ExpressionFoldingMode {
	//! The expression (or any subtree within) can be folded
	FOLD_EXPRESSION,
	//! Only subtrees within the expression can be folded, the expression itself must be kept -
	//! used for positional contexts (ORDER BY / GROUP BY / DISTINCT ON, where a folded integer
	//! literal would become a positional reference) and after a failed folding attempt
	FOLD_CHILDREN_ONLY
};

//! The result of analyzing or rewriting a single expression
struct ExpressionPushdownResult {
	//! The catalog analysis result
	CatalogPushdownResult result {CatalogReferenceType::NO_CATALOG_REFERENCED};
	ExpressionFoldability foldability = ExpressionFoldability::NOT_FOLDABLE;
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
	//! Rewrite an expression, constant-folding maximal foldable subtrees
	CatalogPushdownResult Rewrite(unique_ptr<ParsedExpression> &expr,
	                              ExpressionFoldingMode mode = ExpressionFoldingMode::FOLD_EXPRESSION);
	//! Rewrite an expression, deferring the folding of foldable subtrees to the parent (or the root)
	ExpressionPushdownResult RewriteExpression(unique_ptr<ParsedExpression> &expr, ExpressionFoldingMode mode);
	//! Fold a maximal foldable subtree and record the resulting constant
	CatalogPushdownResult FoldExpression(unique_ptr<ParsedExpression> &expr);
	//! Per-expression-class catalog analysis (catalog qualification, subqueries, local tables),
	//! which also determines whether the expression can be constant-folded
	ExpressionPushdownResult AnalyzeExpression(const ParsedExpression &expr);
	ExpressionPushdownResult AnalyzeExpression(const SubqueryExpression &expr);
	ExpressionPushdownResult AnalyzeExpression(const FunctionExpression &expr);
	ExpressionPushdownResult AnalyzeExpression(const WindowExpression &expr);
	ExpressionPushdownResult AnalyzeExpression(const TypeExpression &expr);
	ExpressionPushdownResult AnalyzeExpression(const ColumnRefExpression &expr);
	//! Bind and evaluate an expression locally, replacing it with the resulting constant
	ConstantFoldResult TryConstantFold(unique_ptr<ParsedExpression> &expr);

	CatalogPushdownResult CheckCatalogQualification(const ParsedExpression &expr, const Identifier &catalog_name,
	                                                const Identifier &schema_name);
	CatalogPushdownResult RewriteTableFunctionOnly(TableFunctionRef &ref);

	//! Records a BaseTableRef's name, alias and columns as local for correlated subquery detection
	void TrackLocalTable(const TableRef &ref);
	void TrackLocalTable(const BaseTableRef &ref);
	void TrackLocalTable(const TableFunctionRef &ref);
	void TrackLocalTable(const SubqueryRef &ref);

	void FinishPushdown(unique_ptr<SQLStatement> &statement, CatalogPushdownResult result);
	void FinishPushdown(unique_ptr<QueryNode> &node, CatalogPushdownResult result);

	static CatalogPushdownResult Merge(CatalogPushdownResult a, CatalogPushdownResult b);
	unique_ptr<TableRef> CreateRemoteFunctionRef(CatalogPushdownResult &result, unique_ptr<QueryNode> node);
	static void StripCatalogName(SQLStatement &statement, const Identifier &catalog_name);
	static void StripCatalogName(QueryNode &node, const Identifier &catalog_name);
	static void StripCatalogName(TableRef &ref, const Identifier &catalog_name);
	//! Strip catalog prefix from expression column refs. When strip_subquery_bodies=false, leaves subquery
	//! bodies untouched (used for partial pushdown where inner subqueries are not being pushed).
	static void StripCatalogName(ParsedExpression &expr, const Identifier &catalog_name);
	bool RefersToLocalTable(const ColumnRefExpression &col_ref) const;

	bool RefersToCTE(const Identifier &cte_name, CatalogPushdownResult &result) const;

private:
	Binder &binder;
	optional_ptr<RemotePushdownOptimizer> parent;
	unique_ptr<RemotePushdownState> owned_pushdown_state;
	RemotePushdownState &pushdown_state;
	//! Names/aliases of non-remote tables seen in the current FROM scope, used to detect correlated subqueries
	identifier_set_t local_table_names;
	//! CTE name to catalog pushdown result, populated as CTEs are analyzed (inner scopes restore on exit)
	identifier_map_t<CatalogPushdownResult> cte_results;
};
} // namespace duckdb
