//===----------------------------------------------------------------------===//
//                         DuckDB
//
// parser/transformer.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "common/constants.hpp"
#include "common/enums/expression_type.hpp"
#include "common/types.hpp"
#include "common/unordered_map.hpp"
#include "parser/tokens.hpp"

namespace postgres {
#include "pg_definitions.h"
// don't reorder
#include "nodes/parsenodes.h"

} // namespace postgres

namespace duckdb {

class ColumnDefinition;
struct OrderByNode;

//! The transformer class is responsible for transforming the internal Postgres
//! parser representation into the DuckDB representation
class Transformer {
public:
	//! Transforms a Postgres parse tree into a set of SQL Statements
	bool TransformParseTree(postgres::List *tree, vector<unique_ptr<SQLStatement>> &statements);
	string NodetypeToString(postgres::NodeTag type);

private:
	//! Transforms a Postgres statement into a single SQL statement
	unique_ptr<SQLStatement> TransformStatement(postgres::Node *stmt);
	//===--------------------------------------------------------------------===//
	// Statement transformation
	//===--------------------------------------------------------------------===//
	//! Transform a Postgres T_SelectStmt node into a SelectStatement
	unique_ptr<SelectStatement> TransformSelect(postgres::Node *node);
	//! Transform a Postgres T_AlterStmt node into a AlterTableStatement
	unique_ptr<AlterTableStatement> TransformAlter(postgres::Node *node);
	//! Transform a Postgres T_RenameStmt node into a RenameStatement
	unique_ptr<AlterTableStatement> TransformRename(postgres::Node *node);
	//! Transform a Postgres T_CreateStmt node into a CreateTableStatement
	unique_ptr<CreateTableStatement> TransformCreateTable(postgres::Node *node);
	//! Transform a Postgres T_CreateStmt node into a CreateTableStatement
	unique_ptr<CreateTableStatement> TransformCreateTableAs(postgres::Node *node);
	//! Transform a Postgres node into a CreateSchemaStatement
	unique_ptr<CreateSchemaStatement> TransformCreateSchema(postgres::Node *node);
	//! Transform a Postgres T_CreateSeqStmt node into a CreateSequenceStatement
	unique_ptr<CreateSequenceStatement> TransformCreateSequence(postgres::Node *node);
	//! Transform a Postgres T_ViewStmt node into a CreateViewStatement
	unique_ptr<CreateViewStatement> TransformCreateView(postgres::Node *node);
	//! Transform a Postgres T_DropStmt node into a Drop[Table,Schema]Statement
	unique_ptr<SQLStatement> TransformDrop(postgres::Node *node);
	//! Transform a Postgres T_InsertStmt node into a InsertStatement
	unique_ptr<InsertStatement> TransformInsert(postgres::Node *node);
	//! Transform a Postgres T_IndexStmt node into CreateIndexStatement
	unique_ptr<CreateIndexStatement> TransformCreateIndex(postgres::Node *node);
	//! Transform a Postgres T_CopyStmt node into a CopyStatement
	unique_ptr<CopyStatement> TransformCopy(postgres::Node *node);
	//! Transform a Postgres T_TransactionStmt node into a TransactionStatement
	unique_ptr<TransactionStatement> TransformTransaction(postgres::Node *node);
	//! Transform a Postgres T_DeleteStatement node into a DeleteStatement
	unique_ptr<DeleteStatement> TransformDelete(postgres::Node *node);
	//! Transform a Postgres T_UpdateStmt node into a UpdateStatement
	unique_ptr<UpdateStatement> TransformUpdate(postgres::Node *node);

	unique_ptr<PrepareStatement> TransformPrepare(postgres::Node *node);
	unique_ptr<ExecuteStatement> TransformExecute(postgres::Node *node);
	unique_ptr<DeallocateStatement> TransformDeallocate(postgres::Node *node);

	//===--------------------------------------------------------------------===//
	// Query Node Transform
	//===--------------------------------------------------------------------===//
	//! Transform a Postgres T_SelectStmt node into a QueryNode
	unique_ptr<QueryNode> TransformSelectNode(postgres::SelectStmt *node);

	//===--------------------------------------------------------------------===//
	// Expression Transform
	//===--------------------------------------------------------------------===//
	//! Transform a Postgres boolean expression into an Expression
	unique_ptr<ParsedExpression> TransformBoolExpr(postgres::BoolExpr *root);
	//! Transform a Postgres case expression into an Expression
	unique_ptr<ParsedExpression> TransformCase(postgres::CaseExpr *root);
	//! Transform a Postgres type cast into an Expression
	unique_ptr<ParsedExpression> TransformTypeCast(postgres::TypeCast *root);
	//! Transform a Postgres coalesce into an Expression
	unique_ptr<ParsedExpression> TransformCoalesce(postgres::A_Expr *root);
	//! Transform a Postgres column reference into an Expression
	unique_ptr<ParsedExpression> TransformColumnRef(postgres::ColumnRef *root);
	//! Transform a Postgres constant value into an Expression
	unique_ptr<ParsedExpression> TransformValue(postgres::Value val);
	//! Transform a Postgres operator into an Expression
	unique_ptr<ParsedExpression> TransformAExpr(postgres::A_Expr *root);
	//! Transform a Postgres abstract expression into an Expression
	unique_ptr<ParsedExpression> TransformExpression(postgres::Node *node);
	//! Transform a Postgres function call into an Expression
	unique_ptr<ParsedExpression> TransformFuncCall(postgres::FuncCall *root);

	//! Transform a Postgres constant value into an Expression
	unique_ptr<ParsedExpression> TransformConstant(postgres::A_Const *c);

	unique_ptr<ParsedExpression> TransformResTarget(postgres::ResTarget *root);
	unique_ptr<ParsedExpression> TransformNullTest(postgres::NullTest *root);
	unique_ptr<ParsedExpression> TransformParamRef(postgres::ParamRef *node);

	unique_ptr<ParsedExpression> TransformSubquery(postgres::SubLink *root);
	//===--------------------------------------------------------------------===//
	// Constraints transform
	//===--------------------------------------------------------------------===//
	unique_ptr<Constraint> TransformConstraint(postgres::ListCell *cell);

	unique_ptr<Constraint> TransformConstraint(postgres::ListCell *cell, ColumnDefinition &column, index_t index);

	//===--------------------------------------------------------------------===//
	// Helpers
	//===--------------------------------------------------------------------===//
	string TransformAlias(postgres::Alias *root);
	void TransformCTE(postgres::WithClause *de_with_clause, SelectStatement &select);
	// Operator String to ExpressionType (e.g. + => OPERATOR_ADD)
	ExpressionType OperatorToExpressionType(string &op);
	//===--------------------------------------------------------------------===//
	// TableRef transform
	//===--------------------------------------------------------------------===//
	//! Transform a Postgres node into a TableRef
	unique_ptr<TableRef> TransformTableRefNode(postgres::Node *node);
	//! Transform a Postgres FROM clause into a TableRef
	unique_ptr<TableRef> TransformFrom(postgres::List *root);
	//! Transform a Postgres table reference into a TableRef
	unique_ptr<TableRef> TransformRangeVar(postgres::RangeVar *root);
	//! Transform a Postgres table-producing function into a TableRef
	unique_ptr<TableRef> TransformRangeFunction(postgres::RangeFunction *root);
	//! Transform a Postgres join node into a TableRef
	unique_ptr<TableRef> TransformJoin(postgres::JoinExpr *root);
	//! Transform a table producing subquery into a TableRef
	unique_ptr<TableRef> TransformRangeSubselect(postgres::RangeSubselect *root);

	//! Transform a Postgres TypeName string into a SQLType
	SQLType TransformTypeName(postgres::TypeName *name);

	//! Transform a Postgres GROUP BY expression into a list of Expression
	bool TransformGroupBy(postgres::List *group, vector<unique_ptr<ParsedExpression>> &result);
	//! Transform a Postgres ORDER BY expression into an OrderByDescription
	bool TransformOrderBy(postgres::List *order, vector<OrderByNode> &result);

	//! Transform a Postgres SELECT clause into a list of Expressions
	bool TransformExpressionList(postgres::List *list, vector<unique_ptr<ParsedExpression>> &result);
	//! Transform a VALUES list into a set of expressions
	void TransformValuesList(postgres::List *list, vector<vector<unique_ptr<ParsedExpression>>> &values);

	void TransformWindowDef(postgres::WindowDef *window_spec, WindowExpression *expr);

	//! Holds window expressions defined by name. We need those when transforming the expressions referring to them.
	unordered_map<string, postgres::WindowDef *> window_clauses;
	index_t prepared_statement_parameter_index = 0;
};

} // namespace duckdb
