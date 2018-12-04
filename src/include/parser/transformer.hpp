//===----------------------------------------------------------------------===// 
// 
//                         DuckDB 
// 
// parser/transformer.hpp
// 
// 
// 
//===----------------------------------------------------------------------===//

#pragma once

#include "parser/column_definition.hpp"
#include "parser/statement/select_statement.hpp"
#include "parser/tokens.hpp"

namespace postgres {
#include "parser/postgres/parsenodes.h"
#include "parser/postgres/pg_list.h"
#include "parser/postgres/pg_query.h"
#include "parser/postgres/pg_trigger.h"
} // namespace postgres

namespace duckdb {

//! The transformer class is responsible for transforming the internal Postgres
//! parser representation into the DuckDB representation
class Transformer {
  public:
	//! Transforms a Postgres parse tree into a set of SQL Statements
	bool
	TransformParseTree(postgres::List *tree,
	                   std::vector<std::unique_ptr<SQLStatement>> &statements);

  private:
	//! Transforms a Postgres statement into a single SQL statement
	std::unique_ptr<SQLStatement> TransformStatement(postgres::Node *stmt);
	//===--------------------------------------------------------------------===//
	// Statement transformation
	//===--------------------------------------------------------------------===//
	//! Transform a Postgres T_SelectStmt node into a SelectStatement
	std::unique_ptr<SelectStatement> TransformSelect(postgres::Node *node);
	//! Transform a Postgres T_AlterStmt node into a AlterTableStatement
	std::unique_ptr<AlterTableStatement> TransformAlter(postgres::Node *node);
	//! Transform a Postgres T_RenameStmt node into a RenameStatement
	std::unique_ptr<AlterTableStatement> TransformRename(postgres::Node *node);
	//! Transform a Postgres T_CreateStmt node into a CreateTableStatement
	std::unique_ptr<CreateTableStatement>
	TransformCreateTable(postgres::Node *node);
	//! Transform a Postgres T_CreateSchemaStmt node into a
	//! CreateSchemaStatement
	std::unique_ptr<CreateSchemaStatement>
	TransformCreateSchema(postgres::Node *node);
	//! Transform a Postgres T_DropStmt node into a Drop[Table,Schema]Statement
	std::unique_ptr<SQLStatement> TransformDrop(postgres::Node *node);
	//! Transform a Postgres T_InsertStmt node into a InsertStatement
	std::unique_ptr<InsertStatement> TransformInsert(postgres::Node *node);
	//! Transform a Postgres T_IndexStmt node into CreateIndexStatement
	std::unique_ptr<CreateIndexStatement>
	TransformCreateIndex(postgres::Node *node);
	//! Transform a Postgres DropStmt node into a DropTableStatement
	std::unique_ptr<DropTableStatement>
	TransformDropTable(postgres::DropStmt *stmt);
	//! Transform a Postgres DropStmt node into a DropIndexStatement
	std::unique_ptr<DropIndexStatement>
	TransformDropIndex(postgres::DropStmt *stmt);
	//! Transform a Postgres DropStmt node into a DropSchemaStatement
	std::unique_ptr<DropSchemaStatement>
	TransformDropSchema(postgres::DropStmt *stmt);
	//! Transform a Postgres T_CopyStmt node into a CopyStatement
	std::unique_ptr<CopyStatement> TransformCopy(postgres::Node *node);
	//! Transform a Postgres T_TransactionStmt node into a TransactionStatement
	std::unique_ptr<TransactionStatement>
	TransformTransaction(postgres::Node *node);
	//! Transform a Postgres T_DeleteStatement node into a DeleteStatement
	std::unique_ptr<DeleteStatement> TransformDelete(postgres::Node *node);
	//! Transform a Postgres T_UpdateStmt node into a UpdateStatement
	std::unique_ptr<UpdateStatement> TransformUpdate(postgres::Node *node);

	//===--------------------------------------------------------------------===//
	// Expression Transform
	//===--------------------------------------------------------------------===//
	//! Transform a Postgres boolean expression into an Expression
	std::unique_ptr<Expression> TransformBoolExpr(postgres::BoolExpr *root);
	//! Transform a Postgres case expression into an Expression
	std::unique_ptr<Expression> TransformCase(postgres::CaseExpr *root);
	//! Transform a Postgres type cast into an Expression
	std::unique_ptr<Expression> TransformTypeCast(postgres::TypeCast *root);
	//! Transform a Postgres coalesce into an Expression
	std::unique_ptr<Expression> TransformCoalesce(postgres::A_Expr *root);
	//! Transform a Postgres column reference into an Expression
	std::unique_ptr<Expression> TransformColumnRef(postgres::ColumnRef *root);
	//! Transform a Postgres constant value into an Expression
	std::unique_ptr<Expression> TransformValue(postgres::value val);
	//! Transform a Postgres operator into an Expression
	std::unique_ptr<Expression> TransformAExpr(postgres::A_Expr *root);
	//! Transform a Postgres abstract expression into an Expression
	std::unique_ptr<Expression> TransformExpression(postgres::Node *node);
	//! Transform a Postgres function call into an Expression
	std::unique_ptr<Expression> TransformFuncCall(postgres::FuncCall *root);
	//! Transform a Postgres constant value into an Expression
	std::unique_ptr<Expression> TransformConstant(postgres::A_Const *c);

	std::unique_ptr<Expression> TransformResTarget(postgres::ResTarget *root);
	std::unique_ptr<Expression> TransformNullTest(postgres::NullTest *root);

	std::unique_ptr<Expression> TransformSubquery(postgres::SubLink *root);
	//===--------------------------------------------------------------------===//
	// Constraints transform
	//===--------------------------------------------------------------------===//
	std::unique_ptr<Constraint> TransformConstraint(postgres::ListCell *cell);

	std::unique_ptr<Constraint> TransformConstraint(postgres::ListCell *cell,
	                                                ColumnDefinition column,
	                                                size_t index);

	//===--------------------------------------------------------------------===//
	// Helpers
	//===--------------------------------------------------------------------===//
	std::string TransformAlias(postgres::Alias *root);
	void TransformCTE(postgres::WithClause *de_with_clause,
	                  SelectStatement &select);
	//===--------------------------------------------------------------------===//
	// TableRef transform
	//===--------------------------------------------------------------------===//
	//! Transform a Postgres node into a TableRef
	std::unique_ptr<TableRef> TransformTableRefNode(postgres::Node *node);
	//! Transform a Postgres FROM clause into an Expression
	std::unique_ptr<TableRef> TransformFrom(postgres::List *root);
	//! Transform a Postgres table reference into an Expression
	std::unique_ptr<TableRef> TransformRangeVar(postgres::RangeVar *root);
	//! Transform a Postgres table-producing function into a TableRef
	std::unique_ptr<TableRef>
	TransformRangeFunction(postgres::RangeFunction *root);
	//! Transform a Postgres join node into a TableRef
	std::unique_ptr<TableRef> TransformJoin(postgres::JoinExpr *root);
	//! Transform a table producing subquery into a TableRef
	std::unique_ptr<TableRef>
	TransformRangeSubselect(postgres::RangeSubselect *root);

	//! Transform a Postgres TypeName string into a TypeId
	TypeId TransformStringToTypeId(char *str);

	//! Transform a Postgres GROUP BY expression into a list of Expression
	bool TransformGroupBy(postgres::List *group,
	                      std::vector<std::unique_ptr<Expression>> &result);
	//! Transform a Postgres ORDER BY expression into an OrderByDescription
	bool TransformOrderBy(postgres::List *order, OrderByDescription &result);

	//! Transform a Postgres SELECT clause into a list of Expression
	bool
	TransformExpressionList(postgres::List *list,
	                        std::vector<std::unique_ptr<Expression>> &result);

  private:
};

} // namespace duckdb
