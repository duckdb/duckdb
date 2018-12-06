//===----------------------------------------------------------------------===//
//                         DuckDB
//
// parser/sql_node_visitor.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "parser/constraint.hpp"
#include "parser/expression.hpp"
#include "parser/sql_statement.hpp"
#include "parser/tableref.hpp"
#include "parser/tokens.hpp"

namespace duckdb {

//! The SQLNodeVisitor is an abstract base class that implements the Visitor
//! pattern on Expression and SQLStatement. It will visit nodes
//! recursively and call the Visit expression corresponding to the expression
//! visited.
class SQLNodeVisitor {
public:
	virtual ~SQLNodeVisitor(){};

	virtual unique_ptr<SQLStatement> Visit(CopyStatement &) {
		return nullptr;
	};
	virtual unique_ptr<SQLStatement> Visit(AlterTableStatement &) {
		return nullptr;
	};
	virtual unique_ptr<SQLStatement> Visit(CreateIndexStatement &) {
		return nullptr;
	};
	virtual unique_ptr<SQLStatement> Visit(CreateSchemaStatement &) {
		return nullptr;
	};
	virtual unique_ptr<SQLStatement> Visit(CreateTableStatement &) {
		return nullptr;
	};
	virtual unique_ptr<SQLStatement> Visit(DeleteStatement &) {
		return nullptr;
	};
	virtual unique_ptr<SQLStatement> Visit(DropSchemaStatement &) {
		return nullptr;
	};
	virtual unique_ptr<SQLStatement> Visit(DropTableStatement &) {
		return nullptr;
	};
	virtual unique_ptr<SQLStatement> Visit(DropIndexStatement &) {
		return nullptr;
	};
	virtual unique_ptr<SQLStatement> Visit(InsertStatement &) {
		return nullptr;
	};
	virtual unique_ptr<SQLStatement> Visit(SelectStatement &) {
		return nullptr;
	};
	virtual unique_ptr<SQLStatement> Visit(TransactionStatement &) {
		return nullptr;
	};
	virtual unique_ptr<SQLStatement> Visit(UpdateStatement &) {
		return nullptr;
	};

	virtual void Visit(SelectNode &node) {
	}
	virtual void Visit(SetOperationNode &node) {
	}

	virtual unique_ptr<Expression> Visit(AggregateExpression &expr);
	virtual unique_ptr<Expression> Visit(CaseExpression &expr);
	virtual unique_ptr<Expression> Visit(CastExpression &expr);
	virtual unique_ptr<Expression> Visit(ColumnRefExpression &expr);
	virtual unique_ptr<Expression> Visit(ComparisonExpression &expr);
	virtual unique_ptr<Expression> Visit(ConjunctionExpression &expr);
	virtual unique_ptr<Expression> Visit(ConstantExpression &expr);
	virtual unique_ptr<Expression> Visit(DefaultExpression &expr);
	virtual unique_ptr<Expression> Visit(FunctionExpression &expr);
	virtual unique_ptr<Expression> Visit(GroupRefExpression &expr);
	virtual unique_ptr<Expression> Visit(OperatorExpression &expr);
	virtual unique_ptr<Expression> Visit(StarExpression &expr);
	virtual unique_ptr<Expression> Visit(SubqueryExpression &expr);
	virtual unique_ptr<Expression> Visit(WindowExpression &expr);

	virtual unique_ptr<Constraint> Visit(NotNullConstraint &expr) {
		return nullptr;
	};
	virtual unique_ptr<Constraint> Visit(CheckConstraint &expr);
	virtual unique_ptr<Constraint> Visit(ParsedConstraint &expr) {
		return nullptr;
	};

	virtual unique_ptr<TableRef> Visit(BaseTableRef &expr) {
		return nullptr;
	};
	virtual unique_ptr<TableRef> Visit(CrossProductRef &expr);
	virtual unique_ptr<TableRef> Visit(JoinRef &expr);
	virtual unique_ptr<TableRef> Visit(SubqueryRef &expr) {
		return nullptr;
	};
	virtual unique_ptr<TableRef> Visit(TableFunction &expr) {
		return nullptr;
	};

	template <class T> void AcceptChild(unique_ptr<T> *accept) {
		assert(accept);
		auto accept_res = (*accept)->Accept(this);
		if (accept_res) {
			(*accept) = unique_ptr<T>((T *)accept_res.release());
		}
	}
};
} // namespace duckdb
