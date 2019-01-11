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

	virtual void Visit(CopyStatement &) {
	}
	virtual void Visit(AlterTableStatement &) {
	}
	virtual void Visit(CreateIndexStatement &) {
	}
	virtual void Visit(CreateSchemaStatement &) {
	}
	virtual void Visit(CreateTableStatement &) {
	}
	virtual void Visit(DeleteStatement &) {
	}
	virtual void Visit(DropSchemaStatement &) {
	}
	virtual void Visit(DropTableStatement &) {
	}
	virtual void Visit(DropIndexStatement &) {
	}
	virtual void Visit(InsertStatement &) {
	}
	virtual void Visit(SelectStatement &) {
	}
	virtual void Visit(TransactionStatement &) {
	}
	virtual void Visit(UpdateStatement &) {
	}

	virtual void Visit(SelectNode &node) {
	}
	virtual void Visit(SetOperationNode &node) {
	}

	virtual void Visit(AggregateExpression &expr);
	virtual void Visit(BoundExpression &expr);
	virtual void Visit(CaseExpression &expr);
	virtual void Visit(CastExpression &expr);
	virtual void Visit(CommonSubExpression &expr);
	virtual void Visit(ColumnRefExpression &expr);
	virtual void Visit(ComparisonExpression &expr);
	virtual void Visit(ConjunctionExpression &expr);
	virtual void Visit(ConstantExpression &expr);
	virtual void Visit(DefaultExpression &expr);
	virtual void Visit(FunctionExpression &expr);
	virtual void Visit(OperatorExpression &expr);
	virtual void Visit(StarExpression &expr);
	virtual void Visit(SubqueryExpression &expr);
	virtual void Visit(WindowExpression &expr);

	virtual void Visit(NotNullConstraint &expr) {
	}
	virtual void Visit(CheckConstraint &expr);
	virtual void Visit(ParsedConstraint &expr) {
	}

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
