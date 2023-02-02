#include "duckdb/parser/visitor/parse_tree_visitor.hpp"

#include "duckdb/parser/sql_statement.hpp"
#include "duckdb/parser/query_node.hpp"

namespace duckdb {

void ParseTreeVisitor::VisitStatement(SQLStatement &stmt) {
	switch (stmt.type) {
	case StatementType::SELECT_STATEMENT:
		VisitSelectStatement((SelectStatement &)stmt);
		break;
	default:
		throw NotImplementedException("Visitor not implemented for SQLStatement!");
	};
}
void ParseTreeVisitor::VisitQueryNode(QueryNode &node) {
	switch (node.type) {
	case QueryNodeType::SELECT_NODE:
		VisitSelectNode((SelectNode &)node);
		break;
	case QueryNodeType::SET_OPERATION_NODE:
		VisitSetOperationNode((SetOperationNode &)node);
		break;
	case QueryNodeType::RECURSIVE_CTE_NODE:
		VisitRecursiveCTENode((RecursiveCTENode &)node);
		break;
	case QueryNodeType::BOUND_SUBQUERY_NODE:
		VisitBoundSubqueryNode((BoundSubqueryNode &)node);
		break;

	default:
		throw NotImplementedException("Visitor not implemented for QueryNode!");
	};
}

// Statements
void ParseTreeVisitor::VisitSelectStatement(SelectStatement &stmt) {
	VisitQueryNode(*stmt.node);
};
void ParseTreeVisitor::VisitCreateTableStatement(CreateTableStatement &stmt) {
	throw NotImplementedException("Visitor not implemented for CreateTableStatement!");
};
void ParseTreeVisitor::VisitCreateIndexStatement(CreateIndexStatement &stmt) {
	throw NotImplementedException("Visitor not implemented for CreateIndexStatement!");
};
void ParseTreeVisitor::VisitCreateSchemaStatement(CreateSchemaStatement &stmt) {
	throw NotImplementedException("Visitor not implemented for CreateSchemaStatement!");
};
void ParseTreeVisitor::VisitCreateViewStatement(CreateViewStatement &stmt) {
	throw NotImplementedException("Visitor not implemented for CreateViewStatement!");
};
void ParseTreeVisitor::VisitCreateSequenceStatement(CreateSequenceStatement &stmt) {
	throw NotImplementedException("Visitor not implemented for CreateSequenceStatement!");
};
void ParseTreeVisitor::VisitCreatePreparedStatement(CreatePreparedStatement &stmt) {
	throw NotImplementedException("Visitor not implemented for CreatePreparedStatement!");
};
void ParseTreeVisitor::VisitCopyStatement(CopyStatement &stmt) {
	throw NotImplementedException("Visitor not implemented for CopyStatement!");
};
void ParseTreeVisitor::VisitDeleteStatement(DeleteStatement &stmt) {
	throw NotImplementedException("Visitor not implemented for DeleteStatement!");
};
void ParseTreeVisitor::VisitDropStatement(DropStatement &stmt) {
	throw NotImplementedException("Visitor not implemented for DropStatement!");
};
void ParseTreeVisitor::VisitExecuteStatement(ExecuteStatement &stmt) {
	throw NotImplementedException("Visitor not implemented for ExecuteStatement!");
};
void ParseTreeVisitor::VisitExportStatement(ExportStatement &stmt) {
	throw NotImplementedException("Visitor not implemented for ExportStatement!");
};
void ParseTreeVisitor::VisitInsertStatement(InsertStatement &stmt) {
	throw NotImplementedException("Visitor not implemented for InsertStatement!");
};
void ParseTreeVisitor::VisitPragmaStatement(PragmaStatement &stmt) {
	throw NotImplementedException("Visitor not implemented for PragmaStatement!");
};
void ParseTreeVisitor::VisitPrepareStatement(PrepareStatement &stmt) {
	throw NotImplementedException("Visitor not implemented for PrepareStatement!");
};
void ParseTreeVisitor::VisitRelationStatement(RelationStatement &stmt) {
	throw NotImplementedException("Visitor not implemented for RelationStatement!");
};

// Nodes
void ParseTreeVisitor::VisitSelectNode(SelectNode &node) {
	throw NotImplementedException("Visitor not implemented for SelectNode!");
};
void ParseTreeVisitor::VisitSetOperationNode(SetOperationNode &node) {
	throw NotImplementedException("Visitor not implemented for SetOperationNode!");
};
void ParseTreeVisitor::VisitRecursiveCTENode(RecursiveCTENode &node) {
	throw NotImplementedException("Visitor not implemented for RecursiveCTENode!");
};
void ParseTreeVisitor::VisitBoundSubqueryNode(BoundSubqueryNode &node) {
	throw NotImplementedException("Visitor not implemented for BoundSubqueryNode!");
};

} // namespace duckdb
