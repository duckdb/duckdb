#include "planner/operator/logical_prepare.hpp"

#include "planner/operator/logical_get.hpp"
#include "planner/operator/logical_insert.hpp"

using namespace duckdb;
using namespace std;

class PrepareTableVisitor : public LogicalOperatorVisitor {
public:
	PrepareTableVisitor(unordered_set<TableCatalogEntry *> &table_list) : table_list(table_list) {
	}

	void VisitOperator(LogicalOperator &op) {
		switch (op.type) {
		case LogicalOperatorType::GET:
			Visit((LogicalGet &)op);
			break;
		case LogicalOperatorType::INSERT:
			Visit((LogicalInsert &)op);
			break;
		default:
			// for the operators we do not handle explicitly, we just visit the children
			LogicalOperatorVisitor::VisitOperator(op);
			break;
		}
	}

protected:
	using SQLNodeVisitor::Visit;
	void Visit(LogicalGet &op) {
		if (op.table) {
			table_list.insert(op.table);
		}
	}

	void Visit(LogicalInsert &op) {
		if (op.table) {
			table_list.insert(op.table);
		}
	}

private:
	unordered_set<TableCatalogEntry *> &table_list;
};

void LogicalPrepare::GetTableBindings(unordered_set<TableCatalogEntry *> &result_list) {
	PrepareTableVisitor ptv(result_list);
	ptv.VisitOperator(*children[0].get());
}
