#include "duckdb/main/relation/distinct_relation.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/planner/bound_statement.hpp"
#include "duckdb/planner/operator/logical_distinct.hpp"

namespace duckdb {

DistinctRelation::DistinctRelation(shared_ptr<Relation> child_p) :
	Relation(child_p->context, RelationType::DISTINCT), child(move(child_p)) {
	vector<ColumnDefinition> dummy_columns;
	context.TryBindRelation(*this, dummy_columns);
}

BoundStatement DistinctRelation::Bind(Binder &binder) {
	// bind the root node
	BoundStatement result = child->Bind(binder);

	auto distinct = make_unique<LogicalDistinct>();
	distinct->AddChild(move(result.plan));
	result.plan = move(distinct);
	return result;
}

const vector<ColumnDefinition> &DistinctRelation::Columns() {
	return child->Columns();
}

string DistinctRelation::ToString(idx_t depth) {
	string str = RenderWhitespace(depth) + "Distinct\n";
	return str + child->ToString(depth + 1);;
}

}