#include "duckdb/main/relation/limit_relation.hpp"
#include "duckdb/planner/bound_statement.hpp"
#include "duckdb/planner/operator/logical_limit.hpp"

namespace duckdb {

LimitRelation::LimitRelation(shared_ptr<Relation> child_p, int64_t limit, int64_t offset) :
	Relation(child_p->context, RelationType::PROJECTION), limit(limit), offset(offset), child(move(child_p)) {
}

BoundStatement LimitRelation::Bind(Binder &binder) {
	BoundStatement result = child->Bind(binder);

	auto limit_plan = make_unique<LogicalLimit>(limit, offset);
	limit_plan->AddChild(move(result.plan));
	result.plan = move(limit_plan);
	return result;
}

const vector<ColumnDefinition> &LimitRelation::Columns() {
	return child->Columns();
}

string LimitRelation::ToString(idx_t depth) {
	string str = RenderWhitespace(depth) + "Limit " + std::to_string(limit);
	if (offset > 0) {
		str += " Offset " + std::to_string(offset);
	}
	str += "\n";
	return str + child->ToString(depth + 1);;
}

}