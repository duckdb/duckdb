#include "duckdb/main/relation/filter_relation.hpp"
#include "duckdb/planner/expression_binder/where_binder.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/planner/operator/logical_filter.hpp"
#include "duckdb/planner/bound_statement.hpp"

namespace duckdb {

FilterRelation::FilterRelation(shared_ptr<Relation> child_p, unique_ptr<ParsedExpression> condition_p) :
	Relation(child_p->context, RelationType::FILTER), condition(move(condition_p)), child(move(child_p)) {
	vector<ColumnDefinition> dummy_columns;
	context.TryBindRelation(*this, dummy_columns);
}

BoundStatement FilterRelation::Bind(Binder &binder) {
	// bind the root node
	BoundStatement result = child->Bind(binder);
	// now bind the condition
	auto copied_condition = condition->Copy();
	WhereBinder where_binder(binder, context);
	auto condition = where_binder.Bind(copied_condition);

	auto filter = make_unique<LogicalFilter>(move(condition));
	filter->AddChild(move(result.plan));
	result.plan = move(filter);
	return result;
}

const vector<ColumnDefinition> &FilterRelation::Columns() {
	return child->Columns();
}

string FilterRelation::ToString(idx_t depth) {
	string str = RenderWhitespace(depth) + "Filter [" + condition->ToString() + "]\n";
	return str + child->ToString(depth + 1);;
}

}