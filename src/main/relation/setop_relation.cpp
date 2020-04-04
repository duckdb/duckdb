#include "duckdb/main/relation/setop_relation.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/planner/binder.hpp"
#include "duckdb/planner/operator/logical_set_operation.hpp"

namespace duckdb {

SetOpRelation::SetOpRelation(shared_ptr<Relation> left_p, shared_ptr<Relation> right_p, LogicalOperatorType setop_type_p) :
	Relation(left_p->context, RelationType::SET_OPERATION), setop_type(setop_type_p) {
	if (&left_p->context != &right_p->context) {
		throw Exception("Cannot combine LEFT and RIGHT relations of different connections!");
	}
	left = left_p->Project("*");
	right = right_p->Project("*");
	assert(setop_type == LogicalOperatorType::UNION || setop_type == LogicalOperatorType::EXCEPT ||
               setop_type == LogicalOperatorType::INTERSECT);
	vector<ColumnDefinition> dummy_columns;
	context.TryBindRelation(*this, dummy_columns);
}

BoundStatement SetOpRelation::Bind(Binder &binder) {
	Binder left_binder(context, &binder), right_binder(context, &binder);
	auto lresult = left->Bind(left_binder);
	auto rresult = right->Bind(right_binder);

	assert(lresult.names.size() == lresult.types.size());
	assert(rresult.names.size() == rresult.types.size());
	if (lresult.names.size() != rresult.names.size()) {
		throw Exception("LEFT and RIGHT relations of set operation must have same number of children!");
	}
	vector<ColumnDefinition> columns;
	for(idx_t i = 0; i < lresult.names.size(); i++) {
		if (lresult.types[i] != rresult.types[i]) {
			throw Exception("LEFT and RIGHT relations of set operation must have same types of children!");
		}
		columns.push_back(ColumnDefinition(lresult.names[i], lresult.types[i]));
	}

	auto union_index = binder.GenerateTableIndex();
	auto setop = make_unique<LogicalSetOperation>(union_index, lresult.names.size(), move(lresult.plan), move(rresult.plan), setop_type);

	// add the nodes of the projection to the current bind context so they can be bound
	binder.bind_context.AddGenericBinding(union_index, "setop", columns);

	lresult.plan = move(setop);
	return lresult;
}

const vector<ColumnDefinition> &SetOpRelation::Columns() {
	return left->Columns();
}

string SetOpRelation::ToString(idx_t depth) {
	string str = RenderWhitespace(depth);
	switch(setop_type) {
	case LogicalOperatorType::UNION:
		str += "Union";
		break;
	case LogicalOperatorType::EXCEPT:
		str += "Except";
		break;
	case LogicalOperatorType::INTERSECT:
		str += "Intersect";
		break;
	default:
		throw Exception("Unknown setop type");
	}
	return str + "\n" + left->ToString(depth + 1) + right->ToString(depth + 1);
}

}