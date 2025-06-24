#include "duckdb/main/relation/setop_relation.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/parser/query_node/set_operation_node.hpp"
#include "duckdb/parser/result_modifier.hpp"

namespace duckdb {

SetOpRelation::SetOpRelation(shared_ptr<Relation> left_p, shared_ptr<Relation> right_p, SetOperationType setop_type_p,
                             bool setop_all)
    : Relation(left_p->context, RelationType::SET_OPERATION_RELATION), left(std::move(left_p)),
      right(std::move(right_p)), setop_type(setop_type_p), setop_all(setop_all) {
	if (left->context->GetContext() != right->context->GetContext()) {
		throw InvalidInputException("Cannot combine LEFT and RIGHT relations of different connections!");
	}
	TryBindRelation(columns);
}

unique_ptr<QueryNode> SetOpRelation::GetQueryNode() {
	auto result = make_uniq<SetOperationNode>();
	if (!setop_all) {
		result->modifiers.push_back(make_uniq<DistinctModifier>());
	}
	result->left = left->GetQueryNode();
	result->right = right->GetQueryNode();
	result->setop_type = setop_type;
	result->setop_all = setop_all;
	return std::move(result);
}

string SetOpRelation::GetAlias() {
	return left->GetAlias();
}

const vector<ColumnDefinition> &SetOpRelation::Columns() {
	return this->columns;
}

string SetOpRelation::ToString(idx_t depth) {
	string str = RenderWhitespace(depth);
	switch (setop_type) {
	case SetOperationType::UNION:
		str += "Union";
		break;
	case SetOperationType::EXCEPT:
		str += "Except";
		break;
	case SetOperationType::INTERSECT:
		str += "Intersect";
		break;
	default:
		throw InternalException("Unknown setop type");
	}
	return str + "\n" + left->ToString(depth + 1) + right->ToString(depth + 1);
}

} // namespace duckdb
