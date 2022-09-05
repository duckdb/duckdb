#include "duckdb/parser/query_node/recursive_cte_node.hpp"
#include "duckdb/common/field_writer.hpp"

namespace duckdb {

string RecursiveCTENode::ToString() const {
	string result;
	result += "(" + left->ToString() + ")";
	result += " UNION ";
	if (union_all) {
		result += " ALL ";
	}
	result += "(" + right->ToString() + ")";
	return result;
}

bool RecursiveCTENode::Equals(const QueryNode *other_p) const {
	if (!QueryNode::Equals(other_p)) {
		return false;
	}
	if (this == other_p) {
		return true;
	}
	auto other = (RecursiveCTENode *)other_p;

	if (other->union_all != union_all) {
		return false;
	}
	if (!left->Equals(other->left.get())) {
		return false;
	}
	if (!right->Equals(other->right.get())) {
		return false;
	}
	return true;
}

unique_ptr<QueryNode> RecursiveCTENode::Copy() const {
	auto result = make_unique<RecursiveCTENode>();
	result->ctename = ctename;
	result->union_all = union_all;
	result->left = left->Copy();
	result->right = right->Copy();
	result->aliases = aliases;
	this->CopyProperties(*result);
	return move(result);
}

void RecursiveCTENode::Serialize(FieldWriter &writer) const {
	writer.WriteString(ctename);
	writer.WriteField<bool>(union_all);
	writer.WriteSerializable(*left);
	writer.WriteSerializable(*right);
	writer.WriteList<string>(aliases);
}

unique_ptr<QueryNode> RecursiveCTENode::Deserialize(FieldReader &reader) {
	auto result = make_unique<RecursiveCTENode>();
	result->ctename = reader.ReadRequired<string>();
	result->union_all = reader.ReadRequired<bool>();
	result->left = reader.ReadRequiredSerializable<QueryNode>();
	result->right = reader.ReadRequiredSerializable<QueryNode>();
	result->aliases = reader.ReadRequiredList<string>();
	return move(result);
}

} // namespace duckdb
