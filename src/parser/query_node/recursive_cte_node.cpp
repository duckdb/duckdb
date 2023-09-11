#include "duckdb/parser/query_node/recursive_cte_node.hpp"
#include "duckdb/common/serializer/serializer.hpp"
#include "duckdb/common/serializer/deserializer.hpp"

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
	auto &other = other_p->Cast<RecursiveCTENode>();

	if (other.union_all != union_all) {
		return false;
	}
	if (!left->Equals(other.left.get())) {
		return false;
	}
	if (!right->Equals(other.right.get())) {
		return false;
	}
	return true;
}

unique_ptr<QueryNode> RecursiveCTENode::Copy() const {
	auto result = make_uniq<RecursiveCTENode>();
	result->ctename = ctename;
	result->union_all = union_all;
	result->left = left->Copy();
	result->right = right->Copy();
	result->aliases = aliases;
	this->CopyProperties(*result);
	return std::move(result);
}

} // namespace duckdb
