#include "duckdb/parser/query_node/cte_node.hpp"
#include "duckdb/common/serializer/serializer.hpp"
#include "duckdb/common/serializer/deserializer.hpp"

namespace duckdb {

string CTENode::ToString() const {
	string result;
	result += child->ToString();
	return result;
}

bool CTENode::Equals(const QueryNode *other_p) const {
	if (!QueryNode::Equals(other_p)) {
		return false;
	}
	if (this == other_p) {
		return true;
	}
	auto &other = other_p->Cast<CTENode>();

	if (!query->Equals(other.query.get())) {
		return false;
	}
	if (!child->Equals(other.child.get())) {
		return false;
	}
	return true;
}

unique_ptr<QueryNode> CTENode::Copy() const {
	auto result = make_uniq<CTENode>();
	result->ctename = ctename;
	result->query = query->Copy();
	result->child = child->Copy();
	result->aliases = aliases;
	this->CopyProperties(*result);
	return std::move(result);
}

} // namespace duckdb
