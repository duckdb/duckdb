#include "duckdb/parser/query_node/cte_node.hpp"

namespace duckdb {

string CTENode::ToString() const {
	throw InternalException("CTENode is a legacy type");
}

bool CTENode::Equals(const QueryNode *other_p) const {
	throw InternalException("CTENode is a legacy type");
}

unique_ptr<QueryNode> CTENode::Copy() const {
	throw InternalException("CTENode is a legacy type");
}

} // namespace duckdb
