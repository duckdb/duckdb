#include "duckdb/parser/query_node/cte_node.hpp"
#include "duckdb/common/serializer/serializer.hpp"
#include "duckdb/common/serializer/deserializer.hpp"
#include "duckdb/parser/statement/select_statement.hpp"

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
