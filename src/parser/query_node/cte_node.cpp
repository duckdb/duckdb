#include "duckdb/parser/query_node/cte_node.hpp"
#include "duckdb/common/field_writer.hpp"
#include "duckdb/common/serializer/format_serializer.hpp"
#include "duckdb/common/serializer/format_deserializer.hpp"

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

void CTENode::Serialize(FieldWriter &writer) const {
	writer.WriteString(ctename);
	writer.WriteSerializable(*query);
	writer.WriteSerializable(*child);
	writer.WriteList<string>(aliases);
}

unique_ptr<QueryNode> CTENode::Deserialize(FieldReader &reader) {
	auto result = make_uniq<CTENode>();
	result->ctename = reader.ReadRequired<string>();
	result->query = reader.ReadRequiredSerializable<QueryNode>();
	result->child = reader.ReadRequiredSerializable<QueryNode>();
	result->aliases = reader.ReadRequiredList<string>();
	return std::move(result);
}

} // namespace duckdb
