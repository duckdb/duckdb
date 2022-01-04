#include "duckdb/parser/statement/select_statement.hpp"

#include "duckdb/common/serializer.hpp"

namespace duckdb {

unique_ptr<SQLStatement> SelectStatement::Copy() const {
	auto result = make_unique<SelectStatement>();
	result->node = node->Copy();
	return result;
}

void SelectStatement::Serialize(Serializer &serializer) {
	node->Serialize(serializer);
}

unique_ptr<SelectStatement> SelectStatement::Deserialize(Deserializer &source) {
	auto result = make_unique<SelectStatement>();
	result->node = QueryNode::Deserialize(source);
	return result;
}

bool SelectStatement::Equals(const SQLStatement *other_p) const {
	if (type != other_p->type) {
		return false;
	}
	auto other = (SelectStatement *)other_p;
	return node->Equals(other->node.get());
}

} // namespace duckdb
