#include "duckdb/parser/statement/select_statement.hpp"

#include "duckdb/common/serializer.hpp"
#include "duckdb/common/limits.hpp"

namespace duckdb {

unique_ptr<SQLStatement> SelectStatement::Copy() const {
	auto result = make_unique<SelectStatement>();
	result->node = node->Copy();
	return move(result);
}

void SelectStatement::Serialize(Serializer &serializer) {
	node->Serialize(serializer);
}

unique_ptr<SelectStatement> SelectStatement::Deserialize(Deserializer &source) {
	auto result = make_unique<SelectStatement>();
	result->node = QueryNode::Deserialize(source);
	return result;
}

bool SelectStatement::Equals(const SQLStatement *other_) const {
	if (type != other_->type) {
		return false;
	}
	auto other = (SelectStatement *)other_;
	return node->Equals(other->node.get());
}

} // namespace duckdb
