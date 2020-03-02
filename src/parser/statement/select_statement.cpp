#include "duckdb/parser/statement/select_statement.hpp"

#include "duckdb/common/assert.hpp"
#include "duckdb/common/serializer.hpp"

using namespace duckdb;
using namespace std;

unique_ptr<SelectStatement> SelectStatement::Copy() {
	auto result = make_unique<SelectStatement>();
	for (auto &cte : cte_map) {
		result->cte_map[cte.first] = cte.second->Copy();
	}
	result->node = node->Copy();
	return result;
}

void SelectStatement::Serialize(Serializer &serializer) {
	// with clauses
	assert(cte_map.size() <= numeric_limits<uint32_t>::max());
	serializer.Write<uint32_t>((uint32_t)cte_map.size());
	for (auto &cte : cte_map) {
		serializer.WriteString(cte.first);
		cte.second->Serialize(serializer);
	}
	node->Serialize(serializer);
}

unique_ptr<SelectStatement> SelectStatement::Deserialize(Deserializer &source) {
	auto result = make_unique<SelectStatement>();
	auto cte_count = source.Read<uint32_t>();
	for (idx_t i = 0; i < cte_count; i++) {
		auto name = source.Read<string>();
		auto statement = QueryNode::Deserialize(source);
		result->cte_map[name] = move(statement);
	}
	result->node = QueryNode::Deserialize(source);
	return result;
}

bool SelectStatement::Equals(const SQLStatement *other_) const {
	if (type != other_->type) {
		return false;
	}
	auto other = (SelectStatement *)other_;
	// WITH clauses (CTEs)
	if (cte_map.size() != other->cte_map.size()) {
		return false;
	}
	for (auto &entry : cte_map) {
		auto other_entry = other->cte_map.find(entry.first);
		if (other_entry == other->cte_map.end()) {
			return false;
		}
		if (!entry.second->Equals(other_entry->second.get())) {
			return false;
		}
	}
	return node->Equals(other->node.get());
}
