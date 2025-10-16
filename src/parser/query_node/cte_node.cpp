#include "duckdb/parser/query_node/cte_node.hpp"
#include "duckdb/common/serializer/serializer.hpp"
#include "duckdb/common/serializer/deserializer.hpp"
#include "duckdb/parser/statement/select_statement.hpp"

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
	result->materialized = materialized;
	this->CopyProperties(*result);
	return std::move(result);
}

// TEMPORARY BUGFIX WARNING - none of this code should make it into main - this is a temporary work-around for v1.4
// TEMPORARY BUGFIX START
// the below code fixes backwards and forwards compatibility of CTEs with the somewhat broken version of CTEs in v1.4
// all of this code has been made obsolete with the CTE binding rework
void QueryNode::ExtractCTENodes(unique_ptr<QueryNode> &query_node) {
	if (query_node->cte_map.map.empty()) {
		return;
	}
	vector<unique_ptr<CTENode>> materialized_ctes;
	for (auto &cte : query_node->cte_map.map) {
		auto &cte_entry = cte.second;
		auto mat_cte = make_uniq<CTENode>();
		mat_cte->ctename = cte.first;
		mat_cte->query = cte_entry->query->node->Copy();
		mat_cte->aliases = cte_entry->aliases;
		mat_cte->materialized = cte_entry->materialized;
		materialized_ctes.push_back(std::move(mat_cte));
	}

	auto root = std::move(query_node);
	while (!materialized_ctes.empty()) {
		unique_ptr<CTENode> node_result;
		node_result = std::move(materialized_ctes.back());
		node_result->cte_map = root->cte_map.Copy();
		node_result->child = std::move(root);
		root = std::move(node_result);
		materialized_ctes.pop_back();
	}
	query_node = std::move(root);
}

void CTENode::Serialize(Serializer &serializer) const {
	if (materialized != CTEMaterialize::CTE_MATERIALIZE_ALWAYS) {
		// for non-materialized CTEs - don't serialize CTENode
		// older DuckDB versions only expect a CTENode to be there for materialized CTEs
		child->Serialize(serializer);
		return;
	}
	QueryNode::Serialize(serializer);
	serializer.WritePropertyWithDefault<string>(200, "cte_name", ctename);
	serializer.WritePropertyWithDefault<unique_ptr<QueryNode>>(201, "query", query);
	serializer.WritePropertyWithDefault<unique_ptr<QueryNode>>(202, "child", child);
	serializer.WritePropertyWithDefault<vector<string>>(203, "aliases", aliases);
}

unique_ptr<QueryNode> CTENode::Deserialize(Deserializer &deserializer) {
	auto result = duckdb::unique_ptr<CTENode>(new CTENode());
	deserializer.ReadPropertyWithDefault<string>(200, "cte_name", result->ctename);
	deserializer.ReadPropertyWithDefault<unique_ptr<QueryNode>>(201, "query", result->query);
	deserializer.ReadPropertyWithDefault<unique_ptr<QueryNode>>(202, "child", result->child);
	deserializer.ReadPropertyWithDefault<vector<string>>(203, "aliases", result->aliases);
	// v1.4.0 and v1.4.1 wrote this property - deserialize it for BC with these versions
	deserializer.ReadPropertyWithExplicitDefault<CTEMaterialize>(204, "materialized", result->materialized,
	                                                             CTEMaterialize::CTE_MATERIALIZE_DEFAULT);
	return std::move(result->child);
}
// TEMPORARY BUGFIX WARNING - none of this code should make it into main - this is a temporary work-around for v1.4
// TEMPORARY BUGFIX END

} // namespace duckdb
