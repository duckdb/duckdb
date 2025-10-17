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

void EraseDuplicateCTE(unique_ptr<QueryNode> &node, const string &ctename) {
	if (node->type != QueryNodeType::CTE_NODE) {
		// not a CTE
		return;
	}
	auto &cte_node = node->Cast<CTENode>();
	if (cte_node.ctename == ctename) {
		// duplicate CTE - erase this CTE node
		node = std::move(cte_node.child);
		EraseDuplicateCTE(node, ctename);
	} else {
		// not a duplicate - recurse into child
		EraseDuplicateCTE(cte_node.child, ctename);
	}
}

void CTENode::Serialize(Serializer &serializer) const {
	if (materialized != CTEMaterialize::CTE_MATERIALIZE_ALWAYS) {
		// for non-materialized CTEs - don't serialize CTENode
		// older DuckDB versions only expect a CTENode to be there for materialized CTEs
		child->Serialize(serializer);
		return;
	}
	auto child_copy = child->Copy();
	EraseDuplicateCTE(child_copy, ctename);

	QueryNode::Serialize(serializer);
	serializer.WritePropertyWithDefault<string>(200, "cte_name", ctename);
	serializer.WritePropertyWithDefault<unique_ptr<QueryNode>>(201, "query", query);
	serializer.WritePropertyWithDefault<unique_ptr<QueryNode>>(202, "child", child_copy);
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
