#include "duckdb/parser/query_node.hpp"

#include "duckdb/parser/query_node/select_node.hpp"
#include "duckdb/parser/query_node/set_operation_node.hpp"
#include "duckdb/parser/query_node/recursive_cte_node.hpp"
#include "duckdb/parser/query_node/cte_node.hpp"
#include "duckdb/common/limits.hpp"
#include "duckdb/common/serializer/serializer.hpp"
#include "duckdb/common/serializer/deserializer.hpp"

namespace duckdb {

CommonTableExpressionMap::CommonTableExpressionMap() {
}

CommonTableExpressionMap CommonTableExpressionMap::Copy() const {
	CommonTableExpressionMap res;
	res.map.resize(this->map.size());
	for (auto &kv_idx : this->map_idx) {
		auto kv_info = make_uniq<CommonTableExpressionInfo>();
		auto &kv = this->map.at(kv_idx.second);
		for (auto &al : kv->aliases) {
			kv_info->aliases.push_back(al);
		}
		kv_info->query = unique_ptr_cast<SQLStatement, SelectStatement>(kv->query->Copy());
		kv_info->materialized = kv->materialized;
		res.map[kv_idx.second] = std::move(kv_info);
		res.map_idx[kv_idx.first] = kv_idx.second;
	}

	return res;
}

string CommonTableExpressionMap::ToString() const {
	if (map.empty()) {
		return string();
	}
	// check if there are any recursive CTEs
	bool has_recursive = false;
	for (auto &kv : map) {
		if (kv->query->node->type == QueryNodeType::RECURSIVE_CTE_NODE) {
			has_recursive = true;
			break;
		}
	}
	string result = "WITH ";
	if (has_recursive) {
		result += "RECURSIVE ";
	}
	bool first_cte = true;

	vector<string> names;
	names.resize(map.size());
	for (auto &kv : map_idx) {
		names[kv.second] = kv.first;
	}

	for (idx_t i = 0; i < map.size(); i++) {
		auto &kv = map[i];
		auto &name = names[i];
		if (!first_cte) {
			result += ", ";
		}
		auto &cte = *kv;
		result += KeywordHelper::WriteOptionallyQuoted(name);
		if (!cte.aliases.empty()) {
			result += " (";
			for (idx_t k = 0; k < cte.aliases.size(); k++) {
				if (k > 0) {
					result += ", ";
				}
				result += KeywordHelper::WriteOptionallyQuoted(cte.aliases[k]);
			}
			result += ")";
		}
		if (kv->materialized == CTEMaterialize::CTE_MATERIALIZE_ALWAYS) {
			result += " AS MATERIALIZED (";
		} else if (kv->materialized == CTEMaterialize::CTE_MATERIALIZE_NEVER) {
			result += " AS NOT MATERIALIZED (";
		} else {
			result += " AS (";
		}
		result += cte.query->ToString();
		result += ")";
		first_cte = false;
	}
	return result;
}

string QueryNode::ResultModifiersToString() const {
	string result;
	for (idx_t modifier_idx = 0; modifier_idx < modifiers.size(); modifier_idx++) {
		auto &modifier = *modifiers[modifier_idx];
		if (modifier.type == ResultModifierType::ORDER_MODIFIER) {
			auto &order_modifier = modifier.Cast<OrderModifier>();
			result += " ORDER BY ";
			for (idx_t k = 0; k < order_modifier.orders.size(); k++) {
				if (k > 0) {
					result += ", ";
				}
				result += order_modifier.orders[k].ToString();
			}
		} else if (modifier.type == ResultModifierType::LIMIT_MODIFIER) {
			auto &limit_modifier = modifier.Cast<LimitModifier>();
			if (limit_modifier.limit) {
				result += " LIMIT " + limit_modifier.limit->ToString();
			}
			if (limit_modifier.offset) {
				result += " OFFSET " + limit_modifier.offset->ToString();
			}
		} else if (modifier.type == ResultModifierType::LIMIT_PERCENT_MODIFIER) {
			auto &limit_p_modifier = modifier.Cast<LimitPercentModifier>();
			if (limit_p_modifier.limit) {
				result += " LIMIT (" + limit_p_modifier.limit->ToString() + ") %";
			}
			if (limit_p_modifier.offset) {
				result += " OFFSET " + limit_p_modifier.offset->ToString();
			}
		}
	}
	return result;
}

bool QueryNode::Equals(const QueryNode *other) const {
	if (!other) {
		return false;
	}
	if (this == other) {
		return true;
	}
	if (other->type != this->type) {
		return false;
	}

	if (modifiers.size() != other->modifiers.size()) {
		return false;
	}
	for (idx_t i = 0; i < modifiers.size(); i++) {
		if (!modifiers[i]->Equals(*other->modifiers[i])) {
			return false;
		}
	}
	// WITH clauses (CTEs)
	if (cte_map.map.size() != other->cte_map.map.size()) {
		return false;
	}

	for (idx_t i = 0; i < cte_map.map.size(); i++) {
		auto &entry = cte_map.map.at(i);
		auto &other_entry = other->cte_map.map.at(i);
		if (entry->aliases != other_entry->aliases) {
			return false;
		}
		if (!entry->query->Equals(*other_entry->query)) {
			return false;
		}
	}
	return other->type == type;
}

void QueryNode::CopyProperties(QueryNode &other) const {
	for (auto &modifier : modifiers) {
		other.modifiers.push_back(modifier->Copy());
	}
	other.cte_map.map.resize(cte_map.map.size());
	for (auto &kv_idx : cte_map.map_idx) {
		auto &kv = cte_map.map[kv_idx.second];
		auto kv_info = make_uniq<CommonTableExpressionInfo>();
		for (auto &al : kv->aliases) {
			kv_info->aliases.push_back(al);
		}
		kv_info->query = unique_ptr_cast<SQLStatement, SelectStatement>(kv->query->Copy());
		kv_info->materialized = kv->materialized;
		other.cte_map.map[kv_idx.second] = std::move(kv_info);
		other.cte_map.map_idx[kv_idx.first] = kv_idx.second;
	}
}

void QueryNode::AddDistinct() {
	// check if we already have a DISTINCT modifier
	for (idx_t modifier_idx = modifiers.size(); modifier_idx > 0; modifier_idx--) {
		auto &modifier = *modifiers[modifier_idx - 1];
		if (modifier.type == ResultModifierType::DISTINCT_MODIFIER) {
			auto &distinct_modifier = modifier.Cast<DistinctModifier>();
			if (distinct_modifier.distinct_on_targets.empty()) {
				// we have a DISTINCT without an ON clause - this distinct does not need to be added
				return;
			}
		} else if (modifier.type == ResultModifierType::LIMIT_MODIFIER ||
		           modifier.type == ResultModifierType::LIMIT_PERCENT_MODIFIER) {
			// we encountered a LIMIT or LIMIT PERCENT - these change the result of DISTINCT, so we do need to push a
			// DISTINCT relation
			break;
		}
	}
	modifiers.push_back(make_uniq<DistinctModifier>());
}

} // namespace duckdb
