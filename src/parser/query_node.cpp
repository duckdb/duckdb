#include "duckdb/parser/query_node.hpp"

#include "duckdb/parser/query_node/select_node.hpp"
#include "duckdb/parser/query_node/set_operation_node.hpp"
#include "duckdb/parser/query_node/recursive_cte_node.hpp"
#include "duckdb/common/limits.hpp"
#include "duckdb/common/field_writer.hpp"

namespace duckdb {

string QueryNode::CTEToString() const {
	if (cte_map.empty()) {
		return string();
	}
	// check if there are any recursive CTEs
	bool has_recursive = false;
	for (auto &kv : cte_map) {
		if (kv.second->query->node->type == QueryNodeType::RECURSIVE_CTE_NODE) {
			has_recursive = true;
			break;
		}
	}
	string result = "WITH ";
	if (has_recursive) {
		result += "RECURSIVE ";
	}
	bool first_cte = true;
	for (auto &kv : cte_map) {
		if (!first_cte) {
			result += ", ";
		}
		auto &cte = *kv.second;
		result += KeywordHelper::WriteOptionallyQuoted(kv.first);
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
		result += " AS (";
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
			auto &order_modifier = (OrderModifier &)modifier;
			result += " ORDER BY ";
			for (idx_t k = 0; k < order_modifier.orders.size(); k++) {
				if (k > 0) {
					result += ", ";
				}
				result += order_modifier.orders[k].ToString();
			}
		} else if (modifier.type == ResultModifierType::LIMIT_MODIFIER) {
			auto &limit_modifier = (LimitModifier &)modifier;
			if (limit_modifier.limit) {
				result += " LIMIT " + limit_modifier.limit->ToString();
			}
			if (limit_modifier.offset) {
				result += " OFFSET " + limit_modifier.offset->ToString();
			}
		} else if (modifier.type == ResultModifierType::LIMIT_PERCENT_MODIFIER) {
			auto &limit_p_modifier = (LimitPercentModifier &)modifier;
			if (limit_p_modifier.limit) {
				result += " LIMIT " + limit_p_modifier.limit->ToString() + " %";
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
		if (!modifiers[i]->Equals(other->modifiers[i].get())) {
			return false;
		}
	}
	// WITH clauses (CTEs)
	if (cte_map.size() != other->cte_map.size()) {
		return false;
	}
	for (auto &entry : cte_map) {
		auto other_entry = other->cte_map.find(entry.first);
		if (other_entry == other->cte_map.end()) {
			return false;
		}
		if (entry.second->aliases != other_entry->second->aliases) {
			return false;
		}
		if (!entry.second->query->Equals(other_entry->second->query.get())) {
			return false;
		}
	}
	return other->type == type;
}

void QueryNode::CopyProperties(QueryNode &other) const {
	for (auto &modifier : modifiers) {
		other.modifiers.push_back(modifier->Copy());
	}
	for (auto &kv : cte_map) {
		auto kv_info = make_unique<CommonTableExpressionInfo>();
		for (auto &al : kv.second->aliases) {
			kv_info->aliases.push_back(al);
		}
		kv_info->query = unique_ptr_cast<SQLStatement, SelectStatement>(kv.second->query->Copy());
		other.cte_map[kv.first] = move(kv_info);
	}
}

void QueryNode::Serialize(Serializer &main_serializer) const {
	FieldWriter writer(main_serializer);
	writer.WriteField<QueryNodeType>(type);
	writer.WriteSerializableList(modifiers);
	// cte_map
	writer.WriteField<uint32_t>((uint32_t)cte_map.size());
	auto &serializer = writer.GetSerializer();
	for (auto &cte : cte_map) {
		serializer.WriteString(cte.first);
		serializer.WriteStringVector(cte.second->aliases);
		cte.second->query->Serialize(serializer);
	}
	Serialize(writer);
	writer.Finalize();
}

unique_ptr<QueryNode> QueryNode::Deserialize(Deserializer &main_source) {
	FieldReader reader(main_source);

	auto type = reader.ReadRequired<QueryNodeType>();
	auto modifiers = reader.ReadRequiredSerializableList<ResultModifier>();
	// cte_map
	auto cte_count = reader.ReadRequired<uint32_t>();
	auto &source = reader.GetSource();
	unordered_map<string, unique_ptr<CommonTableExpressionInfo>> cte_map;
	for (idx_t i = 0; i < cte_count; i++) {
		auto name = source.Read<string>();
		auto info = make_unique<CommonTableExpressionInfo>();
		source.ReadStringVector(info->aliases);
		info->query = SelectStatement::Deserialize(source);
		cte_map[name] = move(info);
	}

	unique_ptr<QueryNode> result;
	switch (type) {
	case QueryNodeType::SELECT_NODE:
		result = SelectNode::Deserialize(reader);
		break;
	case QueryNodeType::SET_OPERATION_NODE:
		result = SetOperationNode::Deserialize(reader);
		break;
	case QueryNodeType::RECURSIVE_CTE_NODE:
		result = RecursiveCTENode::Deserialize(reader);
		break;
	default:
		throw SerializationException("Could not deserialize Query Node: unknown type!");
	}
	result->modifiers = move(modifiers);
	result->cte_map = move(cte_map);
	reader.Finalize();
	return result;
}

} // namespace duckdb
