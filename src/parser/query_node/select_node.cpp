#include "duckdb/parser/query_node/select_node.hpp"
#include "duckdb/parser/expression_util.hpp"
#include "duckdb/common/field_writer.hpp"
#include "duckdb/parser/keyword_helper.hpp"
#include "duckdb/common/serializer/format_serializer.hpp"
#include "duckdb/common/serializer/format_deserializer.hpp"

namespace duckdb {

SelectNode::SelectNode()
    : QueryNode(QueryNodeType::SELECT_NODE), aggregate_handling(AggregateHandling::STANDARD_HANDLING) {
}

string SelectNode::ToString() const {
	string result;
	result = cte_map.ToString();
	result += "SELECT ";

	// search for a distinct modifier
	for (idx_t modifier_idx = 0; modifier_idx < modifiers.size(); modifier_idx++) {
		if (modifiers[modifier_idx]->type == ResultModifierType::DISTINCT_MODIFIER) {
			auto &distinct_modifier = modifiers[modifier_idx]->Cast<DistinctModifier>();
			result += "DISTINCT ";
			if (!distinct_modifier.distinct_on_targets.empty()) {
				result += "ON (";
				for (idx_t k = 0; k < distinct_modifier.distinct_on_targets.size(); k++) {
					if (k > 0) {
						result += ", ";
					}
					result += distinct_modifier.distinct_on_targets[k]->ToString();
				}
				result += ") ";
			}
		}
	}
	for (idx_t i = 0; i < select_list.size(); i++) {
		if (i > 0) {
			result += ", ";
		}
		result += select_list[i]->ToString();
		if (!select_list[i]->alias.empty()) {
			result += StringUtil::Format(" AS %s", SQLIdentifier(select_list[i]->alias));
		}
	}
	if (from_table && from_table->type != TableReferenceType::EMPTY) {
		result += " FROM " + from_table->ToString();
	}
	if (where_clause) {
		result += " WHERE " + where_clause->ToString();
	}
	if (!groups.grouping_sets.empty()) {
		result += " GROUP BY ";
		// if we are dealing with multiple grouping sets, we have to add a few additional brackets
		bool grouping_sets = groups.grouping_sets.size() > 1;
		if (grouping_sets) {
			result += "GROUPING SETS (";
		}
		for (idx_t i = 0; i < groups.grouping_sets.size(); i++) {
			auto &grouping_set = groups.grouping_sets[i];
			if (i > 0) {
				result += ",";
			}
			if (grouping_set.empty()) {
				result += "()";
				continue;
			}
			if (grouping_sets) {
				result += "(";
			}
			bool first = true;
			for (auto &grp : grouping_set) {
				if (!first) {
					result += ", ";
				}
				result += groups.group_expressions[grp]->ToString();
				first = false;
			}
			if (grouping_sets) {
				result += ")";
			}
		}
		if (grouping_sets) {
			result += ")";
		}
	} else if (aggregate_handling == AggregateHandling::FORCE_AGGREGATES) {
		result += " GROUP BY ALL";
	}
	if (having) {
		result += " HAVING " + having->ToString();
	}
	if (qualify) {
		result += " QUALIFY " + qualify->ToString();
	}
	if (sample) {
		result += " USING SAMPLE ";
		result += sample->sample_size.ToString();
		if (sample->is_percentage) {
			result += "%";
		}
		result += " (" + EnumUtil::ToString(sample->method);
		if (sample->seed >= 0) {
			result += ", " + std::to_string(sample->seed);
		}
		result += ")";
	}
	return result + ResultModifiersToString();
}

bool SelectNode::Equals(const QueryNode *other_p) const {
	if (!QueryNode::Equals(other_p)) {
		return false;
	}
	if (this == other_p) {
		return true;
	}
	auto &other = other_p->Cast<SelectNode>();

	// SELECT
	if (!ExpressionUtil::ListEquals(select_list, other.select_list)) {
		return false;
	}
	// FROM
	if (!TableRef::Equals(from_table, other.from_table)) {
		return false;
	}
	// WHERE
	if (!ParsedExpression::Equals(where_clause, other.where_clause)) {
		return false;
	}
	// GROUP BY
	if (!ParsedExpression::ListEquals(groups.group_expressions, other.groups.group_expressions)) {
		return false;
	}
	if (groups.grouping_sets != other.groups.grouping_sets) {
		return false;
	}
	if (!SampleOptions::Equals(sample.get(), other.sample.get())) {
		return false;
	}
	// HAVING
	if (!ParsedExpression::Equals(having, other.having)) {
		return false;
	}
	// QUALIFY
	if (!ParsedExpression::Equals(qualify, other.qualify)) {
		return false;
	}
	return true;
}

unique_ptr<QueryNode> SelectNode::Copy() const {
	auto result = make_uniq<SelectNode>();
	for (auto &child : select_list) {
		result->select_list.push_back(child->Copy());
	}
	result->from_table = from_table ? from_table->Copy() : nullptr;
	result->where_clause = where_clause ? where_clause->Copy() : nullptr;
	// groups
	for (auto &group : groups.group_expressions) {
		result->groups.group_expressions.push_back(group->Copy());
	}
	result->groups.grouping_sets = groups.grouping_sets;
	result->aggregate_handling = aggregate_handling;
	result->having = having ? having->Copy() : nullptr;
	result->qualify = qualify ? qualify->Copy() : nullptr;
	result->sample = sample ? sample->Copy() : nullptr;
	this->CopyProperties(*result);
	return std::move(result);
}

void SelectNode::Serialize(FieldWriter &writer) const {
	writer.WriteSerializableList(select_list);
	writer.WriteOptional(from_table);
	writer.WriteOptional(where_clause);
	writer.WriteSerializableList(groups.group_expressions);
	writer.WriteField<uint32_t>(groups.grouping_sets.size());
	auto &serializer = writer.GetSerializer();
	for (auto &grouping_set : groups.grouping_sets) {
		serializer.Write<idx_t>(grouping_set.size());
		for (auto &idx : grouping_set) {
			serializer.Write<idx_t>(idx);
		}
	}
	writer.WriteField<AggregateHandling>(aggregate_handling);
	writer.WriteOptional(having);
	writer.WriteOptional(sample);
	writer.WriteOptional(qualify);
}

void SelectNode::FormatSerialize(FormatSerializer &serializer) const {
	QueryNode::FormatSerialize(serializer);
	serializer.WriteProperty("select_list", select_list);
	serializer.WriteOptionalProperty("from_table", from_table);
	serializer.WriteOptionalProperty("where_clause", where_clause);
	serializer.WriteProperty("group_expressions", groups.group_expressions);
	serializer.WriteProperty("group_sets", groups.grouping_sets);
	serializer.WriteProperty("aggregate_handling", aggregate_handling);
	serializer.WriteOptionalProperty("having", having);
	serializer.WriteOptionalProperty("sample", sample);
	serializer.WriteOptionalProperty("qualify", qualify);
}

unique_ptr<QueryNode> SelectNode::FormatDeserialize(FormatDeserializer &deserializer) {
	auto result = make_uniq<SelectNode>();

	deserializer.ReadProperty("select_list", result->select_list);
	deserializer.ReadOptionalProperty("from_table", result->from_table);
	deserializer.ReadOptionalProperty("where_clause", result->where_clause);
	deserializer.ReadProperty("group_expressions", result->groups.group_expressions);
	deserializer.ReadProperty("group_sets", result->groups.grouping_sets);
	deserializer.ReadProperty("aggregate_handling", result->aggregate_handling);
	deserializer.ReadOptionalProperty("having", result->having);
	deserializer.ReadOptionalProperty("sample", result->sample);
	deserializer.ReadOptionalProperty("qualify", result->qualify);

	return std::move(result);
}

unique_ptr<QueryNode> SelectNode::Deserialize(FieldReader &reader) {
	auto result = make_uniq<SelectNode>();
	result->select_list = reader.ReadRequiredSerializableList<ParsedExpression>();
	result->from_table = reader.ReadOptional<TableRef>(nullptr);
	result->where_clause = reader.ReadOptional<ParsedExpression>(nullptr);
	result->groups.group_expressions = reader.ReadRequiredSerializableList<ParsedExpression>();

	auto grouping_set_count = reader.ReadRequired<uint32_t>();
	auto &source = reader.GetSource();
	for (idx_t set_idx = 0; set_idx < grouping_set_count; set_idx++) {
		auto set_entries = source.Read<idx_t>();
		GroupingSet grouping_set;
		for (idx_t i = 0; i < set_entries; i++) {
			grouping_set.insert(source.Read<idx_t>());
		}
		result->groups.grouping_sets.push_back(grouping_set);
	}

	result->aggregate_handling = reader.ReadRequired<AggregateHandling>();
	result->having = reader.ReadOptional<ParsedExpression>(nullptr);
	result->sample = reader.ReadOptional<SampleOptions>(nullptr);
	result->qualify = reader.ReadOptional<ParsedExpression>(nullptr);
	return std::move(result);
}

} // namespace duckdb
