#include "duckdb/parser/query_node/select_node.hpp"
#include "duckdb/parser/expression_util.hpp"
#include "duckdb/common/field_writer.hpp"

namespace duckdb {

SelectNode::SelectNode()
    : QueryNode(QueryNodeType::SELECT_NODE), aggregate_handling(AggregateHandling::STANDARD_HANDLING) {
}

bool SelectNode::Equals(const QueryNode *other_p) const {
	if (!QueryNode::Equals(other_p)) {
		return false;
	}
	if (this == other_p) {
		return true;
	}
	auto other = (SelectNode *)other_p;

	// SELECT
	if (!ExpressionUtil::ListEquals(select_list, other->select_list)) {
		return false;
	}
	// FROM
	if (from_table) {
		// we have a FROM clause, compare to the other one
		if (!from_table->Equals(other->from_table.get())) {
			return false;
		}
	} else if (other->from_table) {
		// we don't have a FROM clause, if the other statement has one they are
		// not equal
		return false;
	}
	// WHERE
	if (!BaseExpression::Equals(where_clause.get(), other->where_clause.get())) {
		return false;
	}
	// GROUP BY
	if (!ExpressionUtil::ListEquals(groups.group_expressions, other->groups.group_expressions)) {
		return false;
	}
	if (groups.grouping_sets != other->groups.grouping_sets) {
		return false;
	}
	if (!SampleOptions::Equals(sample.get(), other->sample.get())) {
		return false;
	}
	// HAVING
	if (!BaseExpression::Equals(having.get(), other->having.get())) {
		return false;
	}
	// QUALIFY
	if (!BaseExpression::Equals(qualify.get(), other->qualify.get())) {
		return false;
	}
	return true;
}

unique_ptr<QueryNode> SelectNode::Copy() const {
	auto result = make_unique<SelectNode>();
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
	return move(result);
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

unique_ptr<QueryNode> SelectNode::Deserialize(FieldReader &reader) {
	auto result = make_unique<SelectNode>();
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
	return move(result);
}

} // namespace duckdb
