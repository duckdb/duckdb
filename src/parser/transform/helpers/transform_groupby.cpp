#include "duckdb/parser/parsed_expression.hpp"
#include "duckdb/parser/transformer.hpp"
#include "duckdb/parser/query_node/select_node.hpp"
#include "duckdb/parser/expression_map.hpp"
#include "duckdb/parser/expression/function_expression.hpp"

namespace duckdb {

struct GroupingExpressionMap {
	expression_map_t<idx_t> map;
};

static GroupingSet VectorToGroupingSet(vector<idx_t> &indexes, idx_t start, idx_t end) {
	D_ASSERT(start <= end);
	GroupingSet result;
	for(idx_t i = start; i < end; i++) {
		result.insert(indexes[i]);
	}
	return result;
}

static GroupingSet VectorToGroupingSet(vector<idx_t> &indexes) {
	return VectorToGroupingSet(indexes, 0, indexes.size());
}

void Transformer::AddGroupByExpression(unique_ptr<ParsedExpression> expression, GroupingExpressionMap &map, GroupByNode &result, vector<idx_t> &result_set) {
	if (expression->type == ExpressionType::FUNCTION) {
		auto &func = (FunctionExpression &) *expression;
		if (func.function_name == "row") {
			for(auto &child : func.children) {
				AddGroupByExpression(move(child), map, result, result_set);
			}
			return;
		}
	}
	auto entry = map.map.find(expression.get());
	idx_t result_idx;
	if (entry == map.map.end()) {
		result_idx = result.group_expressions.size();
		map.map[expression.get()] = result_idx;
		result.group_expressions.push_back(move(expression));
	} else {
		result_idx = entry->second;
	}
	result_set.push_back(result_idx);
}

static void AddCubeSets(vector<idx_t> current_set, vector<idx_t> &result_set, vector<GroupingSet> &result_sets, idx_t start_idx = 0) {
	result_sets.push_back(VectorToGroupingSet(current_set));
	for(idx_t k = start_idx; k < result_set.size(); k++) {
		vector<idx_t> child_set = current_set;
		child_set.push_back(result_set[k]);
		AddCubeSets(move(child_set), result_set, result_sets, k + 1);
	}
}

void Transformer::TransformGroupByExpression(duckdb_libpgquery::PGNode *n, GroupingExpressionMap &map, GroupByNode &result, vector<idx_t> &indexes) {
	auto expression = TransformExpression(n, 0);
	AddGroupByExpression(move(expression), map, result, indexes);
}

// If one GROUPING SETS clause is nested inside another,
// the effect is the same as if all the elements of the inner clause had been written directly in the outer clause.
void Transformer::TransformGroupByNode(duckdb_libpgquery::PGNode *n, GroupingExpressionMap &map, GroupByNode &result, vector<GroupingSet> &result_sets) {
	if (n->type == duckdb_libpgquery::T_PGGroupingSet) {
		auto grouping_set = (duckdb_libpgquery::PGGroupingSet *) n;
		switch(grouping_set->kind) {
		case duckdb_libpgquery::GROUPING_SET_EMPTY:
			result_sets.push_back(GroupingSet{});
			break;
		case duckdb_libpgquery::GROUPING_SET_SETS: {
			for(auto node = grouping_set->content->head; node; node = node->next) {
				auto pg_node = (duckdb_libpgquery::PGNode*) node->data.ptr_value;
				TransformGroupByNode(pg_node, map, result, result_sets);
			}
			break;
		}
		case duckdb_libpgquery::GROUPING_SET_ROLLUP: {
			vector<idx_t> rollup_set;
			for(auto node = grouping_set->content->head; node; node = node->next) {
				auto pg_node = (duckdb_libpgquery::PGNode*) node->data.ptr_value;
				TransformGroupByExpression(pg_node, map, result, rollup_set);
			}
			// generate the subsets of the rollup set and add them to the grouping sets
			for(idx_t i = 0; i <= rollup_set.size(); i++) {
				result_sets.push_back(VectorToGroupingSet(rollup_set, 0, rollup_set.size() - i));
			}
			break;
		}
		case duckdb_libpgquery::GROUPING_SET_CUBE: {
			vector<idx_t> cube_set;
			for(auto node = grouping_set->content->head; node; node = node->next) {
				auto pg_node = (duckdb_libpgquery::PGNode*) node->data.ptr_value;
				TransformGroupByExpression(pg_node, map, result, cube_set);
			}
			// generate the subsets of the rollup set and add them to the grouping sets
			vector<idx_t> current_set;
			AddCubeSets(move(current_set), cube_set, result_sets, 0);
			break;
		}
		default:
			throw InternalException("Unsupported GROUPING SET type %d", grouping_set->kind);
		}
	} else {
		vector<idx_t> indexes;
		TransformGroupByExpression(n, map, result, indexes);
		result_sets.push_back(VectorToGroupingSet(indexes));
	}
}

// If multiple grouping items are specified in a single GROUP BY clause,
// then the final list of grouping sets is the cross product of the individual items.
bool Transformer::TransformGroupBy(duckdb_libpgquery::PGList *group, GroupByNode &result) {
	if (!group) {
		return false;
	}
	GroupingExpressionMap map;
	for (auto node = group->head; node != nullptr; node = node->next) {
		auto n = reinterpret_cast<duckdb_libpgquery::PGNode *>(node->data.ptr_value);
		vector<GroupingSet> result_sets;
		TransformGroupByNode(n, map, result, result_sets);
		if (result.grouping_sets.empty()) {
			// no grouping sets yet: use the current set of grouping sets
			result.grouping_sets = move(result_sets);
		} else {
			// compute the cross product
			vector<GroupingSet> new_sets;
			new_sets.reserve(result.grouping_sets.size() * result_sets.size());
			for(idx_t current_idx = 0; current_idx < result.grouping_sets.size(); current_idx++) {
				auto &current_set = result.grouping_sets[current_idx];
				for(idx_t new_idx = 0; new_idx < result_sets.size(); new_idx++) {
					auto &new_set = result_sets[new_idx];
					GroupingSet set;
					set.insert(current_set.begin(), current_set.end());
					set.insert(new_set.begin(), new_set.end());
					new_sets.push_back(move(set));
				}
			}
			result.grouping_sets = move(new_sets);
		}
	}
	return true;
}

} // namespace duckdb
