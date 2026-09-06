#include "duckdb/optimizer/join_order/filter_info.hpp"

#include "duckdb/planner/expression.hpp"

namespace duckdb {

FilterInfo::FilterInfo(unique_ptr<Expression> filter, JoinRelationSet &set, idx_t filter_index, JoinType join_type)
    : filter(std::move(filter)), set(set), filter_index(filter_index), join_type(join_type) {
}

FilterInfo::~FilterInfo() {
}

void FilterInfo::SetLeftSet(optional_ptr<JoinRelationSet> left_set_new) {
	left_set = left_set_new;
}

void FilterInfo::SetRightSet(optional_ptr<JoinRelationSet> right_set_new) {
	right_set = right_set_new;
}

} // namespace duckdb
