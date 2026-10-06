#include "duckdb/optimizer/window_join_pushdown.hpp"

// #include "duckdb/optimizer/apply_functional_dependencies.hpp"

namespace duckdb {

WindowJoinPushdown::WindowJoinPushdown(ClientContext &client) : client(client) {
}

unique_ptr<LogicalOperator> WindowJoinPushdown::Optimize(unique_ptr<LogicalOperator> op) {
	return std::move(op);
}

} // namespace duckdb
