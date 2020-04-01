#include "duckdb/planner/binder.hpp"
#include "duckdb/parser/parsed_data/create_view_info.hpp"
#include "duckdb/planner/parsed_data/bound_create_info.hpp"
#include "duckdb/planner/bound_query_node.hpp"

using namespace duckdb;
using namespace std;

unique_ptr<BoundCreateInfo> Binder::BindCreateViewInfo(unique_ptr<CreateInfo> info) {
	auto &base = (CreateViewInfo &)*info;
	auto result = make_unique<BoundCreateInfo>(move(info));

	// bind the view as if it were a query so we can catch errors
	// note that we bind a copy and don't actually use the bind result
	auto copy = base.query->Copy();
	auto query_node = Bind(*copy);
	if (base.aliases.size() > query_node.names.size()) {
		throw BinderException("More VIEW aliases than columns in query result");
	}
	return result;
}
