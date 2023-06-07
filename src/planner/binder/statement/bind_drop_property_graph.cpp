#include "duckdb/parser/statement/drop_property_graph_statement.hpp"
#include "duckdb/planner/binder.hpp"

namespace duckdb {

BoundStatement Binder::Bind(DropPropertyGraphStatement &stmt) {
	BoundStatement result;
	auto &base = (DropPropertyGraphInfo &)*stmt.info;
	if (stmt.info->type != CatalogType::PROPERTY_GRAPH_ENTRY) {
		throw BinderException("Incorrect CatalogType");
	}
	auto duckpgq_state_entry = context.registered_state.find("duckpgq");
	if (duckpgq_state_entry == context.registered_state.end()) {
		throw MissingExtensionException("The DuckPGQ extension has not been loaded");
	}
	auto sqlpgq_state = reinterpret_cast<DuckPGQState *>(duckpgq_state_entry->second.get());
	auto property_graph = sqlpgq_state->registered_property_graphs.find(base.name);
	if (property_graph == sqlpgq_state->registered_property_graphs.end()) {
		throw BinderException("The property graph %s does not exist", base.name);
	}

	result.plan = make_uniq<LogicalSimple>(LogicalOperatorType::LOGICAL_DROP_PROPERTY_GRAPH, std::move(stmt.info));
	result.names = {"Success"};
	result.types = {LogicalType::BOOLEAN};
	properties.allow_stream_result = false;
	properties.return_type = StatementReturnType::NOTHING;

	return result;
}
} // namespace duckdb
