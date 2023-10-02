#include "duckdb/planner/operator/logical_simple.hpp"
#include "duckdb/parser/parsed_data/alter_info.hpp"
#include "duckdb/parser/parsed_data/attach_info.hpp"
#include "duckdb/parser/parsed_data/drop_info.hpp"
#include "duckdb/parser/parsed_data/load_info.hpp"
#include "duckdb/parser/parsed_data/transaction_info.hpp"
#include "duckdb/parser/parsed_data/vacuum_info.hpp"
#include "duckdb/parser/parsed_data/detach_info.hpp"

namespace duckdb {

idx_t LogicalSimple::EstimateCardinality(ClientContext &context) {
	return 1;
}

} // namespace duckdb
