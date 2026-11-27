#pragma once

#include "duckdb/catalog/catalog_entry/table_catalog_entry.hpp"
#include "duckdb/optimizer/table_column_origin.hpp"
#include "duckdb/common/optional.hpp"
#include "duckdb/planner/logical_operator.hpp"

namespace duckdb {

class Expression;
class LogicalAggregate;

bool HasCompleteGrouping(const LogicalAggregate &aggregate);
optional_ptr<LogicalAggregate> FindUnaryAggregate(LogicalOperator &op);

} // namespace duckdb
