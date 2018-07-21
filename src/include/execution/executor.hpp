
#pragma once

#include "execution/physical_operator.hpp"

#include "duckdb.hpp"

namespace duckdb {

class Executor {
 public:
 	std::unique_ptr<DuckDBResult> Execute(std::unique_ptr<PhysicalOperator> op);
};

}
