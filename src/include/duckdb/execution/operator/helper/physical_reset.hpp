//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/execution/operator/helper/physical_reset.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/enums/set_scope.hpp"
#include "duckdb/execution/physical_operator.hpp"
#include "duckdb/parser/parsed_data/vacuum_info.hpp"

namespace duckdb {

struct DBConfig;
struct ExtensionOption;

//! PhysicalReset represents a RESET operation (e.g. RESET a = 42)
class PhysicalReset : public PhysicalOperator {
public:
	PhysicalReset(const std::string &name_p, SetScope scope_p, idx_t estimated_cardinality)
	    : PhysicalOperator(PhysicalOperatorType::RESET, {LogicalType::BOOLEAN}, estimated_cardinality), name(name_p),
	      scope(scope_p) {
	}

public:
	// Source interface
	void GetData(ExecutionContext &context, DataChunk &chunk, GlobalSourceState &gstate,
	             LocalSourceState &lstate) const override;

public:
	const std::string name;
	const SetScope scope;

private:
	void ResetExtensionVariable(ExecutionContext &context, DBConfig &config, ExtensionOption &extension_option) const;
};

} // namespace duckdb
