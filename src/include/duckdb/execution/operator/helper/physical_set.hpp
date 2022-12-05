//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/execution/operator/helper/physical_set.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/enums/set_scope.hpp"
#include "duckdb/execution/physical_operator.hpp"
#include "duckdb/parser/parsed_data/vacuum_info.hpp"

namespace duckdb {

struct DBConfig;

//! PhysicalSet represents a SET operation (e.g. SET a = 42)
class PhysicalSet : public PhysicalOperator {
public:
	PhysicalSet(const std::string &name_p, Value value_p, SetScope scope_p, idx_t estimated_cardinality)
	    : PhysicalOperator(PhysicalOperatorType::SET, {LogicalType::BOOLEAN}, estimated_cardinality), name(name_p),
	      value(value_p), scope(scope_p) {
	}

public:
	// Source interface
	void GetData(ExecutionContext &context, DataChunk &chunk, GlobalSourceState &gstate,
	             LocalSourceState &lstate) const override;

public:
	const std::string name;
	const Value value;
	const SetScope scope;

private:
	void SetExtensionVariable(ExecutionContext &context, DBConfig &config, ExtensionOption &extension_option) const;
};

} // namespace duckdb
