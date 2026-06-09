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
#include "duckdb/execution/physical_plan_generator.hpp"

namespace duckdb {

struct DBConfig;
struct ExtensionOption;

//! PhysicalReset represents a RESET operation (e.g. RESET a = 42)
class PhysicalReset : public PhysicalOperator {
public:
	static constexpr const PhysicalOperatorType TYPE = PhysicalOperatorType::RESET;

public:
	PhysicalReset(PhysicalPlan &physical_plan, const std::string &name_p, SetScope scope_p, idx_t estimated_cardinality)
	    : PhysicalOperator(physical_plan, PhysicalOperatorType::RESET, {LogicalType::BOOLEAN}, estimated_cardinality),
	      name(physical_plan.ArenaRef().MakeString(name_p)), scope(scope_p) {
	}

public:
	// Source interface
	SourceResultType GetDataInternal(ExecutionContext &context, DataChunk &chunk,
	                                 OperatorSourceInput &input) const override;

	bool IsSource() const override {
		return true;
	}

public:
	const String name;
	const SetScope scope;

private:
	void ResetExtensionVariable(ExecutionContext &context, DBConfig &config, ExtensionOption &extension_option) const;
};

} // namespace duckdb
