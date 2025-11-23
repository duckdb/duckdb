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
#include "duckdb/execution/physical_plan_generator.hpp"

namespace duckdb {

struct DBConfig;
struct ExtensionOption;

//! PhysicalSet represents a SET operation (e.g. SET a = 42)
class PhysicalSet : public PhysicalOperator {
public:
	static constexpr const PhysicalOperatorType TYPE = PhysicalOperatorType::SET;

public:
	PhysicalSet(PhysicalPlan &physical_plan, const string &name_p, Value value_p, SetScope scope_p,
	            idx_t estimated_cardinality)
	    : PhysicalOperator(physical_plan, PhysicalOperatorType::SET, {LogicalType::BOOLEAN}, estimated_cardinality),
	      name(physical_plan.ArenaRef().MakeString(name_p)), value(std::move(value_p)), scope(scope_p) {
	}

public:
	// Source interface
	SourceResultType GetDataInternal(ExecutionContext &context, DataChunk &chunk,
	                                 OperatorSourceInput &input) const override;

	bool IsSource() const override {
		return true;
	}

	static void SetExtensionVariable(ClientContext &context, ExtensionOption &extension_option, const String &name,
	                                 SetScope scope, const Value &value);

	static void SetGenericVariable(ClientContext &context, const String &name, SetScope scope, Value target_value);

public:
	String name;
	const Value value;
	const SetScope scope;
};

} // namespace duckdb
