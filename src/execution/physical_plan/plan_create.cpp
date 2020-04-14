#include "duckdb/execution/physical_plan_generator.hpp"
#include "duckdb/planner/logical_operator.hpp"
#include "duckdb/planner/operator/logical_create.hpp"

#include "duckdb/execution/operator/schema/physical_create_schema.hpp"
#include "duckdb/execution/operator/schema/physical_create_sequence.hpp"
#include "duckdb/execution/operator/schema/physical_create_view.hpp"

namespace duckdb {

unique_ptr<PhysicalOperator> PhysicalPlanGenerator::CreatePlan(LogicalCreate &op) {
	switch (op.type) {
	case LogicalOperatorType::CREATE_SEQUENCE:
		return make_unique<PhysicalCreateSequence>(unique_ptr_cast<CreateInfo, CreateSequenceInfo>(move(op.info)));
	case LogicalOperatorType::CREATE_VIEW:
		return make_unique<PhysicalCreateView>(unique_ptr_cast<CreateInfo, CreateViewInfo>(move(op.info)));
	case LogicalOperatorType::CREATE_SCHEMA:
		return make_unique<PhysicalCreateSchema>(unique_ptr_cast<CreateInfo, CreateSchemaInfo>(move(op.info)));
	default:
		throw NotImplementedException("Unimplemented type for logical simple create");
	}
}

} // namespace duckdb
