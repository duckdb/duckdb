#include "duckdb/execution/operator/schema/physical_create_function.hpp"
#include "duckdb/execution/operator/schema/physical_create_schema.hpp"
#include "duckdb/execution/operator/schema/physical_create_sequence.hpp"
#include "duckdb/execution/operator/schema/physical_create_type.hpp"
#include "duckdb/execution/operator/schema/physical_create_view.hpp"
#include "duckdb/execution/physical_plan_generator.hpp"
#include "duckdb/parser/parsed_data/create_type_info.hpp"
#include "duckdb/planner/logical_operator.hpp"
#include "duckdb/planner/operator/logical_create.hpp"

namespace duckdb {

PhysicalOperator &PhysicalPlanGenerator::CreatePlan(LogicalCreate &op) {
	switch (op.type) {
	case LogicalOperatorType::LOGICAL_CREATE_SEQUENCE:
		return Make<PhysicalCreateSequence>(unique_ptr_cast<CreateInfo, CreateSequenceInfo>(std::move(op.info)),
		                                    op.estimated_cardinality);
	case LogicalOperatorType::LOGICAL_CREATE_VIEW:
		return Make<PhysicalCreateView>(unique_ptr_cast<CreateInfo, CreateViewInfo>(std::move(op.info)),
		                                op.estimated_cardinality);
	case LogicalOperatorType::LOGICAL_CREATE_SCHEMA:
		return Make<PhysicalCreateSchema>(unique_ptr_cast<CreateInfo, CreateSchemaInfo>(std::move(op.info)),
		                                  op.estimated_cardinality);
	case LogicalOperatorType::LOGICAL_CREATE_MACRO:
		return Make<PhysicalCreateFunction>(unique_ptr_cast<CreateInfo, CreateMacroInfo>(std::move(op.info)),
		                                    op.estimated_cardinality);
	case LogicalOperatorType::LOGICAL_CREATE_TYPE: {
		auto &create = Make<PhysicalCreateType>(unique_ptr_cast<CreateInfo, CreateTypeInfo>(std::move(op.info)),
		                                        op.estimated_cardinality);
		if (!op.children.empty()) {
			D_ASSERT(op.children.size() == 1);
			auto &plan = CreatePlan(*op.children[0]);
			create.children.push_back(plan);
		}
		return create;
	}
	default:
		throw NotImplementedException("Unimplemented type for logical simple create");
	}
}

} // namespace duckdb
