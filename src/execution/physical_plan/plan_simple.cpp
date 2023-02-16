#include "duckdb/execution/operator/helper/physical_load.hpp"
#include "duckdb/execution/operator/helper/physical_transaction.hpp"
#include "duckdb/execution/operator/helper/physical_vacuum.hpp"
#include "duckdb/execution/operator/schema/physical_alter.hpp"
#include "duckdb/execution/operator/schema/physical_attach.hpp"
#include "duckdb/execution/operator/schema/physical_create_schema.hpp"
#include "duckdb/execution/operator/schema/physical_create_sequence.hpp"
#include "duckdb/execution/operator/schema/physical_create_view.hpp"
#include "duckdb/execution/operator/schema/physical_detach.hpp"
#include "duckdb/execution/operator/schema/physical_drop.hpp"
#include "duckdb/execution/physical_plan_generator.hpp"
#include "duckdb/planner/logical_operator.hpp"
#include "duckdb/planner/operator/logical_simple.hpp"

namespace duckdb {

unique_ptr<PhysicalOperator> PhysicalPlanGenerator::CreatePlan(LogicalSimple &op) {
	switch (op.type) {
	case LogicalOperatorType::LOGICAL_ALTER:
		return make_unique<PhysicalAlter>(unique_ptr_cast<ParseInfo, AlterInfo>(std::move(op.info)),
		                                  op.estimated_cardinality);
	case LogicalOperatorType::LOGICAL_DROP:
		return make_unique<PhysicalDrop>(unique_ptr_cast<ParseInfo, DropInfo>(std::move(op.info)),
		                                 op.estimated_cardinality);
	case LogicalOperatorType::LOGICAL_TRANSACTION:
		return make_unique<PhysicalTransaction>(unique_ptr_cast<ParseInfo, TransactionInfo>(std::move(op.info)),
		                                        op.estimated_cardinality);
	case LogicalOperatorType::LOGICAL_VACUUM: {
		auto result = make_unique<PhysicalVacuum>(unique_ptr_cast<ParseInfo, VacuumInfo>(std::move(op.info)),
		                                          op.estimated_cardinality);
		if (!op.children.empty()) {
			auto child = CreatePlan(*op.children[0]);
			result->children.push_back(std::move(child));
		}
		return std::move(result);
	}
	case LogicalOperatorType::LOGICAL_LOAD:
		return make_unique<PhysicalLoad>(unique_ptr_cast<ParseInfo, LoadInfo>(std::move(op.info)),
		                                 op.estimated_cardinality);
	case LogicalOperatorType::LOGICAL_ATTACH:
		return make_unique<PhysicalAttach>(unique_ptr_cast<ParseInfo, AttachInfo>(std::move(op.info)),
		                                   op.estimated_cardinality);
	case LogicalOperatorType::LOGICAL_DETACH:
		return make_unique<PhysicalDetach>(unique_ptr_cast<ParseInfo, DetachInfo>(std::move(op.info)),
		                                   op.estimated_cardinality);
	default:
		throw NotImplementedException("Unimplemented type for logical simple operator");
	}
}

} // namespace duckdb
