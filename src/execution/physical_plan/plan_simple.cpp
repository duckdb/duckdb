#include "duckdb/execution/physical_plan_generator.hpp"
#include "duckdb/planner/logical_operator.hpp"
#include "duckdb/planner/operator/logical_simple.hpp"

#include "duckdb/execution/operator/helper/physical_pragma.hpp"
#include "duckdb/execution/operator/helper/physical_transaction.hpp"
#include "duckdb/execution/operator/schema/physical_alter.hpp"
#include "duckdb/execution/operator/schema/physical_create_schema.hpp"
#include "duckdb/execution/operator/schema/physical_create_sequence.hpp"
#include "duckdb/execution/operator/schema/physical_create_view.hpp"
#include "duckdb/execution/operator/schema/physical_drop.hpp"
#include "duckdb/execution/operator/helper/physical_vacuum.hpp"

using namespace duckdb;
using namespace std;

unique_ptr<PhysicalOperator> PhysicalPlanGenerator::CreatePlan(LogicalSimple &op) {
	switch (op.type) {
	case LogicalOperatorType::ALTER:
		return make_unique<PhysicalAlter>(unique_ptr_cast<ParseInfo, AlterInfo>(move(op.info)));
	case LogicalOperatorType::DROP:
		return make_unique<PhysicalDrop>(unique_ptr_cast<ParseInfo, DropInfo>(move(op.info)));
	case LogicalOperatorType::PRAGMA:
		return make_unique<PhysicalPragma>(unique_ptr_cast<ParseInfo, PragmaInfo>(move(op.info)));
	case LogicalOperatorType::TRANSACTION:
		return make_unique<PhysicalTransaction>(unique_ptr_cast<ParseInfo, TransactionInfo>(move(op.info)));
	case LogicalOperatorType::VACUUM:
		return make_unique<PhysicalVacuum>(unique_ptr_cast<ParseInfo, VacuumInfo>(move(op.info)));
	default:
		throw NotImplementedException("Unimplemented type for logical simple operator");
	}
}
