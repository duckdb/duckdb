#include "duckdb/execution/operator/helper/physical_load.hpp"
#include "duckdb/execution/operator/helper/physical_transaction.hpp"
#include "duckdb/execution/operator/helper/physical_vacuum.hpp"
#include "duckdb/execution/operator/helper/physical_update_extensions.hpp"
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
#include "duckdb/parser/tableref/basetableref.hpp"

namespace duckdb {

//! FIXME: Creating a separate LogicalAlter throws off JSON generation because LOGICAL_ALTER is an enum of
//! LogicalSimple.
unique_ptr<PhysicalOperator> CreatePhysicalAlter(PhysicalPlanGenerator &planner, unique_ptr<AlterInfo> alter_info,
                                                 const idx_t estimated_cardinality) {

	if (alter_info->type != AlterType::ALTER_TABLE) {
		return make_uniq<PhysicalAlter>(std::move(alter_info), estimated_cardinality);
	}

	auto &table_info = alter_info->Cast<AlterTableInfo>();
	if (table_info.alter_table_type != AlterTableType::ADD_CONSTRAINT) {
		return make_uniq<PhysicalAlter>(std::move(alter_info), estimated_cardinality);
	}

	auto &constraint_info = table_info.Cast<AddConstraintInfo>();
	if (constraint_info.constraint->type != ConstraintType::UNIQUE) {
		return make_uniq<PhysicalAlter>(std::move(alter_info), estimated_cardinality);
	}

	auto &unique_info = constraint_info.constraint->Cast<UniqueConstraint>();
	if (!unique_info.IsPrimaryKey()) {
		return make_uniq<PhysicalAlter>(std::move(alter_info), estimated_cardinality);
	}

	CreateIndexInfo index_info;
	index_info.table = table_info.name; // TODO: catalog? schema?
	// TODO: index_info.index_name = ...
	index_info.index_type = "ART";
	index_info.constraint_type = IndexConstraintType::PRIMARY;
	// TODO: index_info.column_ids = ...

	auto table_ref = make_uniq<BaseTableRef>();
	table_ref->catalog_name = index_info.catalog;
	table_ref->schema_name = index_info.schema;
	table_ref->table_name = index_info.table;
	//
	//	auto binder = Binder::CreateBinder(planner.context);
	//	auto bound_table = binder.Bind(*table_ref);

	return make_uniq<PhysicalAlter>(std::move(alter_info), estimated_cardinality);
}

unique_ptr<PhysicalOperator> PhysicalPlanGenerator::CreatePlan(LogicalSimple &op) {
	switch (op.type) {
	case LogicalOperatorType::LOGICAL_ALTER:
		return CreatePhysicalAlter(*this, unique_ptr_cast<ParseInfo, AlterInfo>(std::move(op.info)),
		                           op.estimated_cardinality);
	case LogicalOperatorType::LOGICAL_DROP:
		return make_uniq<PhysicalDrop>(unique_ptr_cast<ParseInfo, DropInfo>(std::move(op.info)),
		                               op.estimated_cardinality);
	case LogicalOperatorType::LOGICAL_TRANSACTION:
		return make_uniq<PhysicalTransaction>(unique_ptr_cast<ParseInfo, TransactionInfo>(std::move(op.info)),
		                                      op.estimated_cardinality);
	case LogicalOperatorType::LOGICAL_LOAD:
		return make_uniq<PhysicalLoad>(unique_ptr_cast<ParseInfo, LoadInfo>(std::move(op.info)),
		                               op.estimated_cardinality);
	case LogicalOperatorType::LOGICAL_ATTACH:
		return make_uniq<PhysicalAttach>(unique_ptr_cast<ParseInfo, AttachInfo>(std::move(op.info)),
		                                 op.estimated_cardinality);
	case LogicalOperatorType::LOGICAL_DETACH:
		return make_uniq<PhysicalDetach>(unique_ptr_cast<ParseInfo, DetachInfo>(std::move(op.info)),
		                                 op.estimated_cardinality);
	case LogicalOperatorType::LOGICAL_UPDATE_EXTENSIONS:
		return make_uniq<PhysicalUpdateExtensions>(unique_ptr_cast<ParseInfo, UpdateExtensionsInfo>(std::move(op.info)),
		                                           op.estimated_cardinality);
	default:
		throw NotImplementedException("Unimplemented type for logical simple operator");
	}
}

} // namespace duckdb
