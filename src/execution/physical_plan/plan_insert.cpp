#include "duckdb/catalog/catalog_entry/duck_table_entry.hpp"
#include "duckdb/catalog/catalog_entry/table_catalog_entry.hpp"
#include "duckdb/execution/operator/persistent/physical_insert.hpp"
#include "duckdb/execution/physical_plan_generator.hpp"
#include "duckdb/planner/operator/logical_insert.hpp"
#include "duckdb/main/config.hpp"
#include "duckdb/execution/operator/persistent/physical_batch_insert.hpp"
#include "duckdb/execution/operator/projection/physical_projection.hpp"
#include "duckdb/parallel/task_scheduler.hpp"
#include "duckdb/catalog/duck_catalog.hpp"
#include "duckdb/planner/expression/bound_reference_expression.hpp"
#include "duckdb/main/settings.hpp"

#include "duckdb/function/scalar/struct_functions.hpp"

namespace duckdb {

OrderPreservationType PhysicalPlanGenerator::OrderPreservationRecursive(PhysicalOperator &op) {
	if (op.IsSource()) {
		return op.SourceOrder();
	}

	idx_t child_idx = 0;
	for (auto &child : op.children) {
		// Do not take the materialization phase of physical CTEs into account
		if (op.type == PhysicalOperatorType::CTE && child_idx == 0) {
			child_idx++;
			continue;
		}
		auto child_preservation = OrderPreservationRecursive(child);
		if (child_preservation != OrderPreservationType::INSERTION_ORDER) {
			return child_preservation;
		}
		child_idx++;
	}
	return OrderPreservationType::INSERTION_ORDER;
}

bool PhysicalPlanGenerator::PreserveInsertionOrder(ClientContext &context, PhysicalOperator &plan) {
	auto preservation_type = OrderPreservationRecursive(plan);
	if (preservation_type == OrderPreservationType::FIXED_ORDER) {
		// always need to maintain preservation order
		return true;
	}
	if (preservation_type == OrderPreservationType::NO_ORDER) {
		// never need to preserve order
		return false;
	}
	// preserve insertion order - check flags
	if (!Settings::Get<PreserveInsertionOrderSetting>(context)) {
		// preserving insertion order is disabled by config
		return false;
	}
	return true;
}

bool PhysicalPlanGenerator::PreserveInsertionOrder(PhysicalOperator &plan) {
	return PreserveInsertionOrder(context, plan);
}

bool PhysicalPlanGenerator::UseBatchIndex(ClientContext &context, PhysicalOperator &plan) {
	auto &scheduler = TaskScheduler::GetScheduler(context);
	if (scheduler.NumberOfThreads() == 1) {
		// batch index usage only makes sense if we are using multiple threads
		return false;
	}
	if (!plan.AllSourcesSupportBatchIndex()) {
		// batch index is not supported
		return false;
	}
	return true;
}

bool PhysicalPlanGenerator::UseBatchIndex(PhysicalOperator &plan) {
	return UseBatchIndex(context, plan);
}

namespace {

//! Used to determine if the field of a struct is mapped or not
struct StructFieldMapping {
	case_insensitive_map_t<StructFieldMapping> child_mapping;
};

} // namespace

static Value CreateStructMapping(const LogicalType &struct_type, const string &name) {
	child_list_t<Value> field_mapping;

	auto &struct_children = StructType::GetChildTypes(struct_type);
	for (auto &[field_name, field_type] : struct_children) {
		Value mapping;
		if (field_type.id() == LogicalTypeId::STRUCT) {
			mapping = CreateStructMapping(field_type, field_name);
		} else {
			mapping = Value(field_name);
		}
		field_mapping.emplace_back(field_name, mapping);
	}
	auto struct_value = Value::STRUCT(field_mapping);
	if (name.empty()) {
		//! Root column
		return struct_value;
	}
	return Value::STRUCT({{"", Value(name)}, {"", struct_value}});
}

static Value CreateStructDefault(const Value &value, const case_insensitive_map_t<StructFieldMapping> &mapping = {}) {
	child_list_t<Value> field_defaults;
	auto &field_values = StructValue::GetChildren(value);
	auto &struct_children = StructType::GetChildTypes(value.type());
	for (idx_t j = 0; j < field_values.size(); j++) {
		auto &field_name = struct_children[j].first;
		auto &field_type = struct_children[j].second;
		auto &field_value = field_values[j];

		auto it = mapping.find(field_name);
		const bool is_mapped = it != mapping.end();

		Value field_default;
		if (field_type.id() == LogicalTypeId::STRUCT) {
			if (is_mapped) {
				field_default = CreateStructDefault(field_value, it->second.child_mapping);
			} else {
				field_default = CreateStructDefault(field_value);
			}

			if (field_default.IsNull()) {
				//! All fields were skipped, no need to include this value
				continue;
			}
		} else {
			if (is_mapped) {
				continue;
			}
			field_default = field_value;
		}

		field_defaults.emplace_back(field_name, field_default);
	}
	if (field_defaults.empty()) {
		//! Skipped all fields, signal that the value should be omitted
		return Value();
	}
	return Value::STRUCT(field_defaults);
}

PhysicalOperator &PhysicalPlanGenerator::ResolveDefaultsProjection(LogicalInsert &op, PhysicalOperator &child) {
	if (op.column_index_map.empty()) {
		throw InternalException("No defaults to push");
	}
	// columns specified by the user, push a projection
	vector<LogicalType> types;
	vector<unique_ptr<Expression>> select_list;
	for (auto &col : op.table.GetColumns().Physical()) {
		auto storage_idx = col.StorageOid();
		auto mapped_index = op.column_index_map[col.Physical()];
		if (mapped_index == DConstants::INVALID_INDEX) {
			// push default value
			select_list.push_back(std::move(op.bound_defaults[storage_idx]));
		} else {
			//! Column is present
			if (col.Type().id() == LogicalTypeId::STRUCT) {
				auto &original_type = child.children[0].get().types[mapped_index];
				//! Column is of type STRUCT, create a COALESCE of the default with the input
				vector<unique_ptr<Expression>> children;
				auto &struct_children = StructType::GetChildTypes(original_type);
				auto &bound_default = op.bound_defaults[storage_idx]->Cast<BoundConstantExpression>();
				if (!bound_default.value.IsNull()) {
					//! input
					children.push_back(BoundCastExpression::AddDefaultCastToType(make_uniq<BoundReferenceExpression>(col.Type(), mapped_index), original_type));
					//! target_type
					children.push_back(make_uniq<BoundConstantExpression>(Value(col.Type())));

					//! mapping
					children.push_back(make_uniq<BoundConstantExpression>(CreateStructMapping(original_type, "")));

					//! defaults
					case_insensitive_map_t<StructFieldMapping> mapping;
					mapping["b"].child_mapping["d"];
					children.push_back(make_uniq<BoundConstantExpression>(CreateStructDefault(bound_default.value, mapping)));
					select_list.push_back(RemapStructFun::GetFunction().Bind(context, std::move(children)));
				} else {
					select_list.push_back(make_uniq<BoundReferenceExpression>(col.Type(), mapped_index));
				}
			} else {
				// push reference
				select_list.push_back(make_uniq<BoundReferenceExpression>(col.Type(), mapped_index));
			}
		}
		types.push_back(col.Type());
	}
	auto &proj = Make<PhysicalProjection>(std::move(types), std::move(select_list), child.estimated_cardinality);
	proj.children.push_back(child);
	return proj;
}

PhysicalOperator &DuckCatalog::PlanInsert(ClientContext &context, PhysicalPlanGenerator &planner, LogicalInsert &op,
                                          optional_ptr<PhysicalOperator> plan) {
	D_ASSERT(plan);
	bool parallel_streaming_insert = !PhysicalPlanGenerator::PreserveInsertionOrder(context, *plan);
	bool use_batch_index = PhysicalPlanGenerator::UseBatchIndex(context, *plan);
	auto num_threads = TaskScheduler::GetScheduler(context).NumberOfThreads();
	if (op.return_chunk) {
		// not supported for RETURNING (yet?)
		parallel_streaming_insert = false;
		use_batch_index = false;
	}
	if (op.on_conflict_info.action_type != OnConflictAction::THROW) {
		// We don't support ON CONFLICT clause in batch insertion operation currently
		use_batch_index = false;
	}
	if (op.on_conflict_info.action_type == OnConflictAction::UPDATE) {
		// When we potentially need to perform updates, we have to check that row is not updated twice
		// that currently needs to be done for every chunk, which would add a huge bottleneck to parallelized insertion
		parallel_streaming_insert = false;
	}
	if (!op.column_index_map.empty()) {
		plan = planner.ResolveDefaultsProjection(op, *plan);
	}
	if (use_batch_index && !parallel_streaming_insert) {
		auto &insert = planner.Make<PhysicalBatchInsert>(op.types, op.table.Cast<DuckTableEntry>(),
		                                                 std::move(op.bound_constraints), op.estimated_cardinality);
		insert.children.push_back(*plan);
		return insert;
	}

	auto &insert = planner.Make<PhysicalInsert>(
	    op.types, op.table.Cast<DuckTableEntry>(), std::move(op.bound_constraints), std::move(op.expressions),
	    std::move(op.on_conflict_info.set_columns), std::move(op.on_conflict_info.set_types), op.estimated_cardinality,
	    op.return_chunk, parallel_streaming_insert && num_threads > 1, op.on_conflict_info.action_type,
	    std::move(op.on_conflict_info.on_conflict_condition), std::move(op.on_conflict_info.do_update_condition),
	    std::move(op.on_conflict_info.on_conflict_filter), std::move(op.on_conflict_info.columns_to_fetch),
	    op.on_conflict_info.update_is_del_and_insert);
	insert.children.push_back(*plan);
	return insert;
}

PhysicalOperator &PhysicalPlanGenerator::CreatePlan(LogicalInsert &op) {
	optional_ptr<PhysicalOperator> plan;
	if (!op.children.empty()) {
		D_ASSERT(op.children.size() == 1);
		plan = CreatePlan(*op.children[0]);
	}
	dependencies.AddDependency(op.table);
	return op.table.catalog.PlanInsert(context, *this, op, plan);
}

} // namespace duckdb
