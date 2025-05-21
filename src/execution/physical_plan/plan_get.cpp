#include "duckdb/execution/operator/scan/physical_expression_scan.hpp"
#include "duckdb/execution/operator/scan/physical_dummy_scan.hpp"
#include "duckdb/execution/operator/projection/physical_projection.hpp"
#include "duckdb/execution/operator/projection/physical_tableinout_function.hpp"
#include "duckdb/execution/operator/scan/physical_table_scan.hpp"
#include "duckdb/execution/physical_plan_generator.hpp"
#include "duckdb/function/table/table_scan.hpp"
#include "duckdb/planner/expression/bound_cast_expression.hpp"
#include "duckdb/planner/expression/bound_constant_expression.hpp"
#include "duckdb/planner/expression/bound_reference_expression.hpp"
#include "duckdb/planner/operator/logical_get.hpp"
#include "duckdb/planner/expression/bound_conjunction_expression.hpp"
#include "duckdb/execution/operator/filter/physical_filter.hpp"

namespace duckdb {

unique_ptr<TableFilterSet> CreateTableFilterSet(TableFilterSet &table_filters, const vector<ColumnIndex> &column_ids) {
	// create the table filter map
	auto table_filter_set = make_uniq<TableFilterSet>();
	for (auto &table_filter : table_filters.filters) {
		// find the relative column index from the absolute column index into the table
		optional_idx column_index;
		for (idx_t i = 0; i < column_ids.size(); i++) {
			if (table_filter.first == column_ids[i].GetPrimaryIndex()) {
				column_index = i;
				break;
			}
		}
		if (!column_index.IsValid()) {
			throw InternalException("Could not find column index for table filter");
		}
		table_filter_set->filters[column_index.GetIndex()] = std::move(table_filter.second);
	}
	return table_filter_set;
}

PhysicalOperator &PhysicalPlanGenerator::CreatePlan(LogicalGet &op) {
	auto column_ids = op.GetColumnIds();
	if (!op.children.empty()) {
		reference<PhysicalOperator> child = ResolveAndPlan(std::move(op.children[0]));
		auto &child_types = child.get().types;

		// this is for table producing functions that consume subquery results
		// push a projection node with casts if required
		if (child_types.size() < op.input_table_types.size()) {
			throw InternalException(
			    "Mismatch between input table types and child node types - expected %llu but got %llu",
			    op.input_table_types.size(), child_types.size());
		}

		vector<LogicalType> return_types;
		vector<unique_ptr<Expression>> expressions;
		bool any_cast_required = false;
		for (idx_t proj_idx = 0; proj_idx < child_types.size(); proj_idx++) {
			auto ref = make_uniq<BoundReferenceExpression>(child_types[proj_idx], proj_idx);
			auto &target_type =
			    proj_idx < op.input_table_types.size() ? op.input_table_types[proj_idx] : child_types[proj_idx];
			if (child_types[proj_idx] != target_type) {
				// cast is required - push a cast
				any_cast_required = true;
				auto cast = BoundCastExpression::AddCastToType(context, std::move(ref), target_type);
				expressions.push_back(std::move(cast));
			} else {
				expressions.push_back(std::move(ref));
			}
			return_types.push_back(target_type);
		}

		if (any_cast_required) {
			auto &proj = Make<PhysicalProjection>(std::move(return_types), std::move(expressions),
			                                      child.get().estimated_cardinality);
			proj.children.push_back(child);
			child = proj;
		}

		auto &table_in_out =
		    Make<PhysicalTableInOutFunction>(op.types, op.function, std::move(op.bind_data), column_ids,
		                                     op.estimated_cardinality, std::move(op.projected_input));
		table_in_out.children.push_back(child);
		return table_in_out;
	}

	if (!op.projected_input.empty()) {
		throw InternalException("LogicalGet::project_input can only be set for table-in-out functions");
	}

	unique_ptr<TableFilterSet> table_filters;
	if (!op.table_filters.filters.empty()) {
		table_filters = CreateTableFilterSet(op.table_filters, column_ids);
	}

	if (op.function.dependency) {
		op.function.dependency(dependencies, op.bind_data.get());
	}

	optional_ptr<PhysicalOperator> filter;
	auto &projection_ids = op.projection_ids;

	if (table_filters && op.function.supports_pushdown_type) {
		vector<unique_ptr<Expression>> select_list;
		unique_ptr<Expression> unsupported_filter;
		unordered_set<idx_t> to_remove;
		for (auto &entry : table_filters->filters) {
			auto column_id = column_ids[entry.first].GetPrimaryIndex();
			auto &type = op.returned_types[column_id];
			if (!op.function.supports_pushdown_type(type)) {
				idx_t column_id_filter = entry.first;
				bool found_projection = false;
				for (idx_t i = 0; i < projection_ids.size(); i++) {
					if (column_ids[projection_ids[i]] == column_ids[entry.first]) {
						column_id_filter = i;
						found_projection = true;
						break;
					}
				}
				if (!found_projection) {
					projection_ids.push_back(entry.first);
					column_id_filter = projection_ids.size() - 1;
				}
				auto column = make_uniq<BoundReferenceExpression>(type, column_id_filter);
				select_list.push_back(entry.second->ToExpression(*column));
				to_remove.insert(entry.first);
			}
		}
		for (auto &col : to_remove) {
			table_filters->filters.erase(col);
		}

		if (!select_list.empty()) {
			vector<LogicalType> filter_types;
			for (auto &c : projection_ids) {
				auto column_id = column_ids[c].GetPrimaryIndex();
				filter_types.push_back(op.returned_types[column_id]);
			}
			filter = Make<PhysicalFilter>(filter_types, std::move(select_list), op.estimated_cardinality);
		}
	}
	op.ResolveOperatorTypes();
	// create the table scan node
	if (!op.function.projection_pushdown) {
		// function does not support projection pushdown
		auto &table_scan = Make<PhysicalTableScan>(op.returned_types, op.function, std::move(op.bind_data),
		                                           op.returned_types, column_ids, vector<column_t>(), op.names,
		                                           std::move(table_filters), op.estimated_cardinality, op.extra_info,
		                                           std::move(op.parameters), std::move(op.virtual_columns));
		// first check if an additional projection is necessary
		if (column_ids.size() == op.returned_types.size()) {
			bool projection_necessary = false;
			for (idx_t i = 0; i < column_ids.size(); i++) {
				if (column_ids[i].GetPrimaryIndex() != i) {
					projection_necessary = true;
					break;
				}
			}
			if (!projection_necessary) {
				// a projection is not necessary if all columns have been requested in-order
				// in that case we just return the node
				if (filter) {
					filter->children.push_back(table_scan);
					return *filter;
				}
				return table_scan;
			}
		}
		// push a projection on top that does the projection
		vector<LogicalType> types;
		vector<unique_ptr<Expression>> expressions;
		for (auto &column_id : column_ids) {
			if (column_id.IsVirtualColumn()) {
				throw NotImplementedException("Virtual columns require projection pushdown");
			} else {
				auto col_id = column_id.GetPrimaryIndex();
				auto type = op.returned_types[col_id];
				types.push_back(type);
				expressions.push_back(make_uniq<BoundReferenceExpression>(type, col_id));
			}
		}
		auto &proj = Make<PhysicalProjection>(std::move(types), std::move(expressions), op.estimated_cardinality);
		if (filter) {
			filter->children.push_back(table_scan);
			proj.children.push_back(*filter);
			return proj;
		}
		proj.children.push_back(table_scan);
		return proj;
	}

	auto &table_scan =
	    Make<PhysicalTableScan>(op.types, op.function, std::move(op.bind_data), op.returned_types, column_ids,
	                            op.projection_ids, op.names, std::move(table_filters), op.estimated_cardinality,
	                            op.extra_info, std::move(op.parameters), std::move(op.virtual_columns));
	auto &cast_table_scan = table_scan.Cast<PhysicalTableScan>();
	cast_table_scan.dynamic_filters = op.dynamic_filters;
	if (filter) {
		filter->children.push_back(table_scan);
		return *filter;
	}
	return table_scan;
}

} // namespace duckdb
