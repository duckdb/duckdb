#include "duckdb/execution/operator/scan/physical_table_scan.hpp"
#include "duckdb/catalog/catalog_entry/table_catalog_entry.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/planner/expression/bound_conjunction_expression.hpp"
#include "duckdb/transaction/transaction.hpp"
#include "duckdb/optimizer/cascade/search/CGroupExpression.h"
#include <utility>
#include "duckdb/optimizer/cascade/base/CUtils.h"
#include "duckdb/planner/filter/conjunction_filter.hpp"
#include "duckdb/optimizer/cascade/base/CDrvdPropRelational.h"
#include "duckdb/function/table/table_scan.hpp"

namespace duckdb
{
using namespace gpopt;

PhysicalTableScan::PhysicalTableScan(vector<LogicalType> types, TableFunction function_p, unique_ptr<FunctionData> bind_data_p, vector<column_t> column_ids_p, vector<string> names_p, unique_ptr<TableFilterSet> table_filters_p, idx_t estimated_cardinality)
    : PhysicalOperator(PhysicalOperatorType::TABLE_SCAN, std::move(types), estimated_cardinality), function(std::move(function_p)), bind_data(std::move(bind_data_p)), column_ids(std::move(column_ids_p)), names(std::move(names_p)), table_filters(std::move(table_filters_p))
{
	physical_type = PhysicalOperatorType::TABLE_SCAN;
	m_derived_property_relation = new CDrvdPropRelational();
	m_group_expression = nullptr;
	m_derived_property_plan = nullptr;
	m_required_plan_property = nullptr;
	m_ulTotalOptRequests = 1;
}

PhysicalTableScan::PhysicalTableScan(vector<LogicalType> types, TableFunction function_p, unique_ptr<FunctionData> bind_data_p, vector<LogicalType> returned_types_p, vector<column_t> column_ids_p, vector<idx_t> projection_ids_p, vector<string> names_p, unique_ptr<TableFilterSet> table_filters_p, idx_t estimated_cardinality)
    : PhysicalOperator(PhysicalOperatorType::TABLE_SCAN, std::move(types), estimated_cardinality), function(std::move(function_p)), bind_data(std::move(bind_data_p)), returned_types(std::move(returned_types_p)), column_ids(std::move(column_ids_p)), projection_ids(std::move(projection_ids_p)), names(std::move(names_p)), table_filters(std::move(table_filters_p))
{
	physical_type = PhysicalOperatorType::TABLE_SCAN;
	m_derived_property_relation = new CDrvdPropRelational();
}

class TableScanGlobalSourceState : public GlobalSourceState
{
public:
	TableScanGlobalSourceState(ClientContext &context, const PhysicalTableScan &op)
	{
		if (op.function.init_global)
		{
			TableFunctionInitInput input(op.bind_data.get(), op.column_ids, op.projection_ids, op.table_filters.get());
			global_state = op.function.init_global(context, input);
			if (global_state)
			{
				max_threads = global_state->MaxThreads();
			}
		}
		else
		{
			max_threads = 1;
		}
	}
	idx_t max_threads = 0;
	unique_ptr<GlobalTableFunctionState> global_state;
	idx_t MaxThreads() override
	{
		return max_threads;
	}
};

class TableScanLocalSourceState : public LocalSourceState
{
public:
	TableScanLocalSourceState(ExecutionContext &context, TableScanGlobalSourceState &gstate, const PhysicalTableScan &op)
	{
		if (op.function.init_local)
		{
			TableFunctionInitInput input(op.bind_data.get(), op.column_ids, op.projection_ids, op.table_filters.get());
			local_state = op.function.init_local(context, input, gstate.global_state.get());
		}
	}
	unique_ptr<LocalTableFunctionState> local_state;
};

unique_ptr<LocalSourceState> PhysicalTableScan::GetLocalSourceState(ExecutionContext &context, GlobalSourceState &gstate) const
{
	return make_uniq<TableScanLocalSourceState>(context, gstate.Cast<TableScanGlobalSourceState>(), *this);
}

unique_ptr<GlobalSourceState> PhysicalTableScan::GetGlobalSourceState(ClientContext &context) const
{
	return make_uniq<TableScanGlobalSourceState>(context, *this);
}

void PhysicalTableScan::GetData(ExecutionContext &context, DataChunk &chunk, GlobalSourceState &gstate_p, LocalSourceState &lstate) const
{
	D_ASSERT(!column_ids.empty());
	auto &gstate = gstate_p.Cast<TableScanGlobalSourceState>();
	auto &state = lstate.Cast<TableScanLocalSourceState>();
	TableFunctionInput data(bind_data.get(), state.local_state.get(), gstate.global_state.get());
	function.function(context.client, data, chunk);
}

double PhysicalTableScan::GetProgress(ClientContext &context, GlobalSourceState &gstate_p) const
{
	auto &gstate = gstate_p.Cast<TableScanGlobalSourceState>();
	if (function.table_scan_progress)
	{
		return function.table_scan_progress(context, bind_data.get(), gstate.global_state.get());
	}
	// if table_scan_progress is not implemented we don't support this function yet in the progress bar
	return -1;
}

idx_t PhysicalTableScan::GetBatchIndex(ExecutionContext &context, DataChunk &chunk, GlobalSourceState &gstate_p, LocalSourceState &lstate) const
{
	D_ASSERT(SupportsBatchIndex());
	D_ASSERT(function.get_batch_index);
	auto &gstate = gstate_p.Cast<TableScanGlobalSourceState>();
	auto &state = lstate.Cast<TableScanLocalSourceState>();
	return function.get_batch_index(context.client, bind_data.get(), state.local_state.get(), gstate.global_state.get());
}

string PhysicalTableScan::GetName() const
{
	return StringUtil::Upper(function.name + " " + function.extra_info);
}

string PhysicalTableScan::ParamsToString() const
{
	string result;
	if (function.to_string)
	{
		result = function.to_string(bind_data.get());
		result += "\n[INFOSEPARATOR]\n";
	}
	if (function.projection_pushdown)
	{
		if (function.filter_prune) {
			for (idx_t i = 0; i < projection_ids.size(); i++)
			{
				const auto &column_id = column_ids[projection_ids[i]];
				if (column_id < names.size())
				{
					if (i > 0)
					{
						result += "\n";
					}
					result += names[column_id];
				}
			}
		}
		else
		{
			for (idx_t i = 0; i < column_ids.size(); i++)
			{
				const auto &column_id = column_ids[i];
				if (column_id < names.size())
				{
					if (i > 0)
					{
						result += "\n";
					}
					result += names[column_id];
				}
			}
		}
	}
	if (function.filter_pushdown && table_filters)
	{
		result += "\n[INFOSEPARATOR]\n";
		result += "Filters: ";
		for (auto &f : table_filters->filters)
		{
			auto &column_index = f.first;
			auto &filter = f.second;
			if (column_index < names.size())
			{
				result += filter->ToString(names[column_ids[column_index]]);
				result += "\n";
			}
		}
	}
	result += "\n[INFOSEPARATOR]\n";
	result += StringUtil::Format("EC: %llu", estimated_props->GetCardinality<idx_t>());
	return result;
}

bool PhysicalTableScan::Equals(const PhysicalOperator &other_p) const
{
	if (physical_type != other_p.physical_type)
	{
		return false;
	}
	auto &other = other_p.Cast<PhysicalTableScan>();
	if (function.function != other.function.function)
	{
		return false;
	}
	if (column_ids != other.column_ids)
	{
		return false;
	}
	if (!FunctionData::Equals(bind_data.get(), other.bind_data.get()))
	{
		return false;
	}
	return true;
}

//---------------------------------------------------------------------------
//	@function:
//		PhysicalTableScan::EpetOrder
//
//	@doc:
//		Return the enforcing type for order property based on this operator
//
//---------------------------------------------------------------------------
CEnfdOrder::EPropEnforcingType PhysicalTableScan::EpetOrder(CExpressionHandle &exprhdl, vector<BoundOrderByNode> peo) const
{
	return CEnfdOrder::EpetRequired;
}

//---------------------------------------------------------------------------
//	@function:
//		PhysicalTableScan::FProvidesReqdCols
//
//	@doc:
//		Check if required columns are included in output columns
//
//---------------------------------------------------------------------------
BOOL PhysicalTableScan::FProvidesReqdCols(CExpressionHandle &exprhdl, vector<ColumnBinding> pcrsRequired, ULONG ulOptReq) const
{
	vector<ColumnBinding> pcrs;
	for(auto& child : projection_ids)
	{
		pcrs.emplace_back(ColumnBinding(0, child));
	}
	BOOL result = CUtils::ContainsAll(pcrs, pcrsRequired);
	return result;
}

vector<ColumnBinding> PhysicalTableScan::GetColumnBindings()
{
	if (column_ids.empty())
	{
		return {ColumnBinding(0, 0)};
	}
	vector<ColumnBinding> result;
	if (projection_ids.empty())
	{
		for (idx_t col_idx = 0; col_idx < column_ids.size(); col_idx++)
		{
			result.emplace_back(0, col_idx);
		}
	}
	else
	{
		for (auto proj_id : projection_ids)
		{
			result.emplace_back(0, proj_id);
		}
	}
	return result;
}

CKeyCollection* PhysicalTableScan::DeriveKeyCollection(CExpressionHandle &exprhdl)
{
	return NULL;
}

CPropConstraint* PhysicalTableScan::DerivePropertyConstraint(CExpressionHandle &exprhdl)
{
	return NULL;
}

ULONG PhysicalTableScan::DeriveJoinDepth(CExpressionHandle &exprhdl)
{
	return 1;
}

// Rehydrate expression from a given cost context and child expressions
Operator* PhysicalTableScan::SelfRehydrate(CCostContext* pcc, duckdb::vector<Operator*> pdrgpexpr, CDrvdPropCtxtPlan* pdpctxtplan)
{
	CGroupExpression* pgexpr = pcc->m_group_expression;
	double cost = pcc->m_cost;
	// if (!table_filters->filters.empty())
	// {
	// 	copy_table_filters = CreateTableFilterSet(*table_filters, column_ids);
	// }
	TableFunction tmp_function(function.name, function.arguments, function.function, function.bind, function.init_global, function.init_local);
	// unique_ptr<TableFunctionData> tmp_bind_data = make_uniq<TableFunctionData>();
	unique_ptr<TableScanBindData> tmp_bind_data = make_uniq<TableScanBindData>(bind_data->Cast<TableScanBindData>().table);
	tmp_bind_data->column_ids = bind_data->Cast<TableFunctionData>().column_ids;
	duckdb::unique_ptr<TableFilterSet> table_filters;
	PhysicalTableScan* pexpr = new PhysicalTableScan(returned_types, tmp_function, std::move(tmp_bind_data), column_ids, names, std::move(table_filters), 1);
	pexpr->m_cost = cost;
	pexpr->m_group_expression = pgexpr;
	return pexpr;
}

duckdb::unique_ptr<Operator> PhysicalTableScan::Copy()
{
	TableFunction tmp_function(function.name, function.arguments, function.function, function.bind, function.init_global, function.init_local);
	// unique_ptr<TableFunctionData> tmp_bind_data = make_uniq<TableFunctionData>();
	unique_ptr<TableScanBindData> tmp_bind_data = make_uniq<TableScanBindData>(bind_data->Cast<TableScanBindData>().table);
	tmp_bind_data->column_ids = bind_data->Cast<TableFunctionData>().column_ids;
	duckdb::unique_ptr<TableFilterSet> table_filters;
	unique_ptr<PhysicalTableScan> result = make_uniq<PhysicalTableScan>(returned_types, tmp_function, std::move(tmp_bind_data), column_ids, names, std::move(table_filters), 1);
	result->m_derived_property_relation = m_derived_property_relation;
	result->m_derived_property_plan = m_derived_property_plan;
	result->m_required_plan_property = m_required_plan_property;
	if(nullptr != estimated_props)
	{
		result->estimated_props = estimated_props->Copy();
	}
	result->types = types;
	result->estimated_cardinality = estimated_cardinality;
	result->has_estimated_cardinality = has_estimated_cardinality;
	result->logical_type = logical_type;
	result->physical_type = physical_type;
	for(auto &child : children)
	{
		result->AddChild(child->Copy());
	}
	result->m_group_expression = m_group_expression;
	result->m_cost = m_cost;
	return result;
}

duckdb::unique_ptr<Operator> PhysicalTableScan::CopyWithNewGroupExpression(CGroupExpression* pgexpr)
{
	TableFunction tmp_function(function.name, function.arguments, function.function, function.bind, function.init_global, function.init_local);
	// unique_ptr<TableFunctionData> tmp_bind_data = make_uniq<TableFunctionData>();
	unique_ptr<TableScanBindData> tmp_bind_data = make_uniq<TableScanBindData>(bind_data->Cast<TableScanBindData>().table);
	tmp_bind_data->column_ids = bind_data->Cast<TableFunctionData>().column_ids;
	duckdb::unique_ptr<TableFilterSet> table_filters;
	unique_ptr<PhysicalTableScan> result = make_uniq<PhysicalTableScan>(returned_types, tmp_function, std::move(tmp_bind_data), column_ids, names, std::move(table_filters), 1);
	result->m_derived_property_relation = m_derived_property_relation;
	result->m_derived_property_plan = m_derived_property_plan;
	result->m_required_plan_property = m_required_plan_property;
	if(nullptr != estimated_props)
	{
		result->estimated_props = estimated_props->Copy();
	}
	result->types = types;
	result->estimated_cardinality = estimated_cardinality;
	result->has_estimated_cardinality = has_estimated_cardinality;
	result->logical_type = logical_type;
	result->physical_type = physical_type;
	for(auto &child : children)
	{
		result->AddChild(child->Copy());
	}
	result->m_group_expression = pgexpr;
	result->m_cost = m_cost;
	return result;
}
	
duckdb::unique_ptr<Operator> PhysicalTableScan::CopyWithNewChildren(CGroupExpression* pgexpr, duckdb::vector<duckdb::unique_ptr<Operator>> pdrgpexpr, double cost)
{
	TableFunction tmp_function(function.name, function.arguments, function.function, function.bind, function.init_global, function.init_local);
	// unique_ptr<TableFunctionData> tmp_bind_data = make_uniq<TableFunctionData>();
	unique_ptr<TableScanBindData> tmp_bind_data = make_uniq<TableScanBindData>(bind_data->Cast<TableScanBindData>().table);
	tmp_bind_data->column_ids = bind_data->Cast<TableFunctionData>().column_ids;
	duckdb::unique_ptr<TableFilterSet> table_filters;
	unique_ptr<PhysicalTableScan> result = make_uniq<PhysicalTableScan>(returned_types, tmp_function, std::move(tmp_bind_data), column_ids, names, std::move(table_filters), 1);
	result->m_derived_property_relation = m_derived_property_relation;
	result->m_derived_property_plan = m_derived_property_plan;
	result->m_required_plan_property = m_required_plan_property;
	if(nullptr != estimated_props)
	{
		result->estimated_props = estimated_props->Copy();
	}
	result->types = types;
	result->estimated_cardinality = estimated_cardinality;
	result->has_estimated_cardinality = has_estimated_cardinality;
	result->logical_type = logical_type;
	result->physical_type = physical_type;
	for(auto &child : pdrgpexpr)
	{
		result->AddChild(child->Copy());
	}
	result->m_group_expression = pgexpr;
	result->m_cost = cost;
	return result;
}
} // namespace duckdb
