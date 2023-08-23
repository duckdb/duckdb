#include "duckdb/execution/operator/projection/physical_projection.hpp"
#include "duckdb/parallel/thread_context.hpp"
#include "duckdb/execution/expression_executor.hpp"
#include "duckdb/planner/expression/bound_reference_expression.hpp"
#include "duckdb/optimizer/cascade/base/CDrvdPropRelational.h"

namespace duckdb
{
class ProjectionState : public OperatorState
{
public:
	explicit ProjectionState(ExecutionContext &context, const vector<unique_ptr<Expression>> &expressions)
	    : executor(context.client, expressions)
	{
	}

	ExpressionExecutor executor;

public:
	void Finalize(const PhysicalOperator &op, ExecutionContext &context) override
	{
		context.thread.profiler.Flush(op, executor, "projection", 0);
	}
};

PhysicalProjection::PhysicalProjection(vector<LogicalType> types, vector<unique_ptr<Expression>> select_list, idx_t estimated_cardinality)
    : PhysicalOperator(PhysicalOperatorType::PROJECTION, std::move(types), estimated_cardinality), select_list(std::move(select_list)) {
}

OperatorResultType PhysicalProjection::Execute(ExecutionContext &context, DataChunk &input, DataChunk &chunk, GlobalOperatorState &gstate, OperatorState &state_p) const
{
	auto &state = (ProjectionState &)state_p;
	state.executor.Execute(input, chunk);
	return OperatorResultType::NEED_MORE_INPUT;
}

unique_ptr<OperatorState> PhysicalProjection::GetOperatorState(ExecutionContext &context) const
{
	return make_uniq<ProjectionState>(context, select_list);
}

unique_ptr<PhysicalOperator> PhysicalProjection::CreateJoinProjection(vector<LogicalType> proj_types, const vector<LogicalType> &lhs_types, const vector<LogicalType> &rhs_types, const vector<idx_t> &left_projection_map, const vector<idx_t> &right_projection_map, const idx_t estimated_cardinality)
{
	vector<unique_ptr<Expression>> proj_selects;
	proj_selects.reserve(proj_types.size());
	if (left_projection_map.empty())
	{
		for (storage_t i = 0; i < lhs_types.size(); ++i)
		{
			proj_selects.emplace_back(make_uniq<BoundReferenceExpression>(lhs_types[i], i));
		}
	}
	else
	{
		for (auto i : left_projection_map)
		{
			proj_selects.emplace_back(make_uniq<BoundReferenceExpression>(lhs_types[i], i));
		}
	}
	const auto left_cols = lhs_types.size();
	if (right_projection_map.empty())
	{
		for (storage_t i = 0; i < rhs_types.size(); ++i)
		{
			proj_selects.emplace_back(make_uniq<BoundReferenceExpression>(rhs_types[i], left_cols + i));
		}

	}
	else
	{
		for (auto i : right_projection_map)
		{
			proj_selects.emplace_back(make_uniq<BoundReferenceExpression>(rhs_types[i], left_cols + i));
		}
	}
	return make_uniq<PhysicalProjection>(std::move(proj_types), std::move(proj_selects), estimated_cardinality);
}

string PhysicalProjection::ParamsToString() const
{
	string extra_info;
	for (auto &expr : select_list)
	{
		extra_info += expr->GetName() + "\n";
	}
	return extra_info;
}

Operator* PhysicalProjection::SelfRehydrate(CCostContext* pcc, duckdb::vector<Operator*> pdrgpexpr, CDrvdPropCtxtPlan* pdpctxtplan)
{
	CGroupExpression* pgexpr = pcc->m_group_expression;
	double cost = pcc->m_cost;
	duckdb::vector<unique_ptr<Expression>> v;
    for(auto &child : ((PhysicalProjection*)pgexpr->m_pop.get())->select_list)
    {
        v.push_back(child->Copy());
    }
	PhysicalProjection* pexpr = new PhysicalProjection(((PhysicalProjection*)pgexpr->m_pop.get())->types, std::move(v), ((PhysicalProjection*)pgexpr->m_pop.get())->estimated_cardinality);
	for(auto &child : pdrgpexpr)
	{
		pexpr->AddChild(child->Copy());
	}
	pexpr->m_cost = cost;
	pexpr->m_group_expression = pgexpr;
	return pexpr;
}

duckdb::unique_ptr<Operator> PhysicalProjection::Copy()
{
	duckdb::vector<duckdb::unique_ptr<Expression>> v;
	for(auto &child : this->select_list)
	{
		v.push_back(child->Copy());
	}
	/* PhysicalProjection fields */
	unique_ptr<PhysicalProjection> result =
		make_uniq<PhysicalProjection>(this->types, std::move(v), this->estimated_cardinality);
	
	/* PhysicalOperator fields */
	result->v_column_binding = this->v_column_binding;
	
	/* Operator fields */
	result->m_derived_property_relation = this->m_derived_property_relation;
	result->m_derived_property_plan = this->m_derived_property_plan;
	result->m_required_plan_property = this->m_required_plan_property;
	if(nullptr != this->estimated_props)
	{
		result->estimated_props = this->estimated_props->Copy();
	}
	result->types = this->types;
	result->has_estimated_cardinality = this->has_estimated_cardinality;
	for(auto &child : this->children)
	{
		result->AddChild(child->Copy());
	}
	result->m_group_expression = this->m_group_expression;
	result->m_cost = this->m_cost;
	return result;
}

duckdb::unique_ptr<Operator> PhysicalProjection::CopyWithNewGroupExpression(CGroupExpression* pgexpr)
{
	duckdb::vector<duckdb::unique_ptr<Expression>> v;
	for(auto &child : this->select_list)
	{
		v.push_back(child->Copy());
	}
	/* PhysicalProjection fields */
	unique_ptr<PhysicalProjection> result
		= make_uniq<PhysicalProjection>(this->types, std::move(v), this->estimated_cardinality);

	/* PhysicalOperator fields */
	result->v_column_binding = this->v_column_binding;

	/* Operator fields */
	result->m_derived_property_relation = this->m_derived_property_relation;
	result->m_derived_property_plan = this->m_derived_property_plan;
	result->m_required_plan_property = this->m_required_plan_property;
	if(nullptr != this->estimated_props)
	{
		result->estimated_props = this->estimated_props->Copy();
	}
	result->types = this->types;
	result->has_estimated_cardinality = this->has_estimated_cardinality;
	for(auto &child : this->children)
	{
		result->AddChild(child->Copy());
	}
	result->m_group_expression = pgexpr;
	result->m_cost = m_cost;
	return result;
}
	
duckdb::unique_ptr<Operator> PhysicalProjection::CopyWithNewChildren(CGroupExpression* pgexpr, duckdb::vector<duckdb::unique_ptr<Operator>> pdrgpexpr, double cost)
{
	duckdb::vector<duckdb::unique_ptr<Expression>> v;
	for(auto &child : this->select_list) {
		v.push_back(child->Copy());
	}
	/* PhysicalProjection fields */
	unique_ptr<PhysicalProjection> result
		= make_uniq<PhysicalProjection>(this->types, std::move(v), this->estimated_cardinality);
	
	/* PhysicalOperator fields */
	result->v_column_binding = this->v_column_binding;

	/* Operator fields */
	result->m_derived_property_relation = this->m_derived_property_relation;
	result->m_derived_property_plan = this->m_derived_property_plan;
	result->m_required_plan_property = this->m_required_plan_property;
	if(nullptr != this->estimated_props)
	{
		result->estimated_props = this->estimated_props->Copy();
	}
	result->types = this->types;
	result->has_estimated_cardinality = this->has_estimated_cardinality;
	for(auto &child : pdrgpexpr)
	{
		result->AddChild(std::move(child));
	}
	result->m_group_expression = pgexpr;
	result->m_cost = cost;
	return result;
}

void PhysicalProjection::CE() {
	if(this->has_estimated_cardinality)
		return;
	if(!this->children[0]->has_estimated_cardinality) {
		this->children[0]->CE();
	}
	this->has_estimated_cardinality = true;
	this->estimated_cardinality = children[0]->estimated_cardinality;
}
} // namespace duckdb