#include "duckdb/planner/operator/logical_dummy_scan.hpp"

#include "duckdb/common/field_writer.hpp"

namespace duckdb {

void LogicalDummyScan::Serialize(FieldWriter &writer) const {
	writer.WriteField(table_index);
}

unique_ptr<LogicalOperator> LogicalDummyScan::Deserialize(LogicalDeserializationState &state, FieldReader &reader) {
	auto table_index = reader.ReadRequired<idx_t>();
	return make_uniq<LogicalDummyScan>(table_index);
}

vector<idx_t> LogicalDummyScan::GetTableIndex() const {
	return vector<idx_t> {table_index};
}

//---------------------------------------------------------------------------
//	@function:
//		LogicalDummyScan::PxfsCandidates
//
//	@doc:
//		Get candidate xforms
//
//---------------------------------------------------------------------------
CXform_set *LogicalDummyScan::PxfsCandidates() const {
	CXform_set *xform_set = new CXform_set();
	(void)xform_set->set(CXform::ExfDummyScanImplementation);
	return xform_set;
}

//---------------------------------------------------------------------------
//	@function:
//		LogicalColumnDataGet::DerivePropertyConstraint
//
//	@doc:
//		Derive constraint property
//
//---------------------------------------------------------------------------
CPropConstraint *LogicalDummyScan::DerivePropertyConstraint(CExpressionHandle &expr_handle) {
	return nullptr;
	// return PpcDeriveConstraintPassThru(exprhdl, 0);
}

// Rehydrate expression from a given cost context and child expressions
Operator *LogicalDummyScan::SelfRehydrate(CCostContext *cost_context, duckdb::vector<Operator *> pdr_exprs,
                                          CDrvdPropCtxtPlan *pdpctxtplan) {
	return new LogicalDummyScan(table_index);
}

unique_ptr<Operator> LogicalDummyScan::Copy() {
	unique_ptr<LogicalDummyScan> result = make_uniq<LogicalDummyScan>(table_index);
	result->m_derived_property_relation = m_derived_property_relation;
	result->m_derived_property_plan = m_derived_property_plan;
	result->m_required_plan_property = m_required_plan_property;
	if (nullptr != estimated_props) {
		result->estimated_props = estimated_props->Copy();
	}
	result->types = types;
	result->estimated_cardinality = estimated_cardinality;
	for (auto &child : expressions) {
		result->expressions.push_back(child->Copy());
	}
	result->has_estimated_cardinality = has_estimated_cardinality;
	result->logical_type = logical_type;
	result->physical_type = physical_type;
	for (auto &child : children) {
		result->AddChild(child->Copy());
	}
	result->m_group_expression = m_group_expression;
	result->m_cost = m_cost;

	return result;
}

unique_ptr<Operator> LogicalDummyScan::CopyWithNewGroupExpression(CGroupExpression *expr) {
	auto result = Copy();
	result->m_group_expression = expr;
	return result;
}

unique_ptr<Operator> LogicalDummyScan::CopyWithNewChildren(CGroupExpression *expr,
                                                           duckdb::vector<duckdb::unique_ptr<Operator>> pdr_exprs,
                                                           double cost) {
	auto result = Copy();
	for(auto &child : pdr_exprs)
	{
		result->AddChild(child->Copy());
	}
	result->m_group_expression = expr;
	result->m_cost = cost;
	return result;
}

} // namespace duckdb
