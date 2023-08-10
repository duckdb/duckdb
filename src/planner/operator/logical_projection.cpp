#include "duckdb/planner/operator/logical_projection.hpp"

#include "duckdb/common/field_writer.hpp"
#include "duckdb/optimizer/cascade/base/CDrvdPropRelational.h"

namespace duckdb {

LogicalProjection::LogicalProjection(idx_t table_index, vector<unique_ptr<Expression>> select_list)
    : LogicalOperator(LogicalOperatorType::LOGICAL_PROJECTION, std::move(select_list)), table_index(table_index) {
	logical_type = LogicalOperatorType::LOGICAL_PROJECTION;
	m_pdprel = new CDrvdPropRelational();
	m_pgexpr = nullptr;
	m_pdpplan = nullptr;
	m_prpp = nullptr;
}

vector<ColumnBinding> LogicalProjection::GetColumnBindings() {
	return GenerateColumnBindings(table_index, expressions.size());
}

void LogicalProjection::ResolveTypes() {
	for (auto &expr : expressions) {
		types.push_back(expr->return_type);
	}
}

void LogicalProjection::Serialize(FieldWriter &writer) const {
	writer.WriteField(table_index);
	writer.WriteSerializableList<Expression>(expressions);
}

unique_ptr<LogicalOperator> LogicalProjection::Deserialize(LogicalDeserializationState &state, FieldReader &reader) {
	auto table_index = reader.ReadRequired<idx_t>();
	auto expressions = reader.ReadRequiredSerializableList<Expression>(state.gstate);
	return make_uniq<LogicalProjection>(table_index, std::move(expressions));
}

vector<idx_t> LogicalProjection::GetTableIndex() const {
	return vector<idx_t> {table_index};
}

CKeyCollection *LogicalProjection::DeriveKeyCollection(CExpressionHandle &exprhdl) {
	return PkcDeriveKeysPassThru(exprhdl, 0);
}

//---------------------------------------------------------------------------
//	@function:
//		CLogicalProject::DerivePropertyConstraint
//
//	@doc:
//		Derive constraint property
//
//---------------------------------------------------------------------------
CPropConstraint *LogicalProjection::DerivePropertyConstraint(CExpressionHandle &exprhdl) {
	return nullptr;
	// return PpcDeriveConstraintPassThru(exprhdl, 0);
}

// Rehydrate expression from a given cost context and child expressions
Operator *LogicalProjection::SelfRehydrate(CCostContext *pcc, duckdb::vector<Operator *> pdrgpexpr,
                                           CDrvdPropCtxtPlan *pdpctxtplan) {
	CGroupExpression *pgexpr = pcc->m_pgexpr;
	double cost = pcc->m_cost;
	duckdb::vector<duckdb::unique_ptr<Expression>> v;
	for (auto &child : pgexpr->m_pop.get()->expressions) {
		v.push_back(child->Copy());
	}
	LogicalProjection *pexpr =
	    new LogicalProjection(((LogicalProjection *)pgexpr->m_pop.get())->table_index, std::move(v));
	for (auto &child : pdrgpexpr) {
		pexpr->AddChild(child->Copy());
	}
	pexpr->m_cost = cost;
	pexpr->m_pgexpr = pgexpr;
	return pexpr;
}

//---------------------------------------------------------------------------
//	@function:
//		LogicalProjection::PxfsCandidates
//
//	@doc:
//		Get candidate xforms
//
//---------------------------------------------------------------------------
CXformSet *LogicalProjection::PxfsCandidates() const {
	CXformSet *xform_set = new CXformSet();
	(void)xform_set->set(CXform::ExfLogicalProj2PhysicalProj);
	// (void) xform_set->set(CXform::ExfProject2Apply);
	// (void) xform_set->set(CXform::ExfProject2ComputeScalar);
	// (void) xform_set->set(CXform::ExfCollapseProject);
	return xform_set;
}

duckdb::unique_ptr<Operator> LogicalProjection::Copy() {
	duckdb::vector<duckdb::unique_ptr<Expression>> v;
	for (auto &child : expressions) {
		v.push_back(child->Copy());
	}
	unique_ptr<LogicalProjection> result = make_uniq<LogicalProjection>(table_index, std::move(v));
	result->m_pdprel = m_pdprel;
	result->m_pdpplan = m_pdpplan;
	result->m_prpp = m_prpp;
	if (nullptr != estimated_props) {
		result->estimated_props = estimated_props->Copy();
	}
	result->types = types;
	result->estimated_cardinality = estimated_cardinality;
	result->has_estimated_cardinality = has_estimated_cardinality;
	result->logical_type = logical_type;
	result->physical_type = physical_type;
	for (auto &child : children) {
		result->AddChild(child->Copy());
	}
	result->m_pgexpr = m_pgexpr;
	result->m_cost = m_cost;
	return result;
}

duckdb::unique_ptr<Operator> LogicalProjection::CopywithNewGroupExpression(CGroupExpression *pgexpr) {
	duckdb::vector<duckdb::unique_ptr<Expression>> v;
	for (auto &child : expressions) {
		v.push_back(child->Copy());
	}
	unique_ptr<LogicalProjection> result = make_uniq<LogicalProjection>(table_index, std::move(v));
	result->m_pdprel = m_pdprel;
	result->m_pdpplan = m_pdpplan;
	result->m_prpp = m_prpp;
	if (nullptr != estimated_props) {
		result->estimated_props = estimated_props->Copy();
	}
	result->types = types;
	result->estimated_cardinality = estimated_cardinality;
	result->has_estimated_cardinality = has_estimated_cardinality;
	result->logical_type = logical_type;
	result->physical_type = physical_type;
	for (auto &child : children) {
		result->AddChild(child->Copy());
	}
	result->m_pgexpr = pgexpr;
	result->m_cost = m_cost;
	return result;
}

duckdb::unique_ptr<Operator>
LogicalProjection::CopywithNewChilds(CGroupExpression *pgexpr, duckdb::vector<duckdb::unique_ptr<Operator>> pdrgpexpr,
                                     double cost) {
	duckdb::vector<duckdb::unique_ptr<Expression>> v;
	for (auto &child : expressions) {
		v.push_back(child->Copy());
	}
	unique_ptr<LogicalProjection> result = make_uniq<LogicalProjection>(table_index, std::move(v));
	result->m_pdprel = m_pdprel;
	result->m_pdpplan = m_pdpplan;
	result->m_prpp = m_prpp;
	if (nullptr != estimated_props) {
		result->estimated_props = estimated_props->Copy();
	}
	result->types = types;
	result->estimated_cardinality = estimated_cardinality;
	result->has_estimated_cardinality = has_estimated_cardinality;
	result->logical_type = logical_type;
	result->physical_type = physical_type;
	for (auto &child : pdrgpexpr) {
		result->AddChild(child->Copy());
	}
	result->m_pgexpr = pgexpr;
	result->m_cost = cost;
	return result;
}
} // namespace duckdb