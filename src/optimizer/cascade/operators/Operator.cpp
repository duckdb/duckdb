//---------------------------------------------------------------------------
//	@filename:
//		Operator.cpp
//
//	@doc:
//		Base class for all operators: logical, physical, scalar, patterns
//
//---------------------------------------------------------------------------
#include "duckdb/optimizer/cascade/operators/Operator.h"

#include "duckdb/optimizer/cascade/base/CDrvdPropCtxtPlan.h"
#include "duckdb/optimizer/cascade/operators/CPatternLeaf.h"
#include "duckdb/optimizer/cascade/operators/CPatternTree.h"
#include "duckdb/optimizer/cascade/search/CGroupExpression.h"
#include "duckdb/planner/operator/logical_get.hpp"

using namespace gpopt;

//---------------------------------------------------------------------------
//	@function:
//		COperator::HashValue
//
//	@doc:
//		default hash function based on operator ID
//
//---------------------------------------------------------------------------
ULONG Operator::HashValue() const {
	ULONG ulLogicalType = (ULONG)logical_type;
	ULONG ulPhysicalType = (ULONG)physical_type;
	return CombineHashes(gpos::HashValue<ULONG>(&ulLogicalType), gpos::HashValue<ULONG>(&ulPhysicalType));
}

ULONG Operator::HashValue(const Operator *op) {
	ULONG ulHash = op->HashValue();
	const ULONG arity = op->Arity();
	for (ULONG ul = 0; ul < arity; ul++) {
		ulHash = CombineHashes(ulHash, HashValue(op->children[ul].get()));
	}
	return ulHash;
}

//---------------------------------------------------------------------------
//	@function:
//		Operator::PexprRehydrate
//
//	@doc:
//		Rehydrate expression from a given cost context and child expressions
//
//---------------------------------------------------------------------------
Operator *Operator::PexprRehydrate(CCostContext *pcc, duckdb::vector<Operator *> pdrgpexpr,
                                   CDrvdPropCtxtPlan *pdpctxtplan) {
	CGroupExpression *pgexpr = pcc->m_pgexpr;
	return pgexpr->m_pop->SelfRehydrate(pcc, pdrgpexpr, pdpctxtplan);
}

idx_t Operator::EstimateCardinality(ClientContext &context) {
	// simple estimator, just take the max of the children
	if (has_estimated_cardinality) {
		return estimated_cardinality;
	}
	idx_t max_cardinality = 0;
	for (auto &child : children) {
		Operator *logical_child = (Operator *)(child.get());
		max_cardinality = MaxValue(logical_child->EstimateCardinality(context), max_cardinality);
	}
	has_estimated_cardinality = true;
	estimated_cardinality = max_cardinality;
	return estimated_cardinality;
}

duckdb::vector<CFunctionalDependency *> Operator::DeriveFunctionalDependencies(CExpressionHandle &exprhdl) {
	return m_pdprel->DeriveFunctionalDependencies(exprhdl);
}

//---------------------------------------------------------------------------
//	@function:
//		Operator::FMatchPattern
//
//	@doc:
//		Check a pattern expression against a given group;
//		shallow, do not	match its children, check only arity of the root
//
//---------------------------------------------------------------------------
bool Operator::FMatchPattern(CGroupExpression *pgexpr) {
	if (this->FPattern()) {
		return true;
	} else {
		// match operator id and arity
		if ((this->logical_type == pgexpr->m_pop->logical_type ||
		     this->physical_type == pgexpr->m_pop->physical_type) &&
		    this->Arity() == pgexpr->Arity()) {
			return true;
		}
	}
	return false;
}

CReqdPropPlan *Operator::PrppCompute(CReqdPropPlan *prppInput) {
	// derive plan properties
	CDrvdPropCtxtPlan *pdpctxtplan = new CDrvdPropCtxtPlan();
	(void)PdpDerive(pdpctxtplan);
	// decorate nodes with required properties
	return m_prpp;
}

CDrvdProp *Operator::PdpDerive(CDrvdPropCtxtPlan *pdpctxt) {
	const CDrvdProp::EPropType ept = Ept();
	CExpressionHandle exprhdl;
	exprhdl.Attach(this);
	// see if suitable prop is already cached. This only applies to plan properties.
	// relational properties are never null and are handled in the next case
	if (nullptr == Pdp(ept)) {
		const ULONG arity = Arity();
		for (ULONG ul = 0; ul < arity; ul++) {
			CDrvdProp *pdp = children[ul]->PdpDerive(pdpctxt);
			// add child props to derivation context
			CDrvdPropCtxt::AddDerivedProps(pdp, pdpctxt);
		}
		switch (ept) {
		case CDrvdProp::EptPlan:
			m_pdpplan = new CDrvdPropPlan();
			break;
		default:
			break;
		}
		Pdp(ept)->Derive(exprhdl, pdpctxt);
	}
	// If we havn't derived all properties, do that now. If we've derived some
	// of the properties, this will only derive properties that have not yet been derived.
	else if (!Pdp(ept)->IsComplete()) {
		Pdp(ept)->Derive(exprhdl, pdpctxt);
	}
	// Otherwise, we've already derived all properties and can simply return them
	return Pdp(ept);
}

CReqdPropPlan *Operator::PrppDecorate(CReqdPropPlan *prppInput) {
	return m_prpp;
}

duckdb::unique_ptr<Operator> Operator::Copy() {
	duckdb::unique_ptr<Operator> result = make_uniq<Operator>();
	return result;
}

duckdb::unique_ptr<Operator> Operator::CopywithNewGroupExpression(CGroupExpression *pgexpr) {
	duckdb::unique_ptr<Operator> result = make_uniq<Operator>();
	result->m_pgexpr = pgexpr;
	return result;
}

duckdb::unique_ptr<Operator> Operator::CopywithNewChilds(CGroupExpression *pgexpr,
                                                         duckdb::vector<duckdb::unique_ptr<Operator>> pdrgpexpr,
                                                         double cost) {
	duckdb::unique_ptr<Operator> result = make_uniq<Operator>();
	result->m_pgexpr = pgexpr;
	for (auto &child : pdrgpexpr) {
		result->AddChild(child->Copy());
	}
	result->m_cost = cost;
	return result;
}