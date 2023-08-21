//---------------------------------------------------------------------------
//	@filename:
//		CExpressionHandle.cpp
//
//	@doc:
//		Handle to an expression to abstract topology;
//
//		The handle provides access to an expression and the properties
//		of its children; regardless of whether the expression is a group
//		expression or a stand-alone tree;
//---------------------------------------------------------------------------
#include "duckdb/optimizer/cascade/operators/CExpressionHandle.h"

#include "duckdb/optimizer/cascade/base.h"
#include "duckdb/optimizer/cascade/base/CCostContext.h"
#include "duckdb/optimizer/cascade/base/CDrvdPropCtxtPlan.h"
#include "duckdb/optimizer/cascade/base/CKeyCollection.h"
#include "duckdb/optimizer/cascade/base/COptCtxt.h"
#include "duckdb/optimizer/cascade/base/CReqdPropPlan.h"
#include "duckdb/optimizer/cascade/base/CReqdPropRelational.h"
#include "duckdb/optimizer/cascade/operators/CPattern.h"
#include "duckdb/planner/logical_operator.hpp"

#include <assert.h>

using namespace duckdb;
using namespace std;

namespace gpopt {
//---------------------------------------------------------------------------
//	@function:
//		CExpressionHandle::CExpressionHandle
//
//	@doc:
//		ctor
//
//---------------------------------------------------------------------------
CExpressionHandle::CExpressionHandle()
    : m_pop(nullptr), m_expr(nullptr), m_pgexpr(nullptr), m_pcc(nullptr), m_pdpplan(nullptr), m_prp(nullptr) {
}

//---------------------------------------------------------------------------
//	@function:
//		CExpressionHandle::~CExpressionHandle
//
//	@doc:
//		dtor
//
//		Since handles live on the stack this dtor will be called during
//		exceptions, hence, need to be defensive
//
//---------------------------------------------------------------------------
CExpressionHandle::~CExpressionHandle() {
	m_pgexpr = nullptr;
}

//---------------------------------------------------------------------------
//	@function:
//		CExpressionHandle::Attach
//
//	@doc:
//		Attach to a given operator tree
//
//---------------------------------------------------------------------------
void CExpressionHandle::Attach(Operator *pop) {
	m_pop = pop;
}

//---------------------------------------------------------------------------
//	@function:
//		CExpressionHandle::Attach
//
//	@doc:
//		Attach to a given expression
//
//---------------------------------------------------------------------------
void CExpressionHandle::Attach(Expression *pexpr) {
	m_expr = pexpr;
}

//---------------------------------------------------------------------------
//	@function:
//		CExpressionHandle::Attach
//
//	@doc:
//		Attach to a given group expression
//
//---------------------------------------------------------------------------
void CExpressionHandle::Attach(CGroupExpression *pgexpr) {
	// increment ref count on group expression
	m_pgexpr = pgexpr;
}

//---------------------------------------------------------------------------
//	@function:
//		CExpressionHandle::Attach
//
//	@doc:
//		Attach to a given cost context
//
//---------------------------------------------------------------------------
void CExpressionHandle::Attach(CCostContext *pcc) {
	m_pcc = pcc;
	Attach(pcc->m_group_expression);
}

//---------------------------------------------------------------------------
//	@function:
//		CExpressionHandle::DeriveProps
//
//	@doc:
//		Recursive property derivation
//
//---------------------------------------------------------------------------
void CExpressionHandle::DeriveProps(CDrvdPropCtxt *pdpctxt) {
	if (nullptr != m_pgexpr) {
		return;
	}
	if (nullptr != m_pop->Pdp(m_pop->Ept())) {
		return;
	}
	// copy stats of attached expression
	// CopyStats();
	m_pop->PdpDerive((CDrvdPropCtxtPlan *)pdpctxt);
}

//---------------------------------------------------------------------------
//	@function:
//		CExpressionHandle::FAttachedToLeafPattern
//
//	@doc:
//		Return True if handle is attached to a leaf pattern
//
//---------------------------------------------------------------------------
BOOL CExpressionHandle::FAttachedToLeafPattern() const {
	return 0 == Arity() && nullptr != m_pop && nullptr != m_pop->m_group_expression;
}

// Derive the properties of the plan carried by attached cost context.
// Note that this re-derives the plan properties, instead of using those
// present in the gexpr, for cost contexts only and under the default
// CDrvdPropCtxtPlan.
// On the other hand, the properties in the gexpr may have been derived in
// other non-default contexts (e.g with cte info).
void CExpressionHandle::DerivePlanPropsForCostContext() {
	CDrvdPropCtxtPlan *pdpctxtplan = new CDrvdPropCtxtPlan();
	// CopyStats();
	// create/derive local properties
	m_pdpplan = m_pgexpr->m_pop->PdpCreate();
	m_pdpplan->Derive(*this, pdpctxtplan);
	delete pdpctxtplan;
}

//---------------------------------------------------------------------------
//	@function:
//		CExpressionHandle::InitReqdProps
//
//	@doc:
//		Init required properties containers
//
//
//---------------------------------------------------------------------------
void CExpressionHandle::InitReqdProps(CReqdProp *prpInput) {
	// set required properties of attached expr/gexpr
	m_prp = prpInput;
	// compute required properties of children
	// initialize array with input requirements,
	// the initial requirements are only place holders in the array
	// and they are replaced when computing the requirements of each child
	const ULONG arity = Arity();
	for (ULONG ul = 0; ul < arity; ul++) {
		m_pdrgprp.push_back(m_prp);
	}
}

//---------------------------------------------------------------------------
//	@function:
//		CExpressionHandle::ComputeChildReqdProps
//
//	@doc:
//		Compute required properties of the n-th child
//
//
//---------------------------------------------------------------------------
void CExpressionHandle::ComputeChildReqdProps(ULONG child_index, duckdb::vector<CDrvdProp *> pdrgpdpCtxt,
                                              ULONG ulOptReq) {
	// compute required properties based on child type
	CReqdProp *prp = Pop()->PrpCreate();
	prp->Compute(*this, m_prp, child_index, pdrgpdpCtxt, ulOptReq);
	// replace required properties of given child
	m_pdrgprp[child_index] = prp;
}

//---------------------------------------------------------------------------
//	@function:
//		CExpressionHandle::CopyChildReqdProps
//
//	@doc:
//		Copy required properties of the n-th child
//
//---------------------------------------------------------------------------
void CExpressionHandle::CopyChildReqdProps(ULONG child_index, CReqdProp *prp) {
	m_pdrgprp[child_index] = prp;
}

//---------------------------------------------------------------------------
//	@function:
//		CExpressionHandle::ComputeChildReqdCols
//
//	@doc:
//		Compute required columns of the n-th child
//
//---------------------------------------------------------------------------
void CExpressionHandle::ComputeChildReqdCols(ULONG child_index, duckdb::vector<CDrvdProp *> pdrgpdpCtxt) {
	CReqdProp *prp = Pop()->PrpCreate();
	CReqdPropPlan::Prpp(prp)->ComputeReqdCols(*this, m_prp, child_index, pdrgpdpCtxt);
	// replace required properties of given child
	m_pdrgprp[child_index] = prp;
}

//---------------------------------------------------------------------------
//	@function:
//		CExpressionHandle::ComputeReqdProps
//
//	@doc:
//		Set required properties of attached expr/gexpr, and compute required
//		properties of all children
//
//---------------------------------------------------------------------------
void CExpressionHandle::ComputeReqdProps(CReqdProp *prpInput, ULONG ulOptReq) {
	InitReqdProps(prpInput);
	const ULONG arity = Arity();
	for (ULONG ul = 0; ul < arity; ul++) {
		duckdb::vector<CDrvdProp *> v;
		ComputeChildReqdProps(ul, v, ulOptReq);
	}
}

//---------------------------------------------------------------------------
//	@function:
//		CExpressionHandle::FScalarChild
//
//	@doc:
//		Check if a given child is a scalar expression/group
//
//---------------------------------------------------------------------------
BOOL CExpressionHandle::FScalarChild(ULONG child_index) const {
	return false;
}

//---------------------------------------------------------------------------
//	@function:
//		CExpressionHandle::Arity
//
//	@doc:
//		Return number of children of attached expression/group expression
//		x = 0 for operator, x = 1 for expressions
//---------------------------------------------------------------------------
ULONG CExpressionHandle::Arity(int x) const {
	if (x == 0) {
		if (nullptr != Pop()) {
			return Pop()->Arity();
		}
	} else if (x == 1) {
		if (0 != Pop()->expressions.size()) {
			return Pop()->expressions.size();
		}
	}
	return m_pgexpr->Arity();
}

//---------------------------------------------------------------------------
//	@function:
//		CExpressionHandle::UlLastNonScalarChild
//
//	@doc:
//		Return the index of the last non-scalar child. This is only valid if
//		Arity() is greater than 0
//
//---------------------------------------------------------------------------
ULONG
CExpressionHandle::UlLastNonScalarChild() const {
	const ULONG arity = Arity();
	if (0 == arity) {
		return gpos::ulong_max;
	}

	ULONG ulLastNonScalarChild = arity - 1;
	while (0 < ulLastNonScalarChild && FScalarChild(ulLastNonScalarChild)) {
		ulLastNonScalarChild--;
	}

	if (!FScalarChild(ulLastNonScalarChild)) {
		// we need to check again that index points to a non-scalar child
		// since operator's children may be all scalar (e.g. index-scan)
		return ulLastNonScalarChild;
	}

	return gpos::ulong_max;
}

//---------------------------------------------------------------------------
//	@function:
//		CExpressionHandle::UlFirstNonScalarChild
//
//	@doc:
//		Return the index of the first non-scalar child. This is only valid if
//		Arity() is greater than 0
//
//---------------------------------------------------------------------------
ULONG
CExpressionHandle::UlFirstNonScalarChild() const {
	const ULONG arity = Arity();
	if (0 == arity) {
		return gpos::ulong_max;
	}

	ULONG ulFirstNonScalarChild = 0;
	while (ulFirstNonScalarChild < arity - 1 && FScalarChild(ulFirstNonScalarChild)) {
		ulFirstNonScalarChild++;
	}

	if (!FScalarChild(ulFirstNonScalarChild)) {
		// we need to check again that index points to a non-scalar child
		// since operator's children may be all scalar (e.g. index-scan)
		return ulFirstNonScalarChild;
	}

	return gpos::ulong_max;
}

//---------------------------------------------------------------------------
//	@function:
//		CExpressionHandle::UlNonScalarChildren
//
//	@doc:
//		Return number of non-scalar children
//
//---------------------------------------------------------------------------
ULONG
CExpressionHandle::UlNonScalarChildren() const {
	const ULONG arity = Arity();
	ULONG ulNonScalarChildren = 0;
	for (ULONG ul = 0; ul < arity; ul++) {
		if (!FScalarChild(ul)) {
			ulNonScalarChildren++;
		}
	}

	return ulNonScalarChildren;
}

//---------------------------------------------------------------------------
//	@function:
//		CExpressionHandle::GetRelationalProperties
//
//	@doc:
//		Retrieve derived relational props of n-th child; Assumes caller knows what properties to ask for;
//
//---------------------------------------------------------------------------
CDrvdPropRelational *CExpressionHandle::GetRelationalProperties(ULONG child_index) const {
	if (nullptr != Pop()) {
		// handle is used for required property computation
		if (Pop()->FPhysical()) {
			// relational props were copied from memo, return props directly
			return Pop()->children[child_index]->m_derived_property_relation;
		}
		// return props after calling derivation function
		return CDrvdPropRelational::GetRelationalProperties(Pop()->children[child_index]->PdpDerive());
	}
	// handle is used for deriving plan properties, get relational props from child group
	CDrvdPropRelational *drvdProps = CDrvdPropRelational::GetRelationalProperties((*m_pgexpr)[child_index]->m_pdp);
	return drvdProps;
}

//---------------------------------------------------------------------------
//	@function:
//		CExpressionHandle::GetRelationalProperties
//
//	@doc:
//		Retrieve relational properties of attached expr/gexpr;
//
//---------------------------------------------------------------------------
CDrvdPropRelational *CExpressionHandle::GetRelationalProperties() const {
	if (nullptr != Pop()) {
		if (Pop()->FPhysical()) {
			// relational props were copied from memo, return props directly
			CDrvdPropRelational *drvdProps = Pop()->m_derived_property_relation;
			return drvdProps;
		}
		// return props after calling derivation function
		return CDrvdPropRelational::GetRelationalProperties(Pop()->PdpDerive());
	}
	// get relational props from group
	CDrvdPropRelational *drvdProps = CDrvdPropRelational::GetRelationalProperties(m_pgexpr->m_pgroup->m_pdp);
	return drvdProps;
}

//---------------------------------------------------------------------------
//	@function:
//		CExpressionHandle::Pdpplan
//
//	@doc:
//		Retrieve derived plan props of n-th child;
//		Assumes caller knows what properties to ask for;
//
//---------------------------------------------------------------------------
CDrvdPropPlan *CExpressionHandle::Pdpplan(ULONG child_index) const {
	if (nullptr != m_pop) {
		return CDrvdPropPlan::Pdpplan(m_pop->children[child_index]->Pdp(CDrvdProp::EptPlan));
	}
	CDrvdPropPlan *pdpplan = m_pcc->m_pdrgpoc[child_index]->m_pccBest->m_pdpplan;
	return pdpplan;
}

//---------------------------------------------------------------------------
//	@function:
//		CExpressionHandle::GetReqdRelationalProps
//
//	@doc:
//		Retrieve required relational props of n-th child;
//		Assumes caller knows what properties to ask for;
//
//---------------------------------------------------------------------------
CReqdPropRelational *CExpressionHandle::GetReqdRelationalProps(ULONG child_index) const {
	CReqdProp *prp = m_pdrgprp[child_index];
	return CReqdPropRelational::GetReqdRelationalProps(prp);
}

//---------------------------------------------------------------------------
//	@function:
//		CExpressionHandle::Prpp
//
//	@doc:
//		Retrieve required relational props of n-th child;
//		Assumes caller knows what properties to ask for;
//
//---------------------------------------------------------------------------
CReqdPropPlan *CExpressionHandle::Prpp(ULONG child_index) const {
	CReqdProp *prp = m_pdrgprp[child_index];
	return CReqdPropPlan::Prpp(prp);
}

//---------------------------------------------------------------------------
//	@function:
//		CExpressionHandle::Pop
//
//	@doc:
//		Get operator from handle
//
//---------------------------------------------------------------------------
Operator *CExpressionHandle::Pop() const {
	if (nullptr != m_pop) {
		return m_pop;
	}
	if (nullptr != m_pgexpr) {
		return m_pgexpr->m_pop.get();
	}
	return nullptr;
}

//---------------------------------------------------------------------------
//	@function:
//		CExpressionHandle::Pop
//
//	@doc:
//		Get child operator from handle
//
//---------------------------------------------------------------------------
Operator *CExpressionHandle::Pop(ULONG child_index) const {
	if (nullptr != m_pop) {
		return m_pop->children[child_index].get();
	}
	if (nullptr != m_pcc) {
		COptimizationContext *pocChild = m_pcc->m_pdrgpoc[child_index];
		CCostContext *pccChild = pocChild->m_pccBest;
		return pccChild->m_group_expression->m_pop.get();
	}
	return nullptr;
}

Operator *CExpressionHandle::PopGrandchild(ULONG child_index, ULONG grandchild_index,
                                           CCostContext **grandchildContext) const {
	if (grandchildContext) {
		*grandchildContext = nullptr;
	}
	if (nullptr != m_pop) {
		if (nullptr != m_pop->children[child_index]) {
			return m_pop->children[child_index]->children[grandchild_index].get();
		}
		return nullptr;
	}
	if (nullptr != m_pcc) {
		COptimizationContext *pocChild = (m_pcc->m_pdrgpoc)[child_index];
		CCostContext *pccChild = pocChild->m_pccBest;
		COptimizationContext *pocGrandchild = (pccChild->m_pdrgpoc)[grandchild_index];
		if (nullptr != pocGrandchild) {
			CCostContext *pccgrandchild = pocGrandchild->m_pccBest;
			if (grandchildContext) {
				*grandchildContext = pccgrandchild;
			}
			return pccgrandchild->m_group_expression->m_pop.get();
		}
	}
	return nullptr;
}

//---------------------------------------------------------------------------
// CExpressionHandle::PexprScalarRepChild
//
// Get a representative (inexact) scalar child at given index. Subqueries
// in the child are replaced by a TRUE or nullptr constant. Use this method
// where exactness is not required, e. g. for statistics derivation,
// costing, or for heuristics.
//
//---------------------------------------------------------------------------
Expression *CExpressionHandle::PexprScalarRepChild(ULONG child_index) const {
	if (nullptr != m_pgexpr) {
		// access scalar expression cached on the child scalar group
		Expression *pexprScalar = (*m_pgexpr)[child_index]->m_pexprScalarRep;
		return pexprScalar;
	}
	if (nullptr != m_pop && nullptr != m_pop->children[child_index]->m_group_expression) {
		// access scalar expression cached on the child scalar group
		Expression *pexprScalar = m_pop->expressions[child_index].get();
		return pexprScalar;
	}
	if (nullptr != m_pop) {
		// if the expression does not come from a group, but its child does then
		// get the scalar child from that group
		CGroupExpression *pgexpr = m_pop->children[child_index]->m_group_expression;
		Expression *pexprScalar = pgexpr->m_pgroup->m_pexprScalarRep;
		return pexprScalar;
	}
	// access scalar expression from the child expression node
	return m_pop->expressions[child_index].get();
}

//---------------------------------------------------------------------------
// CExpressionHandle::PexprScalarRep
//
// Get a representative scalar expression attached to handle,
// return nullptr if handle is not attached to a scalar expression.
// Note that this may be inexact if handle is attached to a
// CGroupExpression - subqueries will be replaced by a TRUE or nullptr
// constant. Use this method where exactness is not required, e. g.
// for statistics derivation, costing, or for heuristics.
//
//---------------------------------------------------------------------------
Expression *CExpressionHandle::PexprScalarRep() const {
	if (nullptr != m_expr) {
		return m_expr;
	}
	if (nullptr != m_pgexpr) {
		return m_pgexpr->m_pgroup->m_pexprScalarRep;
	}
	return nullptr;
}

// return an exact scalar child at given index or return null if not possible
// (use this where exactness is required, e.g. for constraint derivation)
Expression *CExpressionHandle::PexprScalarExactChild(ULONG child_index, BOOL error_on_null_return) const {
	Expression *result_expr = nullptr;
	if (nullptr != m_pgexpr && !(*m_pgexpr)[child_index]->m_pexprScalarRepIsExact) {
		result_expr = nullptr;
	} else if (nullptr != m_pop && nullptr != m_pop->children[child_index]->m_group_expression &&
	           !(m_pop->children[child_index]->m_group_expression->m_pgroup->m_pexprScalarRepIsExact)) {
		// the expression does not come from a group, but its child does and
		// the child group does not have an exact expression
		result_expr = nullptr;
	} else {
		result_expr = PexprScalarRepChild(child_index);
	}
	if (nullptr == result_expr && error_on_null_return) {
		assert(false);
	}
	return result_expr;
}

// return an exact scalar expression attached to handle or null if not possible
// (use this where exactness is required, e.g. for constraint derivation)
Expression *CExpressionHandle::PexprScalarExact() const {
	if (nullptr != m_pgexpr && !m_pgexpr->m_pgroup->m_pexprScalarRepIsExact) {
		return nullptr;
	}
	return PexprScalarRep();
}

//---------------------------------------------------------------------------
//	@function:
//		CExpressionHandle::FChildrenHaveVolatileFuncScan
//
//	@doc:
//		Check whether an expression's children have a volatile function
//
//---------------------------------------------------------------------------
BOOL CExpressionHandle::FChildrenHaveVolatileFuncScan() {
	return false;
}

//---------------------------------------------------------------------------
//	@function:
//		CExpressionHandle::UlFirstOptimizedChildIndex
//
//	@doc:
//		Return the index of first child to be optimized
//
//---------------------------------------------------------------------------
ULONG CExpressionHandle::UlFirstOptimizedChildIndex() const {
	return 0;
}

//---------------------------------------------------------------------------
//	@function:
//		CExpressionHandle::UlLastOptimizedChildIndex
//
//	@doc:
//		Return the index of last child to be optimized
//
//---------------------------------------------------------------------------
ULONG CExpressionHandle::UlLastOptimizedChildIndex() const {
	const ULONG arity = Arity();
	return arity - 1;
}

//---------------------------------------------------------------------------
//	@function:
//		CExpressionHandle::UlNextOptimizedChildIndex
//
//	@doc:
//		Return the index of child to be optimized next to the given child,
//		return gpos::ulong_max if there is no next child index
//
//
//---------------------------------------------------------------------------
ULONG CExpressionHandle::UlNextOptimizedChildIndex(ULONG child_index) const {
	ULONG ulNextChildIndex = gpos::ulong_max;
	if (Arity() - 1 > child_index) {
		ulNextChildIndex = child_index + 1;
	}
	return ulNextChildIndex;
}

//---------------------------------------------------------------------------
//	@function:
//		CExpressionHandle::UlPreviousOptimizedChildIndex
//
//	@doc:
//		Return the index of child optimized before the given child,
//		return gpos::ulong_max if there is no previous child index
//
//
//---------------------------------------------------------------------------
ULONG CExpressionHandle::UlPreviousOptimizedChildIndex(ULONG child_index) const {
	ULONG ulPrevChildIndex = gpos::ulong_max;
	if (0 < child_index) {
		ulPrevChildIndex = child_index - 1;
	}
	return ulPrevChildIndex;
}

//---------------------------------------------------------------------------
//	@function:
//		CExpressionHandle::FNextChildIndex
//
//	@doc:
//		Get next child index based on child optimization order, return
//		true if such index could be found
//
//---------------------------------------------------------------------------
BOOL CExpressionHandle::FNextChildIndex(ULONG *pulChildIndex) const {
	GPOS_ASSERT(nullptr != pulChildIndex);

	const ULONG arity = Arity();
	if (0 == arity) {
		// operator does not have children
		return false;
	}

	ULONG ulNextChildIndex = UlNextOptimizedChildIndex(*pulChildIndex);
	if (gpos::ulong_max == ulNextChildIndex) {
		return false;
	}
	*pulChildIndex = ulNextChildIndex;

	return true;
}

//---------------------------------------------------------------------------
//	@function:
//		CExpressionHandle::PcrsUsedColumns
//
//	@doc:
//		Return the columns used by a logical operator and all its scalar children
//
//---------------------------------------------------------------------------
duckdb::vector<ColumnBinding> CExpressionHandle::PcrsUsedColumns() {
	duckdb::vector<ColumnBinding> cols = ((LogicalOperator *)Pop())->GetColumnBindings();
	return cols;
}

CDrvdProp *CExpressionHandle::Pdp() const {
	if (nullptr != m_pcc) {
		return m_pdpplan;
	}
	if (nullptr != m_pop) {
		return m_pop->Pdp(m_pop->Ept());
	}
	return m_pgexpr->m_pgroup->m_pdp;
}

// The below functions use on-demand property derivation
// only if there is an expression associated with the expression handle.
// If there is only a group expression or a cost context assoicated with the handle,
// all properties must have already been derived as we can't derive anything.
duckdb::vector<ColumnBinding> CExpressionHandle::DeriveOuterReferences(ULONG child_index) {
	if (nullptr != m_pop) {
		return m_pop->children[child_index]->GetColumnBindings();
	}
	return GetRelationalProperties(child_index)->GetOuterReferences();
}

duckdb::vector<ColumnBinding> CExpressionHandle::DeriveOuterReferences() {
	if (nullptr != m_pop) {
		return m_pop->GetColumnBindings();
	}
	return GetRelationalProperties()->GetOuterReferences();
}

duckdb::vector<ColumnBinding> CExpressionHandle::DeriveOutputColumns(ULONG child_index) {
	if (nullptr != m_pop) {
		return m_pop->children[child_index]->GetColumnBindings();
	}
	return GetRelationalProperties(child_index)->GetOutputColumns();
}

duckdb::vector<ColumnBinding> CExpressionHandle::DeriveOutputColumns() {
	return Pop()->GetColumnBindings();
}

duckdb::vector<ColumnBinding> CExpressionHandle::DeriveNotNullColumns(ULONG child_index) {
	if (nullptr != m_pop) {
		return m_pop->children[child_index]->GetColumnBindings();
	}

	return GetRelationalProperties(child_index)->GetNotNullColumns();
}

duckdb::vector<ColumnBinding> CExpressionHandle::DeriveNotNullColumns() {
	if (nullptr != m_pop) {
		return m_pop->GetColumnBindings();
	}
	return GetRelationalProperties()->GetNotNullColumns();
}

duckdb::vector<ColumnBinding> CExpressionHandle::DeriveCorrelatedApplyColumns(ULONG child_index) {
	if (nullptr != m_pop) {
		return m_pop->children[child_index]->GetColumnBindings();
	}
	return GetRelationalProperties(child_index)->GetCorrelatedApplyColumns();
}

duckdb::vector<ColumnBinding> CExpressionHandle::DeriveCorrelatedApplyColumns() {
	if (nullptr != m_pop) {
		return m_pop->GetColumnBindings();
	}
	return GetRelationalProperties()->GetCorrelatedApplyColumns();
}

CKeyCollection *CExpressionHandle::DeriveKeyCollection(ULONG child_index) {
	if (nullptr != m_pop) {
		return m_pop->children[child_index]->DeriveKeyCollection(*this);
	}
	return GetRelationalProperties(child_index)->GetKeyCollection();
}

CKeyCollection *CExpressionHandle::DeriveKeyCollection() {
	if (nullptr != m_pop) {
		return m_pop->DeriveKeyCollection(*this);
	}
	return GetRelationalProperties()->GetKeyCollection();
}

CPropConstraint *CExpressionHandle::DerivePropertyConstraint(ULONG child_index) {
	if (nullptr != m_pop) {
		return m_pop->children[child_index]->DerivePropertyConstraint(*this);
	}
	return GetRelationalProperties(child_index)->GetPropertyConstraint();
}

CPropConstraint *CExpressionHandle::DerivePropertyConstraint() {
	if (nullptr != m_pop) {
		return m_pop->DerivePropertyConstraint(*this);
	}
	return GetRelationalProperties()->GetPropertyConstraint();
}

ULONG CExpressionHandle::DeriveJoinDepth(ULONG child_index) {
	if (nullptr != Pexpr()) {
		return m_pop->children[child_index]->DeriveJoinDepth(*this);
	}
	return GetRelationalProperties(child_index)->GetJoinDepth();
}

ULONG CExpressionHandle::DeriveJoinDepth() {
	if (nullptr != m_pop) {
		return m_pop->DeriveJoinDepth(*this);
	}
	return GetRelationalProperties()->GetJoinDepth();
}

duckdb::vector<CFunctionalDependency *> CExpressionHandle::Pdrgpfd(ULONG child_index) {
	if (nullptr != m_pop) {
		return m_pop->children[child_index]->DeriveFunctionalDependencies(*this);
	}
	return GetRelationalProperties(child_index)->GetFunctionalDependencies();
}

duckdb::vector<CFunctionalDependency *> CExpressionHandle::Pdrgpfd() {
	if (nullptr != m_pop) {
		return m_pop->DeriveFunctionalDependencies(*this);
	}
	return GetRelationalProperties()->GetFunctionalDependencies();
}
} // namespace gpopt