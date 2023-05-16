//---------------------------------------------------------------------------
//	@filename:
//		CDistributionSpecHashedNoOp.h
//
//	@doc:
//
//---------------------------------------------------------------------------
#include "duckdb/optimizer/cascade/base/CDistributionSpecHashedNoOp.h"
#include "duckdb/optimizer/cascade/operators/CExpressionHandle.h"
#include "duckdb/optimizer/cascade/operators/CPhysicalMotionHashDistribute.h"

using namespace gpopt;

CDistributionSpecHashedNoOp::CDistributionSpecHashedNoOp(CExpressionArray *pdrgpexpr)
	: CDistributionSpecHashed(pdrgpexpr, true)
{
}

CDistributionSpec::EDistributionType
CDistributionSpecHashedNoOp::Edt() const
{
	return CDistributionSpec::EdtHashedNoOp;
}

BOOL
CDistributionSpecHashedNoOp::Matches(const CDistributionSpec *pds) const
{
	return pds->Edt() == Edt();
}

void
CDistributionSpecHashedNoOp::AppendEnforcers(CMemoryPool *mp,
											 CExpressionHandle &exprhdl,
											 CReqdPropPlan *,
											 CExpressionArray *pdrgpexpr,
											 CExpression *pexpr)
{
	CDrvdProp *pdp = exprhdl.Pdp();
	CDistributionSpec *pdsChild = CDrvdPropPlan::Pdpplan(pdp)->Pds();
	CDistributionSpecHashed *pdsChildHashed = dynamic_cast<CDistributionSpecHashed *>(pdsChild);

	if (NULL == pdsChildHashed)
	{
		return;
	}

	CExpressionArray *pdrgpexprNoOpRedistributionColumns = pdsChildHashed->Pdrgpexpr();
	pdrgpexprNoOpRedistributionColumns->AddRef();
	CDistributionSpecHashedNoOp *pdsNoOp = GPOS_NEW(mp) CDistributionSpecHashedNoOp(pdrgpexprNoOpRedistributionColumns);
	pexpr->AddRef();
	CExpression *pexprMotion = GPOS_NEW(mp) CExpression(mp, GPOS_NEW(mp) CPhysicalMotionHashDistribute(mp, pdsNoOp), pexpr);
	pdrgpexpr->Append(pexprMotion);
}