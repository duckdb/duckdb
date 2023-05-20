//---------------------------------------------------------------------------
//	@filename:
//		CDistributionSpecNonSingleton.cpp
//
//	@doc:
//		Specification of non-singleton distribution
//---------------------------------------------------------------------------
#include "duckdb/optimizer/cascade/base/CDistributionSpecNonSingleton.h"
#include "duckdb/optimizer/cascade/base/CColRefSet.h"
#include "duckdb/optimizer/cascade/base/CColRefSetIter.h"
#include "duckdb/optimizer/cascade/base/CDistributionSpecStrictRandom.h"
#include "duckdb/optimizer/cascade/base/CUtils.h"
#include "duckdb/optimizer/cascade/operators/CPhysicalMotionRandom.h"
#include "duckdb/optimizer/cascade/traceflags/traceflags.h"

using namespace gpopt;

//---------------------------------------------------------------------------
//	@function:
//		CDistributionSpecNonSingleton::CDistributionSpecNonSingleton
//
//	@doc:
//		Ctor
//
//---------------------------------------------------------------------------
CDistributionSpecNonSingleton::CDistributionSpecNonSingleton()
	: m_fAllowReplicated(true)
{
}

//---------------------------------------------------------------------------
//     @function:
//             CDistributionSpecNonSingleton::CDistributionSpecNonSingleton
//
//     @doc:
//             Ctor
//
//---------------------------------------------------------------------------
CDistributionSpecNonSingleton::CDistributionSpecNonSingleton(BOOL fAllowReplicated)
	: m_fAllowReplicated(fAllowReplicated)
{
}


//---------------------------------------------------------------------------
//	@function:
//		CDistributionSpecNonSingleton::FSatisfies
//
//	@doc:
//		Check if this distribution spec satisfies the given one
//
//---------------------------------------------------------------------------
BOOL
CDistributionSpecNonSingleton::FSatisfies(const CDistributionSpec *	 // pds
) const
{
	GPOS_ASSERT(!"Non-Singleton distribution cannot be derived");

	return false;
}

//---------------------------------------------------------------------------
//	@function:
//		CDistributionSpecNonSingleton::AppendEnforcers
//
//	@doc:
//		Add required enforcers to dynamic array;
//		non-singleton distribution is enforced by spraying data randomly
//		on segments
//
//---------------------------------------------------------------------------
void
CDistributionSpecNonSingleton::AppendEnforcers(CMemoryPool *mp,
											   CExpressionHandle &,	 // exprhdl
											   CReqdPropPlan *
#ifdef GPOS_DEBUG
												   prpp
#endif	// GPOS_DEBUG
											   ,
											   CExpressionArray *pdrgpexpr,
											   CExpression *pexpr)
{
	GPOS_ASSERT(NULL != mp);
	GPOS_ASSERT(NULL != prpp);
	GPOS_ASSERT(NULL != pdrgpexpr);
	GPOS_ASSERT(NULL != pexpr);
	GPOS_ASSERT(!GPOS_FTRACE(EopttraceDisableMotions));
	GPOS_ASSERT(
		this == prpp->Ped()->PdsRequired() &&
		"required plan properties don't match enforced distribution spec");


	if (GPOS_FTRACE(EopttraceDisableMotionRandom))
	{
		// random Motion is disabled
		return;
	}

	// add a random distribution enforcer
	CDistributionSpecStrictRandom *pdsrandom =
		GPOS_NEW(mp) CDistributionSpecStrictRandom();
	pexpr->AddRef();
	CExpression *pexprMotion = GPOS_NEW(mp) CExpression(
		mp, GPOS_NEW(mp) CPhysicalMotionRandom(mp, pdsrandom), pexpr);
	pdrgpexpr->Append(pexprMotion);
}

//---------------------------------------------------------------------------
//	@function:
//		CDistributionSpecNonSingleton::OsPrint
//
//	@doc:
//		Print function
//
//---------------------------------------------------------------------------
IOstream &
CDistributionSpecNonSingleton::OsPrint(IOstream &os) const
{
	os << "NON-SINGLETON ";
	if (!m_fAllowReplicated)
	{
		os << " (NON-REPLICATED)";
	}
	return os;
}