//---------------------------------------------------------------------------
//	@filename:
//		CReqdPropRelational.cpp
//
//	@doc:
//		Required relational properties;
//---------------------------------------------------------------------------
#include "duckdb/optimizer/cascade/base/CReqdPropRelational.h"
#include "duckdb/optimizer/cascade/base.h"
#include "duckdb/optimizer/cascade/base/CColRefSet.h"
#include "duckdb/optimizer/cascade/base/COptCtxt.h"
#include "duckdb/optimizer/cascade/base/CUtils.h"
#include "duckdb/optimizer/cascade/engine/CEngine.h"
#include "duckdb/optimizer/cascade/operators/CExpression.h"
#include "duckdb/optimizer/cascade/operators/CExpressionHandle.h"
#include "duckdb/optimizer/cascade/operators/CLogical.h"

using namespace gpopt;

//---------------------------------------------------------------------------
//	@function:
//		CReqdPropRelational::CReqdPropRelational
//
//	@doc:
//		Ctor
//
//---------------------------------------------------------------------------
CReqdPropRelational::CReqdPropRelational()
	: m_pcrsStat(NULL), m_pexprPartPred(NULL)
{
}

//---------------------------------------------------------------------------
//	@function:
//		CReqdPropRelational::CReqdPropRelational
//
//	@doc:
//		Ctor
//
//---------------------------------------------------------------------------
CReqdPropRelational::CReqdPropRelational(CColRefSet *pcrs)
	: m_pcrsStat(pcrs), m_pexprPartPred(NULL)
{
	GPOS_ASSERT(NULL != pcrs);
}

//---------------------------------------------------------------------------
//	@function:
//		CReqdPropRelational::CReqdPropRelational
//
//	@doc:
//		Ctor
//
//---------------------------------------------------------------------------
CReqdPropRelational::CReqdPropRelational(CColRefSet *pcrs,
										 CExpression *pexprPartPred)
	: m_pcrsStat(pcrs), m_pexprPartPred(pexprPartPred)
{
	GPOS_ASSERT(NULL != pcrs);
	GPOS_ASSERT_IMP(NULL != pexprPartPred, pexprPartPred->Pop()->FScalar());
}

//---------------------------------------------------------------------------
//	@function:
//		CReqdPropRelational::~CReqdPropRelational
//
//	@doc:
//		Dtor
//
//---------------------------------------------------------------------------
CReqdPropRelational::~CReqdPropRelational()
{
	CRefCount::SafeRelease(m_pcrsStat);
	CRefCount::SafeRelease(m_pexprPartPred);
}

//---------------------------------------------------------------------------
//	@function:
//		CReqdPropRelational::Compute
//
//	@doc:
//		Compute required props
//
//---------------------------------------------------------------------------
void
CReqdPropRelational::Compute(CMemoryPool *mp, CExpressionHandle &exprhdl,
							 CReqdProp *prpInput, ULONG child_index,
							 CDrvdPropArray *,	// pdrgpdpCtxt
							 ULONG				// ulOptReq
)
{
	GPOS_CHECK_ABORT;

	CReqdPropRelational *prprelInput =
		CReqdPropRelational::GetReqdRelationalProps(prpInput);
	CLogical *popLogical = CLogical::PopConvert(exprhdl.Pop());

	m_pcrsStat =
		popLogical->PcrsStat(mp, exprhdl, prprelInput->PcrsStat(), child_index);
	m_pexprPartPred = popLogical->PexprPartPred(
		mp, exprhdl, prprelInput->PexprPartPred(), child_index);

	exprhdl.DeriveProducerStats(child_index, m_pcrsStat);
}

//---------------------------------------------------------------------------
//	@function:
//		CReqdPropRelational::GetReqdRelationalProps
//
//	@doc:
//		Short hand for conversion
//
//---------------------------------------------------------------------------
CReqdPropRelational *
CReqdPropRelational::GetReqdRelationalProps(CReqdProp *prp)
{
	return dynamic_cast<CReqdPropRelational *>(prp);
}


//---------------------------------------------------------------------------
//	@function:
//		CReqdPropRelational::PrprelDifference
//
//	@doc:
//		Return difference from given properties
//
//---------------------------------------------------------------------------
CReqdPropRelational *
CReqdPropRelational::PrprelDifference(CMemoryPool *mp,
									  CReqdPropRelational *prprel)
{
	GPOS_ASSERT(NULL != prprel);

	CColRefSet *pcrs = GPOS_NEW(mp) CColRefSet(mp);
	pcrs->Union(m_pcrsStat);
	pcrs->Difference(prprel->PcrsStat());

	return GPOS_NEW(mp) CReqdPropRelational(pcrs);
}


//---------------------------------------------------------------------------
//	@function:
//		CReqdPropRelational::IsEmpty
//
//	@doc:
//		Return true if property container is empty
//
//---------------------------------------------------------------------------
BOOL
CReqdPropRelational::IsEmpty() const
{
	return m_pcrsStat->Size() == 0;
}

//---------------------------------------------------------------------------
//	@function:
//		CReqdPropRelational::OsPrint
//
//	@doc:
//		Print function
//
//---------------------------------------------------------------------------
IOstream &
CReqdPropRelational::OsPrint(IOstream &os) const
{
	os << "req stat columns: [" << *m_pcrsStat << "]";
	if (NULL != m_pexprPartPred)
	{
		os << ", partition predicate: " << *m_pexprPartPred;
	}

	return os;
}