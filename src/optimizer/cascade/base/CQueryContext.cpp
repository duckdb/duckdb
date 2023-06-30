//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2011 EMC Corp.
//
//	@filename:
//		CQueryContext.cpp
//
//	@doc:
//		Implementation of optimization context
//---------------------------------------------------------------------------
#include "duckdb/optimizer/cascade/base/CQueryContext.h"
#include "duckdb/optimizer/cascade/base.h"
#include "duckdb/optimizer/cascade/base/CColRefSetIter.h"
#include "duckdb/optimizer/cascade/base/CColumnFactory.h"
#include "duckdb/optimizer/cascade/base/CDistributionSpecAny.h"
#include "duckdb/optimizer/cascade/base/COptCtxt.h"
#include "duckdb/optimizer/cascade/operators/CLogicalLimit.h"
#include "duckdb/optimizer/cascade/test/MyTypeInt4.h"
#include "duckdb/optimizer/cascade/base/CColRefTable.h"

using namespace gpopt;
using namespace duckdb;
//---------------------------------------------------------------------------
//	@function:
//		CQueryContext::CQueryContext
//
//	@doc:
//		Ctor
//
//---------------------------------------------------------------------------
CQueryContext::CQueryContext(CMemoryPool* mp, LogicalOperator* pexpr, CReqdPropPlan* prpp, CColRefArray* colref_array, CMDNameArray* pdrgpmdname, BOOL fDeriveStats)
	: m_mp(mp), m_prpp(prpp), m_pdrgpcr(colref_array), m_pdrgpcrSystemCols(NULL), m_pdrgpmdname(pdrgpmdname), m_fDeriveStats(fDeriveStats)
{
	GPOS_ASSERT(NULL != pexpr);
	GPOS_ASSERT(NULL != prpp);
	GPOS_ASSERT(NULL != colref_array);
	GPOS_ASSERT(NULL != pdrgpmdname);
	GPOS_ASSERT(colref_array->Size() == pdrgpmdname->Size());
	// mark unused CTEs
	CCTEInfo *pcteinfo = COptCtxt::PoctxtFromTLS()->Pcteinfo();
	pcteinfo->MarkUnusedCTEs();
	CColRefSet* pcrsOutputAndOrderingCols = GPOS_NEW(mp) CColRefSet(mp);
	CColRefSet* pcrsOrderSpec = prpp->Peo()->PosRequired()->PcrsUsed(mp);
	pcrsOutputAndOrderingCols->Include(colref_array);
	pcrsOutputAndOrderingCols->Include(pcrsOrderSpec);
	pcrsOrderSpec->Release();

	/* I comment here */
	// m_pexpr = CExpressionPreprocessor::PexprPreprocess(mp, pexpr, pcrsOutputAndOrderingCols);
	m_pexpr = pexpr;

	pcrsOutputAndOrderingCols->Release();
	GPOS_ASSERT(m_pdrgpcr->Size() == ulReqdColumns);
	// collect required system columns
	SetSystemCols(mp);
}


//---------------------------------------------------------------------------
//	@function:
//		CQueryContext::~CQueryContext
//
//	@doc:
//		Dtor
//
//---------------------------------------------------------------------------
CQueryContext::~CQueryContext()
{
	// m_pexpr->Release();
	m_prpp->Release();
	m_pdrgpcr->Release();
	m_pdrgpmdname->Release();
	CRefCount::SafeRelease(m_pdrgpcrSystemCols);
}


//---------------------------------------------------------------------------
//	@function:
//		CQueryContext::PopTop
//
//	@doc:
// 		 Return top level operator in the given expression
//
//---------------------------------------------------------------------------
COperator* CQueryContext::PopTop(CExpression *pexpr)
{
	GPOS_ASSERT(NULL != pexpr);
	// skip CTE anchors if any
	CExpression* pexprCurr = pexpr;
	while (COperator::EopLogicalCTEAnchor == pexprCurr->Pop()->Eopid())
	{
		pexprCurr = (*pexprCurr)[0];
		GPOS_ASSERT(NULL != pexprCurr);
	}
	return pexprCurr->Pop();
}

//---------------------------------------------------------------------------
//	@function:
//		CQueryContext::SetReqdSystemCols
//
//	@doc:
// 		Collect system columns from output columns
//
//---------------------------------------------------------------------------
void CQueryContext::SetSystemCols(CMemoryPool* mp)
{
	GPOS_ASSERT(NULL == m_pdrgpcrSystemCols);
	GPOS_ASSERT(NULL != m_pdrgpcr);
	m_pdrgpcrSystemCols = GPOS_NEW(mp) CColRefArray(mp);
	const ULONG ulReqdCols = m_pdrgpcr->Size();
	for (ULONG ul = 0; ul < ulReqdCols; ul++)
	{
		CColRef *colref = (*m_pdrgpcr)[ul];
		if (colref->FSystemCol())
		{
			m_pdrgpcrSystemCols->Append(colref);
		}
	}
}

//---------------------------------------------------------------------------
//	@function:
//		CQueryContext::PqcGenerate
//
//	@doc:
// 		Generate the query context for the given expression and array of
//		output column ref ids
//
//---------------------------------------------------------------------------
CQueryContext* CQueryContext::PqcGenerate(CMemoryPool *mp, LogicalOperator* pexpr, ULongPtrArray* pdrgpulQueryOutputColRefId, CMDNameArray *pdrgpmdname, BOOL fDeriveStats)
{
	GPOS_ASSERT(NULL != pexpr && NULL != pdrgpulQueryOutputColRefId);
	CColRefSet *pcrs = GPOS_NEW(mp) CColRefSet(mp);
	CColRefArray *colref_array = GPOS_NEW(mp) CColRefArray(mp);
	COptCtxt *poptctxt = COptCtxt::PoctxtFromTLS();
	// Collect required column references (colref_array)
	const ULONG length = pdrgpulQueryOutputColRefId->Size();
	for (ULONG ul = 0; ul < length; ul++)
	{
		ULONG pul = *(*pdrgpulQueryOutputColRefId)[ul];
		GPOS_ASSERT(NULL != pul);
		// Here we start simulation
		IMDType* type = GPOS_NEW(mp) MyTypeInt4(mp); 
		WCHAR str[] = L"A";
		CWStringBase* cwstr = GPOS_NEW(mp) CWStringConst(str);
		CName* cname = GPOS_NEW(mp) CName(mp, cwstr);
		CColRef *colref = GPOS_NEW(mp) CColRefTable(type, 1, 1, false, 0, cname, 1, 20);
		// And we stop here
		GPOS_ASSERT(NULL != colref);
		pcrs->Include(colref);
		colref_array->Append(colref);
	}
	LogicalOperator* pexprResult = pexpr;
	// Collect required properties (prpp) at the top level:
	COrderSpec* pos = GPOS_NEW(mp) COrderSpec(mp);
	// Ensure order, distribution and rewindability meet 'satisfy' matching at the top level
	CEnfdOrder *peo = GPOS_NEW(mp) CEnfdOrder(pos, CEnfdOrder::EomSatisfy);
	// Required CTEs are obtained from the CTEInfo global information in the optimizer context
	CCTEReq *pcter = poptctxt->Pcteinfo()->PcterProducers(mp);
	CReqdPropPlan *prpp = GPOS_NEW(mp) CReqdPropPlan(pcrs, peo, pcter);
	// Finally, create the CQueryContext
	pdrgpmdname->AddRef();
	return GPOS_NEW(mp) CQueryContext(mp, pexprResult, prpp, colref_array, pdrgpmdname, fDeriveStats);
}