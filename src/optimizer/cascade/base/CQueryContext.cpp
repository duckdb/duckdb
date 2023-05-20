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

using namespace gpopt;
//---------------------------------------------------------------------------
//	@function:
//		CQueryContext::CQueryContext
//
//	@doc:
//		Ctor
//
//---------------------------------------------------------------------------
CQueryContext::CQueryContext(CMemoryPool *mp, CExpression *pexpr,
							 CReqdPropPlan *prpp, CColRefArray *colref_array,
							 CMDNameArray *pdrgpmdname, BOOL fDeriveStats)
	: m_mp(mp),
	  m_prpp(prpp),
	  m_pdrgpcr(colref_array),
	  m_pdrgpcrSystemCols(NULL),
	  m_pdrgpmdname(pdrgpmdname),
	  m_fDeriveStats(fDeriveStats)
{
	GPOS_ASSERT(NULL != pexpr);
	GPOS_ASSERT(NULL != prpp);
	GPOS_ASSERT(NULL != colref_array);
	GPOS_ASSERT(NULL != pdrgpmdname);
	GPOS_ASSERT(colref_array->Size() == pdrgpmdname->Size());

#ifdef GPOS_DEBUG
	const ULONG ulReqdColumns = m_pdrgpcr->Size();
#endif	//GPOS_DEBUG

	// mark unused CTEs
	CCTEInfo *pcteinfo = COptCtxt::PoctxtFromTLS()->Pcteinfo();
	pcteinfo->MarkUnusedCTEs();

	CColRefSet *pcrsOutputAndOrderingCols = GPOS_NEW(mp) CColRefSet(mp);
	CColRefSet *pcrsOrderSpec = prpp->Peo()->PosRequired()->PcrsUsed(mp);

	pcrsOutputAndOrderingCols->Include(colref_array);
	pcrsOutputAndOrderingCols->Include(pcrsOrderSpec);
	pcrsOrderSpec->Release();

	m_pexpr = CExpressionPreprocessor::PexprPreprocess(
		mp, pexpr, pcrsOutputAndOrderingCols);

	pcrsOutputAndOrderingCols->Release();
	GPOS_ASSERT(m_pdrgpcr->Size() == ulReqdColumns);

	// collect required system columns
	SetSystemCols(mp);

	// collect CTE predicates and add them to CTE producer expressions
	CExpressionPreprocessor::AddPredsToCTEProducers(mp, m_pexpr);

	CColumnFactory *col_factory = COptCtxt::PoctxtFromTLS()->Pcf();

	// create the mapping between the computed column, defined in the expression
	// and all CTEs, and its corresponding used columns
	MapComputedToUsedCols(col_factory, m_pexpr);
	pcteinfo->MapComputedToUsedCols(col_factory);
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
	m_pexpr->Release();
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
COperator *
CQueryContext::PopTop(CExpression *pexpr)
{
	GPOS_ASSERT(NULL != pexpr);

	// skip CTE anchors if any
	CExpression *pexprCurr = pexpr;
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
void
CQueryContext::SetSystemCols(CMemoryPool *mp)
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
CQueryContext* CQueryContext::PqcGenerate(CMemoryPool *mp, CExpression *pexpr, ULongPtrArray* pdrgpulQueryOutputColRefId, CMDNameArray *pdrgpmdname, BOOL fDeriveStats)
{
	GPOS_ASSERT(NULL != pexpr && NULL != pdrgpulQueryOutputColRefId);
	CColRefSet *pcrs = GPOS_NEW(mp) CColRefSet(mp);
	CColRefArray *colref_array = GPOS_NEW(mp) CColRefArray(mp);
	COptCtxt *poptctxt = COptCtxt::PoctxtFromTLS();
	CColumnFactory *col_factory = poptctxt->Pcf();
	GPOS_ASSERT(NULL != col_factory);
	// Collect required column references (colref_array)
	const ULONG length = pdrgpulQueryOutputColRefId->Size();
	for (ULONG ul = 0; ul < length; ul++)
	{
		ULONG pul = (*pdrgpulQueryOutputColRefId)[ul];
		GPOS_ASSERT(NULL != pul);
		CColRef *colref = col_factory->LookupColRef(pul);
		GPOS_ASSERT(NULL != colref);
		pcrs->Include(colref);
		colref_array->Append(colref);
	}
	// Collect required properties (prpp) at the top level:
	// By default no sort order requirement is added, unless the root operator in
	// the input logical expression is a LIMIT. This is because Orca always
	// attaches top level Sort to a LIMIT node.
	COrderSpec *pos = NULL;
	CExpression *pexprResult = pexpr;
	COperator *popTop = PopTop(pexpr);
	if (COperator::EopLogicalLimit == popTop->Eopid())
	{
		// top level operator is a limit, copy order spec to query context
		pos = CLogicalLimit::PopConvert(popTop)->Pos();
		pos->AddRef();
	}
	else
	{
		// no order required
		pos = GPOS_NEW(mp) COrderSpec(mp);
	}
	CDistributionSpec *pds = NULL;
	BOOL fDML = CUtils::FLogicalDML(pexpr->Pop());
	poptctxt->MarkDMLQuery(fDML);

	// DML commands do not have distribution requirement. Otherwise the
	// distribution requirement is Singleton.
	if (fDML)
	{
		pds = GPOS_NEW(mp) CDistributionSpecAny(COperator::EopSentinel);
	}
	else
	{
		pds = GPOS_NEW(mp) CDistributionSpecSingleton(CDistributionSpecSingleton::EstMaster);
	}

	// By default, no rewindability requirement needs to be satisfied at the top level
	CRewindabilitySpec *prs = GPOS_NEW(mp) CRewindabilitySpec(CRewindabilitySpec::ErtNone, CRewindabilitySpec::EmhtNoMotion);

	// Ensure order, distribution and rewindability meet 'satisfy' matching at the top level
	CEnfdOrder *peo = GPOS_NEW(mp) CEnfdOrder(pos, CEnfdOrder::EomSatisfy);
	CEnfdDistribution *ped = GPOS_NEW(mp) CEnfdDistribution(pds, CEnfdDistribution::EdmSatisfy);
	CEnfdRewindability *per = GPOS_NEW(mp) CEnfdRewindability(prs, CEnfdRewindability::ErmSatisfy);

	// Required CTEs are obtained from the CTEInfo global information in the optimizer context
	CCTEReq *pcter = poptctxt->Pcteinfo()->PcterProducers(mp);

	// NB: Partition propagation requirements are not initialized here.  They are
	// constructed later based on derived relation properties (CPartInfo) by
	// CReqdPropPlan::InitReqdPartitionPropagation().
	CReqdPropPlan *prpp = GPOS_NEW(mp) CReqdPropPlan(pcrs, peo, ped, per, pcter);

	// Finally, create the CQueryContext
	pdrgpmdname->AddRef();
	return GPOS_NEW(mp) CQueryContext(mp, pexprResult, prpp, colref_array, pdrgpmdname, fDeriveStats);
}

#ifdef GPOS_DEBUG

//---------------------------------------------------------------------------
//	@function:
//		CQueryContext::OsPrint
//
//	@doc:
//		Debug print
//
//---------------------------------------------------------------------------
IOstream &
CQueryContext::OsPrint(IOstream &os) const
{
	return os << *m_pexpr << std::endl << *m_prpp;
}

void
CQueryContext::DbgPrint() const
{
	CAutoTrace at(m_mp);
	(void) this->OsPrint(at.Os());
}
#endif	// GPOS_DEBUG


//---------------------------------------------------------------------------
//	@function:
//		CQueryContext::MapComputedToUsedCols
//
//	@doc:
//		Walk the expression and add the mapping between computed column
//		and its used columns
//
//---------------------------------------------------------------------------
void
CQueryContext::MapComputedToUsedCols(CColumnFactory *col_factory,
									 CExpression *pexpr)
{
	GPOS_ASSERT(NULL != pexpr);

	if (COperator::EopLogicalProject == pexpr->Pop()->Eopid())
	{
		CExpression *pexprPrL = (*pexpr)[1];

		const ULONG arity = pexprPrL->Arity();
		for (ULONG ul = 0; ul < arity; ul++)
		{
			CExpression *pexprPrEl = (*pexprPrL)[ul];
			col_factory->AddComputedToUsedColsMap(pexprPrEl);
		}
	}

	// process children
	const ULONG ulChildren = pexpr->Arity();
	for (ULONG ul = 0; ul < ulChildren; ul++)
	{
		MapComputedToUsedCols(col_factory, (*pexpr)[ul]);
	}
}
