//---------------------------------------------------------------------------
//	@filename:
//		CGroupExpression.cpp
//
//	@doc:
//		Implementation of group expressions
//---------------------------------------------------------------------------
#include "duckdb/optimizer/cascade/search/CGroupExpression.h"
#include "duckdb/optimizer/cascade/base.h"
#include "duckdb/optimizer/cascade/error/CAutoTrace.h"
#include "duckdb/optimizer/cascade/io/COstreamString.h"
#include "duckdb/optimizer/cascade/string/CWStringDynamic.h"
#include "duckdb/optimizer/cascade/task/CAutoSuspendAbort.h"
#include "duckdb/optimizer/cascade/task/CWorker.h"
#include "duckdb/optimizer/cascade/base/COptimizationContext.h"
#include "duckdb/optimizer/cascade/base/CUtils.h"
#include "duckdb/optimizer/cascade/operators/ops.h"
#include "duckdb/optimizer/cascade/optimizer/COptimizerConfig.h"
#include "duckdb/optimizer/cascade/search/CGroupProxy.h"
#include "duckdb/optimizer/cascade/xforms/CXformFactory.h"
#include "duckdb/optimizer/cascade/xforms/CXformUtils.h"
#include "duckdb/optimizer/cascade/traceflags/traceflags.h"

using namespace gpopt;

#define GPOPT_COSTCTXT_HT_BUCKETS 100

// invalid group expression
const CGroupExpression CGroupExpression::m_gexprInvalid;


//---------------------------------------------------------------------------
//	@function:
//		CGroupExpression::CGroupExpression
//
//	@doc:
//		ctor
//
//---------------------------------------------------------------------------
CGroupExpression::CGroupExpression(CMemoryPool *mp, COperator *pop,
								   CGroupArray *pdrgpgroup,
								   CXform::EXformId exfid,
								   CGroupExpression *pgexprOrigin,
								   BOOL fIntermediate)
	: m_mp(mp),
	  m_id(GPOPT_INVALID_GEXPR_ID),
	  m_pgexprDuplicate(NULL),
	  m_pop(pop),
	  m_pdrgpgroup(pdrgpgroup),
	  m_pdrgpgroupSorted(NULL),
	  m_pgroup(NULL),
	  m_exfidOrigin(exfid),
	  m_pgexprOrigin(pgexprOrigin),
	  m_fIntermediate(fIntermediate),
	  m_estate(estUnexplored),
	  m_eol(EolLow),
	  m_ppartialplancostmap(NULL),
	  m_ecirculardependency(ecdDefault)
{
	GPOS_ASSERT(NULL != pop);
	GPOS_ASSERT(NULL != pdrgpgroup);
	GPOS_ASSERT_IMP(exfid != CXform::ExfInvalid, NULL != pgexprOrigin);

	// store sorted array of children for faster comparison
	if (1 < pdrgpgroup->Size() && !pop->FInputOrderSensitive())
	{
		m_pdrgpgroupSorted = GPOS_NEW(mp) CGroupArray(mp, pdrgpgroup->Size());
		m_pdrgpgroupSorted->AppendArray(pdrgpgroup);
		m_pdrgpgroupSorted->Sort();

		GPOS_ASSERT(m_pdrgpgroupSorted->IsSorted());
	}

	m_ppartialplancostmap = GPOS_NEW(mp) PartialPlanToCostMap(mp);

	// initialize cost contexts hash table
	m_sht.Init(mp, GPOPT_COSTCTXT_HT_BUCKETS, GPOS_OFFSET(CCostContext, m_link),
			   GPOS_OFFSET(CCostContext, m_poc),
			   &(COptimizationContext::m_pocInvalid),
			   COptimizationContext::HashValue, COptimizationContext::Equals);
}


//---------------------------------------------------------------------------
//	@function:
//		CGroupExpression::~CGroupExpression
//
//	@doc:
//		Dtor
//
//---------------------------------------------------------------------------
CGroupExpression::~CGroupExpression()
{
	if (this != &(CGroupExpression::m_gexprInvalid))
	{
		CleanupContexts();

		m_pop->Release();
		m_pdrgpgroup->Release();

		CRefCount::SafeRelease(m_pdrgpgroupSorted);
		m_ppartialplancostmap->Release();
	}
}


//---------------------------------------------------------------------------
//	@function:
//		CGroupExpression::CleanupContexts
//
//	@doc:
//		 Destroy stored cost contexts in hash table
//
//---------------------------------------------------------------------------
void
CGroupExpression::CleanupContexts()
{
	// need to suspend cancellation while cleaning up
	{
		CAutoSuspendAbort asa;

		ShtIter shtit(m_sht);
		CCostContext *pcc = NULL;
		while (NULL != pcc || shtit.Advance())
		{
			if (NULL != pcc)
			{
				pcc->Release();
			}

			// iter's accessor scope
			{
				ShtAccIter shtitacc(shtit);
				if (NULL != (pcc = shtitacc.Value()))
				{
					shtitacc.Remove(pcc);
				}
			}
		}
	}
}


//---------------------------------------------------------------------------
//	@function:
//		CGroupExpression::Init
//
//	@doc:
//		Init group expression
//
//
//---------------------------------------------------------------------------
void
CGroupExpression::Init(CGroup *pgroup, ULONG id)
{
	SetGroup(pgroup);
	SetId(id);
	SetOptimizationLevel();
}


//---------------------------------------------------------------------------
//	@function:
//		CGroupExpression::SetOptimizationLevel
//
//	@doc:
//		Set optimization level of group expression
//
//
//---------------------------------------------------------------------------
void
CGroupExpression::SetOptimizationLevel()
{
	// a sequence expression with a first child group that contains a CTE
	// producer gets a higher optimization level. This is to be sure that the
	// producer gets optimized before its consumers
	if (COperator::EopPhysicalSequence == m_pop->Eopid())
	{
		CGroup *pgroupFirst = (*this)[0];
		if (pgroupFirst->FHasCTEProducer())
		{
			m_eol = EolHigh;
		}
	}
	else if (CUtils::FHashJoin(m_pop))
	{
		// optimize hash join first to minimize plan cost quickly
		m_eol = EolHigh;
	}
	else if (CUtils::FPhysicalAgg(m_pop))
	{
		BOOL fPreferMultiStageAgg = GPOS_FTRACE(EopttraceForceMultiStageAgg);
		if (!fPreferMultiStageAgg &&
			COperator::EopPhysicalHashAgg == m_pop->Eopid())
		{
			// if we choose agg plans based on cost only (no preference for multi-stage agg),
			// we optimize hash agg first to to minimize plan cost quickly
			m_eol = EolHigh;
			return;
		}

		// if we only want plans with multi-stage agg, we generate multi-stage agg
		// first to avoid later optimization of one stage agg if possible
		BOOL fMultiStage = CPhysicalAgg::PopConvert(m_pop)->FMultiStage();
		if (fPreferMultiStageAgg && fMultiStage)
		{
			// optimize multi-stage agg first to allow avoiding one-stage agg if possible
			m_eol = EolHigh;
		}
	}
}

//---------------------------------------------------------------------------
//	@function:
//		CGroupExpression::FValidContext
//
//	@doc:
//		Check if group expression is valid with respect to given child contexts
//
//		This is called during cost computation phase in group expression
//		optimization after enforcement is complete. Since it is called bottom-up,
//		for the given physical group expression, all the derived properties are
//		already computed.
//
//		Since property enforcement in CEngine::FCheckEnfdProps() only determines
//		whether or not an enforcer is added to the group, it is possible for the
//		enforcer group expression to select a child group expression that did not
//		create the enforcer. This could lead to invalid plans that could not have
//		been prevented earlier because derived physical properties weren't
//		available. For example, a Motion group expression may select as a child a
//		DynamicTableScan that has unresolved part propagators, instead of picking
//		the PartitionSelector enforcer which would resolve it.
//
//		This method can be used to reject such plans.
//
//---------------------------------------------------------------------------
BOOL
CGroupExpression::FValidContext(CMemoryPool *mp, COptimizationContext *poc,
								COptimizationContextArray *pdrgpocChild)
{
	GPOS_ASSERT(m_pop->FPhysical());

	return CPhysical::PopConvert(m_pop)->FValidContext(mp, poc, pdrgpocChild);
}


//---------------------------------------------------------------------------
//	@function:
//		CGroupExpression::SetId
//
//	@doc:
//		Set id of expression
//
//---------------------------------------------------------------------------
void
CGroupExpression::SetId(ULONG id)
{
	GPOS_ASSERT(GPOPT_INVALID_GEXPR_ID == m_id);

	m_id = id;
}


//---------------------------------------------------------------------------
//	@function:
//		CGroupExpression::SetGroup
//
//	@doc:
//		Set group pointer of expression
//
//---------------------------------------------------------------------------
void
CGroupExpression::SetGroup(CGroup *pgroup)
{
	GPOS_ASSERT(NULL == m_pgroup);
	GPOS_ASSERT(NULL != pgroup);

	m_pgroup = pgroup;
}

//---------------------------------------------------------------------------
//	@function:
//		CGroupExpression::FCostContextExists
//
//	@doc:
//		Check if cost context already exists in group expression hash table
//
//---------------------------------------------------------------------------
BOOL
CGroupExpression::FCostContextExists(COptimizationContext *poc,
									 COptimizationContextArray *pdrgpoc)
{
	GPOS_ASSERT(NULL != poc);

	// lookup context based on required properties
	CCostContext *pccFound = NULL;
	{
		ShtAcc shta(Sht(), poc);
		pccFound = shta.Find();
	}

	while (NULL != pccFound)
	{
		if (COptimizationContext::FEqualContextIds(pdrgpoc,
												   pccFound->Pdrgpoc()))
		{
			// a cost context, matching required properties and child contexts, was already created
			return true;
		}

		{
			ShtAcc shta(Sht(), poc);
			pccFound = shta.Next(pccFound);
		}
	}

	return false;
}


//---------------------------------------------------------------------------
//     @function:
//			CGroupExpression::PccRemove
//
//     @doc:
//			Remove cost context in hash table;
//
//---------------------------------------------------------------------------
CCostContext *
CGroupExpression::PccRemove(COptimizationContext *poc, ULONG ulOptReq)
{
	GPOS_ASSERT(NULL != poc);
	ShtAcc shta(Sht(), poc);
	CCostContext *pccFound = shta.Find();
	while (NULL != pccFound)
	{
		if (ulOptReq == pccFound->UlOptReq())
		{
			shta.Remove(pccFound);
			return pccFound;
		}

		pccFound = shta.Next(pccFound);
	}

	return NULL;
}


//---------------------------------------------------------------------------
//     @function:
//			CGroupExpression::PccInsertBest
//
//     @doc:
//			Insert given context in hash table only if a better context
//			does not already exist,
//			return the context that is kept in hash table
//
//---------------------------------------------------------------------------
CCostContext *
CGroupExpression::PccInsertBest(CCostContext *pcc)
{
	GPOS_ASSERT(NULL != pcc);

	COptimizationContext *poc = pcc->Poc();
	const ULONG ulOptReq = pcc->UlOptReq();

	// remove existing cost context, if any
	CCostContext *pccExisting = PccRemove(poc, ulOptReq);
	CCostContext *pccKept = NULL;

	// compare existing context with given context
	if (NULL == pccExisting || pcc->FBetterThan(pccExisting))
	{
		// insert new context
		pccKept = PccInsert(pcc);
		GPOS_ASSERT(pccKept == pcc);

		if (NULL != pccExisting)
		{
			if (pccExisting == poc->PccBest())
			{
				// change best cost context of the corresponding optimization context
				poc->SetBest(pcc);
			}
			pccExisting->Release();
		}
	}
	else
	{
		// re-insert existing context
		pcc->Release();
		pccKept = PccInsert(pccExisting);
		GPOS_ASSERT(pccKept == pccExisting);
	}

	return pccKept;
}


//---------------------------------------------------------------------------
//	@function:
//		CGroupExpression::PccComputeCost
//
//	@doc:
//		Compute and store expression's cost under a given context;
//		the function returns the cost context containing the computed cost
//
//---------------------------------------------------------------------------
CCostContext *
CGroupExpression::PccComputeCost(
	CMemoryPool *mp, COptimizationContext *poc, ULONG ulOptReq,
	COptimizationContextArray *pdrgpoc,	 // array of child contexts
	BOOL fPruned,  // is created cost context pruned based on cost bound
	CCost
		costLowerBound	// lower bound on the cost of plan carried by cost context
)
{
	GPOS_ASSERT(NULL != poc);
	GPOS_ASSERT_IMP(!fPruned, NULL != pdrgpoc);

	if (!fPruned && !FValidContext(mp, poc, pdrgpoc))
	{
		return NULL;
	}

	// check if the same cost context is already created for current group expression
	if (FCostContextExists(poc, pdrgpoc))
	{
		return NULL;
	}

	poc->AddRef();
	this->AddRef();
	CCostContext *pcc = GPOS_NEW(mp) CCostContext(mp, poc, ulOptReq, this);
	BOOL fValid = true;

	// computing cost
	pcc->SetState(CCostContext::estCosting);

	if (!fPruned)
	{
		if (NULL != pdrgpoc)
		{
			pdrgpoc->AddRef();
		}
		pcc->SetChildContexts(pdrgpoc);

		fValid = pcc->IsValid(mp);
		if (fValid)
		{
			CCost cost = CostCompute(mp, pcc);
			pcc->SetCost(cost);
		}
		GPOS_ASSERT_IMP(COptCtxt::FAllEnforcersEnabled(),
						fValid && "Cost context carries an invalid plan");
	}
	else
	{
		pcc->SetPruned();
		pcc->SetCost(costLowerBound);
	}

	pcc->SetState(CCostContext::estCosted);
	if (fValid)
	{
		return PccInsertBest(pcc);
	}

	pcc->Release();

	// invalid cost context
	return NULL;
}


//---------------------------------------------------------------------------
//	@function:
//		CGroupExpression::CostLowerBound
//
//	@doc:
//		Compute a lower bound on plans rooted by current group expression for
//		the given required properties
//
//---------------------------------------------------------------------------
CCost
CGroupExpression::CostLowerBound(CMemoryPool *mp, CReqdPropPlan *prppInput,
								 CCostContext *pccChild, ULONG child_index)
{
	GPOS_ASSERT(NULL != prppInput);
	GPOS_ASSERT(Pop()->FPhysical());

	prppInput->AddRef();
	if (NULL != pccChild)
	{
		pccChild->AddRef();
	}
	CPartialPlan *ppp =
		GPOS_NEW(mp) CPartialPlan(this, prppInput, pccChild, child_index);
	CCost *pcostLowerBound = m_ppartialplancostmap->Find(ppp);
	if (NULL != pcostLowerBound)
	{
		ppp->Release();
		return *pcostLowerBound;
	}

	// compute partial plan cost
	CCost cost = ppp->CostCompute(mp);

#ifdef GPOS_DEBUG
	BOOL fSuccess =
#endif	// GPOS_DEBUG
		m_ppartialplancostmap->Insert(ppp, GPOS_NEW(mp) CCost(cost.Get()));
	GPOS_ASSERT(fSuccess);

	return cost;
}


//---------------------------------------------------------------------------
//	@function:
//		CGroupExpression::SetState
//
//	@doc:
//		Set group expression state;
//
//---------------------------------------------------------------------------
void
CGroupExpression::SetState(EState estNewState)
{
	GPOS_ASSERT(estNewState == (EState)(m_estate + 1));

	m_estate = estNewState;
}


//---------------------------------------------------------------------------
//	@function:
//		CGroupExpression::ResetState
//
//	@doc:
//		Reset group expression state;
//
//---------------------------------------------------------------------------
void
CGroupExpression::ResetState()
{
	m_estate = estUnexplored;
}


//---------------------------------------------------------------------------
//	@function:
//		CGroupExpression::CostCompute
//
//	@doc:
//		Costing scheme.
//
//---------------------------------------------------------------------------
CCost
CGroupExpression::CostCompute(CMemoryPool *mp, CCostContext *pcc) const
{
	GPOS_ASSERT(NULL != pcc);

	// prepare cost array
	COptimizationContextArray *pdrgpoc = pcc->Pdrgpoc();
	CCostArray *pdrgpcostChildren = GPOS_NEW(mp) CCostArray(mp);
	const ULONG length = pdrgpoc->Size();
	for (ULONG ul = 0; ul < length; ul++)
	{
		COptimizationContext *pocChild = (*pdrgpoc)[ul];
		pdrgpcostChildren->Append(GPOS_NEW(mp)
									  CCost(pocChild->PccBest()->Cost()));
	}

	CCost cost = pcc->CostCompute(mp, pdrgpcostChildren);
	pdrgpcostChildren->Release();

	return cost;
}


//---------------------------------------------------------------------------
//	@function:
//		CGroupExpression::FTransitioned
//
//	@doc:
//		Check if transition to the given state is completed;
//
//---------------------------------------------------------------------------
BOOL
CGroupExpression::FTransitioned(EState estate) const
{
	GPOS_ASSERT(estate == estExplored || estate == estImplemented);

	return !Pop()->FLogical() || (estate == estExplored && FExplored()) ||
		   (estate == estImplemented && FImplemented());
}


//---------------------------------------------------------------------------
//	@function:
//		CGroupExpression::PccLookup
//
//	@doc:
//		Lookup cost context in hash table;
//
//---------------------------------------------------------------------------
CCostContext *
CGroupExpression::PccLookup(COptimizationContext *poc, ULONG ulOptReq)
{
	GPOS_ASSERT(NULL != poc);

	ShtAcc shta(Sht(), poc);
	CCostContext *pccFound = shta.Find();
	while (NULL != pccFound)
	{
		if (ulOptReq == pccFound->UlOptReq())
		{
			return pccFound;
		}

		pccFound = shta.Next(pccFound);
	}

	return NULL;
}


//---------------------------------------------------------------------------
//	@function:
//		CGroupExpression::PccLookupAll
//
//	@doc:
//		Lookup all valid cost contexts matching given optimization context
//
//---------------------------------------------------------------------------
CCostContextArray *
CGroupExpression::PdrgpccLookupAll(CMemoryPool *mp, COptimizationContext *poc)
{
	GPOS_ASSERT(NULL != poc);
	CCostContextArray *pdrgpcc = GPOS_NEW(mp) CCostContextArray(mp);

	CCostContext *pccFound = NULL;
	BOOL fValid = false;
	{
		ShtAcc shta(Sht(), poc);
		pccFound = shta.Find();
		fValid = (NULL != pccFound && pccFound->Cost() != GPOPT_INVALID_COST &&
				  !pccFound->FPruned());
	}

	while (NULL != pccFound)
	{
		if (fValid)
		{
			pccFound->AddRef();
			pdrgpcc->Append(pccFound);
		}

		{
			ShtAcc shta(Sht(), poc);
			pccFound = shta.Next(pccFound);
			fValid =
				(NULL != pccFound && pccFound->Cost() != GPOPT_INVALID_COST &&
				 !pccFound->FPruned());
		}
	}

	return pdrgpcc;
}


//---------------------------------------------------------------------------
//	@function:
//		CGroupExpression::PccInsert
//
//	@doc:
//		Insert a cost context in hash table;
//
//---------------------------------------------------------------------------
CCostContext *
CGroupExpression::PccInsert(CCostContext *pcc)
{
	// HERE BE DRAGONS
	// See comment in CCache::InsertEntry
	COptimizationContext *const poc = pcc->Poc();
	ShtAcc shta(Sht(), poc);

	CCostContext *pccFound = shta.Find();
	while (NULL != pccFound)
	{
		if (CCostContext::Equals(*pcc, *pccFound))
		{
			return pccFound;
		}
		pccFound = shta.Next(pccFound);
	}
	GPOS_ASSERT(NULL == pccFound);

	shta.Insert(pcc);
	return pcc;
}


//---------------------------------------------------------------------------
//	@function:
//		CGroupExpression::PreprocessTransform
//
//	@doc:
//		Pre-processing before applying transformation
//
//---------------------------------------------------------------------------
void CGroupExpression::PreprocessTransform(CMemoryPool *pmpLocal, CMemoryPool *pmpGlobal, CXform *pxform)
{
	if (CXformUtils::FDeriveStatsBeforeXform(pxform) && NULL == Pgroup()->Pstats())
	{
		GPOS_ASSERT(Pgroup()->FStatsDerivable(pmpGlobal));

		// derive stats on container group before applying xform
		CExpressionHandle exprhdl(pmpGlobal);
		exprhdl.Attach(this);
		exprhdl.DeriveStats(pmpLocal, pmpGlobal, NULL /*prprel*/, NULL /*stats_ctxt*/);
	}
}


//---------------------------------------------------------------------------
//	@function:
//		CGroupExpression::PostprocessTransform
//
//	@doc:
//		Post-processing after applying transformation
//
//---------------------------------------------------------------------------
void
CGroupExpression::PostprocessTransform(CMemoryPool *, /* pmpLocal */ CMemoryPool *, /* pmpGlobal */ CXform *pxform)
{
	if (CXformUtils::FDeriveStatsBeforeXform(pxform))
	{
		(void) Pgroup()->FResetStats();
	}
}


//---------------------------------------------------------------------------
//	@function:
//		CGroupExpression::Transform
//
//	@doc:
//		Transform group expression using the given xform
//
//---------------------------------------------------------------------------
void
CGroupExpression::Transform(
	CMemoryPool *mp, CMemoryPool *pmpLocal, CXform *pxform,
	CXformResult *pxfres,
	ULONG *pulElapsedTime,	// output: elapsed time in millisecond
	ULONG *pulNumberOfBindings)
{
	GPOS_ASSERT(NULL != pulElapsedTime);
	GPOS_CHECK_ABORT;

	BOOL fPrintOptStats = GPOS_FTRACE(EopttracePrintOptimizationStatistics);
	CTimerUser timer;
	if (fPrintOptStats)
	{
		timer.Restart();
	}

	*pulElapsedTime = 0;
	// check traceflag and compatibility with origin xform
	if (GPOPT_FDISABLED_XFORM(pxform->Exfid()) ||
		!pxform->FCompatible(m_exfidOrigin))
	{
		if (fPrintOptStats)
		{
			*pulElapsedTime = timer.ElapsedMS();
		}
		return;
	}

	// check xform promise
	CExpressionHandle exprhdl(mp);
	exprhdl.Attach(this);
	exprhdl.DeriveProps(NULL /*pdpctxt*/);
	if (CXform::ExfpNone == pxform->Exfp(exprhdl))
	{
		if (GPOS_FTRACE(EopttracePrintOptimizationStatistics))
		{
			*pulElapsedTime = timer.ElapsedMS();
		}
		return;
	}

	// pre-processing before applying xform to group expression
	PreprocessTransform(pmpLocal, mp, pxform);

	// extract memo bindings to apply xform
	CBinding binding;
	CXformContext *pxfctxt = GPOS_NEW(mp) CXformContext(mp);

	COptimizerConfig *optconfig =
		COptCtxt::PoctxtFromTLS()->GetOptimizerConfig();
	ULONG bindThreshold = optconfig->GetHint()->UlXformBindThreshold();
	CExpression *pexprPattern = pxform->PexprPattern();
	CExpression *pexpr = binding.PexprExtract(mp, this, pexprPattern, NULL);
	while (NULL != pexpr)
	{
		++(*pulNumberOfBindings);
		ULONG ulNumResults = pxfres->Pdrgpexpr()->Size();
		pxform->Transform(pxfctxt, pxfres, pexpr);
		ulNumResults = pxfres->Pdrgpexpr()->Size() - ulNumResults;
		PrintXform(mp, pxform, pexpr, pxfres, ulNumResults);

		if ((bindThreshold != 0 && (*pulNumberOfBindings) > bindThreshold) || pxform->IsApplyOnce() || (0 < pxfres->Pdrgpexpr()->Size() && !CXformUtils::FApplyToNextBinding(pxform, pexpr)))
		{
			// do not apply xform to other possible patterns
			pexpr->Release();
			break;
		}

		CExpression *pexprLast = pexpr;
		pexpr = binding.PexprExtract(mp, this, pexprPattern, pexprLast);

		// release last extracted expression
		pexprLast->Release();

		GPOS_CHECK_ABORT;
	}
	pxfctxt->Release();

	// post-prcoessing before applying xform to group expression
	PostprocessTransform(pmpLocal, mp, pxform);

	if (fPrintOptStats)
	{
		*pulElapsedTime = timer.ElapsedMS();
	}
}


//---------------------------------------------------------------------------
//	@function:
//		CGroupExpression::FMatchNonScalarChildren
//
//	@doc:
//		Match children of group expression against given children of
//		passed expression
//
//---------------------------------------------------------------------------
BOOL
CGroupExpression::FMatchNonScalarChildren(const CGroupExpression *pgexpr) const
{
	GPOS_ASSERT(NULL != pgexpr);

	if (0 == Arity())
	{
		return (pgexpr->Arity() == 0);
	}

	return CGroup::FMatchNonScalarGroups(m_pdrgpgroup, pgexpr->m_pdrgpgroup);
}


//---------------------------------------------------------------------------
//	@function:
//		CGroupExpression::Matches
//
//	@doc:
//		Match group expression against given operator and its children
//
//---------------------------------------------------------------------------
BOOL
CGroupExpression::Matches(const CGroupExpression *pgexpr) const
{
	GPOS_ASSERT(NULL != pgexpr);

	// make sure we are not comparing to invalid group expression
	if (NULL == this->Pop() || NULL == pgexpr->Pop())
	{
		return NULL == this->Pop() && NULL == pgexpr->Pop();
	}

	// have same arity
	if (Arity() != pgexpr->Arity())
	{
		return false;
	}

	// match operators
	if (!m_pop->Matches(pgexpr->m_pop))
	{
		return false;
	}

	// compare inputs
	if (0 == Arity())
	{
		return true;
	}
	else
	{
		if (1 == Arity() || m_pop->FInputOrderSensitive())
		{
			return CGroup::FMatchGroups(m_pdrgpgroup, pgexpr->m_pdrgpgroup);
		}
		else
		{
			GPOS_ASSERT(NULL != m_pdrgpgroupSorted &&
						NULL != pgexpr->m_pdrgpgroupSorted);

			return CGroup::FMatchGroups(m_pdrgpgroupSorted,
										pgexpr->m_pdrgpgroupSorted);
		}
	}

	GPOS_ASSERT(!"Unexpected exit from function");
	return false;
}


//---------------------------------------------------------------------------
//	@function:
//		CGroupExpression::HashValue
//
//	@doc:
//		static hash function for operator and group references
//
//---------------------------------------------------------------------------
ULONG
CGroupExpression::HashValue(COperator *pop, CGroupArray *pdrgpgroup)
{
	GPOS_ASSERT(NULL != pop);
	GPOS_ASSERT(NULL != pdrgpgroup);

	ULONG ulHash = pop->HashValue();

	ULONG arity = pdrgpgroup->Size();
	for (ULONG i = 0; i < arity; i++)
	{
		ulHash = CombineHashes(ulHash, (*pdrgpgroup)[i]->HashValue());
	}

	return ulHash;
}


//---------------------------------------------------------------------------
//	@function:
//		CGroupExpression::HashValue
//
//	@doc:
//		static hash function for group expressions
//
//---------------------------------------------------------------------------
ULONG
CGroupExpression::HashValue(const CGroupExpression &gexpr)
{
	return gexpr.HashValue();
}


//---------------------------------------------------------------------------
//	@function:
//		CGroupExpression::PstatsRecursiveDerive
//
//	@doc:
//		Derive stats recursively on group expression
//
//---------------------------------------------------------------------------
IStatistics *
CGroupExpression::PstatsRecursiveDerive(CMemoryPool *,	// pmpLocal
										CMemoryPool *pmpGlobal,
										CReqdPropRelational *prprel,
										IStatisticsArray *stats_ctxt,
										BOOL fComputeRootStats)
{
	GPOS_ASSERT(!Pgroup()->FScalar());
	GPOS_ASSERT(!Pgroup()->FImplemented());
	GPOS_ASSERT(NULL != stats_ctxt);
	GPOS_CHECK_ABORT;

	// trigger recursive property derivation
	CExpressionHandle exprhdl(pmpGlobal);
	exprhdl.Attach(this);
	exprhdl.DeriveProps(NULL /*pdpctxt*/);

	// compute required relational properties on child groups
	exprhdl.ComputeReqdProps(prprel, 0 /*ulOptReq*/);

	// trigger recursive stat derivation
	exprhdl.DeriveStats(stats_ctxt, fComputeRootStats);
	IStatistics *stats = exprhdl.Pstats();
	if (NULL != stats)
	{
		stats->AddRef();
	}

	return stats;
}


//---------------------------------------------------------------------------
//	@function:
//		CGroupExpression::PrintXform
//
//	@doc:
//		Print transformation
//
//---------------------------------------------------------------------------
void
CGroupExpression::PrintXform(CMemoryPool *mp, CXform *pxform,
							 CExpression *pexpr, CXformResult *pxfres,
							 ULONG ulNumResults)
{
	if (NULL != pexpr && GPOS_FTRACE(EopttracePrintXform) &&
		GPOS_FTRACE(EopttracePrintXformResults))
	{
		CAutoTrace at(mp);
		IOstream &os(at.Os());

		os << *pxform << std::endl
		   << "Input:" << std::endl
		   << *pexpr << "Output:" << std::endl
		   << "Alternatives:" << std::endl;
		CExpressionArray *pdrgpexpr = pxfres->Pdrgpexpr();
		ULONG ulStart = pdrgpexpr->Size() - ulNumResults;
		ULONG end = pdrgpexpr->Size();

		for (ULONG i = ulStart; i < end; i++)
		{
			os << i - ulStart << ": " << std::endl;
			(*pdrgpexpr)[i]->OsPrint(os);
		}
	}
}

//---------------------------------------------------------------------------
//	@function:
//		CGroupExpression::OsPrintCostContexts
//
//	@doc:
//		Print group expression cost contexts
//
//---------------------------------------------------------------------------
IOstream &
CGroupExpression::OsPrintCostContexts(IOstream &os, const CHAR *szPrefix) const
{
	if (Pop()->FPhysical() && GPOS_FTRACE(EopttracePrintOptimizationContext))
	{
		// print cost contexts
		os << szPrefix << szPrefix << "Cost Ctxts:" << std::endl;
		CCostContext *pcc = NULL;
		ShtIter shtit(const_cast<CGroupExpression *>(this)->Sht());
		while (shtit.Advance())
		{
			{
				ShtAccIter shtitacc(shtit);
				pcc = shtitacc.Value();
			}

			if (NULL != pcc)
			{
				os << szPrefix << szPrefix << szPrefix;
				(void) pcc->OsPrint(os);
			}
		}
	}

	return os;
}


// Consider the group expression CLogicalSelect [ 0 3 ]
// it has the 0th child coming from Group 0, where Group 0 has Duplicate Group 4
// While deriving Stats, this will cause a circular loop as follows
// 1. CLogicalSelect child 0 -> Will ask the stats to be derived on Group 0
// 2. Group 0 will ask Group 4 to give the stats (as its duplicate),
// which will then again ask CLogicalSelect [0 4] to derive stats resulting in a loop.
// Such Group Expression can be ignored for deriving stats and implementation.
// Group 4 (#GExprs: 5):
// 0: CLogicalSelect [ 0 3 ]
// 1: CLogicalNAryJoin [ 6 7 8 ] Origin: (xform: CXformInlineCTEConsumerUnderSelect, Grp: 4, GrpExpr: 0)
// 2: CLogicalCTEConsumer (0), Columns: ["a" (18), "b" (19), "a" (20), "b" (21)] [ ]
// 3: CLogicalNAryJoin [ 6 7 3 ] Origin: (xform: CXformInlineCTEConsumer, Grp: 4, GrpExpr: 2)
// 4: CLogicalInnerJoin [ 6 7 3 ] Origin: (xform: CXformExpandNAryJoinGreedy, Grp: 4, GrpExpr: 3)
//
// Group 0 (#GExprs: 0, Duplicate Group: 4):
BOOL
CGroupExpression::ContainsCircularDependencies()
{
	// if it's already marked to contain circular dependency, return early
	if (m_ecirculardependency == CGroupExpression::ecdCircularDependency)
	{
		return true;
	}

	GPOS_ASSERT(m_ecirculardependency == CGroupExpression::ecdDefault);

	// if exploration is completed, then the group expression does not have
	// any circular dependency
	if (Pgroup()->FExplored())
	{
		return false;
	}

	// we are still in exploration phase, check if there are any circular dependencies
	CGroupArray *child_groups = Pdrgpgroup();
	for (ULONG ul = 0; ul < child_groups->Size(); ul++)
	{
		CGroup *child_group = (*child_groups)[ul];
		if (child_group->FScalar())
			continue;
		CGroup *child_duplicate_group = child_group->PgroupDuplicate();
		if (child_duplicate_group != NULL)
		{
			ULONG child_duplicate_group_id = child_duplicate_group->Id();
			ULONG current_group_id = Pgroup()->Id();
			if (child_duplicate_group_id == current_group_id)
			{
				m_ecirculardependency = CGroupExpression::ecdCircularDependency;
				GPOS_ASSERT(Pgroup()->UlGExprs() > 1);
				break;
			}
		}
	}

	return m_ecirculardependency == CGroupExpression::ecdCircularDependency;
}

//---------------------------------------------------------------------------
//	@function:
//		CGroupExpression::OsPrint
//
//	@doc:
//		Print function
//
//---------------------------------------------------------------------------
IOstream &
CGroupExpression::OsPrint(IOstream &os) const
{
	return OsPrintWithPrefix(os, "");
}

IOstream &
CGroupExpression::OsPrintWithPrefix(IOstream &os, const CHAR *szPrefix) const
{
	os << szPrefix << m_id << ": ";
	(void) m_pop->OsPrint(os);

	if (EolHigh == m_eol)
	{
		os << " (High)";
	}
	os << " [ ";

	ULONG arity = Arity();
	for (ULONG i = 0; i < arity; i++)
	{
		os << (*m_pdrgpgroup)[i]->Id() << " ";
	}
	os << "]";

	if (NULL != m_pgexprDuplicate)
	{
		os << " Dup. of GrpExpr " << m_pgexprDuplicate->Id() << " in Grp "
		   << m_pgexprDuplicate->Pgroup()->Id();
	}

	if (GPOS_FTRACE(EopttracePrintXform) && ExfidOrigin() != CXform::ExfInvalid)
	{
		os << " Origin: ";
		if (m_fIntermediate)
		{
			os << "intermediate result of ";
		}
		os << "(xform: " << CXformFactory::Pxff()->Pxf(ExfidOrigin())->SzId();
		os << ", Grp: " << m_pgexprOrigin->Pgroup()->Id()
		   << ", GrpExpr: " << m_pgexprOrigin->Id() << ")";
	}
	os << std::endl;

	(void) OsPrintCostContexts(os, szPrefix);

	return os;
}

#ifdef GPOS_DEBUG
//---------------------------------------------------------------------------
//	@function:
//		CGroupExpression::DbgPrint
//
//	@doc:
//		Print driving function for use in interactive debugging;
//		always prints to stderr;
//
//---------------------------------------------------------------------------
void
CGroupExpression::DbgPrintWithProperties()
{
	CAutoTraceFlag atf(EopttracePrintGroupProperties, true);
	CAutoTrace at(m_mp);
	(void) this->OsPrint(at.Os());
}
#endif