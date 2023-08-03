//---------------------------------------------------------------------------
//	@filename:
//		CEngine.cpp
//
//	@doc:
//		Implementation of optimization engine
//---------------------------------------------------------------------------
#include "duckdb/optimizer/cascade/engine/CEngine.h"
#include "duckdb/optimizer/cascade/base.h"
#include "duckdb/optimizer/cascade/common/CAutoTimer.h"
#include "duckdb/optimizer/cascade/common/syslibwrapper.h"
#include "duckdb/optimizer/cascade/io/COstreamString.h"
#include "duckdb/optimizer/cascade/string/CWStringDynamic.h"
#include "duckdb/optimizer/cascade/task/CAutoTaskProxy.h"
#include "duckdb/optimizer/cascade/base/CCostContext.h"
#include "duckdb/optimizer/cascade/base/CDrvdPropCtxtPlan.h"
#include "duckdb/optimizer/cascade/base/COptCtxt.h"
#include "duckdb/optimizer/cascade/base/COptimizationContext.h"
#include "duckdb/optimizer/cascade/base/CQueryContext.h"
#include "duckdb/optimizer/cascade/base/CReqdPropPlan.h"
#include "duckdb/optimizer/cascade/base/CReqdPropRelational.h"
#include "duckdb/optimizer/cascade/engine/CEnumeratorConfig.h"
#include "duckdb/optimizer/cascade/engine/CStatisticsConfig.h"
#include "duckdb/optimizer/cascade/operators/CPattern.h"
#include "duckdb/optimizer/cascade/operators/CPatternLeaf.h"
#include "duckdb/optimizer/cascade/optimizer/COptimizerConfig.h"
#include "duckdb/optimizer/cascade/search/CGroup.h"
#include "duckdb/optimizer/cascade/search/CGroupExpression.h"
#include "duckdb/optimizer/cascade/search/CGroupProxy.h"
#include "duckdb/optimizer/cascade/search/CJob.h"
#include "duckdb/optimizer/cascade/search/CJobFactory.h"
#include "duckdb/optimizer/cascade/search/CMemo.h"
#include "duckdb/optimizer/cascade/search/CScheduler.h"
#include "duckdb/optimizer/cascade/search/CSchedulerContext.h"
#include "duckdb/optimizer/cascade/xforms/CXformFactory.h"
#include "duckdb/optimizer/cascade/optimizer/COptimizerConfig.h"
#include "duckdb/optimizer/cascade/base/CUtils.h"

#define GPOPT_SAMPLING_MAX_ITERS 30
#define GPOPT_JOBS_CAP 5000	 // maximum number of initial optimization jobs
#define GPOPT_JOBS_PER_GROUP 20	// estimated number of needed optimization jobs per memo group

// memory consumption unit in bytes -- currently MB
#define GPOPT_MEM_UNIT (1024 * 1024)
#define GPOPT_MEM_UNIT_NAME "MB"

namespace gpopt
{
//---------------------------------------------------------------------------
//	@function:
//		CEngine::CEngine
//
//	@doc:
//		Ctor
//
//---------------------------------------------------------------------------
CEngine::CEngine()
	:  m_pqc(nullptr), m_ulCurrSearchStage(0), m_pmemo(nullptr), m_xforms(nullptr)
{
	m_pmemo = new CMemo();
	m_pexprEnforcerPattern = make_uniq<CPatternLeaf>();
	m_xforms = new CXformSet();
}

//---------------------------------------------------------------------------
//	@function:
//		CEngine::~CEngine
//
//	@doc:
//		Dtor
//
//---------------------------------------------------------------------------
CEngine::~CEngine()
{
}

//---------------------------------------------------------------------------
//	@function:
//		CEngine::InitLogicalExpression
//
//	@doc:
//		Initialize engine with a given expression
//
//---------------------------------------------------------------------------
void CEngine::InitLogicalExpression(duckdb::unique_ptr<Operator> pexpr)
{
	CGroup* pgroupRoot = PgroupInsert(nullptr, std::move(pexpr), CXform::ExfInvalid,NULL, false);
	m_pmemo->SetRoot(pgroupRoot);
}


//---------------------------------------------------------------------------
//	@function:
//		CEngine::Init
//
//	@doc:
//		Initialize engine using a given query context
//
//---------------------------------------------------------------------------
void CEngine::Init(CQueryContext* pqc, duckdb::vector<CSearchStage*> search_stage_array)
{
	m_search_stage_array = search_stage_array;
	if (0 == search_stage_array.size())
	{
		m_search_stage_array = CSearchStage::PdrgpssDefault();
	}
	m_pqc = pqc;
	InitLogicalExpression(std::move(m_pqc->m_pexpr));
}


//---------------------------------------------------------------------------
//	@function:
//		CEngine::AddEnforcers
//
//	@doc:
//		Add enforcers to a memo group
//
//---------------------------------------------------------------------------
void CEngine::AddEnforcers(CGroupExpression* pgexpr, duckdb::vector<duckdb::unique_ptr<Operator>> pdrgpexprEnforcers)
{
	for (ULONG ul = 0; ul < pdrgpexprEnforcers.size(); ul++)
	{
		// assemble an expression rooted by the enforcer operator
		CGroup* pgroup = PgroupInsert(pgexpr->m_pgroup, std::move(pdrgpexprEnforcers[ul]), CXform::ExfInvalid, NULL, false);
	}
}

//---------------------------------------------------------------------------
//	@function:
//		CEngine::InsertExpressionChildren
//
//	@doc:
//		Insert children of the given expression to memo, and copy the groups
//		they end up at to the given group array
//
//---------------------------------------------------------------------------
void CEngine::InsertExpressionChildren(Operator* pexpr, duckdb::vector<CGroup*> &pdrgpgroupChildren, CXform::EXformId exfidOrigin, CGroupExpression* pgexprOrigin)
{
	ULONG arity = pexpr->Arity();
	for (ULONG i = 0; i < arity; i++)
	{
		// insert child expression recursively
		CGroup* pgroupChild = PgroupInsert(nullptr, pexpr->children[i]->Copy(), exfidOrigin, pgexprOrigin, true);
		pdrgpgroupChildren.emplace_back(pgroupChild);
	}
}


//---------------------------------------------------------------------------
//	@function:
//		CEngine::PgroupInsert
//
//	@doc:
//		Insert an expression tree into the memo, with explicit target group;
//		the function returns a pointer to the group that contains the given
//		group expression
//
//---------------------------------------------------------------------------
CGroup* CEngine::PgroupInsert(CGroup* pgroupTarget, duckdb::unique_ptr<Operator> pexpr, CXform::EXformId exfidOrigin, CGroupExpression* pgexprOrigin, bool fIntermediate)
{
	CGroup* pgroupOrigin;
	// check if expression was produced by extracting
	// a binding from the memo
	if (nullptr != pexpr->m_pgexpr)
	{
		pgroupOrigin = pexpr->m_pgexpr->m_pgroup;
		// if parent has group pointer, all children must have group pointers;
		// terminate recursive insertion here
		return pgroupOrigin;
	}
	// insert expression's children to memo by recursive call
	duckdb::vector<CGroup*> pdrgpgroupChildren;
	InsertExpressionChildren(pexpr.get(), pdrgpgroupChildren, exfidOrigin, pgexprOrigin);
	CGroupExpression* pgexpr = new CGroupExpression(std::move(pexpr), pdrgpgroupChildren, exfidOrigin, pgexprOrigin, fIntermediate);
	// find the group that contains created group expression
	CGroup* pgroupContainer = m_pmemo->PgroupInsert(pgroupTarget, pgexpr);
	if (nullptr == pgexpr->m_pgroup)
	{
		// insertion failed, release created group expression
		delete pgexpr;
		pgexpr = nullptr;
	}
	return pgroupContainer;
}

//---------------------------------------------------------------------------
//	@function:
//		CEngine::InsertXformResult
//
//	@doc:
//		Insert a set of transformation results to memo
//
//---------------------------------------------------------------------------
void CEngine::InsertXformResult(CGroup* pgroupOrigin, CXformResult* pxfres, CXform::EXformId exfidOrigin, CGroupExpression* pgexprOrigin, ULONG ulXformTime, ULONG ulNumberOfBindings)
{
	duckdb::unique_ptr<Operator> pexpr = pxfres->PexprNext();
	while (nullptr != pexpr)
	{
		CGroup* pgroupContainer = PgroupInsert(pgroupOrigin, std::move(pexpr), exfidOrigin, pgexprOrigin, false);
		if (pgroupContainer != pgroupOrigin && FPossibleDuplicateGroups(pgroupContainer, pgroupOrigin))
		{
			m_pmemo->MarkDuplicates(pgroupOrigin, pgroupContainer);
		}
		pexpr = pxfres->PexprNext();
	}
}

//---------------------------------------------------------------------------
//	@function:
//		CEngine::FPossibleDuplicateGroups
//
//	@doc:
//		Check whether the given memo groups can be marked as duplicates. This is
//		true only if they have the same logical properties
//
//---------------------------------------------------------------------------
bool CEngine::FPossibleDuplicateGroups(CGroup* pgroupFst, CGroup* pgroupSnd)
{
	CDrvdPropRelational* pdprelFst = CDrvdPropRelational::GetRelationalProperties(pgroupFst->m_pdp);
	CDrvdPropRelational* pdprelSnd = CDrvdPropRelational::GetRelationalProperties(pgroupSnd->m_pdp);
	// right now we only check the output columns, but we may possibly need to
	// check other properties as well
	duckdb::vector<ColumnBinding> v1 = pdprelFst->GetOutputColumns();
	duckdb::vector<ColumnBinding> v2 = pdprelSnd->GetOutputColumns();
	return CUtils::Equals(v1, v2);
}

//---------------------------------------------------------------------------
//	@function:
//		CEngine::DeriveStats
//
//	@doc:
//		Derive statistics on the root group
//
//---------------------------------------------------------------------------
void CEngine::DeriveStats()
{
	// derive stats on root group
	CEngine::DeriveStats(PgroupRoot(), NULL);
}

//---------------------------------------------------------------------------
//	@function:
//		CEngine::DeriveStats
//
//	@doc:
//		Derive statistics on the group
//
//---------------------------------------------------------------------------
void CEngine::DeriveStats(CGroup* pgroup, CReqdPropRelational* prprel)
{
	CGroupExpression* pgexprFirst = CEngine::PgexprFirst(pgroup);
	CReqdPropRelational* prprelNew = prprel;
	if (nullptr == prprelNew)
	{
		// create empty property container
		duckdb::vector<ColumnBinding> pcrs;
		prprelNew = new CReqdPropRelational(pcrs);
	}
	// (void) pgexprFirst->Pgroup()->PstatsRecursiveDerive(pmpLocal, pmpGlobal, prprelNew, pdrgpstatCtxtNew);
	// pdrgpstatCtxtNew->Release();
}

//---------------------------------------------------------------------------
//	@function:
//		CEngine::PgexprFirst
//
//	@doc:
//		Return the first group expression in a given group
//
//---------------------------------------------------------------------------
CGroupExpression* CEngine::PgexprFirst(CGroup* pgroup)
{
	CGroupExpression* pgexprFirst;
	{
		// group proxy scope
		CGroupProxy gp(pgroup);
		pgexprFirst = *(gp.PgexprFirst());
	}
	return pgexprFirst;
}

//---------------------------------------------------------------------------
//	@function:
//		CEngine::EolDamp
//
//	@doc:
//		Damp optimization level
//
//---------------------------------------------------------------------------
EOptimizationLevel CEngine::EolDamp(EOptimizationLevel eol)
{
	if (EolHigh == eol)
	{
		return EolLow;
	}
	return EolSentinel;
}

//---------------------------------------------------------------------------
//	@function:
//		CEngine::FOptimizeChild
//
//	@doc:
//		Check if parent group expression needs to optimize child group expression.
//		This method is called right before a group optimization job is about to
//		schedule a group expression optimization job.
//
//		Relation properties as well the optimizing parent group expression is
//		available to make the decision. So, operators can reject being optimized
//		under specific parent operators. For example, a GatherMerge under a Sort
//		can be prevented here since it destroys the order from a GatherMerge.
//---------------------------------------------------------------------------
bool CEngine::FOptimizeChild(CGroupExpression* pgexprParent, CGroupExpression* pgexprChild, COptimizationContext* pocChild, EOptimizationLevel eolCurrent)
{
	if (pgexprParent == pgexprChild)
	{
		// a group expression cannot optimize itself
		return false;
	}
	if (pgexprChild->Eol() != eolCurrent)
	{
		// child group expression does not match current optimization level
		return false;
	}
	return COptimizationContext::FOptimize(pgexprParent, pgexprChild, pocChild, UlSearchStages());
}

//---------------------------------------------------------------------------
//	@function:
//		CEngine::FSafeToPrune
//
//	@doc:
//		Determine if a plan rooted by given group expression can be safely
//		pruned during optimization
//
//---------------------------------------------------------------------------
bool CEngine::FSafeToPrune(CGroupExpression* pgexpr, CReqdPropPlan* prpp, CCostContext* pccChild, ULONG child_index, double* pcostLowerBound)
{
	*pcostLowerBound = GPOPT_INVALID_COST;
	// check if container group has a plan for given properties
	CGroup* pgroup = pgexpr->m_pgroup;
	COptimizationContext* pocGroup = pgroup->PocLookupBest(m_ulCurrSearchStage, prpp);
	if (nullptr != pocGroup && nullptr != pocGroup->m_pccBest)
	{
		// compute a cost lower bound for the equivalent plan rooted by given group expression
		double costLowerBound = pgexpr->CostLowerBound(prpp, pccChild, child_index);
		*pcostLowerBound = costLowerBound;
		if (costLowerBound > pocGroup->m_pccBest->m_cost)
		{
			// group expression cannot deliver a better plan for given properties and can be safely pruned
			return true;
		}
	}
	return false;
}

//---------------------------------------------------------------------------
//	@function:
//		CEngine::Pmemotmap
//
//	@doc:
//		Build tree map on memo
//
//---------------------------------------------------------------------------
MemoTreeMap* CEngine::Pmemotmap()
{
	COptimizerConfig* optimizer_config = COptCtxt::PoctxtFromTLS()->m_optimizer_config;
	if (NULL == m_pmemo->Pmemotmap())
	{
		duckdb::vector<ColumnBinding> v;
		COptimizationContext* poc = new COptimizationContext(PgroupRoot(), m_pqc->m_prpp, new CReqdPropRelational(v), 0);
		m_pmemo->BuildTreeMap(poc);
	}
	return m_pmemo->Pmemotmap();
}

//---------------------------------------------------------------------------
//	@function:
//		CEngine::PdrgpocChildren
//
//	@doc:
//		Return array of child optimization contexts corresponding
//		to handle requirements
//
//---------------------------------------------------------------------------
duckdb::vector<COptimizationContext*> CEngine::PdrgpocChildren(CExpressionHandle &exprhdl)
{
	duckdb::vector<COptimizationContext*> pdrgpoc;
	const ULONG arity = exprhdl.Arity();
	for (ULONG ul = 0; ul < arity; ul++)
	{
		CGroup* pgroupChild = (*exprhdl.Pgexpr())[ul];
		if (!pgroupChild->m_fScalar)
		{
			COptimizationContext* poc = pgroupChild->PocLookupBest(m_search_stage_array.size(), exprhdl.Prpp(ul));
			pdrgpoc.emplace_back(poc);
		}
	}
	return pdrgpoc;
}

//---------------------------------------------------------------------------
//	@function:
//		CEngine::ScheduleMainJob
//
//	@doc:
//		Create and schedule the main optimization job
//
//---------------------------------------------------------------------------
void CEngine::ScheduleMainJob(CSchedulerContext* psc, COptimizationContext* poc)
{
	CJobGroupOptimization::ScheduleJob(psc, PgroupRoot(), NULL, poc, NULL);
}

//---------------------------------------------------------------------------
//	@function:
//		CEngine::FinalizeExploration
//
//	@doc:
//		Execute operations after exploration completes
//
//---------------------------------------------------------------------------
void CEngine::FinalizeExploration()
{
	GroupMerge();
	//  if (m_pqc->FDeriveStats())
	//  {
	//		// derive statistics
	//  	m_pmemo->ResetStats();
	//  	DeriveStats();
	//  }
	// if (!GPOS_FTRACE(EopttraceDonotDeriveStatsForAllGroups))
	// {
	//		// derive stats for every group without stats
	//  	m_pmemo->DeriveStatsIfAbsent();
	// }
}

//---------------------------------------------------------------------------
//	@function:
//		CEngine::FinalizeImplementation
//
//	@doc:
//		Execute operations after implementation completes
//
//---------------------------------------------------------------------------
void CEngine::FinalizeImplementation()
{
}

//---------------------------------------------------------------------------
//	@function:
//		CEngine::FinalizeSearchStage
//
//	@doc:
//		Execute operations after search stage completes
//
//---------------------------------------------------------------------------
void CEngine::FinalizeSearchStage()
{
	m_xforms = nullptr;
	m_xforms = new CXformSet();
	m_ulCurrSearchStage++;
	m_pmemo->ResetGroupStates();
}

//---------------------------------------------------------------------------
//	@function:
//		CEngine::Optimize
//
//	@doc:
//		Main driver of optimization engine
//
//---------------------------------------------------------------------------
void CEngine::Optimize()
{
	COptimizerConfig* optimizer_config = COptCtxt::PoctxtFromTLS()->m_optimizer_config;
	const ULONG ulJobs = std::min((ULONG) GPOPT_JOBS_CAP, (ULONG)(m_pmemo->UlpGroups() * GPOPT_JOBS_PER_GROUP));
	CJobFactory jf(ulJobs);
	CScheduler sched(ulJobs);
	CSchedulerContext sc;
	sc.Init(&jf, &sched, this);
	const ULONG ulSearchStages = m_search_stage_array.size();
	for (ULONG ul = 0; !FSearchTerminated() && ul < ulSearchStages; ul++)
	{
		PssCurrent()->RestartTimer();
		// optimize root group
		duckdb::vector<ColumnBinding> v;
		COptimizationContext* poc = new COptimizationContext(PgroupRoot(), m_pqc->m_prpp, new CReqdPropRelational(v), m_ulCurrSearchStage);
		// schedule main optimization job
		ScheduleMainJob(&sc, poc);
		// run optimization job
		CScheduler::Run(&sc);
		// extract best plan found at the end of current search stage
		auto pexprPlan = m_pmemo->PexprExtractPlan(m_pmemo->PgroupRoot(), m_pqc->m_prpp, m_search_stage_array.size());
		PssCurrent()->SetBestExpr(pexprPlan.release());
		FinalizeSearchStage();
	}
}

//---------------------------------------------------------------------------
//	@function:
//		CEngine::CEngine
//
//	@doc:
//		Ctor
//
//---------------------------------------------------------------------------
Operator* CEngine::PexprUnrank(ULLONG plan_id)
{
	// The CTE map will be updated by the Producer instead of the Sequence operator
	// because we are doing a DFS traversal of the TreeMap.
	CDrvdPropCtxtPlan* pdpctxtplan = new CDrvdPropCtxtPlan(false);
	Operator* pexpr = Pmemotmap()->PrUnrank(pdpctxtplan, plan_id);
	delete pdpctxtplan;
	return pexpr;
}

//---------------------------------------------------------------------------
//	@function:
//		CEngine::PexprExtractPlan
//
//	@doc:
//		Extract a physical plan from the memo
//
//---------------------------------------------------------------------------
Operator* CEngine::PexprExtractPlan()
{
	bool fGenerateAlt = false;
	COptimizerConfig* optimizer_config = COptCtxt::PoctxtFromTLS()->m_optimizer_config;
	CEnumeratorConfig* pec = optimizer_config->m_enumerator_cfg;
	/* I comment here */
	//if (pec->FEnumerate())
	if(true)
	{
		/* I comment here */
		//if (0 < pec->GetPlanId())
		if (0 <= pec->GetPlanId())
		{
			// a valid plan number was chosen
			fGenerateAlt = true;
		}
	}
	Operator* pexpr = NULL;
	if (fGenerateAlt)
	{
		/* I comment here */
		// pexpr = PexprUnrank(pec->GetPlanId() - 1);
		pexpr = PexprUnrank(0);
	}
	else
	{
		pexpr = m_pmemo->PexprExtractPlan(m_pmemo->PgroupRoot(), m_pqc->m_prpp, m_search_stage_array.size()).get();
	}
	return pexpr;
}

//---------------------------------------------------------------------------
//	@function:
//		CEngine::UllRandomPlanId
//
//	@doc:
//		Generate random plan id
//
//---------------------------------------------------------------------------
ULLONG CEngine::UllRandomPlanId(ULONG* seed)
{
	ULLONG ullCount = Pmemotmap()->UllCount();
	ULLONG plan_id = 0;
	do
	{
		plan_id = clib::Rand(seed);
	} while (plan_id >= ullCount);
	return plan_id;
}

//---------------------------------------------------------------------------
//	@function:
//		CEngine::FValidPlanSample
//
//	@doc:
//		Extract a plan sample and handle exceptions according to enumerator
//		configurations
//
//---------------------------------------------------------------------------
bool CEngine::FValidPlanSample(CEnumeratorConfig* pec, ULLONG plan_id, Operator** ppexpr)
{
	bool fValidPlan = true;
	if (pec->FSampleValidPlans())
	{
		// if enumerator is configured to extract valid plans only,
		// we extract plan and catch invalid plan exception here
		*ppexpr = PexprUnrank(plan_id);
	}
	else
	{
		// otherwise, we extract plan and leave exception handling to the caller
		*ppexpr = PexprUnrank(plan_id);
	}
	return fValidPlan;
}

//---------------------------------------------------------------------------
//	@function:
//		CEngine::FCheckEnfdProps
//
//	@doc:
//		Check enforceable properties and append enforcers to the current group if
//		required.
//
//		This check is done in two steps:
//
//		First, it determines if any particular property needs to be enforced at
//		all. For example, the EopttraceDisableSort traceflag can disable order
//		enforcement. Also, if there are no partitioned tables referenced in the
//		subtree, partition propagation enforcement can be skipped.
//
//		Second, EPET methods are called for each property to determine if an
//		enforcer needs to be added. These methods in turn call into virtual
//		methods in the different operators. For example, CPhysical::EpetOrder()
//		is used to determine a Sort node needs to be added to the group. These
//		methods are passed an expression handle (to access derived properties of
//		the subtree) and the required properties as a object of a subclass of
//		CEnfdProp.
//
//		Finally, based on return values of the EPET methods,
//		CEnfdProp::AppendEnforcers() is called for each of the enforced
//		properties.
//
//		Returns true if no enforcers were created because they were deemed
//		unnecessary or optional i.e all enforced properties were satisfied for
//		the group expression under the current optimization context.  Returns
//		false otherwise.
//
//		NB: This method is only concerned with a certain enforcer needs to be
//		added into the group. Once added, there is no connection between the
//		enforcer and the operator that created it. That is although some group
//		expression X created the enforcer E, later, during costing, E can still
//		decide to pick some other group expression Y for its child, since
//		theoretically, all group expressions in a group are equivalent.
//
//---------------------------------------------------------------------------
bool CEngine::FCheckEnfdProps(CGroupExpression* pgexpr, COptimizationContext* poc, ULONG ulOptReq, duckdb::vector<COptimizationContext*> pdrgpoc)
{
	// check if all children could be successfully optimized
	if (!FChildrenOptimized(pdrgpoc))
	{
		return false;
	}
	// load a handle with derived plan properties
	CCostContext* pcc = new CCostContext(poc, ulOptReq, pgexpr);
	pcc->SetChildContexts(pdrgpoc);
	CExpressionHandle exprhdl;
	exprhdl.Attach(pcc);
	exprhdl.DerivePlanPropsForCostContext();
	PhysicalOperator* popPhysical = (PhysicalOperator*)(pcc->m_pgexpr->m_pop.get());
	CReqdPropPlan* prpp = poc->m_prpp;
	// Determine if any property enforcement is disable or unnecessary
	bool fOrderReqd = !prpp->m_peo->m_pos->IsEmpty();
	// Determine if adding an enforcer to the group is required, optional,
	// unnecessary or prohibited over the group expression and given the current
	// optimization context (required properties)
	// get order enforcing type
	CEnfdOrder::EPropEnforcingType epetOrder = prpp->m_peo->Epet(exprhdl, popPhysical, fOrderReqd);
	// Skip adding enforcers entirely if any property determines it to be
	// 'prohibited'. In this way, a property may veto out the creation of an
	// enforcer for the current group expression and optimization context.
	//
	// NB: Even though an enforcer E is not added because of some group
	// expression G because it was prohibited, some other group expression H may
	// decide to add it. And if E is added, it is possible for E to consider both
	// G and H as its child.
	if (FProhibited(epetOrder))
	{
		return false;
	}
	duckdb::vector<duckdb::unique_ptr<Operator>> pdrgpexprEnforcers;
	// extract a leaf pattern from target group
	CBinding binding;
	Operator* pexpr = binding.PexprExtract(exprhdl.Pgexpr(), m_pexprEnforcerPattern.get(), NULL);
	prpp->m_peo->AppendEnforcers(prpp, pdrgpexprEnforcers, pexpr->Copy(), epetOrder, exprhdl);
	if (0 < pdrgpexprEnforcers.size())
	{
		AddEnforcers(exprhdl.Pgexpr(), std::move(pdrgpexprEnforcers));
	}
	return FOptimize(epetOrder);
}

//---------------------------------------------------------------------------
//	@function:
//		CEngine::FValidCTEAndPartitionProperties
//
//	@doc:
//		Check if the given expression has valid cte with respect to the given requirements.
//		This function returns true iff
//		ALL the following conditions are met:
//		1. The expression satisfies the CTE requirements
//
//---------------------------------------------------------------------------
bool CEngine::FValidCTEAndPartitionProperties(CExpressionHandle &exprhdl, CReqdPropPlan *prpp)
{
	// PhysicalOperator* popPhysical = (PhysicalOperator*)exprhdl.Pop();
	return true;
	// return popPhysical->FProvidesReqdCTEs(prpp->Pcter());
}

//---------------------------------------------------------------------------
//	@function:
//		CEngine::FChildrenOptimized
//
//	@doc:
//		Check if all children were successfully optimized
//
//---------------------------------------------------------------------------
bool CEngine::FChildrenOptimized(duckdb::vector<COptimizationContext*> pdrgpoc)
{
	const ULONG length = pdrgpoc.size();
	for (ULONG ul = 0; ul < length; ul++)
	{
		if (NULL == pdrgpoc[ul]->PgexprBest())
		{
			return false;
		}
	}
	return true;
}

//---------------------------------------------------------------------------
//	@function:
//		CEngine::FOptimize
//
//	@doc:
//		Check if optimization is possible under the given property enforcing
//		types
//
//---------------------------------------------------------------------------
bool CEngine::FOptimize(CEnfdOrder::EPropEnforcingType epetOrder)
{
	return CEnfdOrder::FOptimize(epetOrder);
}

//---------------------------------------------------------------------------
//	@function:
//		CEngine::FProhibited
//
//	@doc:
//		Check if any of the given property enforcing types prohibits enforcement
//
//---------------------------------------------------------------------------
bool CEngine::FProhibited(CEnfdOrder::EPropEnforcingType epetOrder)
{
	return (CEnfdOrder::EpetProhibited == epetOrder);
}

//---------------------------------------------------------------------------
//	@function:
//		CEngine::FCheckReqdProps
//
//	@doc:
//		Determine if checking required properties is needed.
//		This method is called after a group expression optimization job has
//		started executing and can be used to cancel the job early.
//
//		This is useful to prevent deadlocks when an enforcer optimizes same
//		group with the same optimization context. Also, in case the subtree
//		doesn't provide the required columns we can save optimization time by
//		skipping this optimization request.
//
//		NB: Only relational properties are available at this stage to make this
//		decision.
//---------------------------------------------------------------------------
bool CEngine::FCheckReqdProps(CExpressionHandle &exprhdl, CReqdPropPlan *prpp, ULONG ulOptReq)
{
	// check if operator provides required columns
	if (!prpp->FProvidesReqdCols(exprhdl, ulOptReq))
	{
		return false;
	}
	PhysicalOperator* popPhysical = (PhysicalOperator*)exprhdl.Pop();
	// check if sort operator is passed an empty order spec;
	// this check is required to avoid self-deadlocks, i.e.
	// sort optimizing same group with the same optimization context;
	bool fOrderReqd = !prpp->m_peo->m_pos->IsEmpty();
	if (!fOrderReqd && PhysicalOperatorType::ORDER_BY == popPhysical->physical_type)
	{
		return false;
	}
	return true;
}

duckdb::vector<ULONG_PTR*> CEngine::GetNumberOfBindings()
{
	return m_pdrgpulpXformBindings;
}
}