//---------------------------------------------------------------------------
//	@filename:
//		CEngine.cpp
//
//	@doc:
//		Implementation of optimization engine
//---------------------------------------------------------------------------
#include "duckdb/optimizer/cascade/engine/CEngine.h"

#include "duckdb/optimizer/cascade/base.h"
#include "duckdb/optimizer/cascade/base/CCostContext.h"
#include "duckdb/optimizer/cascade/base/CDrvdPropCtxtPlan.h"
#include "duckdb/optimizer/cascade/base/COptCtxt.h"
#include "duckdb/optimizer/cascade/base/COptimizationContext.h"
#include "duckdb/optimizer/cascade/base/CQueryContext.h"
#include "duckdb/optimizer/cascade/base/CReqdPropPlan.h"
#include "duckdb/optimizer/cascade/base/CReqdPropRelational.h"
#include "duckdb/optimizer/cascade/base/CUtils.h"
#include "duckdb/optimizer/cascade/common/CAutoTimer.h"
#include "duckdb/optimizer/cascade/common/syslibwrapper.h"
#include "duckdb/optimizer/cascade/engine/CEnumeratorConfig.h"
#include "duckdb/optimizer/cascade/engine/CStatisticsConfig.h"
#include "duckdb/optimizer/cascade/io/COstreamString.h"
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
#include "duckdb/optimizer/cascade/string/CWStringDynamic.h"
#include "duckdb/optimizer/cascade/task/CAutoTaskProxy.h"
#include "duckdb/optimizer/cascade/xforms/CXformFactory.h"

#define GPOPT_SAMPLING_MAX_ITERS 30
#define GPOPT_JOBS_CAP           5000 // maximum number of initial optimization jobs
#define GPOPT_JOBS_PER_GROUP     20   // estimated number of needed optimization jobs per memo group

// memory consumption unit in bytes -- currently MB
#define GPOPT_MEM_UNIT      (1024 * 1024)
#define GPOPT_MEM_UNIT_NAME "MB"

namespace gpopt {
//---------------------------------------------------------------------------
//	@function:
//		CEngine::CEngine
//
//	@doc:
//		Ctor
//
//---------------------------------------------------------------------------
CEngine::CEngine() : m_pqc(nullptr), m_ulCurrSearchStage(0), m_pmemo(nullptr), m_xforms(nullptr) {
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
CEngine::~CEngine() {
}

//---------------------------------------------------------------------------
//	@function:
//		CEngine::InitLogicalExpression
//
//	@doc:
//		Initialize engine with a given expression
//
//---------------------------------------------------------------------------
void CEngine::InitLogicalExpression(duckdb::unique_ptr<Operator> pexpr) {
	CGroup *pgroup_root = PgroupInsert(nullptr, std::move(pexpr), CXform::ExfInvalid, NULL, false);
	m_pmemo->SetRoot(pgroup_root);
}

//---------------------------------------------------------------------------
//	@function:
//		CEngine::Init
//
//	@doc:
//		Initialize engine using a given query context
//
//---------------------------------------------------------------------------
void CEngine::Init(CQueryContext *pqc, duckdb::vector<CSearchStage *> search_stage_array) {
	m_search_stage_array = search_stage_array;
	if (search_stage_array.empty()) {
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
void CEngine::AddEnforcers(CGroupExpression *pgexpr, duckdb::vector<duckdb::unique_ptr<Operator>> pdrgpexpr_enforcers) {
	for (ULONG ul = 0; ul < pdrgpexpr_enforcers.size(); ul++) {
		// assemble an expression rooted by the enforcer operator
		CGroup *pgroup =
		    PgroupInsert(pgexpr->m_pgroup, std::move(pdrgpexpr_enforcers[ul]), CXform::ExfInvalid, nullptr, false);
	}
}

//---------------------------------------------------------------------------
//	@function:
//		CEngine::InsertExpressionChildren
//
//	@doc:
//		Insert children of the given expression to memo, and  the groups
//		they end up at to the given group array
//
//---------------------------------------------------------------------------
void CEngine::InsertExpressionChildren(Operator *pexpr, duckdb::vector<CGroup *> &pdrgpgroup_children,
                                       CXform::EXformId exfid_origin, CGroupExpression *pgexpr_origin) {
	ULONG arity = pexpr->Arity();
	for (ULONG i = 0; i < arity; i++) {
		// insert child expression recursively
		CGroup *pgroup_child = PgroupInsert(nullptr, pexpr->children[i]->Copy(), exfid_origin, pgexpr_origin, true);
		pdrgpgroup_children.emplace_back(pgroup_child);
	}
}

//---------------------------------------------------------------------------
//	@function:
//		CEngine::PgroupInsert
//
//	@doc:
//		Insert an expression tree into the memo, with explicit target
// group; 		the function returns a pointer to the group that
// contains the given 		group expression
//
//---------------------------------------------------------------------------
CGroup *CEngine::PgroupInsert(CGroup *pgroup_target, duckdb::unique_ptr<Operator> pexpr, CXform::EXformId exfid_origin,
                              CGroupExpression *pgexpr_origin, bool f_intermediate) {
	CGroup *pgroup_origin;
	// check if expression was produced by extracting
	// a binding from the memo
	if (nullptr != pexpr->m_pgexpr) {
		pgroup_origin = pexpr->m_pgexpr->m_pgroup;
		// if parent has group pointer, all children must have group pointers;
		// terminate recursive insertion here
		return pgroup_origin;
	}
	// insert expression's children to memo by recursive call
	duckdb::vector<CGroup *> pdrgpgroup_children;
	InsertExpressionChildren(pexpr.get(), pdrgpgroup_children, exfid_origin, pgexpr_origin);
	CGroupExpression *pgexpr =
	    new CGroupExpression(std::move(pexpr), pdrgpgroup_children, exfid_origin, pgexpr_origin, f_intermediate);
	// find the group that contains created group expression
	CGroup *pgroup_container = m_pmemo->PgroupInsert(pgroup_target, pgexpr);
	if (nullptr == pgexpr->m_pgroup) {
		// insertion failed, release created group expression
		delete pgexpr;
		pgexpr = nullptr;
	}
	return pgroup_container;
}

//---------------------------------------------------------------------------
//	@function:
//		CEngine::InsertXformResult
//
//	@doc:
//		Insert a set of transformation results to memo
//
//---------------------------------------------------------------------------
void CEngine::InsertXformResult(CGroup *pgroup_origin, CXformResult *pxfres, CXform::EXformId exfid_origin,
                                CGroupExpression *pgexpr_origin, ULONG ul_xform_time, ULONG ul_number_of_bindings) {
	duckdb::unique_ptr<Operator> pexpr = pxfres->PexprNext();
	while (nullptr != pexpr) {
		CGroup *pgroup_container = PgroupInsert(pgroup_origin, std::move(pexpr), exfid_origin, pgexpr_origin, false);
		if (pgroup_container != pgroup_origin && FPossibleDuplicateGroups(pgroup_container, pgroup_origin)) {
			m_pmemo->MarkDuplicates(pgroup_origin, pgroup_container);
		}
		pexpr = pxfres->PexprNext();
	}
}

//---------------------------------------------------------------------------
//	@function:
//		CEngine::FPossibleDuplicateGroups
//
//	@doc:
//		Check whether the given memo groups can be marked as duplicates.
// This is 		true only if they have the same logical properties
//
//---------------------------------------------------------------------------
bool CEngine::FPossibleDuplicateGroups(CGroup *pgroup_fst, CGroup *pgroup_snd) {
	CDrvdPropRelational *pdprel_fst = CDrvdPropRelational::GetRelationalProperties(pgroup_fst->m_pdp);
	CDrvdPropRelational *pdprel_snd = CDrvdPropRelational::GetRelationalProperties(pgroup_snd->m_pdp);
	// right now we only check the output columns, but we may possibly need to
	// check other properties as well
	duckdb::vector<ColumnBinding> v1 = pdprel_fst->GetOutputColumns();
	duckdb::vector<ColumnBinding> v2 = pdprel_snd->GetOutputColumns();
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
void CEngine::DeriveStats() {
	// derive stats on root group
	CEngine::DeriveStats(PgroupRoot(), nullptr);
}

//---------------------------------------------------------------------------
//	@function:
//		CEngine::DeriveStats
//
//	@doc:
//		Derive statistics on the group
//
//---------------------------------------------------------------------------
void CEngine::DeriveStats(CGroup *pgroup, CReqdPropRelational *prprel) {
	CGroupExpression *pgexpr_first = CEngine::PgexprFirst(pgroup);
	CReqdPropRelational *prprel_new = prprel;
	if (nullptr == prprel_new) {
		// create empty property container
		duckdb::vector<ColumnBinding> pcrs;
		prprel_new = new CReqdPropRelational(pcrs);
	}
	// (void) pgexprFirst->Pgroup()->PstatsRecursiveDerive(pmpLocal, pmpGlobal,
	// prprelNew, pdrgpstatCtxtNew); pdrgpstatCtxtNew->Release();
}

//---------------------------------------------------------------------------
//	@function:
//		CEngine::PgexprFirst
//
//	@doc:
//		Return the first group expression in a given group
//
//---------------------------------------------------------------------------
CGroupExpression *CEngine::PgexprFirst(CGroup *pgroup) {
	CGroupExpression *pgexpr_first;
	{
		// group proxy scope
		CGroupProxy gp(pgroup);
		pgexpr_first = *(gp.PgexprFirst());
	}
	return pgexpr_first;
}

//---------------------------------------------------------------------------
//	@function:
//		CEngine::EolDamp
//
//	@doc:
//		Damp optimization level
//
//---------------------------------------------------------------------------
EOptimizationLevel CEngine::EolDamp(EOptimizationLevel eol) {
	if (EolHigh == eol) {
		return EolLow;
	}
	return EolSentinel;
}

//---------------------------------------------------------------------------
//	@function:
//		CEngine::FOptimizeChild
//
//	@doc:
//		Check if parent group expression needs to optimize child group
// expression. 		This method is called right before a group optimization
// job is about to 		schedule a group expression optimization job.
//
//		Relation properties as well the optimizing parent group
// expression
// is 		available to make the decision. So, operators can reject being
// optimized 		under specific parent operators. For example, a
// GatherMerge under
// a Sort 		can be prevented here since it destroys the order from a
// GatherMerge.
//---------------------------------------------------------------------------
bool CEngine::FOptimizeChild(CGroupExpression *pgexpr_parent, CGroupExpression *pgexpr_child,
                             COptimizationContext *poc_child, EOptimizationLevel eol_current) {
	if (pgexpr_parent == pgexpr_child) {
		// a group expression cannot optimize itself
		return false;
	}
	if (pgexpr_child->Eol() != eol_current) {
		// child group expression does not match current optimization level
		return false;
	}
	return COptimizationContext::FOptimize(pgexpr_parent, pgexpr_child, poc_child, UlSearchStages());
}

//---------------------------------------------------------------------------
//	@function:
//		CEngine::FSafeToPrune
//
//	@doc:
//		Determine if a plan rooted by given group expression can be
// safely 		pruned during optimization
//
//---------------------------------------------------------------------------
bool CEngine::FSafeToPrune(CGroupExpression *pgexpr, CReqdPropPlan *prpp, CCostContext *pcc_child, ULONG child_index,
                           double *pcost_lower_bound) {
	*pcost_lower_bound = GPOPT_INVALID_COST;
	// check if container group has a plan for given properties
	CGroup *pgroup = pgexpr->m_pgroup;
	COptimizationContext *poc_group = pgroup->PocLookupBest(m_ulCurrSearchStage, prpp);
	if (nullptr != poc_group && nullptr != poc_group->m_pccBest) {
		// compute a cost lower bound for the equivalent plan rooted by given group
		// expression
		double cost_lower_bound = pgexpr->CostLowerBound(prpp, pcc_child, child_index);
		*pcost_lower_bound = cost_lower_bound;
		if (cost_lower_bound > poc_group->m_pccBest->m_cost) {
			// group expression cannot deliver a better plan for given properties and
			// can be safely pruned
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
MemoTreeMap *CEngine::Pmemotmap() {
	COptimizerConfig *optimizer_config = COptCtxt::PoctxtFromTLS()->m_optimizer_config;
	if (nullptr == m_pmemo->Pmemotmap()) {
		duckdb::vector<ColumnBinding> v;
		COptimizationContext *poc =
		    new COptimizationContext(PgroupRoot(), m_pqc->m_prpp, new CReqdPropRelational(v), 0);
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
duckdb::vector<COptimizationContext *> CEngine::PdrgpocChildren(CExpressionHandle &exprhdl) {
	duckdb::vector<COptimizationContext *> pdrgpoc;
	const ULONG arity = exprhdl.Arity();
	for (ULONG ul = 0; ul < arity; ul++) {
		CGroup *pgroup_child = (*exprhdl.Pgexpr())[ul];
		if (!pgroup_child->m_fScalar) {
			COptimizationContext *poc = pgroup_child->PocLookupBest(m_search_stage_array.size(), exprhdl.Prpp(ul));
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
void CEngine::ScheduleMainJob(CSchedulerContext *psc, COptimizationContext *poc) {
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
void CEngine::FinalizeExploration() {
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
void CEngine::FinalizeImplementation() {
}

//---------------------------------------------------------------------------
//	@function:
//		CEngine::FinalizeSearchStage
//
//	@doc:
//		Execute operations after search stage completes
//
//---------------------------------------------------------------------------
void CEngine::FinalizeSearchStage() {
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
void CEngine::Optimize() {
	COptimizerConfig *optimizer_config = COptCtxt::PoctxtFromTLS()->m_optimizer_config;
	const ULONG ulJobs = std::min((ULONG)GPOPT_JOBS_CAP, (ULONG)(m_pmemo->UlpGroups() * GPOPT_JOBS_PER_GROUP));
	CJobFactory jf(ulJobs);
	CScheduler sched(ulJobs);
	CSchedulerContext sc;
	sc.Init(&jf, &sched, this);
	const ULONG ulSearchStages = m_search_stage_array.size();
	for (ULONG ul = 0; !FSearchTerminated() && ul < ulSearchStages; ul++) {
		PssCurrent()->RestartTimer();
		// optimize root group
		duckdb::vector<ColumnBinding> v;
		COptimizationContext *poc =
		    new COptimizationContext(PgroupRoot(), m_pqc->m_prpp, new CReqdPropRelational(v), m_ulCurrSearchStage);
		// schedule main optimization job
		ScheduleMainJob(&sc, poc);
		// run optimization job
		CScheduler::Run(&sc);
		// extract best plan found at the end of current search stage
		auto pexpr_plan = m_pmemo->PexprExtractPlan(m_pmemo->PgroupRoot(), m_pqc->m_prpp, m_search_stage_array.size());
		PssCurrent()->SetBestExpr(pexpr_plan.release());
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
Operator *CEngine::PexprUnrank(ULLONG plan_id) {
	// The CTE map will be updated by the Producer instead of the Sequence
	// operator because we are doing a DFS traversal of the TreeMap.
	CDrvdPropCtxtPlan *pdpctxtplan = new CDrvdPropCtxtPlan(false);
	Operator *pexpr = Pmemotmap()->PrUnrank(pdpctxtplan, plan_id);
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
Operator *CEngine::PexprExtractPlan() {
	bool f_generate_alt = false;
	COptimizerConfig *optimizer_config = COptCtxt::PoctxtFromTLS()->m_optimizer_config;
	CEnumeratorConfig *pec = optimizer_config->m_enumerator_cfg;
	/* I comment here */
	// if (pec->FEnumerate())
	if (true) {
		/* I comment here */
		// if (0 < pec->GetPlanId())
		if (0 <= pec->GetPlanId()) {
			// a valid plan number was chosen
			f_generate_alt = true;
		}
	}
	Operator *pexpr = nullptr;
	if (f_generate_alt) {
		/* I comment here */
		// pexpr = PexprUnrank(pec->GetPlanId() - 1);
		pexpr = PexprUnrank(0);
	} else {
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
ULLONG CEngine::UllRandomPlanId(ULONG *seed) {
	ULLONG ull_count = Pmemotmap()->UllCount();
	ULLONG plan_id = 0;
	do {
		plan_id = clib::Rand(seed);
	} while (plan_id >= ull_count);
	return plan_id;
}

//---------------------------------------------------------------------------
//	@function:
//		CEngine::FValidPlanSample
//
//	@doc:
//		Extract a plan sample and handle exceptions according to
// enumerator 		configurations
//
//---------------------------------------------------------------------------
bool CEngine::FValidPlanSample(CEnumeratorConfig *pec, ULLONG plan_id, Operator **ppexpr) {
	bool f_valid_plan = true;
	if (pec->FSampleValidPlans()) {
		// if enumerator is configured to extract valid plans only,
		// we extract plan and catch invalid plan exception here
		*ppexpr = PexprUnrank(plan_id);
	} else {
		// otherwise, we extract plan and leave exception handling to the caller
		*ppexpr = PexprUnrank(plan_id);
	}
	return f_valid_plan;
}

//---------------------------------------------------------------------------
//	@function:
//		CEngine::FCheckEnfdProps
//
//	@doc:
//		Check enforceable properties and append enforcers to the current
// group if 		required.
//
//		This check is done in two steps:
//
//		First, it determines if any particular property needs to be
// enforced at 		all. For example, the EopttraceDisableSort traceflag can
// disable
// order 		enforcement. Also, if there are no partitioned tables
// referenced in the 		subtree, partition propagation enforcement can be
// skipped.
//
//		Second, EPET methods are called for each property to determine
// if
// an 		enforcer needs to be added. These methods in turn call into
// virtual 		methods in the different operators. For example,
// CPhysical::EpetOrder() 		is used to determine a Sort node needs
// to be added to the group. These 		methods are passed an expression
// handle (to access derived properties of 		the subtree) and the
// required properties as a object of a subclass of 		CEnfdProp.
//
//		Finally, based on return values of the EPET methods,
//		CEnfdProp::AppendEnforcers() is called for each of the enforced
//		properties.
//
//		Returns true if no enforcers were created because they were
// deemed 		unnecessary or optional i.e all enforced properties were
// satisfied for 		the group expression under the current optimization
// context. Returns 		false otherwise.
//
//		NB: This method is only concerned with a certain enforcer needs
// to
// be 		added into the group. Once added, there is no connection between
// the 		enforcer and the operator that created it. That is although some
// group expression X created the enforcer E, later, during costing, E can still
//		decide to pick some other group expression Y for its child,
// since 		theoretically, all group expressions in a group are
// equivalent.
//
//---------------------------------------------------------------------------
bool CEngine::FCheckEnfdProps(CGroupExpression *pgexpr, COptimizationContext *poc, ULONG ul_opt_req,
                              const duckdb::vector<COptimizationContext *> pdrgpoc) {
	// check if all children could be successfully optimized
	if (!FChildrenOptimized(pdrgpoc)) {
		return false;
	}
	// load a handle with derived plan properties
	CCostContext *pcc = new CCostContext(poc, ul_opt_req, pgexpr);
	pcc->SetChildContexts(pdrgpoc);
	CExpressionHandle exprhdl;
	exprhdl.Attach(pcc);
	exprhdl.DerivePlanPropsForCostContext();
	PhysicalOperator *pop_physical = (PhysicalOperator *)(pcc->m_pgexpr->m_pop.get());
	CReqdPropPlan *prpp = poc->m_prpp;
	// Determine if any property enforcement is disable or unnecessary
	bool f_order_reqd = !prpp->m_peo->m_pos->IsEmpty();
	// Determine if adding an enforcer to the group is required, optional,
	// unnecessary or prohibited over the group expression and given the current
	// optimization context (required properties)
	// get order enforcing type
	CEnfdOrder::EPropEnforcingType epet_order = prpp->m_peo->Epet(exprhdl, pop_physical, f_order_reqd);
	// Skip adding enforcers entirely if any property determines it to be
	// 'prohibited'. In this way, a property may veto out the creation of an
	// enforcer for the current group expression and optimization context.
	//
	// NB: Even though an enforcer E is not added because of some group
	// expression G because it was prohibited, some other group expression H may
	// decide to add it. And if E is added, it is possible for E to consider both
	// G and H as its child.
	if (FProhibited(epet_order)) {
		return false;
	}
	duckdb::vector<duckdb::unique_ptr<Operator>> pdrgpexpr_enforcers;
	// extract a leaf pattern from target group
	CBinding binding;
	Operator *pexpr = binding.PexprExtract(exprhdl.Pgexpr(), m_pexprEnforcerPattern.get(), nullptr);
	prpp->m_peo->AppendEnforcers(prpp, pdrgpexpr_enforcers, pexpr->Copy(), epet_order, exprhdl);
	if (!pdrgpexpr_enforcers.empty()) {
		AddEnforcers(exprhdl.Pgexpr(), std::move(pdrgpexpr_enforcers));
	}
	return FOptimize(epet_order);
}

//---------------------------------------------------------------------------
//	@function:
//		CEngine::FValidCTEAndPartitionProperties
//
//	@doc:
//		Check if the given expression has valid cte with respect to the
// given requirements. 		This function returns true iff 		ALL the
// following conditions are met:
//		1. The expression satisfies the CTE requirements
//
//---------------------------------------------------------------------------
bool CEngine::FValidCTEAndPartitionProperties(CExpressionHandle &exprhdl, CReqdPropPlan *prpp) {
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
bool CEngine::FChildrenOptimized(duckdb::vector<COptimizationContext *> pdrgpoc) {
	const ULONG length = pdrgpoc.size();
	for (ULONG ul = 0; ul < length; ul++) {
		if (nullptr == pdrgpoc[ul]->PgexprBest()) {
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
//		Check if optimization is possible under the given property
// enforcing 		types
//
//---------------------------------------------------------------------------
bool CEngine::FOptimize(CEnfdOrder::EPropEnforcingType epet_order) {
	return CEnfdOrder::FOptimize(epet_order);
}

//---------------------------------------------------------------------------
//	@function:
//		CEngine::FProhibited
//
//	@doc:
//		Check if any of the given property enforcing types prohibits
// enforcement
//
//---------------------------------------------------------------------------
bool CEngine::FProhibited(CEnfdOrder::EPropEnforcingType epet_order) {
	return (CEnfdOrder::EpetProhibited == epet_order);
}

//---------------------------------------------------------------------------
//	@function:
//		CEngine::FCheckReqdProps
//
//	@doc:
//		Determine if checking required properties is needed.
//		This method is called after a group expression optimization job
// has 		started executing and can be used to cancel the job early.
//
//		This is useful to prevent deadlocks when an enforcer optimizes
// same 		group with the same optimization context. Also, in case
// the
// subtree 		doesn't provide the required columns we can save
// optimization time by skipping this optimization request.
//
//		NB: Only relational properties are available at this stage to
// make this 		decision.
//---------------------------------------------------------------------------
bool CEngine::FCheckReqdProps(CExpressionHandle &exprhdl, CReqdPropPlan *prpp, ULONG ul_opt_req) {
	// check if operator provides required columns
	if (!prpp->FProvidesReqdCols(exprhdl, ul_opt_req)) {
		return false;
	}
	PhysicalOperator *pop_physical = (PhysicalOperator *)exprhdl.Pop();
	// check if sort operator is passed an empty order spec;
	// this check is required to avoid self-deadlocks, i.e.
	// sort optimizing same group with the same optimization context;
	bool f_order_reqd = !prpp->m_peo->m_pos->IsEmpty();
	if (!f_order_reqd && PhysicalOperatorType::ORDER_BY == pop_physical->physical_type) {
		return false;
	}
	return true;
}

duckdb::vector<ULONG_PTR *> CEngine::GetNumberOfBindings() {
	return m_pdrgpulpXformBindings;
}
} // namespace gpopt