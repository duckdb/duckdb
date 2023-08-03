//---------------------------------------------------------------------------
//	@filename:
//		CGroup.cpp
//
//	@doc:
//		Implementation of Memo groups; database agnostic
//---------------------------------------------------------------------------
#include "duckdb/optimizer/cascade/search/CGroup.h"
#include "duckdb/optimizer/cascade/base.h"
#include "duckdb/optimizer/cascade/task/CWorker.h"
#include "duckdb/optimizer/cascade/base/CDrvdProp.h"
#include "duckdb/optimizer/cascade/base/CDrvdPropCtxtPlan.h"
#include "duckdb/optimizer/cascade/base/CDrvdPropCtxtRelational.h"
#include "duckdb/optimizer/cascade/base/CReqdPropRelational.h"
#include "duckdb/optimizer/cascade/base/COptimizationContext.h"
#include "duckdb/optimizer/cascade/operators/CExpressionHandle.h"
#include "duckdb/optimizer/cascade/operators/Operator.h"
#include "duckdb/optimizer/cascade/search/CGroupProxy.h"
#include "duckdb/optimizer/cascade/search/CJobGroup.h"
#include "duckdb/optimizer/cascade/base/CUtils.h"

using namespace gpopt;

#define GPOPT_OPTCTXT_HT_BUCKETS 100

//---------------------------------------------------------------------------
//	@function:
//		SContextLink::SContextLink
//
//	@doc:
//		Ctor
//
//---------------------------------------------------------------------------
SContextLink::SContextLink(CCostContext* pccParent, ULONG child_index, COptimizationContext* poc)
	: m_pccParent(pccParent), m_ulChildIndex(child_index), m_poc(poc)
{
}

//---------------------------------------------------------------------------
//	@function:
//		ContextLink::~SContextLink
//
//	@doc:
//		Dtor
//
//---------------------------------------------------------------------------
SContextLink::~SContextLink()
{
}

//---------------------------------------------------------------------------
//	@function:
//		SContextLink::Equals
//
//	@doc:
//		Equality function
//
//---------------------------------------------------------------------------
bool SContextLink::operator==(const SContextLink &pclink2) const
{
	bool fEqualChildIndexes = (this->m_ulChildIndex == pclink2.m_ulChildIndex);
	bool fEqual = false;
	if (fEqualChildIndexes)
	{
		if (nullptr == this->m_pccParent || nullptr == pclink2.m_pccParent)
		{
			fEqual = (nullptr == this->m_pccParent && nullptr == pclink2.m_pccParent);
		}
		else
		{
			fEqual = (*this->m_pccParent == *pclink2.m_pccParent);
		}
	}
	if (fEqual)
	{
		if (nullptr == this->m_poc || nullptr == pclink2.m_poc)
		{
			return (nullptr == this->m_poc && nullptr == pclink2.m_poc);
		}
		return COptimizationContext::Equals(*this->m_poc, *pclink2.m_poc);
	}
	return fEqual;
}

//---------------------------------------------------------------------------
//	@function:
//		CGroup::CGroup
//
//	@doc:
//		Ctor
//
//---------------------------------------------------------------------------
CGroup::CGroup(bool fScalar)
	: m_id(GPOPT_INVALID_GROUP_ID), m_fScalar(fScalar), m_pdp(nullptr), m_pexprScalarRep(nullptr), m_pexprScalarRepIsExact(false), m_pccDummy(nullptr), m_pgroupDuplicate(nullptr), m_ulGExprs(0), m_ulpOptCtxts(0), m_estate(estUnexplored), m_eolMax(EolLow), m_fHasNewLogicalOperators(false)
{
}

//---------------------------------------------------------------------------
//	@function:
//		CGroup::~CGroup
//
//	@doc:
//		Dtor
//
//---------------------------------------------------------------------------
CGroup::~CGroup()
{
	// cleaning-up group expressions
	list<CGroupExpression*>::iterator pgexpr_iter = m_listGExprs.begin();
	CGroupExpression* pgexpr = *pgexpr_iter;
	while (nullptr != pgexpr)
	{
		pgexpr_iter++;
		CGroupExpression* pgexprNext = *pgexpr_iter;
		pgexpr->CleanupContexts();
		pgexpr = pgexprNext;
	}
	// cleaning-up duplicate expressions
	pgexpr_iter = m_listDupGExprs.begin();
	pgexpr = *pgexpr_iter;
	while (nullptr != pgexpr)
	{
		pgexpr_iter++;
		CGroupExpression* pgexprNext = *pgexpr_iter;
		pgexpr->CleanupContexts();
		pgexpr = pgexprNext;
	}
	// cleanup optimization contexts
	m_sht.clear();
}

//---------------------------------------------------------------------------
//	@function:
//		CGroup::UpdateBestCost
//
//	@doc:
//		 Update the group expression with best cost under the given
//		 optimization context
//
//---------------------------------------------------------------------------
void CGroup::UpdateBestCost(COptimizationContext* poc, CCostContext* pcc)
{
	CGroup::ShtOC::iterator itr;
	COptimizationContext* pocFound = nullptr;
	{
		// scope for accessor
		itr = m_sht.find(poc->HashValue());
		pocFound = itr->second;
	}
	// update best cost context
	CCostContext* pccBest = pocFound->m_pccBest;
	if (GPOPT_INVALID_COST != pcc->m_cost && (nullptr == pccBest || pcc->FBetterThan(pccBest)))
	{
		pocFound->SetBest(pcc);
	}
}

//---------------------------------------------------------------------------
//	@function:
//		CGroup::PocLookup
//
//	@doc:
//		Lookup a given context in contexts hash table
//
//---------------------------------------------------------------------------
COptimizationContext* CGroup::PocLookup(CReqdPropPlan* prpp, ULONG ulSearchStageIndex)
{
	duckdb::vector<ColumnBinding> v;
	COptimizationContext* poc = new COptimizationContext(this, prpp, new CReqdPropRelational(v), ulSearchStageIndex);
	COptimizationContext* pocFound = nullptr;
	{
		auto itr = m_sht.find(poc->HashValue());
		pocFound = itr->second;
	}
	return pocFound;
}

//---------------------------------------------------------------------------
//	@function:
//		CGroup::PocLookupBest
//
//	@doc:
//		Lookup the best context across all stages for the given required
//		properties
//
//---------------------------------------------------------------------------
COptimizationContext* CGroup::PocLookupBest(ULONG ulSearchStages, CReqdPropPlan* prpp)
{
	COptimizationContext* pocBest = nullptr;
	CCostContext* pccBest= nullptr;
	for (ULONG ul = 0; ul < ulSearchStages; ul++)
	{
		COptimizationContext* pocCurrent = PocLookup(prpp, ul);
		if (nullptr == pocCurrent)
		{
			continue;
		}
		CCostContext* pccCurrent = pocCurrent->m_pccBest;
		if (nullptr == pccBest || (nullptr != pccCurrent && pccCurrent->FBetterThan(pccBest)))
		{
			pocBest = pocCurrent;
			pccBest = pccCurrent;
		}
	}
	return pocBest;
}

//---------------------------------------------------------------------------
//	@function:
//		CGroup::Ppoc
//
//	@doc:
//		Lookup a context by id
//
//---------------------------------------------------------------------------
COptimizationContext* CGroup::Ppoc(ULONG id) const
{
	COptimizationContext* poc = nullptr;
	auto iter = m_sht.begin();
	while (iter != m_sht.end())
	{
		{
			poc = iter->second;
			if (poc->m_id == id)
			{
				return poc;
			}
			++iter;
		}
	}
	return nullptr;
}

//---------------------------------------------------------------------------
//	@function:
//		CGroup::PocInsert
//
//	@doc:
//		Insert a given context into contexts hash table only if a matching
//		context does not already exist;
//		return either the inserted or the existing matching context
//
//---------------------------------------------------------------------------
COptimizationContext* CGroup::PocInsert(COptimizationContext* poc)
{
	auto itr = m_sht.find(poc->HashValue());
	if (m_sht.end() == itr)
	{
		poc->SetId((ULONG) UlpIncOptCtxts());
		m_sht.insert(make_pair(poc->HashValue(), poc));
		return poc;
	}
	COptimizationContext* pocFound = itr->second;
	return pocFound;
}

//---------------------------------------------------------------------------
//	@function:
//		CGroup::PgexprBest
//
//	@doc:
//		Lookup best group expression under optimization context
//
//---------------------------------------------------------------------------
CGroupExpression* CGroup::PgexprBest(COptimizationContext* poc)
{
	auto itr = m_sht.find(poc->HashValue());
	COptimizationContext* pocFound = itr->second;
	if (nullptr != pocFound)
	{
		return pocFound->PgexprBest();
	}
	return nullptr;
}

//---------------------------------------------------------------------------
//	@function:
//		CGroup::SetId
//
//	@doc:
//		Set group id;
//		separated from constructor to avoid synchronization issues
//
//---------------------------------------------------------------------------
void CGroup::SetId(ULONG id)
{
	m_id = id;
}

//---------------------------------------------------------------------------
//	@function:
//		CGroup::InitProperties
//
//	@doc:
//		Initialize group's properties
//
//---------------------------------------------------------------------------
void CGroup::InitProperties(CDrvdProp* pdp)
{
	m_pdp = pdp;
}

//---------------------------------------------------------------------------
//	@function:
//		CGroup::SetState
//
//	@doc:
//		Set group state;
//
//---------------------------------------------------------------------------
void CGroup::SetState(EState estNewState)
{
	m_estate = estNewState;
}

//---------------------------------------------------------------------------
//	@function:
//		CGroup::HashValue
//
//	@doc:
//		Hash function for group identification
//
//---------------------------------------------------------------------------
ULONG CGroup::HashValue() const
{
	ULONG id = m_id;
	if (FDuplicateGroup() && 0 == m_ulGExprs)
	{
		// group has been merged into another group
		id = m_pgroupDuplicate->m_id;
	}
	return gpos::HashValue<ULONG>(&id);
}

//---------------------------------------------------------------------------
//	@function:
//		CGroup::Insert
//
//	@doc:
//		Insert group expression
//
//---------------------------------------------------------------------------
void CGroup::Insert(CGroupExpression* pgexpr)
{
	m_listGExprs.emplace_back(pgexpr);
	if (pgexpr->m_pop->FLogical())
	{
		m_fHasNewLogicalOperators = true;
	}
	if (pgexpr->Eol() > m_eolMax)
	{
		m_eolMax = pgexpr->Eol();
	}
}

//---------------------------------------------------------------------------
//	@function:
//		CGroup::MoveDuplicateGExpr
//
//	@doc:
//		Move duplicate group expression to duplicates list
//
//---------------------------------------------------------------------------
void CGroup::MoveDuplicateGExpr(CGroupExpression* pgexpr)
{
	m_listGExprs.remove(pgexpr);
	m_ulGExprs--;
	m_listDupGExprs.emplace_back(pgexpr);
}

//---------------------------------------------------------------------------
//	@function:
//		CGroup::PgexprFirst
//
//	@doc:
//		Retrieve first expression in group
//
//---------------------------------------------------------------------------
list<CGroupExpression*>::iterator CGroup::PgexprFirst()
{
	return m_listGExprs.begin();
}

//---------------------------------------------------------------------------
//	@function:
//		CGroup::PgexprNext
//
//	@doc:
//		Retrieve next expression in group
//
//---------------------------------------------------------------------------
list<CGroupExpression*>::iterator CGroup::PgexprNext(list<CGroupExpression*>::iterator pgexpr_iter)
{
	return pgexpr_iter++;
}

//---------------------------------------------------------------------------
//	@function:
//		CGroup::FMatchGroups
//
//	@doc:
//		Determine whether two arrays of groups are equivalent
//
//---------------------------------------------------------------------------
bool CGroup::FMatchGroups(duckdb::vector<CGroup*> pdrgpgroupFst, duckdb::vector<CGroup*> pdrgpgroupSnd)
{
	ULONG arity = pdrgpgroupFst.size();
	for (ULONG i = 0; i < arity; i++)
	{
		CGroup* pgroupFst = pdrgpgroupFst[i];
		CGroup* pgroupSnd = pdrgpgroupSnd[i];
		if (pgroupFst != pgroupSnd && !FDuplicateGroups(pgroupFst, pgroupSnd))
		{
			return false;
		}
	}
	return true;
}

//---------------------------------------------------------------------------
//	@function:
//		CGroup::FMatchNonScalarGroups
//
//	@doc:
//		 Matching of pairs of arrays of groups while skipping scalar groups
//
//---------------------------------------------------------------------------
bool CGroup::FMatchNonScalarGroups(duckdb::vector<CGroup*> pdrgpgroupFst, duckdb::vector<CGroup*> pdrgpgroupSnd)
{
	if (pdrgpgroupFst.size() != pdrgpgroupSnd.size())
	{
		return false;
	}
	ULONG arity = pdrgpgroupFst.size();
	for (ULONG i = 0; i < arity; i++)
	{
		CGroup* pgroupFst = pdrgpgroupFst[i];
		CGroup* pgroupSnd = pdrgpgroupSnd[i];
		if (pgroupFst->m_fScalar)
		{
			// skip scalar groups
			continue;
		}
		if (pgroupFst != pgroupSnd && !FDuplicateGroups(pgroupFst, pgroupSnd))
		{
			return false;
		}
	}
	return true;
}

//---------------------------------------------------------------------------
//	@function:
//		CGroup::FDuplicateGroups
//
//	@doc:
//		Determine whether two groups are equivalent
//
//---------------------------------------------------------------------------
bool CGroup::FDuplicateGroups(CGroup* pgroupFst, CGroup* pgroupSnd)
{
	CGroup* pgroupFstDup = pgroupFst->m_pgroupDuplicate;
	CGroup* pgroupSndDup = pgroupSnd->m_pgroupDuplicate;
	return (pgroupFst == pgroupSnd) || (pgroupFst == pgroupSndDup) || (pgroupSnd == pgroupFstDup) || (nullptr != pgroupFstDup && nullptr != pgroupSndDup && pgroupFstDup == pgroupSndDup);
}

//---------------------------------------------------------------------------
//	@function:
//		CGroup::AddDuplicateGrp
//
//	@doc:
//		Add duplicate group
//
//---------------------------------------------------------------------------
void CGroup::AddDuplicateGrp(CGroup* pgroup)
{
	// add link following monotonic ordering of group IDs
	CGroup* pgroupSrc = this;
	CGroup* pgroupDest = pgroup;
	if (this->m_id > pgroup->m_id)
	{
		std::swap(pgroupSrc, pgroupDest);
	}
	// keep looping until we add link
	while (pgroupSrc->m_pgroupDuplicate != pgroupDest)
	{
		if (nullptr == pgroupSrc->m_pgroupDuplicate)
		{
			pgroupSrc->m_pgroupDuplicate = pgroupDest;
		}
		else
		{
			pgroupSrc = pgroupSrc->m_pgroupDuplicate;
			if (pgroupSrc->m_id > pgroupDest->m_id)
			{
				std::swap(pgroupSrc, pgroupDest);
			}
		}
	}
}

//---------------------------------------------------------------------------
//	@function:
//		CGroup::ResolveDuplicateMaster
//
//	@doc:
//		Resolve master duplicate group
//
//---------------------------------------------------------------------------
void CGroup::ResolveDuplicateMaster()
{
	if (!FDuplicateGroup())
	{
		return;
	}
	CGroup* pgroupTarget = m_pgroupDuplicate;
	while (nullptr != pgroupTarget->m_pgroupDuplicate)
	{
		pgroupTarget = pgroupTarget->m_pgroupDuplicate;
	}
	// update reference to target group
	m_pgroupDuplicate = pgroupTarget;
}

//---------------------------------------------------------------------------
//	@function:
//		CGroup::MergeGroup
//
//	@doc:
//		Merge group with its duplicate - not thread-safe
//
//---------------------------------------------------------------------------
void CGroup::MergeGroup()
{
	if (!FDuplicateGroup())
	{
		return;
	}
	// resolve target group
	ResolveDuplicateMaster();
	CGroup* pgroupTarget = m_pgroupDuplicate;
	// move group expressions from this group to target
	while (!m_listGExprs.empty())
	{
		CGroupExpression* pgexpr = m_listGExprs.front();
		m_listGExprs.pop_front();
		m_ulGExprs--;
		pgexpr->Reset(pgroupTarget, pgroupTarget->m_ulGExprs++);
		pgroupTarget->Insert(pgexpr);
	}
}

//---------------------------------------------------------------------------
//	@function:
//		CGroup::CreateDummyCostContext
//
//	@doc:
//		Create a dummy cost context attached to the first group expression,
//		used for plan enumeration for scalar groups
//
//
//---------------------------------------------------------------------------
void CGroup::CreateDummyCostContext()
{
	CGroupExpression* pgexprFirst;
	{
		CGroupProxy gp(this);
		pgexprFirst = *(gp.PgexprFirst());
	}
	duckdb::vector<ColumnBinding> v;
	COptimizationContext* poc = new COptimizationContext(this, CReqdPropPlan::PrppEmpty(), new CReqdPropRelational(v), 0);
	m_pccDummy = new CCostContext(poc, 0, pgexprFirst);
	m_pccDummy->SetState(CCostContext::estCosting);
	m_pccDummy->SetCost(0.0);
	m_pccDummy->SetState(CCostContext::estCosted);
}

//---------------------------------------------------------------------------
//	@function:
//		CGroup::RecursiveBuildTreeMap
//
//	@doc:
//		Find all cost contexts of current group expression that carry valid
//		implementation of the given optimization context,
//		for all such cost contexts, introduce a link from parent cost context
//		to child cost context and then process child groups recursively
//
//
//---------------------------------------------------------------------------
void CGroup::RecursiveBuildTreeMap(COptimizationContext* poc, CCostContext* pccParent, CGroupExpression* pgexprCurrent, ULONG child_index, CTreeMap<CCostContext, Operator, CDrvdPropCtxtPlan, CCostContext::HashValue, CCostContext::Equals> *ptmap)
{
	duckdb::vector<CCostContext*> pdrgpcc = pgexprCurrent->PdrgpccLookupAll(poc);
	const ULONG ulCCSize = pdrgpcc.size();
	if (0 == ulCCSize)
	{
		// current group expression has no valid implementations of optimization context
		return;
	}
	// iterate over all valid implementations of given optimization context
	for (ULONG ulCC = 0; ulCC < ulCCSize; ulCC++)
	{
		CCostContext* pccCurrent = pdrgpcc[ulCC];
		if (nullptr != pccParent)
		{
			// link parent cost context to child cost context
			ptmap->Insert(pccParent, child_index, pccCurrent);
		}
		duckdb::vector<COptimizationContext*> pdrgpoc = pccCurrent->m_pdrgpoc;
		if (0 != pdrgpoc.size())
		{
			// process children recursively
			const ULONG arity = pgexprCurrent->Arity();
			for (ULONG ul = 0; ul < arity; ul++)
			{
				CGroup* pgroupChild = (*pgexprCurrent)[ul];
				COptimizationContext* pocChild = nullptr;
				if (!pgroupChild->m_fScalar)
				{
					pocChild = pdrgpoc[ul];
				}
				pgroupChild->BuildTreeMap(pocChild, pccCurrent, ul, ptmap);
			}
		}
	}
}

//---------------------------------------------------------------------------
//	@function:
//		CGroup::BuildTreeMap
//
//	@doc:
//		Given a parent cost context and an optimization context,
//		link parent cost context to all cost contexts in current group
//		that carry valid implementation of the given optimization context
//
//
//---------------------------------------------------------------------------
void CGroup::BuildTreeMap(COptimizationContext* poc, CCostContext* pccParent, ULONG child_index, CTreeMap<CCostContext, Operator, CDrvdPropCtxtPlan, CCostContext::HashValue, CCostContext::Equals>* ptmap)
{
	// check if link has been processed before,
	// this is crucial to eliminate unnecessary recursive calls
	SContextLink pclink(pccParent, child_index, poc);
	if (m_plinkmap.find(pclink.HashValue()) != m_plinkmap.end())
	{
		// link is already processed
		return;
	}
	list<CGroupExpression*>::iterator itr;
	// start with first non-logical group expression
	CGroupExpression* pgexprCurrent = nullptr;
	{
		CGroupProxy gp(this);
		itr = gp.m_pgroup->m_listGExprs.begin();
		itr = gp.PgexprSkipLogical(itr);
		pgexprCurrent = *itr;
	}
	while (m_listGExprs.end() != itr)
	{
		if (pgexprCurrent->m_pop->FPhysical())
		{
			// create links recursively
			RecursiveBuildTreeMap(poc, pccParent, pgexprCurrent, child_index, ptmap);
		}
		else
		{
			// this is a scalar group, link parent cost context to group's dummy context
			ptmap->Insert(pccParent, child_index, m_pccDummy);
			// recursively link group's dummy context to child contexts
			const ULONG arity = pgexprCurrent->Arity();
			for (ULONG ul = 0; ul < arity; ul++)
			{
				CGroup* pgroupChild = (*pgexprCurrent)[ul];
				pgroupChild->BuildTreeMap(nullptr, m_pccDummy, ul, ptmap);
			}
		}
		// move to next non-logical group expression
		{
			CGroupProxy gp(this);
			itr = gp.PgexprSkipLogical(itr);
			pgexprCurrent = *itr;
		}
	}
	// remember processed links to avoid re-processing them later
	m_plinkmap.insert(make_pair(pclink.HashValue(), true));
}

//---------------------------------------------------------------------------
//	@function:
//		CGroup::PgexprBestPromise
//
//	@doc:
//		Find group expression with best stats promise and the
//		same children as given expression
//
//---------------------------------------------------------------------------
CGroupExpression* CGroup::PgexprBestPromise(CGroupExpression* pgexprToMatch)
{
	duckdb::vector<ColumnBinding> v;
	CGroupExpression* pgexprCurrent = nullptr;
	CGroupExpression* pgexprBest = nullptr;
	list<CGroupExpression*>::iterator itr;
	// get first logical group expression
	{
		CGroupProxy gp(this);
		itr = gp.m_pgroup->m_listGExprs.begin();
		itr = gp.PgexprNextLogical(itr);
		pgexprCurrent = *itr;
	}
	while (m_listGExprs.end() != itr)
	{
		if (pgexprCurrent->FMatchNonScalarChildren(pgexprToMatch))
		{
			pgexprBest = pgexprCurrent;
		}
		// move to next logical group expression
		{
			CGroupProxy gp(this);
			++itr;
			itr = gp.PgexprNextLogical(itr);
			pgexprCurrent = *itr;
		}
	}
	return pgexprBest;
}

//---------------------------------------------------------------------------
//	@function:
//		CGroup::ResetGroupState
//
//	@doc:
//		Reset group state;
//		resetting state is not thread-safe
//
//---------------------------------------------------------------------------
void CGroup::ResetGroupState()
{
	// reset group expression states
	list<CGroupExpression*>::iterator pgexpr_iter = m_listGExprs.begin();
	while (m_listGExprs.end() != pgexpr_iter)
	{
		CGroupExpression* pgexpr = *pgexpr_iter;
		pgexpr->ResetState();
		pgexpr_iter++;
	}
	// reset group state
	{
		CGroupProxy gp(this);
		m_estate = estUnexplored;
	}
}

//---------------------------------------------------------------------------
//	@function:
//		CGroup::ResetLinkMap
//
//	@doc:
//		Reset link map for plan enumeration;
//		this operation is not thread safe
//
//---------------------------------------------------------------------------
void CGroup::ResetLinkMap()
{
	m_plinkmap.clear();
}

//---------------------------------------------------------------------------
//	@function:
//		CGroup::ResetGroupJobQueues
//
//	@doc:
//		Reset group job queues;
//
//---------------------------------------------------------------------------
void CGroup::ResetGroupJobQueues()
{
	CGroupProxy gp(this);
	m_jqExploration.Reset();
	m_jqImplementation.Reset();
}

//---------------------------------------------------------------------------
//	@function:
//		CGroup::CostLowerBound
//
//	@doc:
//		Compute a cost lower bound on plans, rooted by a group expression
//		in current group, and satisfying the given required properties
//
//---------------------------------------------------------------------------
double CGroup::CostLowerBound(CReqdPropPlan* prppInput)
{
	auto iter = m_pcostmap.find(prppInput);
	double pcostLowerBound = GPOPT_INFINITE_COST;
	if (m_pcostmap.end() != iter)
	{
		pcostLowerBound = iter->second;
		return pcostLowerBound;
	}
	double costLowerBound = GPOPT_INFINITE_COST;
	// start with first non-logical group expression
	CGroupExpression* pgexprCurrent = nullptr;
	list<CGroupExpression*>::iterator itr;
	{
		CGroupProxy gp(this);
		itr = gp.m_pgroup->m_listGExprs.begin();
		itr = gp.PgexprSkipLogical(itr);
		pgexprCurrent = *itr;
	}
	while (m_listGExprs.end() != itr)
	{
		// considering an enforcer introduces a deadlock here since its child is
		// the same group that contains it,
		// since an enforcer must reside on top of another operator from the same
		// group, it cannot produce a better cost lower-bound and can be skipped here
		if (!CUtils::FEnforcer(pgexprCurrent->m_pop.get()))
		{
			double costLowerBoundGExpr = pgexprCurrent->CostLowerBound(prppInput, nullptr, gpos::ulong_max);
			if (costLowerBoundGExpr < costLowerBound)
			{
				costLowerBound = costLowerBoundGExpr;
			}
		}
		// move to next non-logical group expression
		{
			CGroupProxy gp(this);
			itr = gp.PgexprSkipLogical(itr);
			pgexprCurrent = *itr;
		}
	}
	m_pcostmap.insert(map<CReqdPropPlan*, double>::value_type(prppInput, costLowerBound));
	return costLowerBound;
}