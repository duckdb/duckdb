//---------------------------------------------------------------------------
//	@filename:
//		CMemo.cpp
//
//	@doc:
//		Implementation of Memo structure
//---------------------------------------------------------------------------
#include "duckdb/optimizer/cascade/search/CMemo.h"
#include "duckdb/optimizer/cascade/base.h"
#include "duckdb/optimizer/cascade/common/CAutoTimer.h"
#include "duckdb/optimizer/cascade/io/COstreamString.h"
#include "duckdb/optimizer/cascade/string/CWStringDynamic.h"
#include "duckdb/optimizer/cascade/base/CDrvdProp.h"
#include "duckdb/optimizer/cascade/base/CDrvdPropCtxtPlan.h"
#include "duckdb/optimizer/cascade/base/COptCtxt.h"
#include "duckdb/optimizer/cascade/base/COptimizationContext.h"
#include "duckdb/optimizer/cascade/base/CReqdPropPlan.h"
#include "duckdb/optimizer/cascade/engine/CEngine.h"
#include "duckdb/optimizer/cascade/search/CGroupProxy.h"
#include <assert.h>
#include <list>

using namespace std;

#define GPOPT_MEMO_HT_BUCKETS 50000

namespace gpopt
{
//---------------------------------------------------------------------------
//	@function:
//		CMemo::CMemo
//
//	@doc:
//		Ctor
//
//---------------------------------------------------------------------------
CMemo::CMemo()
	: m_aul(0), m_pgroupRoot(nullptr), m_ulpGrps(0), m_pmemotmap(nullptr)
{
}

//---------------------------------------------------------------------------
//	@function:
//		CMemo::~CMemo
//
//	@doc:
//		Dtor
//
//---------------------------------------------------------------------------
CMemo::~CMemo()
{
}

//---------------------------------------------------------------------------
//	@function:
//		CMemo::SetRoot
//
//	@doc:
//		Set root group
//
//---------------------------------------------------------------------------
void CMemo::SetRoot(CGroup* pgroup)
{
	m_pgroupRoot = pgroup;
}

//---------------------------------------------------------------------------
//	@function:
//		CMemo::Add
//
//	@doc:
//		Add new group to list
//
//---------------------------------------------------------------------------
void CMemo::Add(CGroup* pgroup, Operator* pexprOrigin)
{
	// extract expression props
	CDrvdProp* pdp = pexprOrigin->m_derived_property_relation;
	ULONG id = m_aul++;
	{
		CGroupProxy gp(pgroup);
		gp.SetId(id);
		gp.InitProperties(pdp);
	}
	m_listGroups.emplace_back(pgroup);
	m_ulpGrps++;
}

//---------------------------------------------------------------------------
//	@function:
//		CMemo::PgroupInsert
//
//	@doc:
//		Helper for inserting group expression in target group
//
//
//---------------------------------------------------------------------------
CGroup* CMemo::PgroupInsert(CGroup* pgroupTarget, CGroupExpression* pgexpr, Operator* pexprOrigin, bool fNewGroup)
{
	auto itr = m_sht.find(pgexpr->HashValue());
	// we do a lookup since group expression may have been already inserted
	if (m_sht.end() == itr)
	{
		m_sht.insert(make_pair(pgexpr->HashValue(), pgexpr));
		// group proxy scope
		{
			CGroupProxy gp(pgroupTarget);
			gp.Insert(pgexpr);
		}
		if (fNewGroup)
		{
			Add(pgroupTarget, pexprOrigin);
		}
		return pgexpr->m_pgroup;
	}
	CGroupExpression* pgexprFound = itr->second;
	return pgexprFound->m_pgroup;
}

//---------------------------------------------------------------------------
//	@function:
//		CMemo::PgroupInsert
//
//	@doc:
//		Helper to check if a new group needs to be created
//
//---------------------------------------------------------------------------
bool CMemo::FNewGroup(CGroup** ppgroupTarget, CGroupExpression* pgexpr, bool fScalar)
{
	if (nullptr == *ppgroupTarget && nullptr == pgexpr)
	{
		*ppgroupTarget = new CGroup(fScalar);
		return true;
	}
	return false;
}

//---------------------------------------------------------------------------
//	@function:
//		CMemo::PgroupInsert
//
//	@doc:
//		Attempt inserting a group expression in a target group;
//		if group expression is not in the hash table, insertion
//		succeeds and the function returns the input target group;
//		otherwise insertion fails and the function returns the
//		group containing the existing group expression
//
//---------------------------------------------------------------------------
CGroup* CMemo::PgroupInsert(CGroup* pgroupTarget, CGroupExpression* pgexpr)
{
	Operator* pexprOrigin = pgexpr->m_pop.get();
	CGroup* pgroupContainer = nullptr;
	CGroupExpression* pgexprFound = nullptr;
	// hash table accessor's scope
	{
		auto itr = m_sht.find(pgexpr->HashValue());
		if(itr != m_sht.end())
		{
			pgexprFound = itr->second;
		}
	}
	// check if we may need to create a new group
	bool fNewGroup = FNewGroup(&pgroupTarget, pgexprFound, false);
	if (fNewGroup)
	{
		// we may add a new group to Memo, so we derive props here
		(void) pexprOrigin->PdpDerive();
	}
	if (NULL != pgexprFound)
	{
		pgroupContainer = pgexprFound->m_pgroup;
	}
	else
	{
		pgroupContainer = PgroupInsert(pgroupTarget, pgexpr, pexprOrigin, fNewGroup);
	}
	// if insertion failed, release group as needed
	if (NULL == pgexpr->m_pgroup && fNewGroup)
	{
		fNewGroup = false;
	}
	// if a new scalar group is added, we materialize a scalar expression
	// for statistics derivation purposes
	if (fNewGroup && pgroupTarget->m_fScalar)
	{
		pgroupTarget->CreateDummyCostContext();
	}
	return pgroupContainer;
}

//---------------------------------------------------------------------------
//	@function:
//		CMemo::PexprExtractPlan
//
//	@doc:
//		Extract a plan that delivers the given required properties
//
//---------------------------------------------------------------------------
duckdb::unique_ptr<Operator> CMemo::PexprExtractPlan(CGroup* pgroupRoot, CReqdPropPlan* prppInput, ULONG ulSearchStages)
{
	CGroupExpression* pgexprBest;
	COptimizationContext* poc;
	double cost = GPOPT_INVALID_COST;
	if (pgroupRoot->m_fScalar)
	{
		// If the group has scalar expression, this group is called scalar group.
		// It has one and only one group expression, so the expression is also picked
		// up as the best expression for that group.
		// The group expression is a scalar expression, which may have 0 or multiple
		// scalar or non-scalar groups as its children.
		CGroupProxy gp(pgroupRoot);
		pgexprBest = *(gp.PgexprFirst());
	}
	else
	{
		// If the group does not have scalar expression, which means it has only logical
		// or physical expressions. In this case, we lookup the best optimization context
		// for the given required plan properties, and then retrieve the best group
		// expression under the optimization context.
		poc = pgroupRoot->PocLookupBest(ulSearchStages, prppInput);
		pgexprBest = pgroupRoot->PgexprBest(poc);
		if (nullptr != pgexprBest)
		{
			cost = poc->m_pccBest->m_cost;
		}
	}
	if (nullptr == pgexprBest)
	{
		// no plan found
		return nullptr;
	}
	duckdb::vector<duckdb::unique_ptr<Operator>> pdrgpexpr;
	// Get the length of groups for the best group expression
	// i.e. given the best expression is
	// 0: CScalarCmp (>=) [ 1 7 ]
	// the arity is 2, which means the pgexprBest has 2 children:
	// Group 1 and Group 7. Every single child is a CGroup.
	ULONG arity = pgexprBest->Arity();
	for (ULONG i = 0; i < arity; i++)
	{
		CGroup* pgroupChild = (*pgexprBest)[i];
		CReqdPropPlan* prpp = nullptr;
		// If the child group doesn't have scalar expression, we get the optimization
		// context for that child group as well as the required plan properties.
		//
		// But if the child group has scalar expression, which means it has one and
		// only one best scalar group expression, which does not need optimization,
		// because CJobGroupExpressionOptimization does not create optimization context
		// for that group. Besides, the scalar expression doesn't have plan properties.
		// In this case, the prpp is left to be NULL.
		if (!pgroupChild->m_fScalar)
		{
			if (pgroupRoot->m_fScalar)
			{
				// In very rare case, Orca may generate the plan that a group is a scalar
				// group, but it has non-scalar sub groups. i.e.:
				// Group 7 ():  --> pgroupRoot->m_fScalar == true
				//   0: CScalarSubquery["?column?" (19)] [ 6 ]
				// Group 6 (#GExprs: 2): --> pgroupChild->m_fScalar == false
				//   0: CLogicalProject [ 2 5 ]
				//   1: CPhysicalComputeScalar [ 2 5 ]
				// In the above case, because group 7 has scalar expression, Orca skipped
				// generating optimization context for group 7 and its subgroup group 6,
				// even the group 6 doesn't have scalar expression and it needs optimization.
				// Orca doesn't support this feature yet, so falls back to planner.
				assert(false);
			}
			COptimizationContext* pocChild = poc->m_pccBest->m_pdrgpoc[i];
			prpp = pocChild->m_prpp;
		}
		duckdb::unique_ptr<Operator> pexprChild = PexprExtractPlan(pgroupChild, prpp, ulSearchStages);
		pdrgpexpr.emplace_back(std::move(pexprChild));
	}
	duckdb::unique_ptr<Operator> pexpr = pgexprBest->m_pop->CopyWithNewChildren(pgexprBest, std::move(pdrgpexpr), cost);
	return pexpr;
}

//---------------------------------------------------------------------------
//	@function:
//		CMemo::MarkDuplicates
//
//	@doc:
//		Mark groups as duplicates
//
//---------------------------------------------------------------------------
void CMemo::MarkDuplicates(CGroup* pgroupFst, CGroup* pgroupSnd)
{
	pgroupFst->AddDuplicateGrp(pgroupSnd);
	pgroupFst->ResolveDuplicateMaster();
	pgroupSnd->ResolveDuplicateMaster();
}

//---------------------------------------------------------------------------
//	@function:
//		CMemo::FRehash
//
//	@doc:
//		Delete then re-insert all group expressions in memo hash table;
//		we do this at the end of exploration phase since identified
//		duplicate groups during exploration may cause changing hash values
//		of current group expressions,
//		rehashing group expressions using the new hash values allows
//		identifying duplicate group expressions that can be skipped from
//		further processing;
//
//		the function returns TRUE if rehashing resulted in discovering
//		new duplicate groups;
//
//		this function is NOT thread safe, and must not be called while
//		exploration/implementation/optimization is undergoing
//
//
//---------------------------------------------------------------------------
bool CMemo::FRehash()
{
	// dump memo hash table into a local list
	list<CGroupExpression*> listGExprs;
	auto itr = m_sht.begin();
	CGroupExpression* pgexpr;
	while (m_sht.end() != itr)
	{
		pgexpr = itr->second;
		if (NULL != pgexpr)
		{
			itr = m_sht.erase(itr);
			listGExprs.emplace_back(pgexpr);
		}
		else
		{
			++itr;
		}
	}
	// iterate on list and insert non-duplicate group expressions
	// back to memo hash table
	bool fNewDupGroups = false;
	while (!listGExprs.empty())
	{
		pgexpr = *(listGExprs.begin());
		listGExprs.pop_front();
		CGroupExpression* pgexprFound = NULL;
		{
			// hash table accessor scope
			itr = m_sht.find(pgexpr->HashValue());
			if (itr == m_sht.end())
			{
				// group expression has no duplicates, insert back to memo hash table
				m_sht.insert(make_pair(pgexpr->HashValue(), pgexpr));
				continue;
			}
			pgexprFound = itr->second;
		}
		// mark duplicate group expression
		pgexpr->SetDuplicate(pgexprFound);
		CGroup* pgroup = pgexpr->m_pgroup;
		// move group expression to duplicates list in owner group
		{
			// group proxy scope
			CGroupProxy gp(pgroup);
			gp.MoveDuplicateGExpr(pgexpr);
		}
		// check if we need also to mark duplicate groups
		CGroup* pgroupFound = pgexprFound->m_pgroup;
		if (pgroupFound != pgroup)
		{
			CGroup* pgroupDup = pgroup->m_pgroupDuplicate;
			CGroup* pgroupFoundDup = pgroupFound->m_pgroupDuplicate;
			if ((nullptr == pgroupDup && nullptr == pgroupFoundDup) || (pgroupDup != pgroupFoundDup))
			{
				MarkDuplicates(pgroup, pgroupFound);
				fNewDupGroups = true;
			}
		}
	}
	return fNewDupGroups;
}

//---------------------------------------------------------------------------
//	@function:
//		CMemo::GroupMerge
//
//	@doc:
//		Merge duplicate groups
//
//---------------------------------------------------------------------------
void CMemo::GroupMerge()
{
	// keep merging groups until we have no new duplicates
	bool fNewDupGroups = true;
	while (fNewDupGroups)
	{
		auto itr = m_listGroups.begin();
		while (m_listGroups.end() != itr)
		{
			CGroup* pgroup = *itr;
			pgroup->MergeGroup();
			++itr;
		}
		// check if root has been merged
		if (m_pgroupRoot->FDuplicateGroup())
		{
			m_pgroupRoot = m_pgroupRoot->m_pgroupDuplicate;
		}
		fNewDupGroups = FRehash();
	}
}

//---------------------------------------------------------------------------
//	@function:
//		CMemo::DeriveStatsIfAbsent
//
//	@doc:
//		Derive stats when no stats not present for the group
//
//---------------------------------------------------------------------------
void CMemo::DeriveStatsIfAbsent()
{
	/*
	auto itr = m_listGroups.begin();
	while (m_listGroups.end() != itr)
	{
		CGroup* pgroup = *itr;
		++itr;
	}
	*/
}

//---------------------------------------------------------------------------
//	@function:
//		CMemo::ResetGroupStates
//
//	@doc:
//		Reset states and job queues of memo groups
//
//---------------------------------------------------------------------------
void CMemo::ResetGroupStates()
{
	auto itr = m_listGroups.begin();
	while (m_listGroups.end() != itr)
	{
		CGroup* pgroup = *itr;
		pgroup->ResetGroupState();
		pgroup->ResetGroupJobQueues();
		pgroup->ResetHasNewLogicalOperators();
		++itr;
	}
}

//---------------------------------------------------------------------------
//	@function:
//		CMemo::BuildTreeMap
//
//	@doc:
//		Build tree map of member group expressions
//
//---------------------------------------------------------------------------
void CMemo::BuildTreeMap(COptimizationContext* poc)
{
	m_pmemotmap = new MemoTreeMap(gpopt::Operator::PexprRehydrate);
	m_pgroupRoot->BuildTreeMap(poc, NULL, gpos::ulong_max, m_pmemotmap);
}

//---------------------------------------------------------------------------
//	@function:
//		CMemo::ResetTreeMap
//
//	@doc:
//		Reset tree map
//
//---------------------------------------------------------------------------
void CMemo::ResetTreeMap()
{
	if (nullptr != m_pmemotmap)
	{
		delete m_pmemotmap;
		m_pmemotmap = nullptr;
	}
	auto itr = m_listGroups.begin();
	while (m_listGroups.end() != itr)
	{
		// reset link map of all groups
		CGroup* pgroup = *itr;
		pgroup->ResetLinkMap();
		++itr;
	}
}

//---------------------------------------------------------------------------
//	@function:
//		CMemo::UlDuplicateGroups
//
//	@doc:
//		Return number of duplicate groups
//
//---------------------------------------------------------------------------
ULONG CMemo::UlDuplicateGroups()
{
	ULONG ulDuplicates = 0;
	auto itr = m_listGroups.begin();
	while (m_listGroups.end() != itr)
	{
		CGroup* pgroup = *itr;
		if (pgroup->FDuplicateGroup())
		{
			ulDuplicates++;
		}
		++itr;
	}
	return ulDuplicates;
}

//---------------------------------------------------------------------------
//	@function:
//		CMemo::UlGrpExprs
//
//	@doc:
//		Return total number of group expressions
//
//---------------------------------------------------------------------------
ULONG CMemo::UlGrpExprs()
{
	ULONG ulGExprs = 0;
	auto itr = m_listGroups.begin();
	while (m_listGroups.end() != itr)
	{
		CGroup* pgroup = *itr;
		ulGExprs += pgroup->m_ulGExprs;
		++itr;
	}
	return ulGExprs;
}
}