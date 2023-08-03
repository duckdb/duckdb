//---------------------------------------------------------------------------
//	@filename:
//		CGroup.h
//
//	@doc:
//		Group of equivalent expressions in the Memo structure
//---------------------------------------------------------------------------
#ifndef GPOPT_CGroup_H
#define GPOPT_CGroup_H

#include "duckdb/optimizer/cascade/base.h"
#include "duckdb/optimizer/cascade/base/CCostContext.h"
#include "duckdb/optimizer/cascade/common/CSyncList.h"
#include "duckdb/optimizer/cascade/search/CJobQueue.h"
#include "duckdb/optimizer/cascade/search/CTreeMap.h"
#include <unordered_map>
#include <list>

#define GPOPT_INVALID_GROUP_ID gpos::ulong_max

using namespace duckdb;
using namespace std;
using namespace gpos;

namespace gpopt
{
// forward declarations
class CGroup;
class CGroupExpression;
class CDrvdProp;
class CDrvdPropCtxtPlan;
class CGroupProxy;
class COptimizationContext;
class CReqdPropPlan;
class CReqdPropRelational;

// optimization levels in ascending order,
// under a given optimization context, group expressions in higher levels
// must be optimized before group expressions in lower levels,
// a group expression sets its level in CGroupExpression::SetOptimizationLevel()
enum EOptimizationLevel
{
	EolLow = 0, EolHigh, EolSentinel
};

struct SContextLink
{
public:
	// cost context in a parent group
	CCostContext* m_pccParent;

	// index used when treating current group as a child of group expression
	ULONG m_ulChildIndex;

	// optimization context used to locate group expressions in
	// current group to be linked with parent group expression
	COptimizationContext* m_poc;

public:
	// ctor
	SContextLink(CCostContext* pccParent, ULONG child_index, COptimizationContext* poc);

	// dtor
	virtual ~SContextLink();

	// equality function
	bool operator==(const SContextLink &pclink2) const;

	ULONG HashValue() const
    {
        ULONG ulHashPcc = 0;
		if (NULL != m_pccParent)
		{
			ulHashPcc = m_pccParent->HashValue();
		}
		ULONG ulHashPoc = 0;
		if (NULL != m_poc)
		{
			ulHashPoc = m_poc->HashValue();
		}
		return CombineHashes(m_ulChildIndex, CombineHashes(ulHashPcc, ulHashPoc));
    }
};	// struct SContextLink

//---------------------------------------------------------------------------
//	@class:
//		CGroup
//
//	@doc:
//		Group of equivalent expressions in the Memo structure
//
//---------------------------------------------------------------------------
class CGroup
{
friend class CGroupProxy;

public:
	// type definition of optimization context hash table
	typedef unordered_map<ULONG, COptimizationContext*> ShtOC;

	// states of a group
	enum EState
	{
		estUnexplored, estExploring, estExplored, estImplementing, estImplemented, estOptimizing, estOptimized, estSentinel
	};

public:
	//---------------------------------------------------------------------------
	//	@class:
	//		SContextLink
	//
	//	@doc:
	//		Internal structure to remember processed links in plan enumeration
	//
	//---------------------------------------------------------------------------
	
public:
	// id is used when printing memo contents
	ULONG m_id;

	// true if group hold scalar expressions
	bool m_fScalar;

	// join keys for outer child (only for scalar groups) (used by hash & merge joins)
	duckdb::vector<Expression*> m_pdrgpexprJoinKeysOuter;

	// join keys for inner child (only for scalar groups) (used by hash & merge joins)
	duckdb::vector<Expression*> m_pdrgpexprJoinKeysInner;

	// list of group expressions
	std::list<CGroupExpression*> m_listGExprs;

	// list of duplicate group expressions identified by group merge
	std::list<CGroupExpression*> m_listDupGExprs;

	// group derived properties
	CDrvdProp* m_pdp;

	// scalar expression for stat derivation (subqueries substituted with a dummy)
	Expression* m_pexprScalarRep;

	// scalar expression above is exactly the same as the scalar expr in the group
	bool m_pexprScalarRepIsExact;

	// dummy cost context used in scalar groups for plan enumeration
	CCostContext* m_pccDummy;

	// pointer to group containing the group expressions
	// of all duplicate groups
	CGroup* m_pgroupDuplicate;

	// map of processed links
	unordered_map<ULONG, bool> m_plinkmap;

	// hashtable of optimization contexts
	ShtOC m_sht;

	// number of group expressions
	ULONG m_ulGExprs;

	// map of cost lower bounds
	std::map<CReqdPropPlan*, double> m_pcostmap;

	// number of optimization contexts
	ULONG_PTR m_ulpOptCtxts;

	// current state
	EState m_estate;

	// maximum optimization level of member group expressions
	EOptimizationLevel m_eolMax;

	// were new logical operators added to the group?
	bool m_fHasNewLogicalOperators;

	// exploration job queue
	CJobQueue m_jqExploration;

	// implementation job queue
	CJobQueue m_jqImplementation;

public:
	// ctor
	CGroup(bool fScalar = false);

	// no copy ctor
	CGroup(const CGroup &) = delete;

	// dtor
	~CGroup();

public:
	// cleanup optimization contexts on destruction
	void CleanupContexts();

	// increment number of optimization contexts
	ULONG_PTR UlpIncOptCtxts()
	{
		return m_ulpOptCtxts++;
	}

	// the following functions are only accessed through group proxy
	// setter of group id
	void SetId(ULONG id);

	// setter of group state
	void SetState(EState estNewState);

	// insert new group expression
	void Insert(CGroupExpression* pgexpr);

	// move duplicate group expression to duplicates list
	void MoveDuplicateGExpr(CGroupExpression* pgexpr);

	// initialize group's properties
	void InitProperties(CDrvdProp* pdp);

	// retrieve first group expression
	list<CGroupExpression*>::iterator PgexprFirst();

	// retrieve next group expression
	list<CGroupExpression*>::iterator PgexprNext(list<CGroupExpression*>::iterator pgexpr_iter);

	// helper function to add links in child groups
	void RecursiveBuildTreeMap(COptimizationContext* poc, CCostContext* pccParent, CGroupExpression* pgexprCurrent, ULONG child_index, CTreeMap<CCostContext, Operator, CDrvdPropCtxtPlan, CCostContext::HashValue, CCostContext::Equals> *ptmap);

	// find the group expression having the best stats promise
	CGroupExpression* PgexprBestPromise(CReqdPropRelational* prprelInput);

	// hash function
	ULONG HashValue() const;

	// has group been explored?
	bool FExplored() const
	{
		return estExplored <= m_estate;
	}

	// has group been implemented?
	bool FImplemented() const
	{
		return estImplemented <= m_estate;
	}

	// has group been optimized?
	bool FOptimized() const
	{
		return estOptimized <= m_estate;
	}

	// reset has new logical operators flag
	void ResetHasNewLogicalOperators()
	{
		m_fHasNewLogicalOperators = false;
	}

	// reset group state
	void ResetGroupState();

	// reset group job queues
	void ResetGroupJobQueues();

	// check if group has duplicates
	bool FDuplicateGroup() const
	{
		return NULL != m_pgroupDuplicate;
	}

	// resolve master duplicate group;
	// this is the group that will host all expressions in current group after merging
	void ResolveDuplicateMaster();

	// add duplicate group
	void AddDuplicateGrp(CGroup* pgroup);

	// merge group with its duplicate - not thread-safe
	void MergeGroup();

	// lookup a given context in contexts hash table
	COptimizationContext* PocLookup(CReqdPropPlan* prpp, ULONG ulSearchStageIndex);

	// lookup the best context across all stages for the given required properties
	COptimizationContext* PocLookupBest(ULONG ulSearchStages, CReqdPropPlan* prpp);

	// find a context by id
	COptimizationContext* Ppoc(ULONG id) const;

	// insert given context into contexts hash table
	COptimizationContext* PocInsert(COptimizationContext* poc);

	// update the best group cost under the given optimization context
	void UpdateBestCost(COptimizationContext* poc, CCostContext* pcc);

	// lookup best expression under given optimization context
	CGroupExpression* PgexprBest(COptimizationContext* poc);

	// materialize a dummy cost context attached to the first group expression
	void CreateDummyCostContext();

	// find group expression with best stats promise and the same given children
	CGroupExpression* PgexprBestPromise(CGroupExpression* pgexprToMatch);

	// link parent group expression to group members
	void BuildTreeMap(COptimizationContext* poc, CCostContext* pccParent, ULONG child_index, CTreeMap<CCostContext, Operator, CDrvdPropCtxtPlan, CCostContext::HashValue, CCostContext::Equals>* ptmap);

	// reset link map used in plan enumeration
	void ResetLinkMap();

	// compute cost lower bound for the plan satisfying given required properties
	double CostLowerBound(CReqdPropPlan* prppInput);

	// matching of pairs of arrays of groups
	static bool FMatchGroups(duckdb::vector<CGroup*> pdrgpgroupFst, duckdb::vector<CGroup*> pdrgpgroupSnd);

	// matching of pairs of arrays of groups while skipping scalar groups
	static bool FMatchNonScalarGroups(duckdb::vector<CGroup*> pdrgpgroupFst, duckdb::vector<CGroup*> pdrgpgroupSnd);

	// determine if a pair of groups are duplicates
	static bool FDuplicateGroups(CGroup* pgroupFst, CGroup* pgroupSnd);

	// slink for group list in memo
	SLink m_link;
};	// class CGroup
}  // namespace gpopt
#endif