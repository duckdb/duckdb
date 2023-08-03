//---------------------------------------------------------------------------
//	@filename:
//		CMemo.h
//
//	@doc:
//		Memo lookup table for dynamic programming
//---------------------------------------------------------------------------
#ifndef GPOPT_CMemo_H
#define GPOPT_CMemo_H

#include "duckdb/optimizer/cascade/base.h"
#include "duckdb/optimizer/cascade/common/CSyncList.h"
#include "duckdb/optimizer/cascade/search/CGroupExpression.h"
#include <list>

using namespace std;

namespace gpopt
{
class CGroup;
class CDrvdProp;
class CDrvdPropCtxtPlan;
class CMemoProxy;
class COptimizationContext;

// memo tree map definition
typedef CTreeMap<CCostContext, gpopt::Operator, CDrvdPropCtxtPlan, CCostContext::HashValue, CCostContext::Equals> MemoTreeMap;

using namespace gpos;

//---------------------------------------------------------------------------
//	@class:
//		CMemo
//
//	@doc:
//		Dynamic programming table
//
//---------------------------------------------------------------------------
class CMemo
{
public:
	// id counter for groups
	ULONG m_aul;

	// root group
	CGroup* m_pgroupRoot;

	// number of groups
	ULONG_PTR m_ulpGrps;

	// tree map of member group expressions
	MemoTreeMap* m_pmemotmap;

	// list of groups
	list<CGroup*> m_listGroups;

	// hashtable of all group expressions
	unordered_map<ULONG, CGroupExpression*> m_sht;

	// add new group
	void Add(CGroup* pgroup, Operator* pexprOrigin);

	// rehash all group expressions after group merge - not thread-safe
	bool FRehash();

	// helper for inserting group expression in target group
	CGroup* PgroupInsert(CGroup* pgroupTarget, CGroupExpression* pgexpr, Operator* pexprOrigin, bool fNewGroup);

	// helper to check if a new group needs to be created
	bool FNewGroup(CGroup** ppgroupTarget, CGroupExpression* pgexpr, bool fScalar);

public:
	// ctor
	explicit CMemo();
	
	// no copy ctor
	CMemo(const CMemo &) = delete;
	
	// dtor
	~CMemo();

	// return root group
	CGroup* PgroupRoot() const
	{
		return m_pgroupRoot;
	}

	// return number of groups
	ULONG_PTR UlpGroups() const
	{
		return m_ulpGrps;
	}

	// return total number of group expressions
	ULONG UlGrpExprs();

	// return number of duplicate groups
	ULONG UlDuplicateGroups();

	// mark groups as duplicates
	void MarkDuplicates(CGroup* pgroupFst, CGroup* pgroupSnd);

	// return tree map
	MemoTreeMap* Pmemotmap() const
	{
		return m_pmemotmap;
	}

	// set root group
	void SetRoot(CGroup* pgroup);

	// insert group expression into hash table
	CGroup* PgroupInsert(CGroup* pgroupTarget, CGroupExpression* pgexpr);

	// extract a plan that delivers the given required properties
	duckdb::unique_ptr<Operator> PexprExtractPlan(CGroup* pgroupRoot, CReqdPropPlan* prppInput, ULONG ulSearchStages);

	// merge duplicate groups
	void GroupMerge();

	// reset states of all memo groups
	void ResetGroupStates();

	// derive stats when no stats not present for the group
	void DeriveStatsIfAbsent();

	// build tree map
	void BuildTreeMap(COptimizationContext* poc);

	// reset tree map
	void ResetTreeMap();
};	// class CMemo
}  // namespace gpopt
#endif