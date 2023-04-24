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
#include "duckdb/optimizer/cascade/common/CRefCount.h"
#include "duckdb/optimizer/cascade/common/CSyncHashtable.h"
#include "duckdb/optimizer/cascade/common/CSyncList.h"

#include "duckdb/optimizer/cascade/search/CGroupExpression.h"

namespace gpopt
{
class CGroup;
class CDrvdProp;
class CDrvdPropCtxtPlan;
class CMemoProxy;
class COptimizationContext;

// memo tree map definition
typedef CTreeMap<CCostContext, CExpression, CDrvdPropCtxtPlan,
				 CCostContext::HashValue, CCostContext::Equals>
	MemoTreeMap;

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
private:
	// definition of hash table key accessor
	typedef CSyncHashtableAccessByKey<CGroupExpression,	 // entry
									  CGroupExpression>
		ShtAcc;

	// definition of hash table iterator
	typedef CSyncHashtableIter<CGroupExpression,  // entry
							   CGroupExpression>
		ShtIter;

	// definition of hash table iterator accessor
	typedef CSyncHashtableAccessByIter<CGroupExpression,  // entry
									   CGroupExpression>
		ShtAccIter;

	// memory pool
	CMemoryPool *m_mp;

	// id counter for groups
	ULONG m_aul;

	// root group
	CGroup *m_pgroupRoot;

	// number of groups
	ULONG_PTR m_ulpGrps;

	// tree map of member group expressions
	MemoTreeMap *m_pmemotmap;

	// list of groups
	CSyncList<CGroup> m_listGroups;

	// hashtable of all group expressions
	CSyncHashtable<CGroupExpression,  // entry
				   CGroupExpression>
		m_sht;

	// add new group
	void Add(CGroup *pgroup, CExpression *pexprOrigin);

	// rehash all group expressions after group merge - not thread-safe
	BOOL FRehash();

	// helper for inserting group expression in target group
	CGroup *PgroupInsert(CGroup *pgroupTarget, CGroupExpression *pgexpr,
						 CExpression *pexprOrigin, BOOL fNewGroup);

	// helper to check if a new group needs to be created
	BOOL FNewGroup(CGroup **ppgroupTarget, CGroupExpression *pgexpr,
				   BOOL fScalar);

	// private copy ctor
	CMemo(const CMemo &);

public:
	// ctor
	explicit CMemo(CMemoryPool *mp);

	// dtor
	~CMemo();

	// return root group
	CGroup *
	PgroupRoot() const
	{
		return m_pgroupRoot;
	}

	// return number of groups
	ULONG_PTR
	UlpGroups() const
	{
		return m_ulpGrps;
	}

	// return total number of group expressions
	ULONG UlGrpExprs();

	// return number of duplicate groups
	ULONG UlDuplicateGroups();

	// mark groups as duplicates
	void MarkDuplicates(CGroup *pgroupFst, CGroup *pgroupSnd);

	// return tree map
	MemoTreeMap *
	Pmemotmap() const
	{
		return m_pmemotmap;
	}

	// set root group
	void SetRoot(CGroup *pgroup);

	// insert group expression into hash table
	CGroup *PgroupInsert(CGroup *pgroupTarget, CExpression *pexprOrigin,
						 CGroupExpression *pgexpr);

	// extract a plan that delivers the given required properties
	CExpression *PexprExtractPlan(CMemoryPool *mp, CGroup *pgroupRoot,
								  CReqdPropPlan *prppInput,
								  ULONG ulSearchStages);

	// merge duplicate groups
	void GroupMerge();

	// reset states of all memo groups
	void ResetGroupStates();

	// reset statistics of memo groups
	void ResetStats();

	// print driver
	IOstream &OsPrint(IOstream &os);

	// derive stats when no stats not present for the group
	void DeriveStatsIfAbsent(CMemoryPool *mp);

	// build tree map
	void BuildTreeMap(COptimizationContext *poc);

	// reset tree map
	void ResetTreeMap();

	// print memo to output logger
	void Trace();

#ifdef GPOS_DEBUG
	// get group by id
	CGroup *Pgroup(ULONG id);

	// debug print for interactive debugging sessions only
	void DbgPrint();
#endif	// GPOS_DEBUG

};	// class CMemo

}  // namespace gpopt

#endif
