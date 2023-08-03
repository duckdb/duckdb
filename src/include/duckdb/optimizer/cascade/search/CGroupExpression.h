//---------------------------------------------------------------------------
//	@filename:
//		CGroupExpression.h
//
//	@doc:
//		Equivalent of CExpression inside Memo structure
//---------------------------------------------------------------------------
#ifndef GPOPT_CGroupExpression_H
#define GPOPT_CGroupExpression_H

#include "duckdb/optimizer/cascade/base.h"
#include "duckdb/optimizer/cascade/common/CList.h"
#include "duckdb/optimizer/cascade/base/CCostContext.h"
#include "duckdb/optimizer/cascade/engine/CPartialPlan.h"
#include "duckdb/optimizer/cascade/operators/Operator.h"
#include "duckdb/optimizer/cascade/search/CBinding.h"
#include "duckdb/optimizer/cascade/search/CGroup.h"
#include "duckdb/optimizer/cascade/xforms/CXform.h"

#define GPOPT_INVALID_GEXPR_ID gpos::ulong_max

using namespace gpos;
using namespace duckdb;
using namespace std;

namespace gpopt
{
//---------------------------------------------------------------------------
//	@class:
//		CGroupExpression
//
//	@doc:
//		Expression representation inside Memo structure
//
//---------------------------------------------------------------------------
class CGroupExpression
{
public:
	// states of a group expression
	enum EState
	{
		estUnexplored, estExploring, estExplored, estImplementing, estImplemented, estSentinel
	};

	// circular dependency state
	enum ECircularDependency
	{
		ecdDefault, ecdCircularDependency, ecdSentinel
	};

	// type definition of cost context hash table
	typedef unordered_map<ULONG, CCostContext*> ShtCC;

public:
	// expression id
	ULONG m_id;

	// duplicate group expression
	CGroupExpression* m_pgexprDuplicate;

	// operator class
	duckdb::unique_ptr<Operator> m_pop;

	// array of child groups
	duckdb::vector<CGroup*> m_pdrgpgroup;

	// sorted array of children groups for faster comparison
	// of order-insensitive operators
	duckdb::vector<CGroup*> m_pdrgpgroupSorted;

	// back pointer to group
	CGroup* m_pgroup;

	// id of xform that generated group expression
	CXform::EXformId m_exfidOrigin;

	// group expression that generated current group expression via xform
	CGroupExpression* m_pgexprOrigin;

	// flag to indicate if group expression was created as a node at some
	// intermediate level when origin expression was inserted to memo
	bool m_fIntermediate;

	// state of group expression
	EState m_estate;

	// optimization level
	EOptimizationLevel m_eol;

	// map of partial plans to their cost lower bound
	unordered_map<ULONG, double> m_ppartialplancostmap;

	// circular dependency state
	ECircularDependency m_ecirculardependency;

	// hashtable of cost contexts
	ShtCC m_sht;

	// set group back pointer
	void SetGroup(CGroup* pgroup);

	// set group expression id
	void SetId(ULONG id);

	// preprocessing before applying transformation
	void PreprocessTransform(CXform* pxform);

	// postprocessing after applying transformation
	void PostprocessTransform(CXform* pxform);

	// costing scheme
	double CostCompute(CCostContext* pcc) const;

	// set optimization level of group expression
	void SetOptimizationLevel();

	// check validity of group expression
	bool FValidContext(COptimizationContext* poc, duckdb::vector<COptimizationContext*> pdrgpocChild);

	// remove cost context in hash table
	CCostContext* PccRemove(COptimizationContext* poc, ULONG ulOptReq);

	// insert given context in hash table only if a better context does not exist, return the context that is kept it in hash table
	CCostContext* PccInsertBest(CCostContext* pcc);

public:
	// dummy ctor; used for creating invalid gexpr
	CGroupExpression()
		: m_id(GPOPT_INVALID_GEXPR_ID), m_exfidOrigin(CXform::ExfInvalid), m_fIntermediate(false), m_estate(estUnexplored), m_eol(EolLow)
	{
	}
	
	// ctor
	CGroupExpression(duckdb::unique_ptr<Operator> pop, duckdb::vector<CGroup*> pdrgpgroup, CXform::EXformId exfid, CGroupExpression* pgexprOrigin, bool fIntermediate);

	// no copy ctor
	CGroupExpression(const CGroupExpression &) = delete;

	// dtor
	virtual ~CGroupExpression();

public:
	// set duplicate group expression
	void SetDuplicate(CGroupExpression* pgexpr)
	{
		m_pgexprDuplicate = pgexpr;
	}

	// cleanup cost contexts
	void CleanupContexts();

	// check if cost context already exists in group expression hash table
	bool FCostContextExists(COptimizationContext* poc, duckdb::vector<COptimizationContext*> pdrgpoc);

	// compute and store expression's cost under a given context
	CCostContext* PccComputeCost(COptimizationContext* poc, ULONG ulOptReq, duckdb::vector<COptimizationContext*> pdrgpoc, bool fPruned, double costLowerBound);

	// compute a cost lower bound for plans, rooted by current group expression, and satisfying the given required properties
	double CostLowerBound(CReqdPropPlan* prppInput, CCostContext* pccChild, ULONG child_index);

	// initialize group expression
	void Init(CGroup* pgroup, ULONG id);

	// reset group expression
	void Reset(CGroup* pgroup, ULONG id)
	{
		m_pgroup = pgroup;
		m_id = id;
	}

	// optimization level accessor
	EOptimizationLevel Eol() const
	{
		return m_eol;
	}

	// shorthand to access children
	CGroup* operator[](ULONG ulPos) const
	{
		CGroup* pgroup = m_pdrgpgroup[ulPos];
		// during optimization, the operator returns the duplicate group;
		// in exploration and implementation the group may contain
		// group expressions that have not been processed yet;
		if (0 == pgroup->m_ulGExprs)
		{
			return pgroup->m_pgroupDuplicate;
		}
		return pgroup;
	};

	// arity function
	ULONG Arity() const
	{
		return m_pdrgpgroup.size();
	}

	// comparison operator for hashtables
	bool operator==(const CGroupExpression &gexpr) const
	{
		return gexpr.Matches(this);
	}

	// equality function for hash table
	static bool Equals(const CGroupExpression &gexprLeft, const CGroupExpression &gexprRight)
	{
		return gexprLeft == gexprRight;
	}

	// match group expression against given operator and its children
	bool Matches(const CGroupExpression* pgexpr) const;

	// match non-scalar children of group expression against given children of passed expression
	bool FMatchNonScalarChildren(CGroupExpression* pgexpr) const;

	// hash function
	ULONG HashValue() const
	{
		return HashValue(m_pop.get(), m_pdrgpgroup);
	}

	// static hash function for operator and group references
	static ULONG HashValue(Operator* pop, duckdb::vector<CGroup*> drgpgroup);

	// static hash function for group expression
	static ULONG HashValue(const CGroupExpression &);

	// transform group expression
	void Transform(CXform* pxform, CXformResult* pxfres, ULONG* pulElapsedTime, ULONG* pulNumberOfBindings);

	// set group expression state
	void SetState(EState estNewState);

	// reset group expression state
	void ResetState();

	// check if group expression has been explored
	bool FExplored() const
	{
		return (estExplored <= m_estate);
	}

	// check if group expression has been implemented
	bool FImplemented() const
	{
		return (estImplemented == m_estate);
	}

	// check if transition to the given state is completed
	bool FTransitioned(EState estate) const;

	// lookup cost context in hash table
	CCostContext* PccLookup(COptimizationContext* poc, ULONG ulOptReq);

	// lookup all cost contexts matching given optimization context
	duckdb::vector<CCostContext*> PdrgpccLookupAll(COptimizationContext* poc);

	// insert a cost context in hash table
	CCostContext* PccInsert(CCostContext* pcc);

	// link for list in Group
	SLink m_linkGroup;

	// link for group expression hash table
	SLink m_linkMemo;

	// invalid group expression
	static const CGroupExpression m_gexprInvalid;

	virtual bool ContainsCircularDependencies();
};	// class CGroupExpression
}  // namespace gpopt
#endif