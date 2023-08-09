//---------------------------------------------------------------------------
//	@filename:
//		CCostContext.h
//
//	@doc:
//		Cost context object stores the cost of a group expression under
//		a given optimization context
//---------------------------------------------------------------------------
#ifndef GPOPT_CCostContext_H
#define GPOPT_CCostContext_H

#include "duckdb/optimizer/cascade/base.h"
#include "duckdb/optimizer/cascade/base/COptimizationContext.h"
#include "duckdb/optimizer/cascade/cost/ICostModel.h"
#include "duckdb/common/vector.hpp"

using namespace std;
using namespace duckdb;

namespace gpopt
{
// fwd declarations
class CDrvdPropPlan;
class CCostContext;

// cost context pointer definition
typedef CCostContext* COSTCTXT_PTR;

// cost context pointer definition
typedef const CCostContext* CONST_COSTCTXT_PTR;

//---------------------------------------------------------------------------
//	@class:
//		CCostContext
//
//	@doc:
//		Cost context
//
//---------------------------------------------------------------------------
class CCostContext
{
public:
	// states of cost context
	enum EState
	{
		estUncosted, estCosting, estCosted, estSentinel
	};

public:
	struct CCostContextHash
	{
		size_t operator()(const CCostContext &cc) const
		{
			return COptimizationContext::HashValue(*(cc.m_poc));
		}
	};

	struct CCostContextPTRHash
	{
		size_t operator()(const CCostContext* cc) const
		{
			return COptimizationContext::HashValue(*(cc->m_poc));
		}
	};

public:
	// cost of group expression under optimization context
	double m_cost;

	// cost context state
	EState m_estate;

	// back pointer to owner group expression
	CGroupExpression* m_pgexpr;

	// group expression to be used stats derivation during costing
	CGroupExpression* m_pgexprForStats;

	// array of optimization contexts of child groups
	duckdb::vector<COptimizationContext*> m_pdrgpoc;

	// derived properties of the carried plan
	CDrvdPropPlan* m_pdpplan;

	// optimization request number
	ULONG m_ulOptReq;

	// flag to indicate if cost context is pruned,
	// a cost context is pruned during branch-and-bound search if there exists
	// an equivalent context with better cost
	bool m_fPruned;

	// main optimization context
	COptimizationContext* m_poc;

	// link for cost context hash table in CGroupExpression
	SLink m_link;

public:
	// ctor
	CCostContext(COptimizationContext* poc, ULONG ulOptReq, CGroupExpression* pgexpr);

	// private copy ctor
	CCostContext(const CCostContext &) =  delete;

	// dtor
	virtual ~CCostContext();

public:
	// for two cost contexts with join plans of the same cost, break the tie based on join depth,
	// if tie-resolution succeeded, store a pointer to preferred cost context in output argument
	static void BreakCostTiesForJoinPlans(CCostContext* pccFst, CCostContext* pccSnd, CCostContext** ppccPrefered, bool* pfTiesResolved);

	// set pruned flag
	void SetPruned()
	{
		m_fPruned = true;
	}

	// check if we need to derive stats for this context
	bool FNeedsNewStats() const;

	// set cost value
	void SetCost(double cost)
	{
		m_cost = cost;
	}

	// derive properties of the plan carried by cost context
	void DerivePlanProps();

	// set cost context state
	void SetState(EState estNewState)
	{
		m_estate = estNewState;
	}

	// set child contexts
	void SetChildContexts(duckdb::vector<COptimizationContext*> pdrgpoc)
	{
		for(auto &child : pdrgpoc)
		{
			m_pdrgpoc.push_back(child);
		}
	}

	// check validity by comparing derived and required properties
	bool IsValid();

	// comparison operator
	bool operator==(const CCostContext &cc) const;

	// compute cost
	double CostCompute(duckdb::vector<double> pdrgpcostChildren);

	// is current context better than the given equivalent context based on cost?
	bool FBetterThan(CCostContext* pcc) const;

	// equality function
	static bool Equals(const CCostContext &ccLeft, const CCostContext &ccRight)
	{
		// check if we are comparing against invalid context
		if (NULL == ccLeft.m_poc || NULL == ccRight.m_poc)
		{
			return NULL == ccLeft.m_poc && NULL == ccRight.m_poc;
		}
		return ccLeft.m_ulOptReq == ccRight.m_ulOptReq && ccLeft.m_pgexpr == ccRight.m_pgexpr && ccLeft.m_poc->Matches(ccRight.m_poc);
	}

	// equality function
	static bool Equals(const CCostContext* pccLeft, const CCostContext* pccRight)
	{
		return Equals(*pccLeft, *pccRight);
	}

	// hash function
	ULONG HashValue()
	{
		return m_poc->HashValue();
	}

	// hash function
	static ULONG HashValue(const CCostContext &cc)
	{
		return COptimizationContext::HashValue(*(cc.m_poc));
	}

	// hash function
	static ULONG HashValue(const CCostContext* pcc)
	{
		return HashValue(*pcc);
	}
};	// class CCostContext
}  // namespace gpopt
#endif