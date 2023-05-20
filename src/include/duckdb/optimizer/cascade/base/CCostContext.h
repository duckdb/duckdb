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
#include "duckdb/optimizer/cascade/common/CRefCount.h"
#include "duckdb/optimizer/cascade/base/COptimizationContext.h"
#include "duckdb/optimizer/cascade/cost/CCost.h"
#include "duckdb/optimizer/cascade/cost/ICostModel.h"

// infinite plan cost
#define GPOPT_INFINITE_COST CCost(1e+100)

// invalid cost value
#define GPOPT_INVALID_COST CCost(-0.5)

namespace gpopt
{
using namespace gpos;

// fwd declarations
class CDrvdPropPlan;
class CCostContext;

// array of cost contexts
typedef CDynamicPtrArray<CCostContext, CleanupRelease> CCostContextArray;

// cost context pointer definition
typedef CCostContext *COSTCTXT_PTR;

// cost context pointer definition
typedef const CCostContext *CONST_COSTCTXT_PTR;

//---------------------------------------------------------------------------
//	@class:
//		CCostContext
//
//	@doc:
//		Cost context
//
//---------------------------------------------------------------------------
class CCostContext : public CRefCount
{
public:
	// states of cost context
	enum EState
	{
		estUncosted,  // initial state

		estCosting,	 // ongoing costing
		estCosted,	 // done costing

		estSentinel
	};

private:
	// memory pool
	CMemoryPool *m_mp;

	// cost of group expression under optimization context
	CCost m_cost;

	// cost context state
	EState m_estate;

	// back pointer to owner group expression
	CGroupExpression *m_pgexpr;

	// group expression to be used stats derivation during costing
	CGroupExpression *m_pgexprForStats;

	// array of optimization contexts of child groups
	COptimizationContextArray *m_pdrgpoc;

	// derived properties of the carried plan
	CDrvdPropPlan *m_pdpplan;

	// optimization request number
	ULONG m_ulOptReq;

	// flag to indicate if cost context is pruned,
	// a cost context is pruned during branch-and-bound search if there exists
	// an equivalent context with better cost
	BOOL m_fPruned;

	// stats of owner group expression
	IStatistics *m_pstats;

	// derive stats of owner group expression
	void DeriveStats();

	// return the number of rows per host
	CDouble DRowsPerHost() const;

	// for two cost contexts with join plans of the same cost, break the tie based on join depth,
	// if tie-resolution succeeded, store a pointer to preferred cost context in output argument
	static void BreakCostTiesForJoinPlans(const CCostContext *pccFst,
										  const CCostContext *pccSnd,
										  CONST_COSTCTXT_PTR *ppccPrefered,
										  BOOL *pfTiesResolved);

	// private copy ctor
	CCostContext(const CCostContext &);

public:
	// main optimization context
	COptimizationContext *m_poc;

	// link for cost context hash table in CGroupExpression
	SLink m_link;

	// ctor
	CCostContext(CMemoryPool *mp, COptimizationContext *poc, ULONG ulOptReq,
				 CGroupExpression *pgexpr);

	// dtor
	virtual ~CCostContext();

	// main optimization context accessor
	COptimizationContext *
	Poc() const
	{
		return m_poc;
	}

	// accessor of optimization request number
	ULONG
	UlOptReq() const
	{
		return m_ulOptReq;
	}

	// is context pruned based on cost comparison?
	BOOL
	FPruned() const
	{
		return m_fPruned;
	}

	// set pruned flag
	void
	SetPruned()
	{
		GPOS_ASSERT(!m_fPruned);

		m_fPruned = true;
	}

	// accessor of child optimization contexts array
	COptimizationContextArray *
	Pdrgpoc() const
	{
		return m_pdrgpoc;
	}

	// cost accessor
	CCost
	Cost() const
	{
		return m_cost;
	}

	// state accessor
	EState
	Est() const
	{
		return m_estate;
	}

	// owner group expression accessor
	CGroupExpression *
	Pgexpr() const
	{
		return m_pgexpr;
	}

	// group expression for stats derivation
	CGroupExpression *
	PgexprForStats() const
	{
		return m_pgexprForStats;
	}

	// return stats of owner group expression
	IStatistics *
	Pstats() const
	{
		return m_pstats;
	}

	// check if we need to derive stats for this context
	BOOL FNeedsNewStats() const;

	// check if new stats were derived for this context
	BOOL FOwnsStats() const;

	// derived plan properties accessor
	CDrvdPropPlan *
	Pdpplan() const
	{
		return m_pdpplan;
	}

	// set cost value
	void
	SetCost(CCost cost)
	{
		GPOS_ASSERT(cost > GPOPT_INVALID_COST);
		m_cost = cost;
	}

	// derive properties of the plan carried by cost context
	void DerivePlanProps(CMemoryPool *mp);

	// set cost context state
	void
	SetState(EState estNewState)
	{
		GPOS_ASSERT(estNewState == (EState)(m_estate + 1));

		m_estate = estNewState;
	}

	// set child contexts
	void
	SetChildContexts(COptimizationContextArray *pdrgpoc)
	{
		GPOS_ASSERT(NULL == m_pdrgpoc);
		GPOS_ASSERT(NULL != pdrgpoc);

		m_pdrgpoc = pdrgpoc;
	}


	// check validity by comparing derived and required properties
	BOOL IsValid(CMemoryPool *mp);

	// comparison operator
	BOOL operator==(const CCostContext &cc) const;

	// compute cost
	CCost CostCompute(CMemoryPool *mp, CCostArray *pdrgpcostChildren);

	// is current context better than the given equivalent context based on cost?
	BOOL FBetterThan(const CCostContext *pcc) const;

	// is this cost context of a two stage scalar DQA created by CXformSplitDQA
	BOOL IsTwoStageScalarDQACostCtxt(const CCostContext *pcc) const;

	// is this cost context of a three stage scalar DQA created by CXformSplitDQA
	BOOL IsThreeStageScalarDQACostCtxt(const CCostContext *pcc) const;

	// equality function
	static BOOL
	Equals(const CCostContext &ccLeft, const CCostContext &ccRight)
	{
		// check if we are comparing against invalid context
		if (NULL == ccLeft.Poc() || NULL == ccRight.Poc())
		{
			return NULL == ccLeft.Poc() && NULL == ccRight.Poc();
		}

		return ccLeft.UlOptReq() == ccRight.UlOptReq() &&
			   ccLeft.Pgexpr() == ccRight.Pgexpr() &&
			   ccLeft.Poc()->Matches(ccRight.Poc());
	}

	// equality function
	static BOOL
	Equals(const CCostContext *pccLeft, const CCostContext *pccRight)
	{
		return Equals(*pccLeft, *pccRight);
	}

	// hash function
	static ULONG
	HashValue(const CCostContext &cc)
	{
		return COptimizationContext::HashValue(*(cc.Poc()));
	}

	// hash function
	static ULONG
	HashValue(const CCostContext *pcc)
	{
		return HashValue(*pcc);
	}

	// debug print
	virtual IOstream &OsPrint(IOstream &os) const;

};	// class CCostContext

}  // namespace gpopt

#endif
