//---------------------------------------------------------------------------
//	@filename:
//		CPartialPlan.h
//
//	@doc:
//
//		A partial plan is a group expression where none (or not all) of its
//		optimal child plans are discovered yet,
//		by assuming the smallest possible cost of unknown child plans, a partial
//		plan's cost gives a lower bound on the cost of the corresponding complete plan,
//		this information is used to prune the optimization search space during branch
//		and bound search
//---------------------------------------------------------------------------
#ifndef GPOPT_CPartialPlan_H
#define GPOPT_CPartialPlan_H

#include "duckdb/optimizer/cascade/base.h"
#include "duckdb/optimizer/cascade/base/CReqdProp.h"
#include "duckdb/optimizer/cascade/cost/ICostModel.h"
#include <memory>

namespace gpopt
{
using namespace gpos;

class CGroupExpression;
class CCostContext;

//---------------------------------------------------------------------------
//	@class:
//		CPartialPlan
//
//	@doc:
//		Description of partial plans created during optimization
//
//---------------------------------------------------------------------------
class CPartialPlan
{
public:
	// equality function
	bool operator==(const CPartialPlan &pppSnd) const;
	
public:
	// root group expression
	CGroupExpression* m_pgexpr;

	// required plan properties of root operator
	CReqdPropPlan* m_prpp;

	// cost context of known child plan -- can be null if no child plans are known
	CCostContext* m_pccChild;

	// index of known child plan
	ULONG m_ulChildIndex;

public:
	// ctor
	CPartialPlan(CGroupExpression* pgexpr, CReqdPropPlan* prpp, CCostContext* pccChild, ULONG child_index);
	
	// no copy ctor
	CPartialPlan(const CPartialPlan &) = delete;
	
	// dtor
	virtual ~CPartialPlan();

public:
	// extract costing info from children
	void ExtractChildrenCostingInfo(ICostModel* pcm, CExpressionHandle &exprhdl, ICostModel::SCostingInfo* pci);

	// compute partial plan cost
	double CostCompute();

	ULONG HashValue() const;

	// hash function used for cost bounding
	static ULONG HashValue(const CPartialPlan* ppp);

	// equality function used for for cost bounding
	static bool Equals(const CPartialPlan* pppFst, const CPartialPlan* pppSnd);
};	// class CPartialPlan
}  // namespace gpopt
#endif