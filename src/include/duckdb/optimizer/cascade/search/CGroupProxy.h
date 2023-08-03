//---------------------------------------------------------------------------
//	@filename:
//		CGroupProxy.h
//
//	@doc:
//		Lock mechanism for access to a given group
//---------------------------------------------------------------------------
#ifndef GPOPT_CGroupProxy_H
#define GPOPT_CGroupProxy_H

#include "duckdb/optimizer/cascade/base.h"
#include "duckdb/optimizer/cascade/search/CGroup.h"

namespace gpopt
{
using namespace gpos;

// forward declarations
class CGroupExpression;
class CDrvdProp;
class COptimizationContext;

//---------------------------------------------------------------------------
//	@class:
//		CGroupProxy
//
//	@doc:
//		Exclusive access to a given group
//
//---------------------------------------------------------------------------
class CGroupProxy
{
public:
	// group we're operating on
	CGroup*  m_pgroup;

public:
	// ctor
	explicit CGroupProxy(CGroup* pgroup);

	// dtor
	~CGroupProxy();

public:
	// set group id
	void SetId(ULONG id)
	{
		m_pgroup->SetId(id);
	}

	// set group state
	void SetState(CGroup::EState estNewState)
	{
		m_pgroup->SetState(estNewState);
	}

	// skip group expressions starting from the given expression;
	list<CGroupExpression*>::iterator PgexprSkip(list<CGroupExpression*>::iterator pgexprStart, bool fSkipLogical);

	// insert group expression
	void Insert(CGroupExpression* pgexpr);

	// move duplicate group expression to duplicates list
	void MoveDuplicateGExpr(CGroupExpression* pgexpr);

	// initialize group's properties;
	void InitProperties(CDrvdProp* ppdp);

	// retrieve first group expression
	list<CGroupExpression*>::iterator PgexprFirst();

	// get the first non-logical group expression following the given expression
	list<CGroupExpression*>::iterator PgexprSkipLogical(list<CGroupExpression*>::iterator pgexpr);

	// get the next logical group expression following the given expression
	list<CGroupExpression*>::iterator PgexprNextLogical(list<CGroupExpression*>::iterator pgexpr);

	// lookup best expression under optimization context
	CGroupExpression* PgexprLookup(COptimizationContext* poc) const;
};	// class CGroupProxy
}  // namespace gpopt
#endif