//---------------------------------------------------------------------------
//	@filename:
//		CGroupProxy.cpp
//
//	@doc:
//		Implementation of proxy object for group access
//---------------------------------------------------------------------------
#include "duckdb/optimizer/cascade/search/CGroupProxy.h"
#include "duckdb/optimizer/cascade/base.h"
#include "duckdb/optimizer/cascade/base/CDrvdPropRelational.h"
#include "duckdb/optimizer/cascade/base/COptimizationContext.h"
#include "duckdb/optimizer/cascade/search/CGroup.h"
#include "duckdb/optimizer/cascade/search/CGroupExpression.h"
#include "duckdb/optimizer/cascade/search/CJobGroup.h"

using namespace gpopt;

//---------------------------------------------------------------------------
//	@function:
//		CGroupProxy::CGroupProxy
//
//	@doc:
//		Ctor
//
//---------------------------------------------------------------------------
CGroupProxy::CGroupProxy(CGroup* pgroup)
	: m_pgroup(pgroup)
{
}

//---------------------------------------------------------------------------
//	@function:
//		CGroupProxy::~CGroupProxy
//
//	@doc:
//		Dtor
//
//---------------------------------------------------------------------------
CGroupProxy::~CGroupProxy()
{
}

//---------------------------------------------------------------------------
//	@function:
//		CGroupProxy::Insert
//
//	@doc:
//		Insert group expression into group
//
//---------------------------------------------------------------------------
void CGroupProxy::Insert(CGroupExpression* pgexpr)
{
	pgexpr->Init(m_pgroup, m_pgroup->m_ulGExprs++);
	m_pgroup->Insert(pgexpr);
}

//---------------------------------------------------------------------------
//	@function:
//		CGroupProxy::MoveDuplicateGExpr
//
//	@doc:
//		Move duplicate group expression to duplicates list
//
//---------------------------------------------------------------------------
void CGroupProxy::MoveDuplicateGExpr(CGroupExpression* pgexpr)
{
	m_pgroup->MoveDuplicateGExpr(pgexpr);
}

//---------------------------------------------------------------------------
//	@function:
//		CGroupProxy::InitProperties
//
//	@doc:
//		Initialize group's properties
//
//---------------------------------------------------------------------------
void CGroupProxy::InitProperties(CDrvdProp* pdp)
{
	m_pgroup->InitProperties(pdp);
}

//---------------------------------------------------------------------------
//	@function:
//		CGroupProxy::PgexprFirst
//
//	@doc:
//		Retrieve first group expression iterator;
//
//---------------------------------------------------------------------------
list<CGroupExpression*>::iterator CGroupProxy::PgexprFirst()
{
	return m_pgroup->PgexprFirst();
}

//---------------------------------------------------------------------------
//	@function:
//		CGroupProxy::PgexprSkip
//
//	@doc:
//		Skip group expressions starting from the given expression;
//		the type of group expressions to skip is determined by the passed
//		flag
//
//---------------------------------------------------------------------------
list<CGroupExpression*>::iterator CGroupProxy::PgexprSkip(list<CGroupExpression*>::iterator pgexprStart, bool fSkipLogical)
{
	auto iter = pgexprStart;
	while (m_pgroup->m_listGExprs.end() != iter && fSkipLogical == (*iter)->m_pop->FLogical())
	{
		++iter;
	}
	return iter;
}

//---------------------------------------------------------------------------
//	@function:
//		CGroupProxy::PgexprSkipLogical
//
//	@doc:
//		Retrieve the first non-logical group expression including the given
//		expression;
//
//---------------------------------------------------------------------------
list<CGroupExpression*>::iterator CGroupProxy::PgexprSkipLogical(list<CGroupExpression*>::iterator pgexpr)
{
	return PgexprSkip(pgexpr, true);
}

//---------------------------------------------------------------------------
//	@function:
//		CGroupProxy::PgexprNextLogical
//
//	@doc:
//		Find the first logical group expression including the given expression
//
//---------------------------------------------------------------------------
list<CGroupExpression*>::iterator CGroupProxy::PgexprNextLogical(list<CGroupExpression*>::iterator pgexpr)
{
	return PgexprSkip(pgexpr, false);
}