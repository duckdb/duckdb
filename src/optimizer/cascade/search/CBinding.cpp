//---------------------------------------------------------------------------
//	@filename:
//		CBinding.cpp
//
//	@doc:
//		Implementation of Binding structure
//---------------------------------------------------------------------------
#include "duckdb/optimizer/cascade/search/CBinding.h"
#include "duckdb/optimizer/cascade/base.h"
#include "duckdb/optimizer/cascade/operators/CPattern.h"
#include "duckdb/optimizer/cascade/search/CGroupProxy.h"
#include "duckdb/optimizer/cascade/search/CMemo.h"
#include<algorithm>

using namespace std;

namespace gpopt
{
//---------------------------------------------------------------------------
//	@function:
//		CBinding::PgexprNext
//
//	@doc:
//		Move cursor within a group (initialize if NULL)
//
//---------------------------------------------------------------------------
list<CGroupExpression*>::iterator CBinding::PgexprNext(CGroup* pgroup, CGroupExpression* pgexpr) const
{
	CGroupProxy gp(pgroup);
	if (NULL == pgexpr)
	{
		return gp.PgexprFirst();
	}
	auto itr = std::find(gp.m_pgroup->m_listGExprs.begin(), gp.m_pgroup->m_listGExprs.end(), pgexpr);
	if (pgroup->m_fScalar)
	{
		return ++itr;
	}
	// for non-scalar group, we only consider logical expressions in bindings
	return gp.PgexprNextLogical(++itr);
}

//---------------------------------------------------------------------------
//	@function:
//		CBinding::PexprExpandPattern
//
//	@doc:
//		Pattern operators which match more than one operator need to be
//		passed around;
//		Given the pattern determine if we need to re-use the pattern operators;
//
//---------------------------------------------------------------------------
Operator* CBinding::PexprExpandPattern(Operator* pexprPattern, ULONG ulPos, ULONG arity)
{
	// re-use first child if it is a multi-leaf/tree
	if (0 < pexprPattern->Arity())
	{
		if (ulPos == arity - 1)
		{
			// special-case last child
			return pexprPattern->children[pexprPattern->Arity() - 1].get();
		}
		// otherwise re-use multi-leaf/tree child
		return pexprPattern->children[0].get();
	}
	return pexprPattern->children[ulPos].get();
}

//---------------------------------------------------------------------------
//	@function:
//		CBinding::PexprFinalize
//
//	@doc:
//		Assemble expression; substitute operator with pattern as necessary
//
//---------------------------------------------------------------------------
Operator* CBinding::PexprFinalize(CGroupExpression* pgexpr, duckdb::vector<duckdb::unique_ptr<Operator>> pdrgpexpr)
{
	pgexpr->m_pop->children.clear();
	for(auto &child : pdrgpexpr)
	{
		pgexpr->m_pop->AddChild(std::move(child));
	}
	pgexpr->m_pop->m_cost = GPOPT_INVALID_COST;
	return pgexpr->m_pop.get();
}

//---------------------------------------------------------------------------
//	@function:
//		CBinding::PexprExtract
//
//	@doc:
//		Extract a binding according to a given pattern;
//		Keep root node fixed;
//
//---------------------------------------------------------------------------
Operator* CBinding::PexprExtract(CGroupExpression* pgexpr, Operator* pexprPattern, Operator* pexprLast)
{
	if (!pexprPattern->FMatchPattern(pgexpr))
	{
		// shallow matching fails
		return NULL;
	}
	if (pexprPattern->FPattern() && ((CPattern*)pexprPattern)->FLeaf())
	{
		// return immediately; no deep extraction for leaf patterns
		return pgexpr->m_pop.get();
	}
	// for a scalar operator, there is always only one group expression in it's
	// group. scalar operators are required to derive the scalar properties only
	// and no xforms are applied to them (i.e no PxfsCandidates in scalar op)
	// specifically which will generate equivalent scalar operators in the same group.
	// so, if a scalar op been extracted once, there is no need to explore
	// all the child bindings, as the scalar properites will remain the same.
	if (NULL != pexprLast && pgexpr->m_pgroup->m_fScalar)
	{
		return NULL;
	}
	duckdb::vector<duckdb::unique_ptr<Operator>> pdrgpexpr;
	ULONG arity = pgexpr->Arity();
	if (0 == arity && NULL != pexprLast)
	{
		// no more bindings
		return NULL;
	}
	else
	{
		// attempt binding to children
		if (!FExtractChildren(pgexpr, pexprPattern, pexprLast, pdrgpexpr))
		{
			return NULL;
		}
	}
	Operator* pexpr = PexprFinalize(pgexpr, std::move(pdrgpexpr));
	return pexpr;
}

//---------------------------------------------------------------------------
//	@function:
//		CBinding::FInitChildCursors
//
//	@doc:
//		Initialize cursors of child expressions
//
//---------------------------------------------------------------------------
BOOL CBinding::FInitChildCursors(CGroupExpression* pgexpr, Operator* pexprPattern, duckdb::vector<duckdb::unique_ptr<Operator>> &pdrgpexpr)
{
	const ULONG arity = pgexpr->Arity();
	// grab first expression from each cursor
	for (ULONG ul = 0; ul < arity; ul++)
	{
		CGroup* pgroup = (*pgexpr)[ul];
		Operator* pexprPatternChild = PexprExpandPattern(pexprPattern, ul, arity);
		Operator* pexprNewChild = PexprExtract(pgroup, pexprPatternChild, NULL);
		if (NULL == pexprNewChild)
		{
			// failure means we have no more expressions
			return false;
		}
		pdrgpexpr.emplace_back(pexprNewChild->Copy());
	}
	return true;
}

//---------------------------------------------------------------------------
//	@function:
//		CBinding::FAdvanceChildCursors
//
//	@doc:
//		Advance cursors of child expressions and populate the given array
//		with the next child expressions
//
//---------------------------------------------------------------------------
bool CBinding::FAdvanceChildCursors(CGroupExpression* pgexpr, Operator* pexprPattern, Operator* pexprLast, duckdb::vector<duckdb::unique_ptr<Operator>> &pdrgpexpr)
{
	const ULONG arity = pgexpr->Arity();
	if (NULL == pexprLast)
	{
		// first call, initialize cursors
		return FInitChildCursors(pgexpr, pexprPattern, pdrgpexpr);
	}
	// could we advance a child's cursor?
	bool fCursorAdvanced = false;
	// number of exhausted cursors
	ULONG ulExhaustedCursors = 0;
	for (ULONG ul = 0; ul < arity; ul++)
	{
		CGroup* pgroup = (*pgexpr)[ul];
		Operator* pexprPatternChild = PexprExpandPattern(pexprPattern, ul, arity);
		Operator* pexprNewChild = NULL;
		if (fCursorAdvanced)
		{
			// re-use last extracted child expression
			pexprNewChild = pexprLast->children[ul].get();
		}
		else
		{
			Operator* pexprLastChild = pexprLast->children[ul].get();
			// advance current cursor
			pexprNewChild = PexprExtract(pgroup, pexprPatternChild, pexprLastChild);
			if (NULL == pexprNewChild)
			{
				// cursor is exhausted, we need to reset it
				pexprNewChild = PexprExtract(pgroup, pexprPatternChild, NULL);
				ulExhaustedCursors++;
			}
			else
			{
				// advancing current cursor has succeeded
				fCursorAdvanced = true;
			}
		}
		pdrgpexpr.emplace_back(pexprNewChild->Copy());
	}
	return ulExhaustedCursors < arity;
}

//---------------------------------------------------------------------------
//	@function:
//		CBinding::FExtractChildren
//
//	@doc:
//		For a given root, extract children into a dynamic array;
//		Allocates the array for the children as needed;
//
//---------------------------------------------------------------------------
bool CBinding::FExtractChildren(CGroupExpression* pgexpr, Operator* pexprPattern, Operator* pexprLast, duckdb::vector<duckdb::unique_ptr<Operator>> &pdrgpexpr)
{
	ULONG arity = pgexpr->Arity();
	if (arity < pexprPattern->Arity())
	{
		// does not have enough children
		return false;
	}
	if (0 == arity)
	{
		return true;
	}
	return FAdvanceChildCursors(pgexpr, pexprPattern, pexprLast, pdrgpexpr);
}

//---------------------------------------------------------------------------
//	@function:
//		CBinding::PexprExtract
//
//	@doc:
//		Extract a binding according to a given pattern;
//		If no appropriate child pattern can be matched advance the root node
//		until group is exhausted;
//
//---------------------------------------------------------------------------
Operator* CBinding::PexprExtract(CGroup* pgroup, Operator* pexprPattern, Operator* pexprLast)
{
	CGroupExpression* pgexpr = NULL;
	list<CGroupExpression*>::iterator itr;
	if (NULL != pexprLast)
	{
		itr = find(pgroup->m_listGExprs.begin(), pgroup->m_listGExprs.end(), pexprLast->m_group_expression);
		pgexpr = *itr;
	}
	else
	{
		// init cursor
		itr = PgexprNext(pgroup, nullptr);
		pgexpr = *itr;
	}
	if (pexprPattern->FPattern() && ((CPattern*)pexprPattern)->FLeaf())
	{
		// for leaf patterns, we do not iterate on group expressions
		if (nullptr != pexprLast)
		{
			// if a leaf was extracted before, then group is exhausted
			return nullptr;
		}
		return PexprExtract(pgexpr, pexprPattern, pexprLast);
	}
	// start position for next binding
	Operator* pexprStart = pexprLast;
	do
	{
		if (pexprPattern->FMatchPattern(pgexpr))
		{
			Operator* pexprResult = PexprExtract(pgexpr, pexprPattern, pexprStart);
			if (NULL != pexprResult)
			{
				return pexprResult;
			}
		}
		// move cursor and reset start position
		itr = PgexprNext(pgroup, pgexpr);
		pgexpr = *itr;
		pexprStart = nullptr;
	} while (pgroup->m_listGExprs.end() != itr);
	// group exhausted
	return NULL;
}
}