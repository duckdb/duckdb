//---------------------------------------------------------------------------
//	@filename:
//		CPropConstraint.cpp
//
//	@doc:
//		Implementation of constraint property
//---------------------------------------------------------------------------
#include "duckdb/optimizer/cascade/base/CPropConstraint.h"
#include "duckdb/optimizer/cascade/base.h"
#include "duckdb/optimizer/cascade/base/COptCtxt.h"
#include <algorithm>
#include "duckdb/planner/expression/bound_conjunction_expression.hpp"

using namespace gpopt;

//---------------------------------------------------------------------------
//	@function:
//		CPropConstraint::CPropConstraint
//
//	@doc:
//		Ctor
//
//---------------------------------------------------------------------------
CPropConstraint::CPropConstraint(duckdb::vector<duckdb::vector<ColumnBinding>> pdrgpcrs, Expression* pcnstr)
	: m_pdrgpcrs(pdrgpcrs), m_pcnstr(pcnstr)
{
	InitHashMap();
}

//---------------------------------------------------------------------------
//	@function:
//		CPropConstraint::~CPropConstraint
//
//	@doc:
//		Dtor
//
//---------------------------------------------------------------------------
CPropConstraint::~CPropConstraint()
{
}

//---------------------------------------------------------------------------
//	@function:
//		CPropConstraint::InitHashMap
//
//	@doc:
//		Initialize mapping between columns and equivalence classes
//
//---------------------------------------------------------------------------
void CPropConstraint::InitHashMap()
{
	const ULONG ulEquiv = m_pdrgpcrs.size();
	// m_phmcrcrs is only needed when storing equivalent columns
	for (ULONG ul = 0; ul < ulEquiv; ul++)
	{
		duckdb::vector<ColumnBinding> pcrs = m_pdrgpcrs[ul];
		auto it = pcrs.begin();
		while (it != pcrs.end())
		{
			m_phmcrcrs.insert(make_pair((*it).HashValue(), pcrs));
			++it;
		}
	}
}

//---------------------------------------------------------------------------
//	@function:
//		CPropConstraint::FContradiction
//
//	@doc:
//		Is this a contradiction
//
//---------------------------------------------------------------------------
BOOL CPropConstraint::FContradiction() const
{
	return (NULL != m_pcnstr);
}

/*
//---------------------------------------------------------------------------
//	@function:
//		CPropConstraint::PexprScalarMappedFromEquivCols
//
//	@doc:
//		Return scalar expression on the given column mapped from all constraints
//		on its equivalent columns
//
//---------------------------------------------------------------------------
Expression* CPropConstraint::PexprScalarMappedFromEquivCols(ColumnBinding colref, CPropConstraint* constraintsForOuterRefs) const
{
	if (NULL == m_pcnstr || m_phmcrcrs.size())
	{
		return NULL;
	}
	duckdb::vector<ColumnBinding> pcrs = (*(m_phmcrcrs.find(colref))).second;
	duckdb::vector<ColumnBinding> equivOuterRefs;
	if (NULL != constraintsForOuterRefs && 0 != constraintsForOuterRefs->m_phmcrcrs.size())
	{
		equivOuterRefs = (*(constraintsForOuterRefs->m_phmcrcrs.find(colref))).second;
	}
	if ((0 == pcrs.size() || 1 == pcrs.size()) && (0 == equivOuterRefs.size() || 1 == equivOuterRefs.size()))
	{
		// we have no columns that are equivalent to 'colref'
		return NULL;
	}
	// get constraints for all other columns in this equivalence class
	// except the current column
	duckdb::vector<ColumnBinding> pcrsEquiv;
	pcrsEquiv.insert(pcrsEquiv.end(), pcrs.begin(), pcrs.end());
	if (0 != equivOuterRefs.size())
	{
		pcrsEquiv.insert(pcrsEquiv.end(), equivOuterRefs.begin(), equivOuterRefs.end());
	}
	auto iter = std::find(pcrsEquiv.begin(), pcrsEquiv.end(), colref);
	pcrsEquiv.erase(iter);
	// local constraints on the equivalent column(s)
	Expression* pcnstr = m_pcnstr->Pcnstr(mp, pcrsEquiv);
	Expression* pcnstrFromOuterRefs = NULL;
	if (NULL != constraintsForOuterRefs && NULL != constraintsForOuterRefs->m_pcnstr)
	{
		// constraints that exist in the outer scope
		pcnstrFromOuterRefs = constraintsForOuterRefs->m_pcnstr->Pcnstr(mp, pcrsEquiv);
	}
	// combine local and outer ref constraints, if we have any, into pcnstr
	if (NULL == pcnstr && NULL == pcnstrFromOuterRefs)
	{
		// neither local nor outer ref constraints
		return NULL;
	}
	else if (NULL == pcnstr)
	{
		// only constraints from outer refs, move to pcnstr
		pcnstr = pcnstrFromOuterRefs;
		pcnstrFromOuterRefs = NULL;
	}
	else if (NULL != pcnstr && NULL != pcnstrFromOuterRefs)
	{
		// constraints from both local and outer refs, make a conjunction
		// and store it in pcnstr
		pcnstr = GPOS_NEW(mp) BoundConjunctionExpression(ExpressionType::CONJUNCTION_AND, make_uniq<Expression>(pcnstr), make_uniq<Expression>(pcnstrFromOuterRefs));
	}
	// Now, pcnstr contains constraints on columns that are equivalent
	// to 'colref'. These constraints may be local or in an outer scope.
	// Generate a copy of all these constraints for the current column.
	Expression *pcnstrCol = pcnstr->PcnstrRemapForColumn(mp, colref);
	Expression* pexprScalar = pcnstrCol->PexprScalar(mp);
	return pcnstr;
}
*/