//---------------------------------------------------------------------------
//	@filename:
//		CSearchStage.cpp
//
//	@doc:
//		Implementation of optimizer search stage
//---------------------------------------------------------------------------
#include "duckdb/optimizer/cascade/search/CSearchStage.h"
#include "duckdb/optimizer/cascade/xforms/CXformFactory.h"

using namespace gpopt;
using namespace gpos;

//---------------------------------------------------------------------------
//	@function:
//		CSearchStage::CSearchStage
//
//	@doc:
//		Ctor
//
//---------------------------------------------------------------------------
CSearchStage::CSearchStage(CXform_set * xform_set, ULONG ulTimeThreshold, double costThreshold)
	: m_xforms(xform_set), m_time_threshold(ulTimeThreshold), m_cost_threshold(costThreshold), m_costBest(-0.5)
{
	// include all implementation rules in any search strategy
	*m_xforms |= *(CXformFactory::XformFactory()->XformImplementation());
	m_pexprBest = nullptr;
}


//---------------------------------------------------------------------------
//	@function:
//		CSearchStage::~CSearchStage
//
//	@doc:
//		Dtor
//
//---------------------------------------------------------------------------
CSearchStage::~CSearchStage()
{
	delete m_xforms;
	// CRefCount::SafeRelease(m_pexprBest);
}

//---------------------------------------------------------------------------
//	@function:
//		CSearchStage::SetBestExpr
//
//	@doc:
//		Set best plan found at the end of search stage
//
//---------------------------------------------------------------------------
void CSearchStage::SetBestExpr(Operator* pexpr)
{
	m_pexprBest = pexpr->Copy();
	if (NULL != m_pexprBest)
	{
		m_costBest = m_pexprBest->m_cost;
	}
}

//---------------------------------------------------------------------------
//	@function:
//		CSearchStage::PdrgpssDefault
//
//	@doc:
//		Generate default search strategy;
//		one stage with all xforms and no time/cost thresholds
//
//---------------------------------------------------------------------------
duckdb::vector<CSearchStage*> CSearchStage::PdrgpssDefault()
{
	CXform_set * xform_set = new CXform_set();
	*xform_set |= *(CXformFactory::XformFactory()->XformExploration());
	duckdb::vector<CSearchStage*> search_stage_array;
	search_stage_array.push_back(new CSearchStage(xform_set));
	return search_stage_array;
}