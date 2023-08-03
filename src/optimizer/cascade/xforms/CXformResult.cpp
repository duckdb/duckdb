//---------------------------------------------------------------------------
//	@filename:
//		CXformResult.cpp
//
//	@doc:
//		Implementation of result container
//---------------------------------------------------------------------------
#include "duckdb/optimizer/cascade/xforms/CXformResult.h"
#include "duckdb/optimizer/cascade/base.h"

using namespace gpopt;

//---------------------------------------------------------------------------
//	@function:
//		CXformResult::CXformResult
//
//	@doc:
//		ctor
//
//---------------------------------------------------------------------------
CXformResult::CXformResult()
	: m_ulExpr(0)
{
}

//---------------------------------------------------------------------------
//	@function:
//		CXformResult::~CXformResult
//
//	@doc:
//		dtor
//
//---------------------------------------------------------------------------
CXformResult::~CXformResult()
{
}

//---------------------------------------------------------------------------
//	@function:
//		CXformResult::Add
//
//	@doc:
//		add alternative
//
//---------------------------------------------------------------------------
void CXformResult::Add(duckdb::unique_ptr<Operator> pexpr)
{
	m_pdrgpexpr.push_back(std::move(pexpr));
}

//---------------------------------------------------------------------------
//	@function:
//		CXformResult::PexprNext
//
//	@doc:
//		retrieve next alternative
//
//---------------------------------------------------------------------------
duckdb::unique_ptr<Operator> CXformResult::PexprNext()
{
	duckdb::unique_ptr<Operator> pexpr = nullptr;
	if (m_ulExpr < m_pdrgpexpr.size())
	{
		pexpr = std::move(m_pdrgpexpr[m_ulExpr]);
	}
	m_ulExpr++;
	return pexpr;
}