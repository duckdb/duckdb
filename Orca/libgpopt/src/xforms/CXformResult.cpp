//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2009 Greenplum, Inc.
//
//	@filename:
//		CXformResult.cpp
//
//	@doc:
//		Implementation of result container
//---------------------------------------------------------------------------

#include "gpopt/xforms/CXformResult.h"

#include "gpos/base.h"

using namespace gpopt;


//---------------------------------------------------------------------------
//	@function:
//		CXformResult::CXformResult
//
//	@doc:
//		ctor
//
//---------------------------------------------------------------------------
CXformResult::CXformResult(CMemoryPool *mp) : m_ulExpr(0)
{
	GPOS_ASSERT(NULL != mp);
	m_pdrgpexpr = GPOS_NEW(mp) CExpressionArray(mp);
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
	// release array (releases all elements)
	m_pdrgpexpr->Release();
}


//---------------------------------------------------------------------------
//	@function:
//		CXformResult::Add
//
//	@doc:
//		add alternative
//
//---------------------------------------------------------------------------
void
CXformResult::Add(CExpression *pexpr)
{
	GPOS_ASSERT(0 == m_ulExpr &&
				"Incorrect workflow: cannot add further alternatives");

	GPOS_ASSERT(NULL != pexpr);
	m_pdrgpexpr->Append(pexpr);
}


//---------------------------------------------------------------------------
//	@function:
//		CXformResult::PexprNext
//
//	@doc:
//		retrieve next alternative
//
//---------------------------------------------------------------------------
CExpression *
CXformResult::PexprNext()
{
	CExpression *pexpr = NULL;
	if (m_ulExpr < m_pdrgpexpr->Size())
	{
		pexpr = (*m_pdrgpexpr)[m_ulExpr];
	}

	GPOS_ASSERT(m_ulExpr <= m_pdrgpexpr->Size());
	m_ulExpr++;

	return pexpr;
}


//---------------------------------------------------------------------------
//	@function:
//		CXformResult::OsPrint
//
//	@doc:
//		debug print
//
//---------------------------------------------------------------------------
IOstream &
CXformResult::OsPrint(IOstream &os) const
{
	os << "Alternatives:" << std::endl;

	for (ULONG i = 0; i < m_pdrgpexpr->Size(); i++)
	{
		os << i << ": " << std::endl;
		(*m_pdrgpexpr)[i]->OsPrint(os);
	}

	return os;
}

// EOF
