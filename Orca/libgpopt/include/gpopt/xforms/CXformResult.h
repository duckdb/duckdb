//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2009 Greenplum, Inc.
//
//	@filename:
//		CXformResult.h
//
//	@doc:
//		Result container for all transformations
//---------------------------------------------------------------------------
#ifndef GPOPT_CXformResult_H
#define GPOPT_CXformResult_H

#include "gpos/base.h"

#include "gpopt/operators/CExpression.h"

namespace gpopt
{
using namespace gpos;

//---------------------------------------------------------------------------
//	@class:
//		CXformResult
//
//	@doc:
//		result container
//
//---------------------------------------------------------------------------
class CXformResult : public CRefCount
{
private:
	// set of alternatives
	CExpressionArray *m_pdrgpexpr;

	// cursor for retrieval
	ULONG m_ulExpr;

	// private copy ctor
	CXformResult(const CXformResult &);

public:
	// ctor
	explicit CXformResult(CMemoryPool *);

	// dtor
	~CXformResult();

	// accessor
	inline CExpressionArray *
	Pdrgpexpr() const
	{
		return m_pdrgpexpr;
	}

	// add alternative
	void Add(CExpression *pexpr);

	// retrieve next alternative
	CExpression *PexprNext();

	// print function
	virtual IOstream &OsPrint(IOstream &os) const;

};	// class CXformResult

// shorthand for printing
inline IOstream &
operator<<(IOstream &os, CXformResult &xfres)
{
	return xfres.OsPrint(os);
}

}  // namespace gpopt


#endif	// !GPOPT_CXformResult_H

// EOF
