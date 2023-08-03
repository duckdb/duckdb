//---------------------------------------------------------------------------
//	@filename:
//		CXformResult.h
//
//	@doc:
//		Result container for all transformations
//---------------------------------------------------------------------------
#ifndef GPOPT_CXformResult_H
#define GPOPT_CXformResult_H

#include "duckdb/optimizer/cascade/base.h"
#include "duckdb/optimizer/cascade/operators/Operator.h"

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
class CXformResult
{
public:
	// set of alternatives
	duckdb::vector<duckdb::unique_ptr<Operator>> m_pdrgpexpr;

	// cursor for retrieval
	ULONG m_ulExpr;

public:
	// ctor
	explicit CXformResult();

	CXformResult(const CXformResult &) = delete;
	
	// dtor
	~CXformResult();

	// add alternative
	void Add(duckdb::unique_ptr<Operator> pexpr);

	// retrieve next alternative
	duckdb::unique_ptr<Operator> PexprNext();
};	// class CXformResult
}  // namespace gpopt

#endif