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

namespace gpopt {
using namespace gpos;

//---------------------------------------------------------------------------
//	@class:
//		CXformResult
//
//	@doc:
//		result container
//
//---------------------------------------------------------------------------
class CXformResult {
public:
	// set of alternatives
	duckdb::vector<duckdb::unique_ptr<Operator>> m_alternative_expressions;

	// cursor for retrieval
	ULONG m_expression;

public:
	// ctor
	explicit CXformResult() : m_expression(0) {};

	CXformResult(const CXformResult &) = delete;

	// dtor
	~CXformResult() = default;

	// add alternative
	void Add(duckdb::unique_ptr<Operator> expression);

	// retrieve next alternative
	duckdb::unique_ptr<Operator> NextExpression();
}; // class CXformResult
} // namespace gpopt

#endif