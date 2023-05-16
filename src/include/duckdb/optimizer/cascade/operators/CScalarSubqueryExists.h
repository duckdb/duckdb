//---------------------------------------------------------------------------
//	@filename:
//		CScalarSubqueryExists.h
//
//	@doc:
//		Scalar subquery EXISTS operator
//---------------------------------------------------------------------------
#ifndef GPOPT_CScalarSubqueryExists_H
#define GPOPT_CScalarSubqueryExists_H

#include "duckdb/optimizer/cascade/base.h"
#include "duckdb/optimizer/cascade/operators/CScalarSubqueryExistential.h"

namespace gpopt
{
using namespace gpos;

//---------------------------------------------------------------------------
//	@class:
//		CScalarSubqueryExists
//
//	@doc:
//		Scalar subquery EXISTS.
//
//---------------------------------------------------------------------------
class CScalarSubqueryExists : public CScalarSubqueryExistential
{
private:
	// private copy ctor
	CScalarSubqueryExists(const CScalarSubqueryExists &);

public:
	// ctor
	CScalarSubqueryExists(CMemoryPool *mp) : CScalarSubqueryExistential(mp)
	{
	}

	// dtor
	virtual ~CScalarSubqueryExists()
	{
	}

	// ident accessors
	virtual EOperatorId Eopid() const
	{
		return EopScalarSubqueryExists;
	}

	// return a string for scalar subquery
	virtual const CHAR* SzId() const
	{
		return "CScalarSubqueryExists";
	}

	// conversion function
	static CScalarSubqueryExists* PopConvert(COperator *pop)
	{
		GPOS_ASSERT(NULL != pop);
		GPOS_ASSERT(EopScalarSubqueryExists == pop->Eopid());
		return reinterpret_cast<CScalarSubqueryExists *>(pop);
	}

};	// class CScalarSubqueryExists
}  // namespace gpopt

#endif