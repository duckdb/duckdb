//---------------------------------------------------------------------------
//	@filename:
//		CScalarWindowFunc.h
//
//	@doc:
//		Class for scalar window function
//
//	@owner:
//
//
//	@test:
//
//---------------------------------------------------------------------------
#ifndef GPOPT_CScalarWindowFunc_H
#define GPOPT_CScalarWindowFunc_H

#include "duckdb/optimizer/cascade/base.h"
#include "duckdb/optimizer/cascade/base/CDrvdProp.h"
#include "duckdb/optimizer/cascade/base/COptCtxt.h"
#include "duckdb/optimizer/cascade/operators/CScalarFunc.h"
#include "duckdb/optimizer/cascade/md/IMDId.h"

namespace gpopt
{
using namespace gpos;
using namespace gpmd;

//---------------------------------------------------------------------------
//	@class:
//		CScalarWindowFunc
//
//	@doc:
//		Class for scalar window function
//
//---------------------------------------------------------------------------
class CScalarWindowFunc : public CScalarFunc
{
public:
	// window stage
	enum EWinStage
	{
		EwsImmediate,
		EwsPreliminary,
		EwsRowKey,

		EwsSentinel
	};

private:
	// window stage
	EWinStage m_ewinstage;

	// distinct window computation
	BOOL m_is_distinct;

	/* TRUE if argument list was really '*' */
	BOOL m_is_star_arg;

	/* is function a simple aggregate? */
	BOOL m_is_simple_agg;

	// aggregate window function, e.g. count(*) over()
	BOOL m_fAgg;

	// private copy ctor
	CScalarWindowFunc(const CScalarWindowFunc &);

public:
	// ctor
	CScalarWindowFunc(CMemoryPool *mp, IMDId *mdid_func,
					  IMDId *mdid_return_type, const CWStringConst *pstrFunc,
					  EWinStage ewinstage, BOOL is_distinct, BOOL is_star_arg,
					  BOOL is_simple_agg);

	// dtor
	virtual ~CScalarWindowFunc()
	{
	}

	// ident accessors
	virtual EOperatorId
	Eopid() const
	{
		return EopScalarWindowFunc;
	}

	// return a string for window function
	virtual const CHAR *
	SzId() const
	{
		return "CScalarWindowFunc";
	}

	EWinStage
	Ews() const
	{
		return m_ewinstage;
	}

	// operator specific hash function
	ULONG HashValue() const;

	// match function
	BOOL Matches(COperator *pop) const;

	// conversion function
	static CScalarWindowFunc *
	PopConvert(COperator *pop)
	{
		GPOS_ASSERT(NULL != pop);
		GPOS_ASSERT(EopScalarWindowFunc == pop->Eopid());

		return reinterpret_cast<CScalarWindowFunc *>(pop);
	}

	// does window function definition include Distinct?
	BOOL
	IsDistinct() const
	{
		return m_is_distinct;
	}

	BOOL
	IsStarArg() const
	{
		return m_is_star_arg;
	}

	BOOL
	IsSimpleAgg() const
	{
		return m_is_simple_agg;
	}

	// is window function defined as Aggregate?
	BOOL
	FAgg() const
	{
		return m_fAgg;
	}

	// print
	virtual IOstream &OsPrint(IOstream &os) const;


};	// class CScalarWindowFunc

}  // namespace gpopt

#endif