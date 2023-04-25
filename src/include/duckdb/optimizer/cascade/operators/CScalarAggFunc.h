//---------------------------------------------------------------------------
//	@filename:
//		CScalarAggFunc.h
//
//	@doc:
//		Class for scalar aggregate function calls
//---------------------------------------------------------------------------
#ifndef GPOPT_CScalarAggFunc_H
#define GPOPT_CScalarAggFunc_H

#include "duckdb/optimizer/cascade/base.h"

#include "duckdb/optimizer/cascade/base/CDrvdProp.h"
#include "duckdb/optimizer/cascade/operators/CScalar.h"
#include "duckdb/optimizer/cascade/md/IMDId.h"

namespace gpopt
{
using namespace gpos;
using namespace gpmd;

enum EAggfuncStage
{
	EaggfuncstageGlobal,
	EaggfuncstageIntermediate,	// Intermediate stage of a 3-stage aggregation
	EaggfuncstageLocal,	 // First (lower, earlier) stage of 2-stage aggregation
	EaggfuncstageSentinel
};

//---------------------------------------------------------------------------
//	@class:
//		CScalarAggFunc
//
//	@doc:
//		scalar aggregate function
//
//---------------------------------------------------------------------------
class CScalarAggFunc : public CScalar
{
private:
	// aggregate func id
	IMDId *m_pmdidAggFunc;

	// resolved return type refers to a non-ambiguous type that was resolved during query
	// parsing if the actual return type of Agg is ambiguous (e.g., AnyElement in GPDB)
	// if resolved return type is NULL, then we can get Agg return type by looking up MD cache
	// using Agg MDId
	IMDId *m_pmdidResolvedRetType;

	// return type obtained by looking up MD cache
	IMDId *m_return_type_mdid;

	// aggregate function name
	const CWStringConst *m_pstrAggFunc;

	// distinct aggregate computation
	BOOL m_is_distinct;

	// stage of the aggregate function
	EAggfuncStage m_eaggfuncstage;

	// is result of splitting aggregates
	BOOL m_fSplit;

	// private copy ctor
	CScalarAggFunc(const CScalarAggFunc &);

public:
	// ctor
	CScalarAggFunc(CMemoryPool *mp, IMDId *pmdidAggFunc,
				   IMDId *resolved_rettype, const CWStringConst *pstrAggFunc,
				   BOOL is_distinct, EAggfuncStage eaggfuncstage, BOOL fSplit);

	// dtor
	virtual ~CScalarAggFunc()
	{
		m_pmdidAggFunc->Release();
		CRefCount::SafeRelease(m_pmdidResolvedRetType);
		CRefCount::SafeRelease(m_return_type_mdid);
		GPOS_DELETE(m_pstrAggFunc);
	}


	// ident accessors
	virtual EOperatorId
	Eopid() const
	{
		return EopScalarAggFunc;
	}

	// return a string for aggregate function
	virtual const CHAR *
	SzId() const
	{
		return "CScalarAggFunc";
	}


	// operator specific hash function
	ULONG HashValue() const;

	// match function
	BOOL Matches(COperator *pop) const;

	// sensitivity to order of inputs
	BOOL
	FInputOrderSensitive() const
	{
		return true;
	}

	// return a copy of the operator with remapped columns
	virtual COperator *
	PopCopyWithRemappedColumns(CMemoryPool *,		//mp,
							   UlongToColRefMap *,	//colref_mapping,
							   BOOL					//must_exist
	)
	{
		return PopCopyDefault();
	}

	// conversion function
	static CScalarAggFunc *
	PopConvert(COperator *pop)
	{
		GPOS_ASSERT(NULL != pop);
		GPOS_ASSERT(EopScalarAggFunc == pop->Eopid());

		return reinterpret_cast<CScalarAggFunc *>(pop);
	}


	// aggregate function name
	const CWStringConst *PstrAggFunc() const;

	// aggregate func id
	IMDId *MDId() const;

	// ident accessors
	BOOL
	IsDistinct() const
	{
		return m_is_distinct;
	}

	void
	SetIsDistinct(BOOL val)
	{
		m_is_distinct = val;
	}

	// stage of the aggregate function
	EAggfuncStage
	Eaggfuncstage() const
	{
		return m_eaggfuncstage;
	}

	// global or local aggregate function
	BOOL
	FGlobal() const
	{
		return (EaggfuncstageGlobal == m_eaggfuncstage);
	}

	// is result of splitting aggregates
	BOOL
	FSplit() const
	{
		return m_fSplit;
	}

	// type of expression's result
	virtual IMDId *
	MdidType() const
	{
		if (NULL == m_pmdidResolvedRetType)
		{
			return m_return_type_mdid;
		}

		return m_pmdidResolvedRetType;
	}

	// is return type of Agg ambiguous?
	BOOL
	FHasAmbiguousReturnType() const
	{
		return (NULL != m_pmdidResolvedRetType);
	}

	// is function count(*)?
	BOOL FCountStar() const;

	// is function count(Any)?
	BOOL FCountAny() const;

	// is function either min() or max()?
	BOOL IsMinMax(const IMDType *mdtype) const;

	// print
	virtual IOstream &OsPrint(IOstream &os) const;

	// lookup mdid of return type for given Agg function
	static IMDId *PmdidLookupReturnType(IMDId *pmdidAggFunc, BOOL fGlobal,
										CMDAccessor *pmdaInput = NULL);

};	// class CScalarAggFunc

}  // namespace gpopt

#endif
