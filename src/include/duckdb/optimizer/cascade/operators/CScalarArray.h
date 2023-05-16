//---------------------------------------------------------------------------
//	@filename:
//		CScalarArray.h
//
//	@doc:
//		Class for scalar arrays
//---------------------------------------------------------------------------
#ifndef GPOPT_CScalarArray_H
#define GPOPT_CScalarArray_H

#include "duckdb/optimizer/cascade/base.h"

#include "duckdb/optimizer/cascade/operators/CScalar.h"
#include "duckdb/optimizer/cascade/operators/CScalarConst.h"
#include "duckdb/optimizer/cascade/md/IMDId.h"

namespace gpopt
{
using namespace gpos;
using namespace gpmd;

typedef CDynamicPtrArray<CScalarConst, CleanupRelease> CScalarConstArray;

//---------------------------------------------------------------------------
//	@class:
//		CScalarArray
//
//	@doc:
//		Scalar array
//
//---------------------------------------------------------------------------
class CScalarArray : public CScalar
{
private:
	// element type id
	IMDId *m_pmdidElem;

	// array type id
	IMDId *m_pmdidArray;

	// is array multidimensional
	BOOL m_fMultiDimensional;

	// const values
	CScalarConstArray *m_pdrgPconst;

	// private copy ctor
	CScalarArray(const CScalarArray &);

public:
	// ctor
	CScalarArray(CMemoryPool *mp, IMDId *elem_type_mdid, IMDId *array_type_mdid,
				 BOOL is_multidimenstional);

	// ctor
	CScalarArray(CMemoryPool *mp, IMDId *elem_type_mdid, IMDId *array_type_mdid,
				 BOOL is_multidimenstional, CScalarConstArray *pdrgPconst);

	// dtor
	virtual ~CScalarArray();

	// ident accessors
	virtual EOperatorId
	Eopid() const
	{
		return EopScalarArray;
	}

	// return a string for aggregate function
	virtual const CHAR *
	SzId() const
	{
		return "CScalarArray";
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
	static CScalarArray *
	PopConvert(COperator *pop)
	{
		GPOS_ASSERT(NULL != pop);
		GPOS_ASSERT(EopScalarArray == pop->Eopid());

		return reinterpret_cast<CScalarArray *>(pop);
	}

	// element type id
	IMDId *PmdidElem() const;

	// array type id
	IMDId *PmdidArray() const;

	// is array multi-dimensional
	BOOL FMultiDimensional() const;

	// type of expression's result
	virtual IMDId *MdidType() const;

	// CScalarConst array
	CScalarConstArray *PdrgPconst() const;

	// print
	IOstream &OsPrint(IOstream &os) const;

};	// class CScalarArray

}  // namespace gpopt

#endif