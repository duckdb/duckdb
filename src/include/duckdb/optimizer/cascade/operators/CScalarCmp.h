//---------------------------------------------------------------------------
//	@filename:
//		CScalarCmp.h
//
//	@doc:
//		Base class for all ScalarCmp operators
//---------------------------------------------------------------------------
#ifndef GPOPT_CScalarCmp_H
#define GPOPT_CScalarCmp_H

#include "duckdb/optimizer/cascade/base.h"
#include "duckdb/optimizer/cascade/base/CDrvdProp.h"
#include "duckdb/optimizer/cascade/operators/CScalar.h"
#include "duckdb/optimizer/cascade/md/IMDId.h"
#include "duckdb/optimizer/cascade/md/IMDType.h"

namespace gpopt
{
using namespace gpos;
using namespace gpmd;

//---------------------------------------------------------------------------
//	@class:
//		CScalarCmp
//
//	@doc:
//		scalar comparison operator
//
//---------------------------------------------------------------------------
class CScalarCmp : public CScalar
{
private:
	// metadata id in the catalog
	IMDId *m_mdid_op;

	// comparison operator name
	const CWStringConst *m_pstrOp;

	// comparison type
	IMDType::ECmpType m_comparision_type;

	// does operator return NULL on NULL input?
	BOOL m_returns_null_on_null_input;

	// is comparison commutative
	BOOL m_fCommutative;

	// private copy ctor
	CScalarCmp(const CScalarCmp &);

public:
	// ctor
	CScalarCmp(CMemoryPool *mp, IMDId *mdid_op, const CWStringConst *pstrOp,
			   IMDType::ECmpType cmp_type);

	// dtor
	virtual ~CScalarCmp()
	{
		m_mdid_op->Release();
		GPOS_DELETE(m_pstrOp);
	}


	// ident accessors
	virtual EOperatorId
	Eopid() const
	{
		return EopScalarCmp;
	}

	// comparison type
	IMDType::ECmpType
	ParseCmpType() const
	{
		return m_comparision_type;
	}

	// return a string for operator name
	virtual const CHAR *
	SzId() const
	{
		return "CScalarCmp";
	}


	// operator specific hash function
	ULONG HashValue() const;

	// match function
	BOOL Matches(COperator *pop) const;

	// sensitivity to order of inputs
	BOOL FInputOrderSensitive() const;

	// return a copy of the operator with remapped columns
	virtual COperator *
	PopCopyWithRemappedColumns(CMemoryPool *,		//mp,
							   UlongToColRefMap *,	//colref_mapping,
							   BOOL					//must_exist
	)
	{
		return PopCopyDefault();
	}

	// is operator commutative
	BOOL FCommutative() const;

	// boolean expression evaluation
	virtual EBoolEvalResult Eber(ULongPtrArray *pdrgpulChildren) const;

	// name of the comparison operator
	const CWStringConst *Pstr() const;

	// metadata id
	IMDId *MdIdOp() const;

	// the type of the scalar expression
	virtual IMDId *MdidType() const;

	// print
	virtual IOstream &OsPrint(IOstream &os) const;

	// conversion function
	static CScalarCmp *
	PopConvert(COperator *pop)
	{
		GPOS_ASSERT(NULL != pop);
		GPOS_ASSERT(EopScalarCmp == pop->Eopid());

		return dynamic_cast<CScalarCmp *>(pop);
	}

	// get commuted scalar comparision operator
	virtual CScalarCmp *PopCommutedOp(CMemoryPool *mp, COperator *pop);

	// get the string representation of a metadata object
	static CWStringConst *Pstr(CMemoryPool *mp, CMDAccessor *md_accessor,
							   IMDId *mdid);

	// get metadata id of the commuted operator
	static IMDId *PmdidCommuteOp(CMDAccessor *md_accessor, COperator *pop);



};	// class CScalarCmp

}  // namespace gpopt

#endif
