//---------------------------------------------------------------------------
//	@filename:
//		CScalarIdent.h
//
//	@doc:
//		Scalar column identifier
//---------------------------------------------------------------------------
#ifndef GPOPT_CScalarIdent_H
#define GPOPT_CScalarIdent_H

#include "duckdb/optimizer/cascade/base.h"

#include "duckdb/optimizer/cascade/base/CDrvdProp.h"
#include "duckdb/optimizer/cascade/base/COptCtxt.h"
#include "duckdb/optimizer/cascade/operators/CScalar.h"

namespace gpopt
{
using namespace gpos;

//---------------------------------------------------------------------------
//	@class:
//		CScalarIdent
//
//	@doc:
//		scalar identifier operator
//
//---------------------------------------------------------------------------
class CScalarIdent : public CScalar
{
private:
	// column
	const CColRef *m_pcr;

	// private copy ctor
	CScalarIdent(const CScalarIdent &);


public:
	// ctor
	CScalarIdent(CMemoryPool *mp, const CColRef *colref)
		: CScalar(mp), m_pcr(colref)
	{
	}

	// dtor
	virtual ~CScalarIdent()
	{
	}

	// ident accessors
	virtual EOperatorId
	Eopid() const
	{
		return EopScalarIdent;
	}

	// return a string for operator name
	virtual const CHAR *
	SzId() const
	{
		return "CScalarIdent";
	}

	// accessor
	const CColRef *
	Pcr() const
	{
		return m_pcr;
	}

	// operator specific hash function
	ULONG HashValue() const;

	// match function
	BOOL Matches(COperator *pop) const;

	// sensitivity to order of inputs
	BOOL FInputOrderSensitive() const;

	// return a copy of the operator with remapped columns
	virtual COperator *PopCopyWithRemappedColumns(
		CMemoryPool *mp, UlongToColRefMap *colref_mapping, BOOL must_exist);


	// return locally used columns
	virtual CColRefSet *
	PcrsUsed(CMemoryPool *mp,
			 CExpressionHandle &  // exprhdl

	)
	{
		CColRefSet *pcrs = GPOS_NEW(mp) CColRefSet(mp);
		pcrs->Include(m_pcr);

		return pcrs;
	}

	// conversion function
	static CScalarIdent *
	PopConvert(COperator *pop)
	{
		GPOS_ASSERT(NULL != pop);
		GPOS_ASSERT(EopScalarIdent == pop->Eopid());

		return reinterpret_cast<CScalarIdent *>(pop);
	}

	// the type of the scalar expression
	virtual IMDId *MdidType() const;

	// the type modifier of the scalar expression
	virtual INT TypeModifier() const;

	// print
	virtual IOstream &OsPrint(IOstream &os) const;

	// is the given expression a scalar cast of a scalar identifier
	static BOOL FCastedScId(CExpression *pexpr);

	// is the given expression a scalar cast of given scalar identifier
	static BOOL FCastedScId(CExpression *pexpr, CColRef *colref);
};	// class CScalarIdent

}  // namespace gpopt

#endif