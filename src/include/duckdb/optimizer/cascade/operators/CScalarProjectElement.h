//---------------------------------------------------------------------------
//	@filename:
//		CScalarProjectElement.h
//
//	@doc:
//		Scalar project element operator
//---------------------------------------------------------------------------
#ifndef GPOPT_CScalarProjectElement_H
#define GPOPT_CScalarProjectElement_H

#include "duckdb/optimizer/cascade/base.h"

#include "duckdb/optimizer/cascade/base/CDrvdProp.h"
#include "duckdb/optimizer/cascade/base/COptCtxt.h"
#include "duckdb/optimizer/cascade/operators/CScalar.h"

namespace gpopt
{
using namespace gpos;

//---------------------------------------------------------------------------
//	@class:
//		CScalarProjectElement
//
//	@doc:
//		Scalar project element operator is used to define a column reference
//		as equivalent to a scalar expression
//
//---------------------------------------------------------------------------
class CScalarProjectElement : public CScalar
{
private:
	// defined column reference
	CColRef *m_pcr;

	// private copy ctor
	CScalarProjectElement(const CScalarProjectElement &);


public:
	// ctor
	CScalarProjectElement(CMemoryPool *mp, CColRef *colref)
		: CScalar(mp), m_pcr(colref)
	{
		GPOS_ASSERT(NULL != colref);
	}

	// dtor
	virtual ~CScalarProjectElement()
	{
	}

	// identity accessor
	virtual EOperatorId
	Eopid() const
	{
		return EopScalarProjectElement;
	}

	// return a string for operator name
	virtual const CHAR *
	SzId() const
	{
		return "CScalarProjectElement";
	}

	// defined column reference accessor
	CColRef *
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

	// return locally defined columns
	virtual CColRefSet *
	PcrsDefined(CMemoryPool *mp,
				CExpressionHandle &	 // exprhdl
	)
	{
		CColRefSet *pcrs = GPOS_NEW(mp) CColRefSet(mp);
		pcrs->Include(m_pcr);

		return pcrs;
	}

	// conversion function
	static CScalarProjectElement *
	PopConvert(COperator *pop)
	{
		GPOS_ASSERT(NULL != pop);
		GPOS_ASSERT(EopScalarProjectElement == pop->Eopid());

		return reinterpret_cast<CScalarProjectElement *>(pop);
	}

	virtual IMDId *
	MdidType() const
	{
		GPOS_ASSERT(!"Invalid function call: CScalarProjectElemet::MdidType()");
		return NULL;
	}

	// print
	virtual IOstream &OsPrint(IOstream &os) const;

};	// class CScalarProjectElement

}  // namespace gpopt

#endif
