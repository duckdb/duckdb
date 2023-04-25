//---------------------------------------------------------------------------
//	@filename:
//		CScalarSubquery.h
//
//	@doc:
//		Class for scalar subqueries
//---------------------------------------------------------------------------
#ifndef GPOPT_CScalarSubquery_H
#define GPOPT_CScalarSubquery_H

#include "duckdb/optimizer/cascade/base.h"

#include "duckdb/optimizer/cascade/operators/CScalar.h"

namespace gpopt
{
using namespace gpos;

//---------------------------------------------------------------------------
//	@class:
//		CScalarSubquery
//
//	@doc:
//		Scalar subquery
//
//---------------------------------------------------------------------------
class CScalarSubquery : public CScalar
{
private:
	// computed column reference
	const CColRef *m_pcr;

	// is subquery generated from existential subquery?
	BOOL m_fGeneratedByExist;

	// is subquery generated from quantified subquery?
	BOOL m_fGeneratedByQuantified;

	// private copy ctor
	CScalarSubquery(const CScalarSubquery &);

public:
	// ctor
	CScalarSubquery(CMemoryPool *mp, const CColRef *colref,
					BOOL fGeneratedByExist, BOOL fGeneratedByQuantified);

	// dtor
	virtual ~CScalarSubquery();

	// ident accessors
	virtual EOperatorId
	Eopid() const
	{
		return EopScalarSubquery;
	}

	// return a string for scalar subquery
	virtual const CHAR *
	SzId() const
	{
		return "CScalarSubquery";
	}

	// accessor to computed column reference
	const CColRef *
	Pcr() const
	{
		return m_pcr;
	}

	// the type of the scalar expression
	virtual IMDId *MdidType() const;

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
	virtual COperator *PopCopyWithRemappedColumns(
		CMemoryPool *mp, UlongToColRefMap *colref_mapping, BOOL must_exist);

	// return locally used columns
	virtual CColRefSet *PcrsUsed(CMemoryPool *mp, CExpressionHandle &exprhdl);

	// is subquery generated from existential subquery?
	BOOL
	FGeneratedByExist() const
	{
		return m_fGeneratedByExist;
	}

	// is subquery generated from quantified subquery?
	BOOL
	FGeneratedByQuantified() const
	{
		return m_fGeneratedByQuantified;
	}

	// derive partition consumer info
	virtual CPartInfo *PpartinfoDerive(CMemoryPool *mp,
									   CExpressionHandle &exprhdl) const;

	// conversion function
	static CScalarSubquery *
	PopConvert(COperator *pop)
	{
		GPOS_ASSERT(NULL != pop);
		GPOS_ASSERT(EopScalarSubquery == pop->Eopid());

		return reinterpret_cast<CScalarSubquery *>(pop);
	}

	// print
	virtual IOstream &OsPrint(IOstream &os) const;

};	// class CScalarSubquery
}  // namespace gpopt

#endif
