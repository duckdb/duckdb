//---------------------------------------------------------------------------
//	@filename:
//		CScalarSubqueryQuantified.h
//
//	@doc:
//		Parent class for quantified subquery operators
//---------------------------------------------------------------------------
#ifndef GPOPT_CScalarSubqueryQuantified_H
#define GPOPT_CScalarSubqueryQuantified_H

#include "duckdb/optimizer/cascade/base.h"
#include "duckdb/optimizer/cascade/operators/CScalar.h"
#include "duckdb/optimizer/cascade/xforms/CSubqueryHandler.h"

namespace gpopt
{
using namespace gpos;

// fwd declarations
class CExpressionHandle;

//---------------------------------------------------------------------------
//	@class:
//		CScalarSubqueryQuantified
//
//	@doc:
//		Parent class for quantified subquery operators (ALL/ANY subqueries);
//		A quantified subquery expression has two children:
//		- Logical child: the inner logical expression
//		- Scalar child:	the scalar expression in the outer expression that
//		is used in quantified comparison;
//
//		Example: SELECT * from R where a+b = ANY (SELECT c from S);
//		- logical child: (SELECT c from S)
//		- scalar child : (a+b)
//
//---------------------------------------------------------------------------
class CScalarSubqueryQuantified : public CScalar
{
private:
	// id of comparison operator
	IMDId *m_scalar_op_mdid;

	// name of comparison operator
	const CWStringConst *m_pstrScalarOp;

	// column reference used in comparison
	const CColRef *m_pcr;

	// private copy ctor
	CScalarSubqueryQuantified(const CScalarSubqueryQuantified &);

protected:
	// ctor
	CScalarSubqueryQuantified(CMemoryPool *mp, IMDId *scalar_op_mdid,
							  const CWStringConst *pstrScalarOp,
							  const CColRef *colref);

	// dtor
	virtual ~CScalarSubqueryQuantified();

public:
	// operator mdid accessor
	IMDId *MdIdOp() const;

	// operator name accessor
	const CWStringConst *PstrOp() const;

	// column reference accessor
	const CColRef *
	Pcr() const
	{
		return m_pcr;
	}

	// return the type of the scalar expression
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

	// return locally used columns
	virtual CColRefSet *PcrsUsed(CMemoryPool *mp, CExpressionHandle &exprhdl);

	// derive partition consumer info
	virtual CPartInfo *PpartinfoDerive(CMemoryPool *mp,
									   CExpressionHandle &exprhdl) const;

	// conversion function
	static CScalarSubqueryQuantified *
	PopConvert(COperator *pop)
	{
		GPOS_ASSERT(NULL != pop);
		GPOS_ASSERT(EopScalarSubqueryAny == pop->Eopid() ||
					EopScalarSubqueryAll == pop->Eopid());

		return reinterpret_cast<CScalarSubqueryQuantified *>(pop);
	}

	// print
	virtual IOstream &OsPrint(IOstream &os) const;

};	// class CScalarSubqueryQuantified
}  // namespace gpopt

#endif