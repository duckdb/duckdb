//---------------------------------------------------------------------------
//	@filename:
//		CLogicalSelect.h
//
//	@doc:
//		Select operator
//---------------------------------------------------------------------------
#ifndef GPOS_CLogicalSelect_H
#define GPOS_CLogicalSelect_H

#include "duckdb/optimizer/cascade/base.h"
#include "duckdb/optimizer/cascade/operators/CExpressionHandle.h"
#include "duckdb/optimizer/cascade/operators/CLogicalUnary.h"

namespace gpopt
{
typedef CHashMap<CExpression, CExpression, CExpression::HashValue, CUtils::Equals, CleanupRelease<CExpression>, CleanupRelease<CExpression>> ExprPredToExprPredPartMap;

//---------------------------------------------------------------------------
//	@class:
//		CLogicalSelect
//
//	@doc:
//		Select operator
//
//---------------------------------------------------------------------------
class CLogicalSelect : public CLogicalUnary
{
private:
	// private copy ctor
	CLogicalSelect(const CLogicalSelect &);

	ExprPredToExprPredPartMap *m_phmPexprPartPred;

	// table descriptor
	CTableDescriptor *m_ptabdesc;

public:
	// ctor
	explicit CLogicalSelect(CMemoryPool *mp);

	// ctor
	CLogicalSelect(CMemoryPool *mp, CTableDescriptor *ptabdesc);

	// dtor
	virtual ~CLogicalSelect();

	// ident accessors
	virtual EOperatorId
	Eopid() const
	{
		return EopLogicalSelect;
	}

	virtual const CHAR *
	SzId() const
	{
		return "CLogicalSelect";
	}

	// return table's descriptor
	CTableDescriptor *
	Ptabdesc() const
	{
		return m_ptabdesc;
	}

	//-------------------------------------------------------------------------------------
	// Derived Relational Properties
	//-------------------------------------------------------------------------------------

	// derive output columns
	virtual CColRefSet *DeriveOutputColumns(CMemoryPool *, CExpressionHandle &);

	// dervive keys
	virtual CKeyCollection *DeriveKeyCollection(
		CMemoryPool *mp, CExpressionHandle &exprhdl) const;

	// derive max card
	virtual CMaxCard DeriveMaxCard(CMemoryPool *mp,
								   CExpressionHandle &exprhdl) const;

	// derive constraint property
	virtual CPropConstraint *
	DerivePropertyConstraint(CMemoryPool *mp, CExpressionHandle &exprhdl) const
	{
		return PpcDeriveConstraintFromPredicates(mp, exprhdl);
	}

	// derive table descriptor
	virtual CTableDescriptor *
	DeriveTableDescriptor(CMemoryPool *,  // mp
						  CExpressionHandle &exprhdl) const
	{
		return exprhdl.DeriveTableDescriptor(0);
	}

	// compute partition predicate to pass down to n-th child
	virtual CExpression *PexprPartPred(CMemoryPool *mp,
									   CExpressionHandle &exprhdl,
									   CExpression *pexprInput,
									   ULONG child_index) const;

	//-------------------------------------------------------------------------------------
	// Transformations
	//-------------------------------------------------------------------------------------

	// candidate set of xforms
	virtual CXformSet *PxfsCandidates(CMemoryPool *) const;

	//-------------------------------------------------------------------------------------
	//-------------------------------------------------------------------------------------
	//-------------------------------------------------------------------------------------

	// return true if operator can select a subset of input tuples based on some predicate,
	virtual BOOL
	FSelectionOp() const
	{
		return true;
	}

	// conversion function
	static CLogicalSelect *
	PopConvert(COperator *pop)
	{
		GPOS_ASSERT(NULL != pop);
		GPOS_ASSERT(EopLogicalSelect == pop->Eopid());

		return reinterpret_cast<CLogicalSelect *>(pop);
	}

	// derive statistics
	virtual IStatistics *PstatsDerive(CMemoryPool *mp,
									  CExpressionHandle &exprhdl,
									  IStatisticsArray *stats_ctxt) const;

};	// class CLogicalSelect

}  // namespace gpopt

#endif