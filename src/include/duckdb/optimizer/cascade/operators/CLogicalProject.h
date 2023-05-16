//---------------------------------------------------------------------------
//	@filename:
//		CLogicalProject.h
//
//	@doc:
//		Project operator
//---------------------------------------------------------------------------
#ifndef GPOS_CLogicalProject_H
#define GPOS_CLogicalProject_H

#include "duckdb/optimizer/cascade/base.h"
#include "duckdb/optimizer/cascade/operators/CExpressionHandle.h"
#include "duckdb/optimizer/cascade/operators/CLogicalUnary.h"

namespace gpopt
{
// fwd declaration
class CColRefSet;

//---------------------------------------------------------------------------
//	@class:
//		CLogicalProject
//
//	@doc:
//		Project operator
//
//---------------------------------------------------------------------------
class CLogicalProject : public CLogicalUnary
{
private:
	// private copy ctor
	CLogicalProject(const CLogicalProject &);

	// return equivalence class from scalar ident project element
	static CColRefSetArray *PdrgpcrsEquivClassFromScIdent(
		CMemoryPool *mp, CExpression *pexprPrEl, CColRefSet *not_null_columns);

	// extract constraint from scalar constant project element
	static void ExtractConstraintFromScConst(CMemoryPool *mp,
											 CExpression *pexprPrEl,
											 CConstraintArray *pdrgpcnstr,
											 CColRefSetArray *pdrgpcrs);

public:
	// ctor
	explicit CLogicalProject(CMemoryPool *mp);

	// dtor
	virtual ~CLogicalProject()
	{
	}

	// ident accessors
	virtual EOperatorId
	Eopid() const
	{
		return EopLogicalProject;
	}

	virtual const CHAR *
	SzId() const
	{
		return "CLogicalProject";
	}

	//-------------------------------------------------------------------------------------
	// Derived Relational Properties
	//-------------------------------------------------------------------------------------

	// derive output columns
	virtual CColRefSet *DeriveOutputColumns(CMemoryPool *mp,
											CExpressionHandle &exprhdl);

	// dervive keys
	virtual CKeyCollection *DeriveKeyCollection(
		CMemoryPool *mp, CExpressionHandle &exprhdl) const;

	// derive max card
	virtual CMaxCard DeriveMaxCard(CMemoryPool *mp,
								   CExpressionHandle &exprhdl) const;

	// derive constraint property
	virtual CPropConstraint *DerivePropertyConstraint(
		CMemoryPool *mp, CExpressionHandle &exprhdl) const;

	//-------------------------------------------------------------------------------------
	// Transformations
	//-------------------------------------------------------------------------------------

	// candidate set of xforms
	virtual CXformSet *PxfsCandidates(CMemoryPool *mp) const;

	// derive statistics
	virtual IStatistics *PstatsDerive(CMemoryPool *mp,
									  CExpressionHandle &exprhdl,
									  IStatisticsArray *stats_ctxt) const;

	//-------------------------------------------------------------------------------------
	//-------------------------------------------------------------------------------------
	//-------------------------------------------------------------------------------------

	// conversion function
	static CLogicalProject *
	PopConvert(COperator *pop)
	{
		GPOS_ASSERT(NULL != pop);
		GPOS_ASSERT(EopLogicalProject == pop->Eopid());

		return dynamic_cast<CLogicalProject *>(pop);
	}

};	// class CLogicalProject

}  // namespace gpopt

#endif