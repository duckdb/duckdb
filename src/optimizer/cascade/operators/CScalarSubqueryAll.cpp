//---------------------------------------------------------------------------
//	@filename:
//		CScalarSubqueryAll.cpp
//
//	@doc:
//		Implementation of scalar subquery ALL operator
//---------------------------------------------------------------------------
#include "duckdb/optimizer/cascade/operators/CScalarSubqueryAll.h"
#include "duckdb/optimizer/cascade/base.h"
#include "duckdb/optimizer/cascade/base/CUtils.h"

using namespace gpopt;

//---------------------------------------------------------------------------
//	@function:
//		CScalarSubqueryAll::CScalarSubqueryAll
//
//	@doc:
//		Ctor
//
//---------------------------------------------------------------------------
CScalarSubqueryAll::CScalarSubqueryAll(CMemoryPool *mp, IMDId *scalar_op_mdid, const CWStringConst *pstrScalarOp, const CColRef *colref)
	: CScalarSubqueryQuantified(mp, scalar_op_mdid, pstrScalarOp, colref)
{
}

//---------------------------------------------------------------------------
//	@function:
//		CScalarSubqueryAll::PopCopyWithRemappedColumns
//
//	@doc:
//		Return a copy of the operator with remapped columns
//
//---------------------------------------------------------------------------
COperator* CScalarSubqueryAll::PopCopyWithRemappedColumns(CMemoryPool *mp, UlongToColRefMap *colref_mapping, BOOL must_exist)
{
	CColRef *colref = CUtils::PcrRemap(Pcr(), colref_mapping, must_exist);
	IMDId *scalar_op_mdid = MdIdOp();
	scalar_op_mdid->AddRef();
	CWStringConst *pstrScalarOp = GPOS_NEW(mp) CWStringConst(mp, PstrOp()->GetBuffer());
	return GPOS_NEW(mp) CScalarSubqueryAll(mp, scalar_op_mdid, pstrScalarOp, colref);
}