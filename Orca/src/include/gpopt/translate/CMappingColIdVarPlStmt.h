//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2011 Greenplum, Inc.
//
//	@filename:
//		CMappingColIdVarPlStmt.h
//
//	@doc:
//		Class defining the functions that provide the mapping between Var, Param
//		and variables of Sub-query to CDXLNode during Query->DXL translation
//
//	@test:
//
//
//---------------------------------------------------------------------------
#ifndef GPDXL_CMappingColIdVarPlStmt_H
#define GPDXL_CMappingColIdVarPlStmt_H

#include "gpos/base.h"
#include "gpos/common/CDynamicPtrArray.h"
#include "gpos/common/CHashMap.h"

#include "gpopt/translate/CDXLTranslateContext.h"
#include "gpopt/translate/CMappingColIdVar.h"

//fwd decl
struct Var;
struct Plan;

namespace gpdxl
{
// fwd decl
class CDXLTranslateContextBaseTable;
class CContextDXLToPlStmt;

//---------------------------------------------------------------------------
//	@class:
//		CMappingColIdVarPlStmt
//
//	@doc:
//	Class defining functions that provide the mapping between Var, Param
//	and variables of Sub-query to CDXLNode during Query->DXL translation
//
//---------------------------------------------------------------------------
class CMappingColIdVarPlStmt : public CMappingColIdVar
{
private:
	const CDXLTranslateContextBaseTable *m_base_table_context;

	// the array of translator context (one for each child of the DXL operator)
	CDXLTranslationContextArray *m_child_contexts;

	CDXLTranslateContext *m_output_context;

	// translator context used to translate initplan and subplans associated
	// with a param node
	CContextDXLToPlStmt *m_dxl_to_plstmt_context;

public:
	CMappingColIdVarPlStmt(
		CMemoryPool *mp,
		const CDXLTranslateContextBaseTable *base_table_context,
		CDXLTranslationContextArray *child_contexts,
		CDXLTranslateContext *output_context,
		CContextDXLToPlStmt *dxl_to_plstmt_context);

	// translate DXL ScalarIdent node into GPDB Var node
	Var *VarFromDXLNodeScId(const CDXLScalarIdent *dxlop) override;

	// translate DXL ScalarIdent node into GPDB Param node
	Param *ParamFromDXLNodeScId(const CDXLScalarIdent *dxlop);

	// get the output translator context
	CDXLTranslateContext *GetOutputContext();

	// return the context of the DXL->PlStmt translation
	CContextDXLToPlStmt *GetDXLToPlStmtContext();
};
}  // namespace gpdxl

#endif	// GPDXL_CMappingColIdVarPlStmt_H

// EOF
