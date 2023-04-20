//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2011 Greenplum, Inc.
//
//	@filename:
//		CMappingColIdVarPlStmt.cpp
//
//	@doc:
//		Implentation of the functions that provide the mapping between Var, Param
//		and variables of Sub-query to CDXLNode during Query->DXL translation
//
//	@test:
//
//
//---------------------------------------------------------------------------

extern "C" {
#include "postgres.h"

#include "nodes/primnodes.h"
}

#include "gpos/base.h"
#include "gpos/common/CAutoP.h"

#include "gpopt/gpdbwrappers.h"
#include "gpopt/translate/CDXLTranslateContextBaseTable.h"
#include "gpopt/translate/CMappingColIdVarPlStmt.h"
#include "naucrates/dxl/operators/CDXLScalarIdent.h"
#include "naucrates/exception.h"
#include "naucrates/md/CMDIdGPDB.h"

using namespace gpdxl;
using namespace gpos;
using namespace gpmd;

//---------------------------------------------------------------------------
//	@function:
//		CMappingColIdVarPlStmt::CMappingColIdVarPlStmt
//
//	@doc:
//		Constructor
//
//---------------------------------------------------------------------------
CMappingColIdVarPlStmt::CMappingColIdVarPlStmt(
	CMemoryPool *mp, const CDXLTranslateContextBaseTable *base_table_context,
	CDXLTranslationContextArray *child_contexts,
	CDXLTranslateContext *output_context,
	CContextDXLToPlStmt *dxl_to_plstmt_context)
	: CMappingColIdVar(mp),
	  m_base_table_context(base_table_context),
	  m_child_contexts(child_contexts),
	  m_output_context(output_context),
	  m_dxl_to_plstmt_context(dxl_to_plstmt_context)
{
}

//---------------------------------------------------------------------------
//	@function:
//		CMappingColIdVarPlStmt::GetDXLToPlStmtContext
//
//	@doc:
//		Returns the DXL->PlStmt translation context
//
//---------------------------------------------------------------------------
CContextDXLToPlStmt *
CMappingColIdVarPlStmt::GetDXLToPlStmtContext()
{
	return m_dxl_to_plstmt_context;
}

//---------------------------------------------------------------------------
//	@function:
//		CMappingColIdVarPlStmt::GetOutputContext
//
//	@doc:
//		Returns the output translation context
//
//---------------------------------------------------------------------------
CDXLTranslateContext *
CMappingColIdVarPlStmt::GetOutputContext()
{
	return m_output_context;
}

//---------------------------------------------------------------------------
//	@function:
//		CMappingColIdVarPlStmt::ParamFromDXLNodeScId
//
//	@doc:
//		Translates a DXL scalar identifier operator into a GPDB Param node
//
//---------------------------------------------------------------------------
Param *
CMappingColIdVarPlStmt::ParamFromDXLNodeScId(const CDXLScalarIdent *dxlop)
{
	GPOS_ASSERT(NULL != m_output_context);

	Param *param = NULL;

	const ULONG colid = dxlop->GetDXLColRef()->Id();
	const CMappingElementColIdParamId *elem =
		m_output_context->GetParamIdMappingElement(colid);

	if (NULL != elem)
	{
		param = MakeNode(Param);
		param->paramkind = PARAM_EXEC;
		param->paramid = elem->ParamId();
		param->paramtype = CMDIdGPDB::CastMdid(elem->MdidType())->Oid();
		param->paramtypmod = elem->TypeModifier();
	}

	return param;
}

//---------------------------------------------------------------------------
//	@function:
//		CMappingColIdVarPlStmt::PvarFromDXLNodeScId
//
//	@doc:
//		Translates a DXL scalar identifier operator into a GPDB Var node
//
//---------------------------------------------------------------------------
Var *
CMappingColIdVarPlStmt::VarFromDXLNodeScId(const CDXLScalarIdent *dxlop)
{
	Index varno = 0;
	AttrNumber attno = 0;

	Index varno_old = 0;
	AttrNumber attno_old = 0;

	const ULONG colid = dxlop->GetDXLColRef()->Id();
	if (NULL != m_base_table_context)
	{
		// scalar id is used in a base table operator node
		varno = m_base_table_context->GetRelIndex();
		attno = (AttrNumber) m_base_table_context->GetAttnoForColId(colid);

		varno_old = varno;
		attno_old = attno;
	}

	// if lookup has failed in the first step, attempt lookup again using outer and inner contexts
	if (0 == attno && NULL != m_child_contexts)
	{
		GPOS_ASSERT(0 != m_child_contexts->Size());

		const CDXLTranslateContext *left_context = (*m_child_contexts)[0];

		// not a base table
		GPOS_ASSERT(NULL != left_context);

		// lookup column in the left child translation context
		const TargetEntry *target_entry = left_context->GetTargetEntry(colid);

		if (NULL != target_entry)
		{
			// identifier comes from left child
			varno = OUTER_VAR;
		}
		else
		{
			const ULONG num_contexts = m_child_contexts->Size();
			if (2 > num_contexts)
			{
				// there are no more children. col id not found in this tree
				// and must be an outer ref
				return NULL;
			}

			const CDXLTranslateContext *right_context = (*m_child_contexts)[1];

			// identifier must come from right child
			GPOS_ASSERT(NULL != right_context);

			target_entry = right_context->GetTargetEntry(colid);

			varno = INNER_VAR;

			// check any additional contexts if col is still not found yet
			for (ULONG ul = 2; NULL == target_entry && ul < num_contexts; ul++)
			{
				const CDXLTranslateContext *context = (*m_child_contexts)[ul];
				GPOS_ASSERT(NULL != context);

				target_entry = context->GetTargetEntry(colid);
				if (NULL == target_entry)
				{
					continue;
				}

				Var *var = (Var *) target_entry->expr;
				varno = var->varno;
			}
		}

		if (NULL == target_entry)
		{
			GPOS_RAISE(gpdxl::ExmaDXL, gpdxl::ExmiDXL2PlStmtAttributeNotFound,
					   colid);
		}

		attno = target_entry->resno;

		// find the original varno and attno for this column
		if (IsA(target_entry->expr, Var))
		{
			Var *var = (Var *) target_entry->expr;
			varno_old = var->varnoold;
			attno_old = var->varoattno;
		}
		else
		{
			varno_old = varno;
			attno_old = attno;
		}
	}

	Var *var = gpdb::MakeVar(varno, attno,
							 CMDIdGPDB::CastMdid(dxlop->MdidType())->Oid(),
							 dxlop->TypeModifier(),
							 0	// varlevelsup
	);

	// set varnoold and varoattno since makeVar does not set them properly
	var->varnoold = varno_old;
	var->varoattno = attno_old;

	return var;
}

// EOF
