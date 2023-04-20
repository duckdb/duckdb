//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2012 EMC Corp.
//
//	@filename:
//		CMappingElementColIdParamId.cpp
//
//	@doc:
//		Implementation of the functions for the mapping element between ColId
//		and ParamId during DXL->PlStmt translation
//
//	@test:
//
//
//---------------------------------------------------------------------------

extern "C" {
#include "postgres.h"

#include "nodes/makefuncs.h"
#include "nodes/primnodes.h"
}

#include "gpopt/translate/CMappingElementColIdParamId.h"

using namespace gpdxl;
using namespace gpos;

//---------------------------------------------------------------------------
//	@function:
//		CMappingElementColIdParamId::CMappingElementColIdParamId
//
//	@doc:
//		Ctor
//
//---------------------------------------------------------------------------
CMappingElementColIdParamId::CMappingElementColIdParamId(ULONG colid,
														 ULONG paramid,
														 IMDId *mdid,
														 INT type_modifier)
	: m_colid(colid),
	  m_paramid(paramid),
	  m_mdid(mdid),
	  m_type_modifier(type_modifier)
{
}

// EOF
