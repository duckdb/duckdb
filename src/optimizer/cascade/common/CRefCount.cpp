//---------------------------------------------------------------------------
//	@filename:
//		CRefCount.cpp
//
//	@doc:
//		Implementation of class for ref-counted objects
//---------------------------------------------------------------------------
#include "duckdb/optimizer/cascade/common/CRefCount.h"
#include "duckdb/optimizer/cascade/error/CAutoTrace.h"
#include "duckdb/optimizer/cascade/task/CTask.h"

using namespace gpos;

#ifdef GPOS_DEBUG
// debug print for interactive debugging sessions only
void
CRefCount::DbgPrint() const
{
	CAutoTrace at(CTask::Self()->Pmp());

	OsPrint(at.Os());
}
#endif