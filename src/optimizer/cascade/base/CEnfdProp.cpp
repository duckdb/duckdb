//---------------------------------------------------------------------------
//	@filename:
//		CEnfdProp.cpp
//
//	@doc:
//		Implementation of enforced property
//---------------------------------------------------------------------------
#include "duckdb/optimizer/cascade/base/CEnfdProp.h"
#include "duckdb/optimizer/cascade/base.h"

#ifdef GPOS_DEBUG
#include "duckdb/optimizer/cascade/error/CAutoTrace.h"
#include "duckdb/optimizer/cascade/base/COptCtxt.h"
#endif	// GPOS_DEBUG

namespace gpopt
{
IOstream& operator<<(IOstream &os, CEnfdProp &efdprop)
{
	return efdprop.OsPrint(os);
}

}  // namespace gpopt