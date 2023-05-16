//---------------------------------------------------------------------------
//	@filename:
//		CDrvdPropCtxt.cpp
//
//	@doc:
//		Implementation of derived properties context
//---------------------------------------------------------------------------
#include "duckdb/optimizer/cascade/base/CDrvdPropCtxt.h"
#include "duckdb/optimizer/cascade/base.h"

#ifdef GPOS_DEBUG
#include "duckdb/optimizer/cascade/error/CAutoTrace.h"
#endif	// GPOS_DEBUG

namespace gpopt
{
IOstream& operator<<(IOstream &os, CDrvdPropCtxt &drvdpropctxt)
{
	return drvdpropctxt.OsPrint(os);
}

}  // namespace gpopt

// EOF
