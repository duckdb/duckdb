//---------------------------------------------------------------------------
//	@filename:
//		CFunctionProp.cpp
//
//	@doc:
//		Implementation of function properties
//---------------------------------------------------------------------------
#include "duckdb/optimizer/cascade/base/CFunctionProp.h"
#include "duckdb/optimizer/cascade/base.h"

using namespace gpopt;

//---------------------------------------------------------------------------
//	@function:
//		CFunctionProp::CFunctionProp
//
//	@doc:
//		Ctor
//
//---------------------------------------------------------------------------
CFunctionProp::CFunctionProp(IMDFunction::EFuncStbl func_stability, IMDFunction::EFuncDataAcc func_data_access, BOOL fHasVolatileFunctionScan, BOOL fScan)
	: m_efs(func_stability), m_efda(func_data_access), m_fHasVolatileFunctionScan(fHasVolatileFunctionScan), m_fScan(fScan)
{
	GPOS_ASSERT(IMDFunction::EfsSentinel > func_stability);
	GPOS_ASSERT(IMDFunction::EfdaSentinel > func_data_access);
	GPOS_ASSERT_IMP(fScan && IMDFunction::EfsVolatile == func_stability, fHasVolatileFunctionScan);
}

//---------------------------------------------------------------------------
//	@function:
//		CFunctionProp::~CFunctionProp
//
//	@doc:
//		Dtor
//
//---------------------------------------------------------------------------
CFunctionProp::~CFunctionProp()
{
}

//---------------------------------------------------------------------------
//	@function:
//		CFunctionProp::SingletonExecution
//
//	@doc:
//		Check if must execute on a single host based on function properties
//
//---------------------------------------------------------------------------
BOOL CFunctionProp::NeedsSingletonExecution() const
{
	// a function needs to execute on a single host if any of the following holds:
	// a) it reads or modifies SQL data
	// b) it is volatile and used as a scan operator (i.e. in the from clause)
    return m_fScan && (IMDFunction::EfsVolatile == m_efs || IMDFunction::EfsStable == m_efs);
}

//---------------------------------------------------------------------------
//	@function:
//		CFunctionProp::OsPrint
//
//	@doc:
//		debug print
//
//---------------------------------------------------------------------------
IOstream& CFunctionProp::OsPrint(IOstream &os) const
{
	const CHAR *rgszStability[] = {"Immutable", "Stable", "Volatile"};
	const CHAR *rgszDataAccess[] = {"NoSQL", "ContainsSQL", "ReadsSQLData", "ModifiesSQLData"};
	os << rgszStability[m_efs] << ", " << rgszDataAccess[m_efda];
	return os;
}