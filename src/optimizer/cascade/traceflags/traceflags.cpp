//---------------------------------------------------------------------------
//	@filename:
//		traceflags.cpp
//
//	@doc:
//		Implementation of trace flags routines
//---------------------------------------------------------------------------
#include "duckdb/optimizer/cascade/traceflags/traceflags.h"
#include "duckdb/optimizer/cascade/base.h"
#include "duckdb/optimizer/cascade/common/CBitSetIter.h"
#include "duckdb/optimizer/cascade/task/CAutoTraceFlag.h"

using namespace gpos;

//---------------------------------------------------------------------------
//	@function:
//		SetTraceflags
//
//	@doc:
//		Set trace flags based on given bit set, and return two output bit
//		sets of old trace flags values
//
//---------------------------------------------------------------------------
void
SetTraceflags(
	CMemoryPool *mp,
	const CBitSet *pbsInput,  // set of trace flags to be enabled
	CBitSet *
		*ppbsEnabled,  // output: enabled trace flags before function is called
	CBitSet *
		*ppbsDisabled  // output: disabled trace flags before function is called
)
{
	if (NULL == pbsInput)
	{
		// bail out if input set is null
		return;
	}

	GPOS_ASSERT(NULL != ppbsEnabled);
	GPOS_ASSERT(NULL != ppbsDisabled);

	// suppress error simulation while setting trace flags
	CAutoTraceFlag atf1(EtraceSimulateAbort, false);
	CAutoTraceFlag atf2(EtraceSimulateOOM, false);
	CAutoTraceFlag atf3(EtraceSimulateNetError, false);
	CAutoTraceFlag atf4(EtraceSimulateIOError, false);

	*ppbsEnabled = GPOS_NEW(mp) CBitSet(mp, EopttraceSentinel);
	*ppbsDisabled = GPOS_NEW(mp) CBitSet(mp, EopttraceSentinel);
	CBitSetIter bsiter(*pbsInput);
	while (bsiter.Advance())
	{
		ULONG ulTraceFlag = bsiter.Bit();
		if (GPOS_FTRACE(ulTraceFlag))
		{
			// set trace flag in the enabled set
#ifdef GPOS_DEBUG
			BOOL fSet =
#endif	// GPOS_DEBUG
				(*ppbsEnabled)->ExchangeSet(ulTraceFlag);
			GPOS_ASSERT(!fSet);
		}
		else
		{
			// set trace flag in the disabled set
#ifdef GPOS_DEBUG
			BOOL fSet =
#endif	// GPOS_DEBUG
				(*ppbsDisabled)->ExchangeSet(ulTraceFlag);
			GPOS_ASSERT(!fSet);
		}

		// set trace flag
		GPOS_SET_TRACE(ulTraceFlag);
	}
}


//---------------------------------------------------------------------------
//	@function:
//		ResetTraceflags
//
//	@doc:
//		Reset trace flags based on values given by input sets
//
//---------------------------------------------------------------------------
void
ResetTraceflags(CBitSet *pbsEnabled, CBitSet *pbsDisabled)
{
	if (NULL == pbsEnabled || NULL == pbsDisabled)
	{
		// bail out if input sets are null
		return;
	}

	GPOS_ASSERT(NULL != pbsEnabled);
	GPOS_ASSERT(NULL != pbsDisabled);

	// suppress error simulation while resetting trace flags
	CAutoTraceFlag atf1(EtraceSimulateAbort, false);
	CAutoTraceFlag atf2(EtraceSimulateOOM, false);
	CAutoTraceFlag atf3(EtraceSimulateNetError, false);
	CAutoTraceFlag atf4(EtraceSimulateIOError, false);

	CBitSetIter bsiterEnabled(*pbsEnabled);
	while (bsiterEnabled.Advance())
	{
		ULONG ulTraceFlag = bsiterEnabled.Bit();
		GPOS_SET_TRACE(ulTraceFlag);
	}

	CBitSetIter bsiterDisabled(*pbsDisabled);
	while (bsiterDisabled.Advance())
	{
		ULONG ulTraceFlag = bsiterDisabled.Bit();
		GPOS_UNSET_TRACE(ulTraceFlag);
	}
}