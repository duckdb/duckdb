//---------------------------------------------------------------------------
//	@filename:
//		ICostModel.cpp
//
//	@doc:
//		Cost model implementation
//---------------------------------------------------------------------------
#include "duckdb/optimizer/cascade/base.h"
#include "duckdb/optimizer/cascade/cost/ICostModel.h"
#include "duckdb/optimizer/cascade/string/CWStringConst.h"

using namespace gpopt;

// default number segments for the cost model
#define GPOPT_DEFAULT_SEGMENT_COUNT 2

//---------------------------------------------------------------------------
//	@function:
//		ICostModel::PcmDefault
//
//	@doc:
//		Create default cost model
//
//---------------------------------------------------------------------------
ICostModel* ICostModel::PcmDefault(CMemoryPool *mp)
{
	return NULL;
}


//---------------------------------------------------------------------------
//	@function:
//		ICostModel::SetParams
//
//	@doc:
//		Set cost model params
//
//---------------------------------------------------------------------------
void ICostModel::SetParams(ICostModelParamsArray *pdrgpcp)
{
	if (NULL == pdrgpcp)
	{
		return;
	}

	// overwrite default values of cost model parameters
	const ULONG size = pdrgpcp->Size();
	for (ULONG ul = 0; ul < size; ul++)
	{
		ICostModelParams::SCostParam *pcp = (*pdrgpcp)[ul];
		GetCostModelParams()->SetParam(pcp->Id(), pcp->Get(), pcp->GetLowerBoundVal(), pcp->GetUpperBoundVal());
	}
}