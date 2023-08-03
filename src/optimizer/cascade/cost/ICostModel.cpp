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
#include "duckdb/common/vector.hpp"

using namespace std;
using namespace duckdb;

// default number segments for the cost model
#define GPOPT_DEFAULT_SEGMENT_COUNT 2

namespace gpopt
{
//---------------------------------------------------------------------------
//	@function:
//		ICostModel::PcmDefault
//
//	@doc:
//		Create default cost model
//
//---------------------------------------------------------------------------
ICostModel* ICostModel::PcmDefault()
{
	return nullptr;
}

//---------------------------------------------------------------------------
//	@function:
//		ICostModel::SetParams
//
//	@doc:
//		Set cost model params
//
//---------------------------------------------------------------------------
void ICostModel::SetParams(duckdb::vector<ICostModelParams::SCostParam*> pdrgpcp)
{
	if (0 == pdrgpcp.size())
	{
		return;
	}
	// overwrite default values of cost model parameters
	const ULONG size = pdrgpcp.size();
	for (ULONG ul = 0; ul < size; ul++)
	{
		ICostModelParams::SCostParam* pcp = pdrgpcp[ul];
		GetCostModelParams()->SetParam(pcp->Id(), pcp->Get(), pcp->GetLowerBoundVal(), pcp->GetUpperBoundVal());
	}
}
}