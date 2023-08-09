//---------------------------------------------------------------------------
//	@filename:
//		CAutoOptCtxt.h
//
//	@doc:
//		Optimizer context object; contains all global objects pertaining to
//		one optimization
//---------------------------------------------------------------------------
#ifndef GPOPT_CAutoOptCtxt_H
#define GPOPT_CAutoOptCtxt_H

#include "duckdb/optimizer/cascade/base.h"
#include "duckdb/optimizer/cascade/base/COptCtxt.h"

namespace gpopt
{
using namespace gpos;

// forward declaration
class CCostParams;
class ICostModel;
class COptimizerConfig;
class IConstExprEvaluator;

//---------------------------------------------------------------------------
//	@class:
//		CAutoOptCtxt
//
//	@doc:
//		Auto optimizer context object creates and installs optimizer context
//		for unittesting
//
//---------------------------------------------------------------------------
class CAutoOptCtxt
{
public:
	// ctor
	CAutoOptCtxt(IConstExprEvaluator* pceeval, COptimizerConfig* optimizer_config);

	// no copy ctor
	CAutoOptCtxt(CAutoOptCtxt &) = delete;

	// ctor
	CAutoOptCtxt(IConstExprEvaluator* pceeval, ICostModel* pcm);

	// dtor
	~CAutoOptCtxt();
};	// class CAutoOptCtxt
}  // namespace gpopt
#endif