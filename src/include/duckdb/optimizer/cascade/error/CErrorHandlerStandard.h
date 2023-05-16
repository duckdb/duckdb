//---------------------------------------------------------------------------
//	@filename:
//		CErrorHandlerStandard.h
//
//	@doc:
//		Standard error handler
//---------------------------------------------------------------------------
#ifndef GPOS_CErrorHandlerStandard_H
#define GPOS_CErrorHandlerStandard_H

#include "duckdb/optimizer/cascade/error/CErrorHandler.h"

namespace gpos
{
//---------------------------------------------------------------------------
//	@class:
//		CErrorHandlerStandard
//
//	@doc:
//		Default error handler;
//
//---------------------------------------------------------------------------
class CErrorHandlerStandard : public CErrorHandler
{
private:
	// private copy ctor
	CErrorHandlerStandard(const CErrorHandlerStandard &);

public:
	// ctor
	CErrorHandlerStandard()
	{
	}

	// dtor
	virtual ~CErrorHandlerStandard()
	{
	}

	// process error
	virtual void Process(CException exception);

};	// class CErrorHandlerStandard
}  // namespace gpos

#endif