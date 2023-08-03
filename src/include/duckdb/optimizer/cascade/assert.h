//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2008 Greenplum, Inc.
//
//	@filename:
//		assert.h
//
//	@doc:
//		Macros for assertions in gpos
//
//		Use '&& "explanation"' in assert condition to provide additional
//		information about the failed condition.
//
//			GPOS_ASSERT(!FSpinlockHeld() && "Must not hold spinlock during allcoation");
//
//		There is no GPOS_ASSERT_STOP macro, instead use
//
//			GPOS_ASSERT(!"Should not get here because of xyz");
//
//		Avoid using GPOS_ASSERT(false);
//---------------------------------------------------------------------------
#ifndef assert_H
#define assert_H

// retail assert; available in all builds
#define GPOS_ASSERT(x) ;

// implication assert
#define GPOS_ASSERT_IMP(x, y) GPOS_ASSERT(!(x) || (y))

// if-and-only-if assert
#define GPOS_ASSERT_IFF(x, y) GPOS_ASSERT((!(x) || (y)) && (!(y) || (x)))

// compile assert
#define GPOS_CPL_ASSERT(x) extern int assert_array[(x) ? 1 : -1]

// debug assert, with message
#define GPOS_ASSERT_MSG(x, msg) GPOS_ASSERT((x) && (msg))

#endif