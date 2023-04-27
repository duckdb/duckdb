//---------------------------------------------------------------------------
//	@filename:
//		IComparator.h
//
//	@doc:
//		Interface for comparing IDatum instances.
//
//	@owner:
//
//
//	@test:
//
//---------------------------------------------------------------------------

#ifndef GPOPT_IComparator_H
#define GPOPT_IComparator_H

#include "duckdb/optimizer/cascade/base.h"

namespace gpnaucrates
{
// fwd declarations
class IDatum;
}  // namespace gpnaucrates

namespace gpopt
{
using gpnaucrates::IDatum;

//---------------------------------------------------------------------------
//	@class:
//		IComparator
//
//	@doc:
//		Interface for comparing IDatum instances.
//
//---------------------------------------------------------------------------
class IComparator
{
public:
	virtual ~IComparator()
	{
	}

	// tests if the two arguments are equal
	virtual gpos::BOOL Equals(const IDatum *datum1,
							  const IDatum *datum2) const = 0;

	// tests if the first argument is less than the second
	virtual gpos::BOOL IsLessThan(const IDatum *datum1,
								  const IDatum *datum2) const = 0;

	// tests if the first argument is less or equal to the second
	virtual gpos::BOOL IsLessThanOrEqual(const IDatum *datum1,
										 const IDatum *datum2) const = 0;

	// tests if the first argument is greater than the second
	virtual gpos::BOOL IsGreaterThan(const IDatum *datum1,
									 const IDatum *datum2) const = 0;

	// tests if the first argument is greater or equal to the second
	virtual gpos::BOOL IsGreaterThanOrEqual(const IDatum *datum1,
											const IDatum *datum2) const = 0;
};
}  // namespace gpopt

#endif
