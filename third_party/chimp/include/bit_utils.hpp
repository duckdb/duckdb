#pragma once

namespace duckdb_chimp {

#include <climits>

template <typename R>
static constexpr R bitmask(unsigned int const bits)
{
	return (((uint64_t) (bits < (sizeof(R) * 8))) << (bits & ((sizeof(R) * 8) - 1))) - 1U;
    //if (param < sizeof(R) * CHAR_BIT)
    //    return (1UL << param) - 1;
    //else
    //    return -1;
	//return static_cast<R>(-(onecount != 0))
	//    & (static_cast<R>(-1) >> ((sizeof(R) * CHAR_BIT) - onecount));
}

} //namespace duckdb_chimp
