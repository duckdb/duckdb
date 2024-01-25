/*
* When dividing by a known compile time constant, the division can be replaced
* by a multiply+shift operation. GCC will do this automatically, 
* *BUT ONLY FOR DIVISION OF REGISTER-WIDTH OR NARROWER*.
*
* So on an 8-bit system, 16-bit divides will *NOT* be optimised.
*
* The templates here manually apply the multiply+shift operation for 16-bit numbers.
*/

#pragma once
#include "libdivide.h"
#include "u16_ldparams.h"
#include "s16_ldparams.h"

#ifdef __cplusplus
namespace libdivide {
  
  // Implementation details
  namespace detail {

    // Specialized templates containing precomputed libdivide constants
    // Primary template for pre-generated libdivide constants
    template<typename IntT, IntT divisor> struct libdivide_constants {};
    #include "u16_ldparams.hpp"
    #include "s16_ldparams.hpp"

    // Primary template - divide as normal. Performant for divisors that are a power of 2
    template <typename T, T divisor, bool is_power2>
    struct fast_divide_t { 
        static LIBDIVIDE_INLINE T divide(T n) { return n/divisor; }
    };

    // Divide by 1 - no-op
    template <bool is_power2>
    struct fast_divide_t<uint16_t, 1U, is_power2> { 
        static LIBDIVIDE_INLINE uint16_t divide(uint16_t n) { return n; } 
    };
    template <bool is_power2>
    struct fast_divide_t<int16_t, 1, is_power2> { 
        static LIBDIVIDE_INLINE int16_t divide(int16_t n) { return n; }
    };

    // Specialzed template for non-power of 2 uint16_t divisors
    template<uint16_t divisor>
    struct fast_divide_t<uint16_t, divisor, false> {
      static LIBDIVIDE_INLINE uint16_t divide(uint16_t n) { 
        return libdivide_u16_do_raw(n, libdivide_constants<uint16_t, divisor>::libdivide.magic, 
                                       libdivide_constants<uint16_t, divisor>::libdivide.more);
      }
    };

    // Specialzed template for non-power of 2 int16_t divisors
    template<int16_t divisor>
    struct fast_divide_t<int16_t, divisor, false> {
      static LIBDIVIDE_INLINE int16_t divide(int16_t n) { 
        return libdivide_s16_do_raw(n, libdivide_constants<int16_t, divisor>::libdivide.magic,
                                       libdivide_constants<int16_t, divisor>::libdivide.more); 
      }
    };

    // Power of 2 test
    template <typename T, T N>
    struct is_power_of_two {
        static constexpr bool val = N!=0 && (N & (N - 1))==0;
    };
  }

  // Public API. 
  template <typename T, T divisor>
  LIBDIVIDE_INLINE T fast_divide(T n) {
      return detail::fast_divide_t<T, divisor, detail::is_power_of_two<T, divisor>::val>::divide(n);
  }

}
#endif