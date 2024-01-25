# libdivide
[![Build Status](https://ci.appveyor.com/api/projects/status/github/ridiculousfish/libdivide?branch=master&svg=true)](https://ci.appveyor.com/project/kimwalisch/libdivide)
[![Github Releases](https://img.shields.io/github/release/ridiculousfish/libdivide.svg)](https://github.com/ridiculousfish/libdivide/releases)

```libdivide.h```  is a header-only C/C++ library for optimizing integer division.
Integer division is one of the slowest instructions on most CPUs e.g. on
current x64 CPUs a 64-bit integer division has a latency of up to 90 clock
cycles whereas a multiplication has a latency of only 3 clock cycles.
libdivide allows you to replace expensive integer divsion instructions by
a sequence of shift, add and multiply instructions that will calculate
the integer division much faster.

On current CPUs you can get a **speedup of up to 10x** for 64-bit integer division
and a speedup of up to to 5x for 32-bit integer division when using libdivide.
libdivide also supports [SSE2](https://en.wikipedia.org/wiki/SSE2),
[AVX2](https://en.wikipedia.org/wiki/Advanced_Vector_Extensions) and
[AVX512](https://en.wikipedia.org/wiki/Advanced_Vector_Extensions)
vector division which provides an even larger speedup. You can test how much
speedup you can achieve on your CPU using the [benchmark](#benchmark-program)
program.

libdivide is compatible with 8-bit microcontrollers, such as the AVR series: [the CI build includes a AtMega2560 target](test/avr/readme.md). Since low end hardware such as this often do not include a hardware divider, libdivide is particulary useful. In addition to the runtime [C](https://github.com/ridiculousfish/libdivide/blob/master/doc/C-API.md) & [C++](https://github.com/ridiculousfish/libdivide/blob/master/doc/CPP-API.md) APIs, a set of [predefined macros](constant_fast_div.h) and [templates](constant_fast_div.hpp) is included to speed up division by 16-bit constants: division by a 16-bit constant is [not optimized by avr-gcc on 8-bit systems](https://stackoverflow.com/questions/47994933/why-doesnt-gcc-or-clang-on-arm-use-division-by-invariant-integers-using-multip). 

See https://libdivide.com for more information on libdivide.

# C++ example

The first code snippet divides all integers in a vector using integer division.
This is slow as integer division is at least one order of magnitude slower than
any other integer arithmetic operation on current CPUs.

```C++
void divide(std::vector<int64_t>& vect, int64_t divisor)
{
    // Slow, uses integer division
    for (auto& n : vect)
        n /= divisor;
}
```

The second code snippet runs much faster, it uses libdivide to compute the
integer division using a sequence of shift, add and multiply instructions hence
avoiding the slow integer divison operation.

```C++
#include "libdivide.h"

void divide(std::vector<int64_t>& vect, int64_t divisor)
{
    libdivide::divider<int64_t> fast_d(divisor);

    // Fast, computes division using libdivide
    for (auto& n : vect)
        n /= fast_d;
}
```

Generally libdivide will give at significant speedup if:

* The divisor is only known at runtime
* The divisor is reused multiple times e.g. in a loop

# C example

You first need to generate a libdivide divider using one of the ```libdivide_*_gen``` functions (```*```:&nbsp;```s32```,&nbsp;```u32```,&nbsp;```s64```,&nbsp;```u64```)
which can then be used to compute the actual integer division using the
corresponding ```libdivide_*_do``` function.

```C
#include "libdivide.h"

void divide(int64_t *array, size_t size, int64_t divisor)
{
    struct libdivide_s64_t fast_d = libdivide_s64_gen(divisor);

    // Fast, computes division using libdivide
    for (size_t i = 0; i < size; i++)
        array[i] = libdivide_s64_do(array[i], &fast_d);
}
```

# API reference

* [C API](https://github.com/ridiculousfish/libdivide/blob/master/doc/C-API.md)
* [C++ API](https://github.com/ridiculousfish/libdivide/blob/master/doc/CPP-API.md)
* [Macro Invariant Division](constant_fast_div.h)
* [Template Based Invariant Division](constant_fast_div.hpp)

# Branchfull vs branchfree

The default libdivide divider makes use of
[branches](https://en.wikipedia.org/wiki/Branch_(computer_science)) to compute the integer
division. When the same divider is used inside a hot loop as in the C++ example section the
CPU will accurately predict the branches and there will be no performance slowdown. Often
the compiler is even able to move the branches outside the body of the loop hence
completely eliminating the branches, this is called loop-invariant code motion.

libdivide also has a branchfree divider type which computes the integer division without
using any branch instructions. The branchfree divider generally uses a few more instructions
than the default branchfull divider. The main use case for the branchfree divider is when
you have an array of different divisors and you need to iterate over the divisors. In this
case the default branchfull divider would exhibit poor performance as the CPU won't be
able to correctly predict the branches.

```C++
#include "libdivide.h"

// 64-bit branchfree divider type
using branchfree_t = libdivide::branchfree_divider<uint64_t>;

uint64_t divide(uint64_t x, std::vector<branchfree_t>& vect)
{
    uint64_t sum = 0;

    for (auto& fast_d : vect)
        sum += x / fast_d;

    return sum;
}
```

Caveats of branchfree divider:

* Unsigned branchfree divider cannot be ```1```
* Faster for unsigned types than for signed types

# Vector division

libdivide supports [SSE2](https://en.wikipedia.org/wiki/SSE2),
[AVX2](https://en.wikipedia.org/wiki/Advanced_Vector_Extensions) and
[AVX512](https://en.wikipedia.org/wiki/Advanced_Vector_Extensions)
vector division on x86 and x64 CPUs. In the example below we divide the packed 32-bit
integers inside an AVX512 vector using libdivide. libdivide supports 32-bit and 64-bit
vector division for both signed and unsigned integers.

```C++
#include "libdivide.h"

void divide(std::vector<__m512i>& vect, uint32_t divisor)
{
    libdivide::divider<uint32_t> fast_d(divisor);

    // AVX512 vector division
    for (auto& n : vect)
        n /= fast_d;
}
```

Note that you need to define one of macros below to enable vector division:

* ```LIBDIVIDE_SSE2```
* ```LIBDIVIDE_AVX2```
* ```LIBDIVIDE_AVX512```
* ```LIBDIVIDE_NEON```

# Performance tips

* If possible use unsigned integer types because libdivide's unsigned division is measurably
  faster than its signed division. This is especially true for the branchfree divider.
* Try both the default branchfull divider and the branchfree divider in your program and
  choose the one that performs best. The branchfree divider is more likely to get auto
  vectorized by the compiler (if you compile with e.g. ```-march=native```). But don't forget
  that the unsigned branchfree divider cannot be 1.
* Vector division is much faster for 32-bit than for 64-bit. This is because there are
  currently no vector multiplication instructions on x86 to efficiently calculate
  64-bit * 64-bit to 128-bit. 

# Build instructions

libdivide has one test program and two benchmark programs which can be built using cmake and
a recent C++ compiler that supports C++11 or later. Optionally ```libdivide.h``` can also be
installed to ```/usr/local/include```.

```bash
cmake .
make -j
sudo make install
```

# Tester program

You can pass the **tester** program one or more of the following arguments: ```u32```,
```s32```, ```u64```, ```s64``` to test the four cases (signed, unsigned, 32-bit, or 64-bit), or
run it with no arguments to test all four. The tester will verify the correctness of libdivide
via a set of randomly chosen numerators and denominators, by comparing the result of libdivide's
division to hardware division. It will stop with an error message as soon as it finds a
discrepancy.

# Benchmark program

You can pass the **benchmark** program one or more of the following arguments: ```u16```, ```s16```, ```u32```,
```s32```, ```u64```, ```s64``` to compare libdivide's speed against hardware division.
**benchmark** tests a simple function that inputs an array of random numerators and a single
divisor, and returns the sum of their quotients. It tests this using both hardware division, and
the various division approaches supported by libdivide, including vector division.

It will output data like this:

```bash
 #   system  scalar  scl_bf  vector  vec_bf   gener   algo
 1   9.684   0.792   0.783   0.426   0.426    1.346   0
 2   9.078   0.781   1.609   0.426   1.529    1.346   0
 3   9.078   1.355   1.609   1.334   1.531   29.045   1
 4   9.076   0.787   1.609   0.426   1.529    1.346   0
 5   9.074   1.349   1.609   1.334   1.531   29.045   1
 6   9.078   1.349   1.609   1.334   1.531   29.045   1
...
```

It will keep going as long as you let it, so it's best to stop it when you are happy with the
denominators tested. These columns have the following significance. All times are in
nanoseconds, lower is better.

```
     #:  The divisor that is tested
system:  Hardware divide time
scalar:  libdivide time, using scalar division
scl_bf:  libdivide time, using scalar branchfree division
vector:  libdivide time, using vector division
vec_bf:  libdivide time, using vector branchfree division
 gener:  Time taken to generate the divider struct
  algo:  The algorithm used.
```

The **benchmark** program will also verify that each function returns the same value,
so benchmark is valuable for its verification as well.

# Contributing

Although there are no individual unit tests, the supplied ```cmake``` builds do include several safety nets:

* They compile with:
  * All warnings on and;
  * Warnings as errors
* The CI build will build and run with sanitizers on ; these are available as part of the cmake build: ```-DCMAKE_BUILD_TYPE=Sanitize```
* The ```cmake``` and CI builds will compile and run both ```C``` and ```C++``` test programs.

Before sending in patches, build and run at least the ```tester``` and ```benchmark``` using the supplied ```cmake``` scripts on at least ```MSVC``` and ```GCC``` (or ```Clang```).

### Happy hacking!
