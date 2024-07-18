#ifndef stdbool_h
#define stdbool_h

#include <wtypes.h>

/* MSVC doesn't define _Bool or bool in C, but does have BOOL */
/* Note this doesn't pass autoconf's test because (bool) 0.5 != true */
/* Clang-cl uses MSVC headers, so needs msvc_compat, but has _Bool as
 * a built-in type. */
#ifndef __clang__
typedef BOOL _Bool;
#endif

#define bool _Bool
#define true 1
#define false 0

#define __bool_true_false_are_defined 1

#endif /* stdbool_h */
