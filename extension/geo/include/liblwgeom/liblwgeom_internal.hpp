#pragma one

#include "liblwgeom/liblwgeom.hpp"

#include <float.h>
#include <math.h>

namespace duckdb {
/**
 * Macro that returns:
 * -1 if n < 0,
 *  1 if n > 0,
 *  0 if n == 0
 */
#define SIGNUM(n) (((n) > 0) - ((n) < 0))

/**
 * Floating point comparators.
 */
#define FP_TOLERANCE                1e-12
#define FP_IS_ZERO(A)               (fabs(A) <= FP_TOLERANCE)
#define FP_MAX(A, B)                (((A) > (B)) ? (A) : (B))
#define FP_MIN(A, B)                (((A) < (B)) ? (A) : (B))
#define FP_ABS(a)                   ((a) < (0) ? -(a) : (a))
#define FP_EQUALS(A, B)             (fabs((A) - (B)) <= FP_TOLERANCE)
#define FP_NEQUALS(A, B)            (fabs((A) - (B)) > FP_TOLERANCE)
#define FP_LT(A, B)                 (((A) + FP_TOLERANCE) < (B))
#define FP_LTEQ(A, B)               (((A)-FP_TOLERANCE) <= (B))
#define FP_GT(A, B)                 (((A)-FP_TOLERANCE) > (B))
#define FP_GTEQ(A, B)               (((A) + FP_TOLERANCE) >= (B))
#define FP_CONTAINS_TOP(A, X, B)    (FP_LT(A, X) && FP_LTEQ(X, B))
#define FP_CONTAINS_BOTTOM(A, X, B) (FP_LTEQ(A, X) && FP_LT(X, B))
#define FP_CONTAINS_INCL(A, X, B)   (FP_LTEQ(A, X) && FP_LTEQ(X, B))
#define FP_CONTAINS_EXCL(A, X, B)   (FP_LT(A, X) && FP_LT(X, B))
#define FP_CONTAINS(A, X, B)        FP_CONTAINS_EXCL(A, X, B)

/*
 * this will change to NaN when I figure out how to
 * get NaN in a platform-independent way
 */
#define NO_VALUE   0.0
#define NO_Z_VALUE NO_VALUE
#define NO_M_VALUE NO_VALUE

/**
 * Well-Known Text (WKT) Output Variant Types
 */
#define WKT_NO_TYPE   0x08 /* Internal use only */
#define WKT_NO_PARENS 0x10 /* Internal use only */
#define WKT_IS_CHILD  0x20 /* Internal use only */

/**
 * Well-Known Binary (WKB) Output Variant Types
 */

#define WKB_DOUBLE_SIZE 8 /* Internal use only */
#define WKB_INT_SIZE    4 /* Internal use only */
#define WKB_BYTE_SIZE   1 /* Internal use only */

/**
 * Well-Known Binary (WKB) Geometry Types
 */
#define WKB_POINT_TYPE              1
#define WKB_LINESTRING_TYPE         2
#define WKB_POLYGON_TYPE            3
#define WKB_MULTIPOINT_TYPE         4
#define WKB_MULTILINESTRING_TYPE    5
#define WKB_MULTIPOLYGON_TYPE       6
#define WKB_GEOMETRYCOLLECTION_TYPE 7
#define WKB_CIRCULARSTRING_TYPE     8
#define WKB_COMPOUNDCURVE_TYPE      9
#define WKB_CURVEPOLYGON_TYPE       10
#define WKB_MULTICURVE_TYPE         11
#define WKB_MULTISURFACE_TYPE       12
#define WKB_CURVE_TYPE              13 /* from ISO draft, not sure is real */
#define WKB_SURFACE_TYPE            14 /* from ISO draft, not sure is real */
#define WKB_POLYHEDRALSURFACE_TYPE  15
#define WKB_TIN_TYPE                16
#define WKB_TRIANGLE_TYPE           17

/*
 * Export functions
 */

/* Any (absolute) values outside this range will be printed in scientific notation */
#define OUT_MIN_DOUBLE             1E-8
#define OUT_MAX_DOUBLE             1E15
#define OUT_DEFAULT_DECIMAL_DIGITS 15

/* 17 digits are sufficient for round-tripping
 * Then we might add up to 8 (from OUT_MIN_DOUBLE) max leading zeroes (or 2 digits for "e+") */
#define OUT_MAX_DIGITS 17 + 8

/* Limit for the max amount of characters that a double can use, including dot and sign */
/* */
#define OUT_MAX_BYTES_DOUBLE   (1 /* Sign */ + 2 /* 0.x */ + OUT_MAX_DIGITS)
#define OUT_DOUBLE_BUFFER_SIZE OUT_MAX_BYTES_DOUBLE + 1 /* +1 including NULL */

/* Utilities */
int lwprint_double(double d, int maxdd, char *buf);

int p3d_same(const POINT3D *p1, const POINT3D *p2);

/*
 * What side of the line formed by p1 and p2 does q fall?
 * Returns -1 for left and 1 for right and 0 for co-linearity
 */
int lw_segment_side(const POINT2D *p1, const POINT2D *p2, const POINT2D *q);

/*
 * Force dims
 */
LWGEOM *lwgeom_force_dims(const LWGEOM *lwgeom, int hasz, int hasm, double zval, double mval);
LWPOINT *lwpoint_force_dims(const LWPOINT *lwpoint, int hasz, int hasm, double zval, double mval);
POINTARRAY *ptarray_force_dims(const POINTARRAY *pa, int hasz, int hasm, double zval, double mval);

int ptarray_startpoint(const POINTARRAY *pa, POINT4D *pt);

} // namespace duckdb
