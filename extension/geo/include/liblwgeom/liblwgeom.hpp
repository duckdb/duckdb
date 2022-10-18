/**********************************************************************
 *
 * PostGIS - Spatial Types for PostgreSQL
 * http://postgis.net
 *
 * PostGIS is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 2 of the License, or
 * (at your option) any later version.
 *
 * PostGIS is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with PostGIS.  If not, see <http://www.gnu.org/licenses/>.
 *
 **********************************************************************
 *
 * Copyright 2011 Sandro Santilli <strk@kbt.io>
 * Copyright 2011 Paul Ramsey <pramsey@cleverelephant.ca>
 * Copyright 2007-2008 Mark Cave-Ayland
 * Copyright 2001-2006 Refractions Research Inc.
 *
 **********************************************************************/

#pragma once

#include "postgres.hpp"

#include <iostream>
#include <stdarg.h>
#include <stdint.h>
#include <stdio.h>

namespace duckdb {

#ifndef _LIBLWGEOM_H
#define _LIBLWGEOM_H 1

#if POSTGIS_PROJ_VERSION < 49
/* Use the old (pre-2.2) geodesic functions */
#undef PROJ_GEODESIC
#else
/* Enable new geodesic functions API */
#define PROJ_GEODESIC
#endif

// /* For PROJ6 we cache several extra values to avoid calls to proj_get_source_crs
//  * or proj_get_target_crs since those are very costly
//  */
// typedef struct LWPROJ
// {
// 	PJ* pj;
//         /* CRSs are swapped: Used in transformation calls */
// 	uint8_t source_swapped;
// 	uint8_t target_swapped;
//         /* Source crs is geographic: Used in geography calls (source srid == dst srid) */
//         uint8_t source_is_latlong;

//         /* Source ellipsoid parameters */
//         double source_semi_major_metre;
//         double source_semi_minor_metre;
// } LWPROJ;

struct pg_varlena {
	char vl_len_[4]; /* Do not touch this field directly! */
	char vl_dat[1];  /* Data content is here */
};

typedef struct pg_varlena bytea;

/**
 * Return types for functions with status returns.
 */
#define LW_TRUE    1
#define LW_FALSE   0
#define LW_UNKNOWN 2
#define LW_FAILURE 0
#define LW_SUCCESS 1

/**
 * LWTYPE numbers, used internally by PostGIS
 */
#define POINTTYPE             1
#define LINETYPE              2
#define POLYGONTYPE           3
#define MULTIPOINTTYPE        4
#define MULTILINETYPE         5
#define MULTIPOLYGONTYPE      6
#define COLLECTIONTYPE        7
#define CIRCSTRINGTYPE        8
#define COMPOUNDTYPE          9
#define CURVEPOLYTYPE         10
#define MULTICURVETYPE        11
#define MULTISURFACETYPE      12
#define POLYHEDRALSURFACETYPE 13
#define TRIANGLETYPE          14
#define TINTYPE               15

#define NUMTYPES 16

/**
 * Flags applied in EWKB to indicate Z/M dimensions and
 * presence/absence of SRID and bounding boxes
 */
#define WKBZOFFSET  0x80000000
#define WKBMOFFSET  0x40000000
#define WKBSRIDFLAG 0x20000000
#define WKBBBOXFLAG 0x10000000

/**
 * Macros for manipulating the 'flags' byte. A uint8_t used as follows:
 * VVSRGBMZ
 * Version bit, followed by
 * Validty, Solid, ReadOnly, Geodetic, HasBBox, HasM and HasZ flags.
 */
#define LWFLAG_Z        0x01
#define LWFLAG_M        0x02
#define LWFLAG_BBOX     0x04
#define LWFLAG_GEODETIC 0x08
#define LWFLAG_READONLY 0x10
#define LWFLAG_SOLID    0x20

#define FLAGS_GET_Z(flags)        ((flags)&LWFLAG_Z)
#define FLAGS_GET_M(flags)        (((flags)&LWFLAG_M) >> 1)
#define FLAGS_GET_BBOX(flags)     (((flags)&LWFLAG_BBOX) >> 2)
#define FLAGS_GET_GEODETIC(flags) (((flags)&LWFLAG_GEODETIC) >> 3)
#define FLAGS_GET_READONLY(flags) (((flags)&LWFLAG_READONLY) >> 4)
#define FLAGS_GET_SOLID(flags)    (((flags)&LWFLAG_SOLID) >> 5)

#define FLAGS_SET_Z(flags, value)    ((flags) = (value) ? ((flags) | LWFLAG_Z) : ((flags) & ~LWFLAG_Z))
#define FLAGS_SET_M(flags, value)    ((flags) = (value) ? ((flags) | LWFLAG_M) : ((flags) & ~LWFLAG_M))
#define FLAGS_SET_BBOX(flags, value) ((flags) = (value) ? ((flags) | LWFLAG_BBOX) : ((flags) & ~LWFLAG_BBOX))
#define FLAGS_SET_GEODETIC(flags, value)                                                                               \
	((flags) = (value) ? ((flags) | LWFLAG_GEODETIC) : ((flags) & ~LWFLAG_GEODETIC))
#define FLAGS_SET_READONLY(flags, value)                                                                               \
	((flags) = (value) ? ((flags) | LWFLAG_READONLY) : ((flags) & ~LWFLAG_READONLY))
#define FLAGS_SET_SOLID(flags, value) ((flags) = (value) ? ((flags) | LWFLAG_SOLID) : ((flags) & ~LWFLAG_SOLID))

#define FLAGS_NDIMS(flags)     (2 + FLAGS_GET_Z(flags) + FLAGS_GET_M(flags))
#define FLAGS_GET_ZM(flags)    (FLAGS_GET_M(flags) + FLAGS_GET_Z(flags) * 2)
#define FLAGS_NDIMS_BOX(flags) (FLAGS_GET_GEODETIC(flags) ? 3 : FLAGS_NDIMS(flags))

/**
 * Maximum allowed SRID value in serialized geometry.
 * Currently we are using 21 bits (2097152) of storage for SRID.
 */
#define SRID_MAXIMUM 999999

/**
 * Maximum valid SRID value for the user
 * We reserve 1000 values for internal use
 */
#define SRID_USER_MAXIMUM 998999

/** Unknown SRID value */
#define SRID_UNKNOWN       0
#define SRID_IS_UNKNOWN(x) ((int)x <= 0)

/* Invalid SRID value, for internal use */
#define SRID_INVALID (999999 + 2)

/*
** EPSG WGS84 geographics, OGC standard default SRS, better be in
** the SPATIAL_REF_SYS table!
*/
#define SRID_DEFAULT 4326

#ifndef __GNUC__
#define __attribute__(x)
#endif

/* To return a NULL do this: */
#define PG_ERROR_NULL() throw std::runtime_error("NULL VALUE")

/**
 * Return a valid SRID from an arbitrary integer
 * Raises a notice if what comes out is different from
 * what went in.
 * Raises an error if SRID value is out of bounds.
 */
extern int32_t clamp_srid(int32_t srid);

/**********************************************************************
** Spherical radius.
** Moritz, H. (1980). Geodetic Reference System 1980, by resolution of
** the XVII General Assembly of the IUGG in Canberra.
** http://en.wikipedia.org/wiki/Earth_radius
** http://en.wikipedia.org/wiki/World_Geodetic_System
*/

#define WGS84_MAJOR_AXIS         6378137.0
#define WGS84_INVERSE_FLATTENING 298.257223563
#define WGS84_MINOR_AXIS         (WGS84_MAJOR_AXIS - WGS84_MAJOR_AXIS / WGS84_INVERSE_FLATTENING)
#define WGS84_RADIUS             ((2.0 * WGS84_MAJOR_AXIS + WGS84_MINOR_AXIS) / 3.0)
#define WGS84_SRID               4326

/******************************************************************
 * LWGEOM and GBOX both use LWFLAGS bit mask.
 * Serializations (may) use different bit mask schemes.
 */
typedef uint16_t lwflags_t;

/******************************************************************
 * LWGEOM varlena equivalent type that contains both the size and
 * data(see Postgresql c.h)
 */
typedef struct lwvarlena_t {
	uint32_t size; /* Do not touch this field directly! */
	char data[1];  /* Data content is here */
} lwvarlena_t;

#define LWVARHDRSZ ((int32_t)sizeof(int32_t))

/******************************************************************
 * GBOX structure.
 * We include the flags (information about dimensionality),
 * so we don't have to constantly pass them
 * into functions that use the GBOX.
 */
typedef struct {
	lwflags_t flags;
	double xmin;
	double xmax;
	double ymin;
	double ymax;
	double zmin;
	double zmax;
	double mmin;
	double mmax;
} GBOX;

/******************************************************************
 * SPHEROID
 *
 *  Standard definition of an ellipsoid (what wkt calls a spheroid)
 *    f = (a-b)/a
 *    e_sq = (a*a - b*b)/(a*a)
 *    b = a - fa
 */
typedef struct {
	double a;      /* semimajor axis */
	double b;      /* semiminor axis b = (a - fa) */
	double f;      /* flattening f = (a-b)/a */
	double e;      /* eccentricity (first) */
	double e_sq;   /* eccentricity squared (first) e_sq = (a*a-b*b)/(a*a) */
	double radius; /* spherical average radius = (2*a+b)/3 */
	char name[20]; /* name of ellipse */
} SPHEROID;

/******************************************************************
 * POINT2D, POINT3D, POINT3DM, POINT4D
 */
typedef struct {
	double x, y;
} POINT2D;

typedef struct {
	double x, y, z;
} POINT3DZ;

typedef struct {
	double x, y, z;
} POINT3D;

typedef struct {
	double x, y, m;
} POINT3DM;

typedef struct {
	double x, y, z, m;
} POINT4D;

/******************************************************************
 *  POINTARRAY
 *  Point array abstracts a lot of the complexity of points and point lists.
 *  It handles 2d/3d translation
 *    (2d points converted to 3d will have z=0 or NaN)
 *  DO NOT MIX 2D and 3D POINTS! EVERYTHING* is either one or the other
 */
typedef struct {
	uint32_t npoints;   /* how many points we are currently storing */
	uint32_t maxpoints; /* how many points we have space for in serialized_pointlist */

	/* Use FLAGS_* macros to handle */
	lwflags_t flags;

	/* Array of POINT 2D, 3D or 4D, possibly misaligned. */
	uint8_t *serialized_pointlist;
} POINTARRAY;

/******************************************************************
 * GSERIALIZED
 */

typedef struct GSERIALIZED {
	uint32_t size;   /* For PgSQL use only, use VAR* macros to manipulate. */
	uint8_t srid[3]; /* 24 bits of SRID */
	uint8_t gflags;  /* HasZ, HasM, HasBBox, IsGeodetic */
	uint8_t data[1]; /* See gserialized.txt */
} GSERIALIZED;

/******************************************************************
 * LWGEOM (any geometry type)
 *
 * Abstract type, note that 'type', 'bbox' and 'srid' are available in
 * all geometry variants.
 */
typedef struct {
	GBOX *bbox;
	void *data;
	int32_t srid;
	lwflags_t flags;
	uint8_t type;
	char pad[1]; /* Padding to 24 bytes (unused) */
} LWGEOM;

/* POINTYPE */
typedef struct {
	GBOX *bbox;
	POINTARRAY *point; /* hide 2d/3d (this will be an array of 1 point) */
	int32_t srid;
	lwflags_t flags;
	uint8_t type; /* POINTTYPE */
	char pad[1];  /* Padding to 24 bytes (unused) */
} LWPOINT;        /* "light-weight point" */

extern LWGEOM *lwpoint_as_lwgeom(const LWPOINT *obj);

/*
 * set point N to the given value
 * NOTE that the pointarray can be of any
 * dimension, the appropriate ordinate values
 * will be extracted from it
 *
 * N must be a valid point index
 */
extern void ptarray_set_point4d(POINTARRAY *pa, uint32_t n, const POINT4D *p4d);

/**
 * Construct an empty pointarray, allocating storage and setting
 * the npoints, but not filling in any information. Should be used in conjunction
 * with ptarray_set_point4d to fill in the information in the array.
 */
extern POINTARRAY *ptarray_construct(char hasz, char hasm, uint32_t npoints);

/**
 * Construct a new #POINTARRAY, <em>copying</em> in the data from ptlist
 */
extern POINTARRAY *ptarray_construct_copy_data(char hasz, char hasm, uint32_t npoints, const uint8_t *ptlist);

/**
 * Construct a new #POINTARRAY, <em>referencing</em> to the data from ptlist
 */
extern POINTARRAY *ptarray_construct_reference_data(char hasz, char hasm, uint32_t npoints, uint8_t *ptlist);

/**
 * Create a new #POINTARRAY with no points. Allocate enough storage
 * to hold maxpoints vertices before having to reallocate the storage
 * area.
 */
extern POINTARRAY *ptarray_construct_empty(char hasz, char hasm, uint32_t maxpoints);

/**
 * Append a point to the end of an existing #POINTARRAY
 * If allow_duplicate is LW_FALSE, then a duplicate point will
 * not be added.
 */
extern int ptarray_append_point(POINTARRAY *pa, const POINT4D *pt, int allow_duplicates);

/**
 * Insert a point into an existing #POINTARRAY. Zero
 * is the index of the start of the array.
 */
extern int ptarray_insert_point(POINTARRAY *pa, const POINT4D *p, uint32_t where);

/**
 * Construct a new flags bitmask.
 */
extern lwflags_t lwflags(int hasz, int hasm, int geodetic);

/******************************************************************
 * SERIALIZED FORM functions
 ******************************************************************/

/**
 * Set the SRID on an LWGEOM
 * For collections, only the parent gets an SRID, all
 * the children get SRID_UNKNOWN.
 */
extern void lwgeom_set_srid(LWGEOM *geom, int32_t srid);

/**
* Return SRID number
*/
extern int32_t lwgeom_get_srid(const LWGEOM *geom);

/****************************************************************
 * MEMORY MANAGEMENT
 ****************************************************************/

/*
 * The *_free family of functions frees *all* memory associated
 * with the pointer. When the recursion gets to the level of the
 * POINTARRAY, the POINTARRAY is only freed if it is not flagged
 * as "read only". LWGEOMs constructed on top of GSERIALIZED
 * from PgSQL use read only point arrays.
 */

extern void ptarray_free(POINTARRAY *pa);
extern void lwpoint_free(LWPOINT *pt);
extern void lwgeom_free(LWGEOM *geom);

/**
 * Strip out the Z/M components of an #LWGEOM
 */
extern LWGEOM *lwgeom_force_2d(const LWGEOM *geom);

extern float next_float_down(double d);
extern float next_float_up(double d);

/* general utilities 2D */
extern double lwgeom_mindistance2d(const LWGEOM *lw1, const LWGEOM *lw2);
extern double lwgeom_mindistance2d_tolerance(const LWGEOM *lw1, const LWGEOM *lw2, double tolerance);

/*
 * Geometry constructors. These constructors to not copy the point arrays
 * passed to them, they just take references, so do not free them out
 * from underneath the geometries.
 */
extern LWPOINT *lwpoint_construct(int32_t srid, GBOX *bbox, POINTARRAY *point);

/**
 * @brief Check whether or not a lwgeom is big enough to warrant a bounding box.
 *
 * Check whether or not a lwgeom is big enough to warrant a bounding box
 * when stored in the serialized form on disk. Currently only points are
 * considered small enough to not require a bounding box, because the
 * index operations can generate a large number of box-retrieval operations
 * when scanning keys.
 */
extern int lwgeom_needs_bbox(const LWGEOM *geom);

/**
 * Compute a bbox if not already computed
 *
 * After calling this function lwgeom->bbox is only
 * NULL if the geometry is empty.
 */
extern void lwgeom_add_bbox(LWGEOM *lwgeom);

/**
 * Return true or false depending on whether a geometry has
 * a valid SRID set.
 */
extern int lwgeom_has_srid(const LWGEOM *geom);

/*
 * copies a point from the point array into the parameter point
 * will set point's z=0 (or NaN) if pa is 2d
 * will set point's m=0 (or NaN) if pa is 3d or 2d
 * NOTE: this will modify the point4d pointed to by 'point'.
 */
extern int getPoint4d_p(const POINTARRAY *pa, uint32_t n, POINT4D *point);

/*
 * Empty geometry constructors.
 */
extern LWGEOM *lwgeom_construct_empty(uint8_t type, int32_t srid, char hasz, char hasm);
extern LWPOINT *lwpoint_construct_empty(int32_t srid, char hasz, char hasm);

/* Other constructors */
extern LWPOINT *lwpoint_make2d(int32_t srid, double x, double y);
extern LWPOINT *lwpoint_make3dz(int32_t srid, double x, double y, double z);

/**
 * Create an LWGEOM object from a GeoJSON representation
 *
 * @param geojson the GeoJSON input
 * @param srs output parameter. Will be set to a newly allocated
 *            string holding the spatial reference string, or NULL
 *            if no such parameter is found in input.
 *            If not null, the pointer must be freed with lwfree.
 */
extern LWGEOM *lwgeom_from_geojson(const char *geojson, char **srs);

/**
 * Initialize a spheroid object for use in geodetic functions.
 */
extern void spheroid_init(SPHEROID *s, double a, double b);

/**
 * Global functions for memory/logging handlers.
 */
typedef void *(*lwallocator)(size_t size);
typedef void *(*lwreallocator)(void *mem, size_t size);
typedef void (*lwfreeor)(void *mem);
typedef void (*lwreporter)(const char *fmt, va_list ap) __attribute__((format(printf, 1, 0)));
typedef void (*lwdebuglogger)(int level, const char *fmt, va_list ap) __attribute__((format(printf, 2, 0)));

/**
 * Macro for reading the size from the GSERIALIZED size attribute.
 * Cribbed from PgSQL, top 30 bits are size. Use VARSIZE() when working
 * internally with PgSQL. See SET_VARSIZE_4B / VARSIZE_4B in
 * PGSRC/src/include/postgres.h for details.
 */
#ifdef WORDS_BIGENDIAN
#define LWSIZE_GET(varsize)      ((varsize)&0x3FFFFFFF)
#define LWSIZE_SET(varsize, len) ((varsize) = ((len)&0x3FFFFFFF))
#define IS_BIG_ENDIAN            1
#else
#define LWSIZE_GET(varsize)      (((varsize) >> 2) & 0x3FFFFFFF)
#define LWSIZE_SET(varsize, len) ((varsize) = (((uint32_t)(len)) << 2))
#define IS_BIG_ENDIAN            0
#endif

/**
 * Parser check flags
 *
 *  @see lwgeom_from_wkb
 *  @see lwgeom_from_hexwkb
 *  @see lwgeom_parse_wkt
 */
#define LW_PARSER_CHECK_MINPOINTS 1
#define LW_PARSER_CHECK_ODD       2
#define LW_PARSER_CHECK_CLOSURE   4
#define LW_PARSER_CHECK_ZCLOSURE  8

#define LW_PARSER_CHECK_NONE 0
#define LW_PARSER_CHECK_ALL  (LW_PARSER_CHECK_MINPOINTS | LW_PARSER_CHECK_ODD | LW_PARSER_CHECK_CLOSURE)

/**
 * Parser result structure: returns the result of attempting to convert
 * (E)WKT/(E)WKB to LWGEOM
 */
typedef struct struct_lwgeom_parser_result {
	const char *wkinput;        /* Copy of pointer to input WKT/WKB */
	uint8_t *serialized_lwgeom; /* Pointer to serialized LWGEOM */
	size_t size;                /* Size of serialized LWGEOM in bytes */
	LWGEOM *geom;               /* Pointer to LWGEOM struct */
	const char *message;        /* Error/warning message */
	int errcode;                /* Error/warning number */
	int errlocation;            /* Location of error */
	int parser_check_flags;     /* Bitmask of validity checks run during this parse */
} LWGEOM_PARSER_RESULT;

/*
 * Parser error messages (these must match the message array in lwgparse.c)
 */
#define PARSER_ERROR_MOREPOINTS     1
#define PARSER_ERROR_ODDPOINTS      2
#define PARSER_ERROR_UNCLOSED       3
#define PARSER_ERROR_MIXDIMS        4
#define PARSER_ERROR_INVALIDGEOM    5
#define PARSER_ERROR_INVALIDWKBTYPE 6
#define PARSER_ERROR_INCONTINUOUS   7
#define PARSER_ERROR_TRIANGLEPOINTS 8
#define PARSER_ERROR_LESSPOINTS     9
#define PARSER_ERROR_OTHER          10

/*
** Variants available for WKB and WKT output types
*/

#define WKB_ISO        0x01
#define WKB_SFSQL      0x02
#define WKB_EXTENDED   0x04
#define WKB_NDR        0x08
#define WKB_XDR        0x10
#define WKB_HEX        0x20
#define WKB_NO_NPOINTS 0x40 /* Internal use only */
#define WKB_NO_SRID    0x80 /* Internal use only */

#define WKT_ISO      0x01
#define WKT_SFSQL    0x02
#define WKT_EXTENDED 0x04

/* Number of digits of precision in WKT produced. */
#define WKT_PRECISION 15

/**
 * Check if a #GSERIALIZED has a bounding box without deserializing first.
 */
extern int gserialized_has_bbox(const GSERIALIZED *gser);

/**
 * @param geom geometry to convert to HEXWKB
 * @param variant output format to use
 *                (WKB_ISO, WKB_SFSQL, WKB_EXTENDED, WKB_NDR, WKB_XDR)
 * @param size_out (Out parameter) size of the buffer
 */
extern char *lwgeom_to_hexwkb_buffer(const LWGEOM *geom, uint8_t variant);

/*
 * WKT detailed parsing support
 */
extern int lwgeom_parse_wkt(LWGEOM_PARSER_RESULT *parser_result, char *wktstr, int parse_flags);
void lwgeom_parser_result_init(LWGEOM_PARSER_RESULT *parser_result);
void lwgeom_parser_result_free(LWGEOM_PARSER_RESULT *parser_result);

/**
 * Return the type name string associated with a type number
 * (e.g. Point, LineString, Polygon)
 */
extern const char *lwtype_name(uint8_t type);

/*
** New parsing and unparsing functions.
*/

/**
 * @param geom geometry to convert to WKT
 * @param variant output format to use (WKT_ISO, WKT_SFSQL, WKT_EXTENDED)
 * @param precision Double precision
 * @param size_out (Out parameter) size of the buffer
 */
extern char *lwgeom_to_wkt(const LWGEOM *geom, uint8_t variant, int precision, size_t *size_out);

extern uint8_t *bytes_from_hexbytes(const char *hexbuf, size_t hexsize);

/**
 * Calculate the geodetic bounding box for an LWGEOM. Z/M coordinates are
 * ignored for this calculation. Pass in non-null, geodetic bounding box for function
 * to fill out. LWGEOM must have been built from a GSERIALIZED to provide
 * double aligned point arrays.
 */
extern int lwgeom_calculate_gbox_geodetic(const LWGEOM *geom, GBOX *gbox);

/**
 * Calculate the 2-4D bounding box of a geometry. Z/M coordinates are honored
 * for this calculation, though for curves they are not included in calculations
 * of curvature.
 */
extern int lwgeom_calculate_gbox_cartesian(const LWGEOM *lwgeom, GBOX *gbox);

/**
 * Calculate bounding box of a geometry, automatically taking into account
 * whether it is cartesian or geodetic.
 */
extern int lwgeom_calculate_gbox(const LWGEOM *lwgeom, GBOX *gbox);

/**
 * Calculate geodetic (x/y/z) box and add values to gbox. Return #LW_SUCCESS on success.
 */
extern int ptarray_calculate_gbox_geodetic(const POINTARRAY *pa, GBOX *gbox);

/**
 * Calculate box (x/y) and add values to gbox. Return #LW_SUCCESS on success.
 */
extern int ptarray_calculate_gbox_cartesian(const POINTARRAY *pa, GBOX *gbox);

/**
 * @param wkb_size length of WKB byte buffer
 * @param wkb WKB byte buffer
 * @param check parser check flags, see LW_PARSER_CHECK_* macros
 */
extern LWGEOM *lwgeom_from_wkb(const uint8_t *wkb, const size_t wkb_size, const char check);

/**
 * Create a new gbox with the dimensionality indicated by the flags. Caller
 * is responsible for freeing.
 */
extern GBOX *gbox_new(lwflags_t flags);

/**
 * Zero out all the entries in the #GBOX. Useful for cleaning
 * statically allocated gboxes.
 */
extern void gbox_init(GBOX *gbox);

/**
 * Update the merged #GBOX to be large enough to include itself and the new box.
 */
extern int gbox_merge(const GBOX *new_box, GBOX *merged_box);

/**
 * Return a copy of the #GBOX, based on dimensionality of flags.
 */
extern GBOX *gbox_copy(const GBOX *gbox);

/**
 * Initialize a #GBOX using the values of the point.
 */
extern int gbox_init_point3d(const POINT3D *p, GBOX *gbox);

/**
 * Update the #GBOX to be large enough to include itself and the new point.
 */
extern int gbox_merge_point3d(const POINT3D *p, GBOX *gbox);

/**
 * Copy the values of original #GBOX into duplicate.
 */
extern void gbox_duplicate(const GBOX *original, GBOX *duplicate);

/**
 * Return the number of bytes necessary to hold a #GBOX of this dimension in
 * serialized form.
 */
extern size_t gbox_serialized_size(lwflags_t flags);

/**
 * Extract the geometry type from the serialized form (it hides in
 * the anonymous data area, so this is a handy function).
 */
extern uint32_t gserialized_get_type(const GSERIALIZED *g);

/**
 * Pull the first point values of a #GSERIALIZED. Only works for POINTTYPE
 */
extern int gserialized_peek_first_point(const GSERIALIZED *g, POINT4D *out_point);

/**
 * @param geom geometry to convert to WKB
 * @param variant output format to use
 *                (WKB_ISO, WKB_SFSQL, WKB_EXTENDED, WKB_NDR, WKB_XDR)
 */
extern uint8_t *lwgeom_to_wkb_buffer(const LWGEOM *geom, uint8_t variant);
extern size_t lwgeom_to_wkb_size(const LWGEOM *geom, uint8_t variant);

/* Memory management */
extern void *lwalloc(size_t size);
extern void *lwrealloc(void *mem, size_t size);
extern void lwfree(void *mem);

void lwerror(const char *fmt, ...);

extern lwvarlena_t *lwgeom_to_geojson(const LWGEOM *geo, const char *srs, int precision, int has_bbox);

extern int lwgeom_startpoint(const LWGEOM *lwgeom, POINT4D *pt);

#endif /* !defined _LIBLWGEOM_H  */

} // namespace duckdb
