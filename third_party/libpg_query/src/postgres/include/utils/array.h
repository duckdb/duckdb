/*-------------------------------------------------------------------------
 *
 * array.h
 *	  Declarations for Postgres arrays.
 *
 * A standard varlena array has the following internal structure:
 *	  <vl_len_>		- standard varlena header word
 *	  <ndim>		- number of dimensions of the array
 *	  <dataoffset>	- offset to stored data, or 0 if no nulls bitmap
 *	  <elemtype>	- element type OID
 *	  <dimensions>	- length of each array axis (C array of int)
 *	  <lower bnds>	- lower boundary of each dimension (C array of int)
 *	  <null bitmap> - bitmap showing locations of nulls (OPTIONAL)
 *	  <actual data> - whatever is the stored data
 *
 * The <dimensions> and <lower bnds> arrays each have ndim elements.
 *
 * The <null bitmap> may be omitted if the array contains no NULL elements.
 * If it is absent, the <dataoffset> field is zero and the offset to the
 * stored data must be computed on-the-fly.  If the bitmap is present,
 * <dataoffset> is nonzero and is equal to the offset from the array start
 * to the first data element (including any alignment padding).  The bitmap
 * follows the same conventions as tuple null bitmaps, ie, a 1 indicates
 * a non-null entry and the LSB of each bitmap byte is used first.
 *
 * The actual data starts on a MAXALIGN boundary.  Individual items in the
 * array are aligned as specified by the array element type.  They are
 * stored in row-major order (last subscript varies most rapidly).
 *
 * NOTE: it is important that array elements of toastable datatypes NOT be
 * toasted, since the tupletoaster won't know they are there.  (We could
 * support compressed toasted items; only out-of-line items are dangerous.
 * However, it seems preferable to store such items uncompressed and allow
 * the toaster to compress the whole array as one input.)
 *
 *
 * The OIDVECTOR and INT2VECTOR datatypes are storage-compatible with
 * generic arrays, but they support only one-dimensional arrays with no
 * nulls (and no null bitmap).
 *
 * There are also some "fixed-length array" datatypes, such as NAME and
 * POINT.  These are simply a sequence of a fixed number of items each
 * of a fixed-length datatype, with no overhead; the item size must be
 * a multiple of its alignment requirement, because we do no padding.
 * We support subscripting on these types, but array_in() and array_out()
 * only work with varlena arrays.
 *
 * In addition, arrays are a major user of the "expanded object" TOAST
 * infrastructure.  This allows a varlena array to be converted to a
 * separate representation that may include "deconstructed" Datum/isnull
 * arrays holding the elements.
 *
 *
 * Portions Copyright (c) 1996-2015, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/utils/array.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef ARRAY_H
#define ARRAY_H

#include "fmgr.h"
#include "utils/expandeddatum.h"


/*
 * Arrays are varlena objects, so must meet the varlena convention that
 * the first int32 of the object contains the total object size in bytes.
 * Be sure to use VARSIZE() and SET_VARSIZE() to access it, though!
 *
 * CAUTION: if you change the header for ordinary arrays you will also
 * need to change the headers for oidvector and int2vector!
 */
typedef struct
{
	int32		vl_len_;		/* varlena header (do not touch directly!) */
	int			ndim;			/* # of dimensions */
	int32		dataoffset;		/* offset to data, or 0 if no bitmap */
	Oid			elemtype;		/* element type OID */
} ArrayType;

/*
 * An expanded array is contained within a private memory context (as
 * all expanded objects must be) and has a control structure as below.
 *
 * The expanded array might contain a regular "flat" array if that was the
 * original input and we've not modified it significantly.  Otherwise, the
 * contents are represented by Datum/isnull arrays plus dimensionality and
 * type information.  We could also have both forms, if we've deconstructed
 * the original array for access purposes but not yet changed it.  For pass-
 * by-reference element types, the Datums would point into the flat array in
 * this situation.  Once we start modifying array elements, new pass-by-ref
 * elements are separately palloc'd within the memory context.
 */
#define EA_MAGIC 689375833		/* ID for debugging crosschecks */

typedef struct ExpandedArrayHeader
{
	/* Standard header for expanded objects */
	ExpandedObjectHeader hdr;

	/* Magic value identifying an expanded array (for debugging only) */
	int			ea_magic;

	/* Dimensionality info (always valid) */
	int			ndims;			/* # of dimensions */
	int		   *dims;			/* array dimensions */
	int		   *lbound;			/* index lower bounds for each dimension */

	/* Element type info (always valid) */
	Oid			element_type;	/* element type OID */
	int16		typlen;			/* needed info about element datatype */
	bool		typbyval;
	char		typalign;

	/*
	 * If we have a Datum-array representation of the array, it's kept here;
	 * else dvalues/dnulls are NULL.  The dvalues and dnulls arrays are always
	 * palloc'd within the object private context, but may change size from
	 * time to time.  For pass-by-ref element types, dvalues entries might
	 * point either into the fstartptr..fendptr area, or to separately
	 * palloc'd chunks.  Elements should always be fully detoasted, as they
	 * are in the standard flat representation.
	 *
	 * Even when dvalues is valid, dnulls can be NULL if there are no null
	 * elements.
	 */
	Datum	   *dvalues;		/* array of Datums */
	bool	   *dnulls;			/* array of is-null flags for Datums */
	int			dvalueslen;		/* allocated length of above arrays */
	int			nelems;			/* number of valid entries in above arrays */

	/*
	 * flat_size is the current space requirement for the flat equivalent of
	 * the expanded array, if known; otherwise it's 0.  We store this to make
	 * consecutive calls of get_flat_size cheap.
	 */
	Size		flat_size;

	/*
	 * fvalue points to the flat representation if it is valid, else it is
	 * NULL.  If we have or ever had a flat representation then
	 * fstartptr/fendptr point to the start and end+1 of its data area; this
	 * is so that we can tell which Datum pointers point into the flat
	 * representation rather than being pointers to separately palloc'd data.
	 */
	ArrayType  *fvalue;			/* must be a fully detoasted array */
	char	   *fstartptr;		/* start of its data area */
	char	   *fendptr;		/* end+1 of its data area */
} ExpandedArrayHeader;

/*
 * Functions that can handle either a "flat" varlena array or an expanded
 * array use this union to work with their input.
 */
typedef union AnyArrayType
{
	ArrayType	flt;
	ExpandedArrayHeader xpn;
} AnyArrayType;

/*
 * working state for accumArrayResult() and friends
 * note that the input must be scalars (legal array elements)
 */
typedef struct ArrayBuildState
{
	MemoryContext mcontext;		/* where all the temp stuff is kept */
	Datum	   *dvalues;		/* array of accumulated Datums */
	bool	   *dnulls;			/* array of is-null flags for Datums */
	int			alen;			/* allocated length of above arrays */
	int			nelems;			/* number of valid entries in above arrays */
	Oid			element_type;	/* data type of the Datums */
	int16		typlen;			/* needed info about datatype */
	bool		typbyval;
	char		typalign;
	bool		private_cxt;	/* use private memory context */
} ArrayBuildState;

/*
 * working state for accumArrayResultArr() and friends
 * note that the input must be arrays, and the same array type is returned
 */
typedef struct ArrayBuildStateArr
{
	MemoryContext mcontext;		/* where all the temp stuff is kept */
	char	   *data;			/* accumulated data */
	bits8	   *nullbitmap;		/* bitmap of is-null flags, or NULL if none */
	int			abytes;			/* allocated length of "data" */
	int			nbytes;			/* number of bytes used so far */
	int			aitems;			/* allocated length of bitmap (in elements) */
	int			nitems;			/* total number of elements in result */
	int			ndims;			/* current dimensions of result */
	int			dims[MAXDIM];
	int			lbs[MAXDIM];
	Oid			array_type;		/* data type of the arrays */
	Oid			element_type;	/* data type of the array elements */
	bool		private_cxt;	/* use private memory context */
} ArrayBuildStateArr;

/*
 * working state for accumArrayResultAny() and friends
 * these functions handle both cases
 */
typedef struct ArrayBuildStateAny
{
	/* Exactly one of these is not NULL: */
	ArrayBuildState *scalarstate;
	ArrayBuildStateArr *arraystate;
} ArrayBuildStateAny;

/*
 * structure to cache type metadata needed for array manipulation
 */
typedef struct ArrayMetaState
{
	Oid			element_type;
	int16		typlen;
	bool		typbyval;
	char		typalign;
	char		typdelim;
	Oid			typioparam;
	Oid			typiofunc;
	FmgrInfo	proc;
} ArrayMetaState;

/*
 * private state needed by array_map (here because caller must provide it)
 */
typedef struct ArrayMapState
{
	ArrayMetaState inp_extra;
	ArrayMetaState ret_extra;
} ArrayMapState;

/* ArrayIteratorData is private in arrayfuncs.c */
typedef struct ArrayIteratorData *ArrayIterator;

/* fmgr macros for regular varlena array objects */
#define DatumGetArrayTypeP(X)		  ((ArrayType *) PG_DETOAST_DATUM(X))
#define DatumGetArrayTypePCopy(X)	  ((ArrayType *) PG_DETOAST_DATUM_COPY(X))
#define PG_GETARG_ARRAYTYPE_P(n)	  DatumGetArrayTypeP(PG_GETARG_DATUM(n))
#define PG_GETARG_ARRAYTYPE_P_COPY(n) DatumGetArrayTypePCopy(PG_GETARG_DATUM(n))
#define PG_RETURN_ARRAYTYPE_P(x)	  PG_RETURN_POINTER(x)

/* fmgr macros for expanded array objects */
#define PG_GETARG_EXPANDED_ARRAY(n)  DatumGetExpandedArray(PG_GETARG_DATUM(n))
#define PG_GETARG_EXPANDED_ARRAYX(n, metacache) \
	DatumGetExpandedArrayX(PG_GETARG_DATUM(n), metacache)
#define PG_RETURN_EXPANDED_ARRAY(x)  PG_RETURN_DATUM(EOHPGetRWDatum(&(x)->hdr))

/* fmgr macros for AnyArrayType (ie, get either varlena or expanded form) */
#define PG_GETARG_ANY_ARRAY(n)	DatumGetAnyArray(PG_GETARG_DATUM(n))

/*
 * Access macros for varlena array header fields.
 *
 * ARR_DIMS returns a pointer to an array of array dimensions (number of
 * elements along the various array axes).
 *
 * ARR_LBOUND returns a pointer to an array of array lower bounds.
 *
 * That is: if the third axis of an array has elements 5 through 8, then
 * ARR_DIMS(a)[2] == 4 and ARR_LBOUND(a)[2] == 5.
 *
 * Unlike C, the default lower bound is 1.
 */
#define ARR_SIZE(a)				VARSIZE(a)
#define ARR_NDIM(a)				((a)->ndim)
#define ARR_HASNULL(a)			((a)->dataoffset != 0)
#define ARR_ELEMTYPE(a)			((a)->elemtype)

#define ARR_DIMS(a) \
		((int *) (((char *) (a)) + sizeof(ArrayType)))
#define ARR_LBOUND(a) \
		((int *) (((char *) (a)) + sizeof(ArrayType) + \
				  sizeof(int) * ARR_NDIM(a)))

#define ARR_NULLBITMAP(a) \
		(ARR_HASNULL(a) ? \
		 (bits8 *) (((char *) (a)) + sizeof(ArrayType) + \
					2 * sizeof(int) * ARR_NDIM(a)) \
		 : (bits8 *) NULL)

/*
 * The total array header size (in bytes) for an array with the specified
 * number of dimensions and total number of items.
 */
#define ARR_OVERHEAD_NONULLS(ndims) \
		MAXALIGN(sizeof(ArrayType) + 2 * sizeof(int) * (ndims))
#define ARR_OVERHEAD_WITHNULLS(ndims, nitems) \
		MAXALIGN(sizeof(ArrayType) + 2 * sizeof(int) * (ndims) + \
				 ((nitems) + 7) / 8)

#define ARR_DATA_OFFSET(a) \
		(ARR_HASNULL(a) ? (a)->dataoffset : ARR_OVERHEAD_NONULLS(ARR_NDIM(a)))

/*
 * Returns a pointer to the actual array data.
 */
#define ARR_DATA_PTR(a) \
		(((char *) (a)) + ARR_DATA_OFFSET(a))

/*
 * Macros for working with AnyArrayType inputs.  Beware multiple references!
 */
#define AARR_NDIM(a) \
	(VARATT_IS_EXPANDED_HEADER(a) ? (a)->xpn.ndims : ARR_NDIM(&(a)->flt))
#define AARR_HASNULL(a) \
	(VARATT_IS_EXPANDED_HEADER(a) ? \
	 ((a)->xpn.dvalues != NULL ? (a)->xpn.dnulls != NULL : ARR_HASNULL((a)->xpn.fvalue)) : \
	 ARR_HASNULL(&(a)->flt))
#define AARR_ELEMTYPE(a) \
	(VARATT_IS_EXPANDED_HEADER(a) ? (a)->xpn.element_type : ARR_ELEMTYPE(&(a)->flt))
#define AARR_DIMS(a) \
	(VARATT_IS_EXPANDED_HEADER(a) ? (a)->xpn.dims : ARR_DIMS(&(a)->flt))
#define AARR_LBOUND(a) \
	(VARATT_IS_EXPANDED_HEADER(a) ? (a)->xpn.lbound : ARR_LBOUND(&(a)->flt))


/*
 * GUC parameter
 */
extern bool Array_nulls;

/*
 * prototypes for functions defined in arrayfuncs.c
 */
extern Datum array_in(PG_FUNCTION_ARGS);
extern Datum array_out(PG_FUNCTION_ARGS);
extern Datum array_recv(PG_FUNCTION_ARGS);
extern Datum array_send(PG_FUNCTION_ARGS);
extern Datum array_eq(PG_FUNCTION_ARGS);
extern Datum array_ne(PG_FUNCTION_ARGS);
extern Datum array_lt(PG_FUNCTION_ARGS);
extern Datum array_gt(PG_FUNCTION_ARGS);
extern Datum array_le(PG_FUNCTION_ARGS);
extern Datum array_ge(PG_FUNCTION_ARGS);
extern Datum btarraycmp(PG_FUNCTION_ARGS);
extern Datum hash_array(PG_FUNCTION_ARGS);
extern Datum arrayoverlap(PG_FUNCTION_ARGS);
extern Datum arraycontains(PG_FUNCTION_ARGS);
extern Datum arraycontained(PG_FUNCTION_ARGS);
extern Datum array_ndims(PG_FUNCTION_ARGS);
extern Datum array_dims(PG_FUNCTION_ARGS);
extern Datum array_lower(PG_FUNCTION_ARGS);
extern Datum array_upper(PG_FUNCTION_ARGS);
extern Datum array_length(PG_FUNCTION_ARGS);
extern Datum array_cardinality(PG_FUNCTION_ARGS);
extern Datum array_larger(PG_FUNCTION_ARGS);
extern Datum array_smaller(PG_FUNCTION_ARGS);
extern Datum generate_subscripts(PG_FUNCTION_ARGS);
extern Datum generate_subscripts_nodir(PG_FUNCTION_ARGS);
extern Datum array_fill(PG_FUNCTION_ARGS);
extern Datum array_fill_with_lower_bounds(PG_FUNCTION_ARGS);
extern Datum array_unnest(PG_FUNCTION_ARGS);
extern Datum array_remove(PG_FUNCTION_ARGS);
extern Datum array_replace(PG_FUNCTION_ARGS);
extern Datum width_bucket_array(PG_FUNCTION_ARGS);

extern void CopyArrayEls(ArrayType *array,
			 Datum *values,
			 bool *nulls,
			 int nitems,
			 int typlen,
			 bool typbyval,
			 char typalign,
			 bool freedata);

extern Datum array_get_element(Datum arraydatum, int nSubscripts, int *indx,
				  int arraytyplen, int elmlen, bool elmbyval, char elmalign,
				  bool *isNull);
extern Datum array_set_element(Datum arraydatum, int nSubscripts, int *indx,
				  Datum dataValue, bool isNull,
				  int arraytyplen, int elmlen, bool elmbyval, char elmalign);
extern Datum array_get_slice(Datum arraydatum, int nSubscripts,
				int *upperIndx, int *lowerIndx,
				int arraytyplen, int elmlen, bool elmbyval, char elmalign);
extern Datum array_set_slice(Datum arraydatum, int nSubscripts,
				int *upperIndx, int *lowerIndx,
				Datum srcArrayDatum, bool isNull,
				int arraytyplen, int elmlen, bool elmbyval, char elmalign);

extern Datum array_ref(ArrayType *array, int nSubscripts, int *indx,
		  int arraytyplen, int elmlen, bool elmbyval, char elmalign,
		  bool *isNull);
extern ArrayType *array_set(ArrayType *array, int nSubscripts, int *indx,
		  Datum dataValue, bool isNull,
		  int arraytyplen, int elmlen, bool elmbyval, char elmalign);

extern Datum array_map(FunctionCallInfo fcinfo, Oid retType,
		  ArrayMapState *amstate);

extern void array_bitmap_copy(bits8 *destbitmap, int destoffset,
				  const bits8 *srcbitmap, int srcoffset,
				  int nitems);

extern ArrayType *construct_array(Datum *elems, int nelems,
				Oid elmtype,
				int elmlen, bool elmbyval, char elmalign);
extern ArrayType *construct_md_array(Datum *elems,
				   bool *nulls,
				   int ndims,
				   int *dims,
				   int *lbs,
				   Oid elmtype, int elmlen, bool elmbyval, char elmalign);
extern ArrayType *construct_empty_array(Oid elmtype);
extern ExpandedArrayHeader *construct_empty_expanded_array(Oid element_type,
							   MemoryContext parentcontext,
							   ArrayMetaState *metacache);
extern void deconstruct_array(ArrayType *array,
				  Oid elmtype,
				  int elmlen, bool elmbyval, char elmalign,
				  Datum **elemsp, bool **nullsp, int *nelemsp);
extern bool array_contains_nulls(ArrayType *array);

extern ArrayBuildState *initArrayResult(Oid element_type,
				MemoryContext rcontext, bool subcontext);
extern ArrayBuildState *accumArrayResult(ArrayBuildState *astate,
				 Datum dvalue, bool disnull,
				 Oid element_type,
				 MemoryContext rcontext);
extern Datum makeArrayResult(ArrayBuildState *astate,
				MemoryContext rcontext);
extern Datum makeMdArrayResult(ArrayBuildState *astate, int ndims,
				  int *dims, int *lbs, MemoryContext rcontext, bool release);

extern ArrayBuildStateArr *initArrayResultArr(Oid array_type, Oid element_type,
				   MemoryContext rcontext, bool subcontext);
extern ArrayBuildStateArr *accumArrayResultArr(ArrayBuildStateArr *astate,
					Datum dvalue, bool disnull,
					Oid array_type,
					MemoryContext rcontext);
extern Datum makeArrayResultArr(ArrayBuildStateArr *astate,
				   MemoryContext rcontext, bool release);

extern ArrayBuildStateAny *initArrayResultAny(Oid input_type,
				   MemoryContext rcontext, bool subcontext);
extern ArrayBuildStateAny *accumArrayResultAny(ArrayBuildStateAny *astate,
					Datum dvalue, bool disnull,
					Oid input_type,
					MemoryContext rcontext);
extern Datum makeArrayResultAny(ArrayBuildStateAny *astate,
				   MemoryContext rcontext, bool release);

extern ArrayIterator array_create_iterator(ArrayType *arr, int slice_ndim, ArrayMetaState *mstate);
extern bool array_iterate(ArrayIterator iterator, Datum *value, bool *isnull);
extern void array_free_iterator(ArrayIterator iterator);

/*
 * prototypes for functions defined in arrayutils.c
 */

extern int	ArrayGetOffset(int n, const int *dim, const int *lb, const int *indx);
extern int	ArrayGetOffset0(int n, const int *tup, const int *scale);
extern int	ArrayGetNItems(int ndim, const int *dims);
extern void mda_get_range(int n, int *span, const int *st, const int *endp);
extern void mda_get_prod(int n, const int *range, int *prod);
extern void mda_get_offset_values(int n, int *dist, const int *prod, const int *span);
extern int	mda_next_tuple(int n, int *curr, const int *span);
extern int32 *ArrayGetIntegerTypmods(ArrayType *arr, int *n);

/*
 * prototypes for functions defined in array_expanded.c
 */
extern Datum expand_array(Datum arraydatum, MemoryContext parentcontext,
			 ArrayMetaState *metacache);
extern ExpandedArrayHeader *DatumGetExpandedArray(Datum d);
extern ExpandedArrayHeader *DatumGetExpandedArrayX(Datum d,
					   ArrayMetaState *metacache);
extern AnyArrayType *DatumGetAnyArray(Datum d);
extern void deconstruct_expanded_array(ExpandedArrayHeader *eah);

/*
 * prototypes for functions defined in array_userfuncs.c
 */
extern Datum array_append(PG_FUNCTION_ARGS);
extern Datum array_prepend(PG_FUNCTION_ARGS);
extern Datum array_cat(PG_FUNCTION_ARGS);

extern ArrayType *create_singleton_array(FunctionCallInfo fcinfo,
					   Oid element_type,
					   Datum element,
					   bool isNull,
					   int ndims);

extern Datum array_agg_transfn(PG_FUNCTION_ARGS);
extern Datum array_agg_finalfn(PG_FUNCTION_ARGS);
extern Datum array_agg_array_transfn(PG_FUNCTION_ARGS);
extern Datum array_agg_array_finalfn(PG_FUNCTION_ARGS);

extern Datum array_position(PG_FUNCTION_ARGS);
extern Datum array_position_start(PG_FUNCTION_ARGS);
extern Datum array_positions(PG_FUNCTION_ARGS);

/*
 * prototypes for functions defined in array_typanalyze.c
 */
extern Datum array_typanalyze(PG_FUNCTION_ARGS);

#endif   /* ARRAY_H */
