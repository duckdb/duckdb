/*-------------------------------------------------------------------------
 *
 * expandeddatum.h
 *	  Declarations for access to "expanded" value representations.
 *
 * Complex data types, particularly container types such as arrays and
 * records, usually have on-disk representations that are compact but not
 * especially convenient to modify.  What's more, when we do modify them,
 * having to recopy all the rest of the value can be extremely inefficient.
 * Therefore, we provide a notion of an "expanded" representation that is used
 * only in memory and is optimized more for computation than storage.
 * The format appearing on disk is called the data type's "flattened"
 * representation, since it is required to be a contiguous blob of bytes --
 * but the type can have an expanded representation that is not.  Data types
 * must provide means to translate an expanded representation back to
 * flattened form.
 *
 * An expanded object is meant to survive across multiple operations, but
 * not to be enormously long-lived; for example it might be a local variable
 * in a PL/pgSQL procedure.  So its extra bulk compared to the on-disk format
 * is a worthwhile trade-off.
 *
 * References to expanded objects are a type of TOAST pointer.
 * Because of longstanding conventions in Postgres, this means that the
 * flattened form of such an object must always be a varlena object.
 * Fortunately that's no restriction in practice.
 *
 * There are actually two kinds of TOAST pointers for expanded objects:
 * read-only and read-write pointers.  Possession of one of the latter
 * authorizes a function to modify the value in-place rather than copying it
 * as would normally be required.  Functions should always return a read-write
 * pointer to any new expanded object they create.  Functions that modify an
 * argument value in-place must take care that they do not corrupt the old
 * value if they fail partway through.
 *
 *
 * Portions Copyright (c) 1996-2015, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/utils/expandeddatum.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef EXPANDEDDATUM_H
#define EXPANDEDDATUM_H

/* Size of an EXTERNAL datum that contains a pointer to an expanded object */
#define EXPANDED_POINTER_SIZE (VARHDRSZ_EXTERNAL + sizeof(varatt_expanded))

/*
 * "Methods" that must be provided for any expanded object.
 *
 * get_flat_size: compute space needed for flattened representation (total,
 * including header).
 *
 * flatten_into: construct flattened representation in the caller-allocated
 * space at *result, of size allocated_size (which will always be the result
 * of a preceding get_flat_size call; it's passed for cross-checking).
 *
 * The flattened representation must be a valid in-line, non-compressed,
 * 4-byte-header varlena object.
 *
 * Note: construction of a heap tuple from an expanded datum calls
 * get_flat_size twice, so it's worthwhile to make sure that that doesn't
 * incur too much overhead.
 */
typedef Size (*EOM_get_flat_size_method) (ExpandedObjectHeader *eohptr);
typedef void (*EOM_flatten_into_method) (ExpandedObjectHeader *eohptr,
										  void *result, Size allocated_size);

/* Struct of function pointers for an expanded object's methods */
typedef struct ExpandedObjectMethods
{
	EOM_get_flat_size_method get_flat_size;
	EOM_flatten_into_method flatten_into;
} ExpandedObjectMethods;

/*
 * Every expanded object must contain this header; typically the header
 * is embedded in some larger struct that adds type-specific fields.
 *
 * It is presumed that the header object and all subsidiary data are stored
 * in eoh_context, so that the object can be freed by deleting that context,
 * or its storage lifespan can be altered by reparenting the context.
 * (In principle the object could own additional resources, such as malloc'd
 * storage, and use a memory context reset callback to free them upon reset or
 * deletion of eoh_context.)
 *
 * We set up two TOAST pointers within the standard header, one read-write
 * and one read-only.  This allows functions to return either kind of pointer
 * without making an additional allocation, and in particular without worrying
 * whether a separately palloc'd object would have sufficient lifespan.
 * But note that these pointers are just a convenience; a pointer object
 * appearing somewhere else would still be legal.
 *
 * The typedef declaration for this appears in postgres.h.
 */
struct ExpandedObjectHeader
{
	/* Phony varlena header */
	int32		vl_len_;		/* always EOH_HEADER_MAGIC, see below */

	/* Pointer to methods required for object type */
	const ExpandedObjectMethods *eoh_methods;

	/* Memory context containing this header and subsidiary data */
	MemoryContext eoh_context;

	/* Standard R/W TOAST pointer for this object is kept here */
	char		eoh_rw_ptr[EXPANDED_POINTER_SIZE];

	/* Standard R/O TOAST pointer for this object is kept here */
	char		eoh_ro_ptr[EXPANDED_POINTER_SIZE];
};

/*
 * Particularly for read-only functions, it is handy to be able to work with
 * either regular "flat" varlena inputs or expanded inputs of the same data
 * type.  To allow determining which case an argument-fetching function has
 * returned, the first int32 of an ExpandedObjectHeader always contains -1
 * (EOH_HEADER_MAGIC to the code).  This works since no 4-byte-header varlena
 * could have that as its first 4 bytes.  Caution: we could not reliably tell
 * the difference between an ExpandedObjectHeader and a short-header object
 * with this trick.  However, it works fine if the argument fetching code
 * always returns either a 4-byte-header flat object or an expanded object.
 */
#define EOH_HEADER_MAGIC (-1)
#define VARATT_IS_EXPANDED_HEADER(PTR) \
	(((ExpandedObjectHeader *) (PTR))->vl_len_ == EOH_HEADER_MAGIC)

/*
 * Generic support functions for expanded objects.
 * (More of these might be worth inlining later.)
 */

#define EOHPGetRWDatum(eohptr)	PointerGetDatum((eohptr)->eoh_rw_ptr)
#define EOHPGetRODatum(eohptr)	PointerGetDatum((eohptr)->eoh_ro_ptr)

extern ExpandedObjectHeader *DatumGetEOHP(Datum d);
extern void EOH_init_header(ExpandedObjectHeader *eohptr,
				const ExpandedObjectMethods *methods,
				MemoryContext obj_context);
extern Size EOH_get_flat_size(ExpandedObjectHeader *eohptr);
extern void EOH_flatten_into(ExpandedObjectHeader *eohptr,
				 void *result, Size allocated_size);
extern bool DatumIsReadWriteExpandedObject(Datum d, bool isnull, int16 typlen);
extern Datum MakeExpandedObjectReadOnly(Datum d, bool isnull, int16 typlen);
extern Datum TransferExpandedObject(Datum d, MemoryContext new_parent);
extern void DeleteExpandedObject(Datum d);

#endif   /* EXPANDEDDATUM_H */
