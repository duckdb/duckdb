//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2012 EMC Corp.
//
//	@filename:
//		gpdbwrappers.h
//
//	@doc:
//		Definition of GPDB function wrappers
//
//	@test:
//
//
//---------------------------------------------------------------------------
#ifndef GPDB_gpdbwrappers_H
#define GPDB_gpdbwrappers_H

extern "C" {
#include "postgres.h"

#include "access/attnum.h"
#include "parser/parse_coerce.h"
#include "utils/faultinjector.h"
#include "utils/lsyscache.h"
}

#include "gpos/types.h"

// fwd declarations
typedef struct SysScanDescData *SysScanDesc;
struct TypeCacheEntry;
typedef struct NumericData *Numeric;
typedef struct HeapTupleData *HeapTuple;
typedef struct RelationData *Relation;
struct Value;
typedef struct TupleDescData *TupleDesc;
struct Query;
typedef struct ScanKeyData *ScanKey;
struct Bitmapset;
struct Plan;
struct ListCell;
struct TargetEntry;
struct Expr;
struct ExtTableEntry;
struct ForeignScan;
struct Uri;
struct CdbComponentDatabases;
struct StringInfoData;
typedef StringInfoData *StringInfo;
struct LogicalIndexes;
struct LogicalIndexInfo;
struct ParseState;
struct DefElem;
struct GpPolicy;
struct PartitionSelector;
struct Motion;
struct Var;
struct Const;
struct ArrayExpr;

#include "gpopt/utils/RelationWrapper.h"

namespace gpdb
{
// convert datum to bool
bool BoolFromDatum(Datum d);

// convert bool to datum
Datum DatumFromBool(bool b);

// convert datum to char
char CharFromDatum(Datum d);

// convert char to datum
Datum DatumFromChar(char c);

// convert datum to int8
int8 Int8FromDatum(Datum d);

// convert int8 to datum
Datum DatumFromInt8(int8 i8);

// convert datum to uint8
uint8 Uint8FromDatum(Datum d);

// convert uint8 to datum
Datum DatumFromUint8(uint8 ui8);

// convert datum to int16
int16 Int16FromDatum(Datum d);

// convert int16 to datum
Datum DatumFromInt16(int16 i16);

// convert datum to uint16
uint16 Uint16FromDatum(Datum d);

// convert uint16 to datum
Datum DatumFromUint16(uint16 ui16);

// convert datum to int32
int32 Int32FromDatum(Datum d);

// convert int32 to datum
Datum DatumFromInt32(int32 i32);

// convert datum to uint32
uint32 lUint32FromDatum(Datum d);

// convert uint32 to datum
Datum DatumFromUint32(uint32 ui32);

// convert datum to int64
int64 Int64FromDatum(Datum d);

// convert int64 to datum
Datum DatumFromInt64(int64 i64);

// convert datum to uint64
uint64 Uint64FromDatum(Datum d);

// convert uint64 to datum
Datum DatumFromUint64(uint64 ui64);

// convert datum to oid
Oid OidFromDatum(Datum d);

// convert datum to generic object with pointer handle
void *PointerFromDatum(Datum d);

// convert datum to float4
float4 Float4FromDatum(Datum d);

// convert datum to float8
float8 Float8FromDatum(Datum d);

// convert pointer to datum
Datum DatumFromPointer(const void *p);

// does an aggregate exist with the given oid
bool AggregateExists(Oid oid);

// add member to Bitmapset
Bitmapset *BmsAddMember(Bitmapset *a, int x);

// create a copy of an object
void *CopyObject(void *from);

// datum size
Size DatumSize(Datum value, bool type_by_val, int type_len);

bool ExpressionReturnsSet(Node *clause);

// expression type
Oid ExprType(Node *expr);

// expression type modifier
int32 ExprTypeMod(Node *expr);

// expression collation
Oid ExprCollation(Node *expr);

// expression collation - GPDB_91_MERGE_FIXME
Oid TypeCollation(Oid type);

// extract nodes with specific tag from a plan tree
List *ExtractNodesPlan(Plan *pl, int node_tag, bool descend_into_subqueries);

// extract nodes with specific tag from an expression tree
List *ExtractNodesExpression(Node *node, int node_tag,
							 bool descend_into_subqueries);

// intermediate result type of given aggregate
Oid GetAggIntermediateResultType(Oid aggid);

// Identify the specific datatypes passed to an aggregate call.
int GetAggregateArgTypes(Aggref *aggref, Oid *inputTypes);

// Identify the transition state value's datatype for an aggregate call.
Oid ResolveAggregateTransType(Oid aggfnoid, Oid aggtranstype, Oid *inputTypes,
							  int numArguments);

// replace Vars that reference JOIN outputs with references to the original
// relation variables instead
Query *FlattenJoinAliasVar(Query *query, gpos::ULONG query_level);

// is aggregate ordered
bool IsOrderedAgg(Oid aggid);

// does aggregate have a combine function (and serial/deserial functions, if needed)
bool IsAggPartialCapable(Oid aggid);

// intermediate result type of given aggregate
Oid GetAggregate(const char *agg, Oid type_oid);

// array type oid
Oid GetArrayType(Oid typid);

// attribute stats slot
bool GetAttrStatsSlot(AttStatsSlot *sslot, HeapTuple statstuple, int reqkind,
					  Oid reqop, int flags);

// free attribute stats slot
void FreeAttrStatsSlot(AttStatsSlot *sslot);

// attribute statistics
HeapTuple GetAttStats(Oid relid, AttrNumber attnum);

// does a function exist with the given oid
bool FunctionExists(Oid oid);

// is the given function an allowed lossy cast for PS
bool IsFuncAllowedForPartitionSelection(Oid funcid);

// is the given function strict
bool FuncStrict(Oid funcid);

// does this preserve the NDVs of its inputs?
bool IsFuncNDVPreserving(Oid funcid);

// stability property of given function
char FuncStability(Oid funcid);

// data access property of given function
char FuncDataAccess(Oid funcid);

// exec location property of given function
char FuncExecLocation(Oid funcid);

// trigger name
char *GetTriggerName(Oid triggerid);

// trigger relid
Oid GetTriggerRelid(Oid triggerid);

// trigger funcid
Oid GetTriggerFuncid(Oid triggerid);

// trigger type
int32 GetTriggerType(Oid triggerid);

// is trigger enabled
bool IsTriggerEnabled(Oid triggerid);

// does trigger exist
bool TriggerExists(Oid oid);

// does check constraint exist
bool CheckConstraintExists(Oid check_constraint_oid);

// check constraint name
char *GetCheckConstraintName(Oid check_constraint_oid);

// check constraint relid
Oid GetCheckConstraintRelid(Oid check_constraint_oid);

// check constraint expression tree
Node *PnodeCheckConstraint(Oid check_constraint_oid);

// get the list of check constraints for a given relation
List *GetCheckConstraintOids(Oid rel_oid);

// part constraint expression tree
Node *GetRelationPartConstraints(Relation rel);

// get the cast function for the specified source and destination types
bool GetCastFunc(Oid src_oid, Oid dest_oid, bool *is_binary_coercible,
				 Oid *cast_fn_oid, CoercionPathType *pathtype);

// get type of operator
unsigned int GetComparisonType(Oid op_oid);

// get scalar comparison between given types
Oid GetComparisonOperator(Oid left_oid, Oid right_oid, unsigned int cmpt);

// get equality operator for given type
Oid GetEqualityOp(Oid type_oid);

// get equality operator for given ordering op (i.e. < or >)
Oid GetEqualityOpForOrderingOp(Oid opno, bool *reverse);

// get ordering operator for given equality op (i.e. =)
Oid GetOrderingOpForEqualityOp(Oid opno, bool *reverse);

// function name
char *GetFuncName(Oid funcid);

// output argument types of the given function
List *GetFuncOutputArgTypes(Oid funcid);

// argument types of the given function
List *GetFuncArgTypes(Oid funcid);

// does a function return a set of rows
bool GetFuncRetset(Oid funcid);

// return type of the given function
Oid GetFuncRetType(Oid funcid);

// commutator operator of the given operator
Oid GetCommutatorOp(Oid opno);

// inverse operator of the given operator
Oid GetInverseOp(Oid opno);

// function oid corresponding to the given operator oid
RegProcedure GetOpFunc(Oid opno);

// operator name
char *GetOpName(Oid opno);

#if 0
	// parts of a partitioned table
	bool IsLeafPartition(Oid oid);

	// partition table has an external partition
	bool HasExternalPartition(Oid oid);

	// find the oid of the root partition given partition oid belongs to
	Oid GetRootPartition(Oid oid);
	
	// partition attributes
	List *GetPartitionAttrs(Oid oid);

	// get partition keys and kinds ordered by partition level
	void GetOrderedPartKeysAndKinds(Oid oid, List **pkeys, List **pkinds);

	/* GPDB_12_MERGE_FIXME: mergings stats not yet implemented with new partitioning implementation */
	// parts of a partitioned table
	//PartitionNode *GetParts(Oid relid, int16 level, Oid parent, bool inctemplate, bool includesubparts);
#endif

// keys of the relation with the given oid
List *GetRelationKeys(Oid relid);

// relid of a composite type
Oid GetTypeRelid(Oid typid);

// name of the type with the given oid
char *GetTypeName(Oid typid);

// number of GP segments
int GetGPSegmentCount(void);

// heap attribute is null
bool HeapAttIsNull(HeapTuple tup, int attnum);

// free heap tuple
void FreeHeapTuple(HeapTuple htup);

// does an index exist with the given oid
bool IndexExists(Oid oid);

// get the default hash opclass for type
Oid GetDefaultDistributionOpclassForType(Oid typid);

// get the column-definition hash opclass for type
Oid GetColumnDefOpclassForType(List *opclassName, Oid typid);

// get the default hash opfamily for type
Oid GetDefaultDistributionOpfamilyForType(Oid typid);

// get the hash function in an opfamily for given datatype
Oid GetHashProcInOpfamily(Oid opfamily, Oid typid);

// is the given hash function a legacy cdbhash function?
Oid IsLegacyCdbHashFunction(Oid hashfunc);

// is the given hash function a legacy cdbhash function?
Oid GetLegacyCdbHashOpclassForBaseType(Oid typid);

// return the operator family the given opclass belongs to
Oid GetOpclassFamily(Oid opclass);

// append an element to a list
List *LAppend(List *list, void *datum);

// append an integer to a list
List *LAppendInt(List *list, int datum);

// append an oid to a list
List *LAppendOid(List *list, Oid datum);

// prepend a new element to the list
List *LPrepend(void *datum, List *list);

// prepend an integer to the list
List *LPrependInt(int datum, List *list);

// prepend an oid to a list
List *LPrependOid(Oid datum, List *list);

// concatenate lists
List *ListConcat(List *list1, List *list2);

// copy list
List *ListCopy(List *list);

// first cell in a list
ListCell *ListHead(List *l);

// last cell in a list
ListCell *ListTail(List *l);

// number of items in a list
uint32 ListLength(List *l);

// return the nth element in a list of pointers
void *ListNth(List *list, int n);

// return the nth element in a list of ints
int ListNthInt(List *list, int n);

// return the nth element in a list of oids
Oid ListNthOid(List *list, int n);

// check whether the given oid is a member of the given list
bool ListMemberOid(List *list, Oid oid);

// free list
void ListFree(List *list);

// deep free of a list
void ListFreeDeep(List *list);

#if 0
	// does a partition table have an appendonly child
	bool IsAppendOnlyPartitionTable(Oid root_oid);

	// does a multi-level partitioned table have uniform partitioning hierarchy
	bool IsMultilevelPartitionUniform(Oid root_oid);
#endif

// lookup type cache
TypeCacheEntry *LookupTypeCache(Oid type_id, int flags);

// create a value node for a string
Value *MakeStringValue(char *str);

// create a value node for an integer
Value *MakeIntegerValue(long i);

// create a constant of type int4
Node *MakeIntConst(int32 intValue);

// create a bool constant
Node *MakeBoolConst(bool value, bool isnull);

// make a NULL constant of the given type
Node *MakeNULLConst(Oid type_oid);

// make a NULL constant of the given type
Node *MakeSegmentFilterExpr(int segid);

// create a new target entry
TargetEntry *MakeTargetEntry(Expr *expr, AttrNumber resno, char *resname,
							 bool resjunk);

// create a new var node
Var *MakeVar(Index varno, AttrNumber varattno, Oid vartype, int32 vartypmod,
			 Index varlevelsup);

// memory allocation functions
void *MemCtxtAllocZeroAligned(MemoryContext context, Size size);
void *MemCtxtAllocZero(MemoryContext context, Size size);
void *MemCtxtRealloc(void *pointer, Size size);
void *GPDBAlloc(Size size);
void GPDBFree(void *ptr);

// create a duplicate of the given string in the given memory context
char *MemCtxtStrdup(MemoryContext context, const char *string);

// similar to ereport for logging messages
void GpdbEreportImpl(int xerrcode, int severitylevel, const char *xerrmsg,
					 const char *xerrhint, const char *filename, int lineno,
					 const char *funcname);
#define GpdbEreport(xerrcode, severitylevel, xerrmsg, xerrhint)       \
	gpdb::GpdbEreportImpl(xerrcode, severitylevel, xerrmsg, xerrhint, \
						  __FILE__, __LINE__, PG_FUNCNAME_MACRO)

// string representation of a node
char *NodeToString(void *obj);

// node representation from a string
Node *StringToNode(char *string);

// return the default value of the type
Node *GetTypeDefault(Oid typid);

// convert numeric to double; if out of range, return +/- HUGE_VAL
double NumericToDoubleNoOverflow(Numeric num);

// is the given Numeric value NaN?
bool NumericIsNan(Numeric num);

// convert time-related datums to double for stats purpose
double ConvertTimeValueToScalar(Datum datum, Oid typid);

// convert network-related datums to double for stats purpose
double ConvertNetworkToScalar(Datum datum, Oid typid);

// is the given operator hash-joinable
bool IsOpHashJoinable(Oid opno, Oid inputtype);

// is the given operator merge-joinable
bool IsOpMergeJoinable(Oid opno, Oid inputtype);

// is the given operator strict
bool IsOpStrict(Oid opno);

// does it preserve the NDVs of its inputs
bool IsOpNDVPreserving(Oid opno);

// get input types for a given operator
void GetOpInputTypes(Oid opno, Oid *lefttype, Oid *righttype);

// does an operator exist with the given oid
bool OperatorExists(Oid oid);

// expression tree walker
bool WalkExpressionTree(Node *node, bool (*walker)(), void *context);

// query or expression tree walker
bool WalkQueryOrExpressionTree(Node *node, bool (*walker)(), void *context,
							   int flags);

// modify the components of a Query tree
Query *MutateQueryTree(Query *query, Node *(*mutator)(), void *context,
					   int flags);

// modify an expression tree
Node *MutateExpressionTree(Node *node, Node *(*mutator)(), void *context);

// modify a query or an expression tree
Node *MutateQueryOrExpressionTree(Node *node, Node *(*mutator)(), void *context,
								  int flags);

#if 0
	// check whether the part with the given oid is the root of a partition table
	bool RelPartIsRoot(Oid relid);
	
	// check whether the part with the given oid is an interior subpartition
	bool RelPartIsInterior(Oid relid);
#endif

bool RelIsPartitioned(Oid relid);

// check whether table with the given oid is a regular table and not part of a partitioned table
bool RelPartIsNone(Oid relid);

// check whether a relation is inherited
bool HasSubclassSlow(Oid rel_oid);

// check whether table with given oid is an external table
bool RelIsExternalTable(Oid relid);

// return the distribution policy of a relation; if the table is partitioned
// and the parts are distributed differently, return Random distribution
GpPolicy *GetDistributionPolicy(Relation rel);

#if 0
    // return true if the table is partitioned and hash-distributed, and one of  
    // the child partitions is randomly distributed
    gpos::BOOL IsChildPartDistributionMismatched(Relation rel);

    // return true if the table is partitioned and any of the child partitions
    // have a trigger of the given type
    gpos::BOOL ChildPartHasTriggers(Oid oid, int trigger_type);
#endif

// does a relation exist with the given oid
bool RelationExists(Oid oid);

// estimate the relation size using the real number of blocks and tuple density
void CdbEstimateRelationSize(RelOptInfo *relOptInfo, Relation rel,
							 int32 *attr_widths, BlockNumber *pages,
							 double *tuples, double *allvisfrac);
double CdbEstimatePartitionedNumTuples(Relation rel);

// close the given relation
void CloseRelation(Relation rel);

#if 0
	// return the logical indexes for a partitioned table
	LogicalIndexes *GetLogicalPartIndexes(Oid oid);
	
	// return the logical info structure for a given logical index oid
	LogicalIndexInfo *GetLogicalIndexInfo(Oid root_oid, Oid index_oid);
#endif

// return a list of index oids for a given relation
List *GetRelationIndexes(Relation relation);

// build an array of triggers for this relation
void BuildRelationTriggers(Relation rel);

// get relation with given oid
RelationWrapper GetRelation(Oid rel_oid);

// get external table entry with given oid
ExtTableEntry *GetExternalTableEntry(Oid rel_oid);

// get ForeignScan node to scan an external table
ForeignScan *CreateForeignScanForExternalTable(Oid rel_oid, Index scanrelid,
											   List *qual, List *targetlist);

// return the first member of the given targetlist whose expression is
// equal to the given expression, or NULL if no such member exists
TargetEntry *FindFirstMatchingMemberInTargetList(Node *node, List *targetlist);

// return a list of members of the given targetlist whose expression is
// equal to the given expression, or NULL if no such member exists
List *FindMatchingMembersInTargetList(Node *node, List *targetlist);

// check if two gpdb objects are equal
bool Equals(void *p1, void *p2);

// does a type exist with the given oid
bool TypeExists(Oid oid);

// check whether a type is composite
bool IsCompositeType(Oid typid);

bool IsTextRelatedType(Oid typid);

// get integer value from an Integer value node
int GetIntFromValue(Node *node);

// parse external table URI
Uri *ParseExternalTableUri(const char *uri);

// returns ComponentDatabases
CdbComponentDatabases *GetComponentDatabases(void);

// compare two strings ignoring case
int StrCmpIgnoreCase(const char *s1, const char *s2);

// construct random segment map
bool *ConstructRandomSegMap(int total_primaries, int total_to_skip);

// create an empty 'StringInfoData' & return a pointer to it
StringInfo MakeStringInfo(void);

// append the two given strings to the StringInfo object
void AppendStringInfo(StringInfo str, const char *str1, const char *str2);

// look for the given node tags in the given tree and return the index of
// the first one found, or -1 if there are none
int FindNodes(Node *node, List *nodeTags);

// GPDB_91_MERGE_FIXME: collation
// look for nodes with non-default collation; returns 1 if any exist, -1 otherwise
int CheckCollation(Node *node);

Node *CoerceToCommonType(ParseState *pstate, Node *node, Oid target_type,
						 const char *context);

// replace any polymorphic type with correct data type deduced from input arguments
bool ResolvePolymorphicArgType(int numargs, Oid *argtypes, char *argmodes,
							   FuncExpr *call_expr);

// hash a list of const values with GPDB's hash function
int32 CdbHashConstList(List *constants, int num_segments, Oid *hashfuncs);

// get a random segment number
unsigned int CdbHashRandomSeg(int num_segments);

// check permissions on range table
void CheckRTPermissions(List *rtable);

// throw an error if table has update triggers.
bool HasUpdateTriggers(Oid relid);

// get index operator family properties
void IndexOpProperties(Oid opno, Oid opfamily, int *strategy, Oid *subtype);

// get oids of families this operator belongs to
List *GetOpFamiliesForScOp(Oid opno);

// get the OID of hash equality operator(s) compatible with the given op
Oid GetCompatibleHashOpFamily(Oid opno);

// get the OID of legacy hash equality operator(s) compatible with the given op
Oid GetCompatibleLegacyHashOpFamily(Oid opno);

// get oids of op classes for the index keys
List *GetIndexOpFamilies(Oid index_oid);

// get oids of op classes for the merge join
List *GetMergeJoinOpFamilies(Oid opno);

// returns the result of evaluating 'expr' as an Expr. Caller keeps ownership of 'expr'
// and takes ownership of the result
Expr *EvaluateExpr(Expr *expr, Oid result_type, int32 typmod);

// interpret the value of "With oids" option from a list of defelems
bool InterpretOidsOption(List *options, bool allowOids);

// extract string value from defelem's value
char *DefGetString(DefElem *defelem);

// transform array Const to an ArrayExpr
Expr *TransformArrayConstToArrayExpr(Const *constant);

// transform array Const to an ArrayExpr
Node *EvalConstExpressions(Node *node);

#if 0
	// static partition selection given a PartitionSelector node
	SelectedParts *RunStaticPartitionSelection(PartitionSelector *ps);
#endif

#ifdef FAULT_INJECTOR
// simple fault injector used by COptTasks.cpp to inject GPDB fault
FaultInjectorType_e InjectFaultInOptTasks(const char *fault_name);
#endif

#if 0
	// return the number of leaf partition for a given table oid
	gpos::ULONG CountLeafPartTables(Oid oidRelation);
#endif

// Does the metadata cache need to be reset (because of a catalog
// table has been changed?)
bool MDCacheNeedsReset(void);

// returns true if a query cancel is requested in GPDB
bool IsAbortRequested(void);

// Given the type OID, get the typelem (InvalidOid if not an array type).
Oid GetElementType(Oid array_type_oid);

GpPolicy *MakeGpPolicy(GpPolicyType ptype, int nattrs, int numsegments);


uint32 HashChar(Datum d);

uint32 HashBpChar(Datum d);

uint32 HashText(Datum d);

uint32 HashName(Datum d);

uint32 UUIDHash(Datum d);

void *GPDBMemoryContextAlloc(MemoryContext context, Size size);

MemoryContext GPDBAllocSetContextCreate();

void GPDBMemoryContextDelete(MemoryContext context);

void GPDBLockRelationOid(Oid reloid, int lockmode);

}  //namespace gpdb

#define ForEach(cell, l) \
	for ((cell) = gpdb::ListHead(l); (cell) != NULL; (cell) = lnext(cell))

#define ForBoth(cell1, list1, cell2, list2)                                \
	for ((cell1) = gpdb::ListHead(list1), (cell2) = gpdb::ListHead(list2); \
		 (cell1) != NULL && (cell2) != NULL;                               \
		 (cell1) = lnext(cell1), (cell2) = lnext(cell2))

#define ForThree(cell1, list1, cell2, list2, cell3, list3)                 \
	for ((cell1) = gpdb::ListHead(list1), (cell2) = gpdb::ListHead(list2), \
		(cell3) = gpdb::ListHead(list3);                                   \
		 (cell1) != NULL && (cell2) != NULL && (cell3) != NULL;            \
		 (cell1) = lnext(cell1), (cell2) = lnext(cell2),                   \
		(cell3) = lnext(cell3))

#define ForEachWithCount(cell, list, counter)                          \
	for ((cell) = gpdb::ListHead(list), (counter) = 0; (cell) != NULL; \
		 (cell) = lnext(cell), ++(counter))

#define ListMake1(x1) gpdb::LPrepend(x1, NIL)

#define ListMake2(x1, x2) gpdb::LPrepend(x1, ListMake1(x2))

#define ListMake1Int(x1) gpdb::LPrependInt(x1, NIL)

#define ListMake1Oid(x1) gpdb::LPrependOid(x1, NIL)
#define ListMake2Oid(x1, x2) gpdb::LPrependOid(x1, ListMake1Oid(x2))

#define LInitial(l) lfirst(gpdb::ListHead(l))

#define LInitialOID(l) lfirst_oid(gpdb::ListHead(l))

#define Palloc0Fast(sz)                                              \
	(MemSetTest(0, (sz))                                             \
		 ? gpdb::MemCtxtAllocZeroAligned(CurrentMemoryContext, (sz)) \
		 : gpdb::MemCtxtAllocZero(CurrentMemoryContext, (sz)))

#ifdef __GNUC__

/* With GCC, we can use a compound statement within an expression */
#define NewNode(size, tag)                                                \
	({                                                                    \
		Node *_result;                                                    \
		AssertMacro((size) >= sizeof(Node)); /* need the tag, at least */ \
		_result = (Node *) Palloc0Fast(size);                             \
		_result->type = (tag);                                            \
		_result;                                                          \
	})
#else

/*
 *	There is no way to dereference the palloc'ed pointer to assign the
 *	tag, and also return the pointer itself, so we need a holder variable.
 *	Fortunately, this macro isn't recursive so we just define
 *	a global variable for this purpose.
 */
extern PGDLLIMPORT Node *newNodeMacroHolder;

#define NewNode(size, tag)                                             \
	(AssertMacro((size) >= sizeof(Node)), /* need the tag, at least */ \
	 newNodeMacroHolder = (Node *) Palloc0Fast(size),                  \
	 newNodeMacroHolder->type = (tag), newNodeMacroHolder)
#endif	// __GNUC__

#define MakeNode(_type_) ((_type_ *) NewNode(sizeof(_type_), T_##_type_))

#define PStrDup(str) gpdb::MemCtxtStrdup(CurrentMemoryContext, (str))

#endif	// !GPDB_gpdbwrappers_H

// EOF
