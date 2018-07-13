/*-------------------------------------------------------------------------
 *
 * parse_coerce.h
 *	Routines for type coercion.
 *
 *
 * Portions Copyright (c) 1996-2015, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/parser/parse_coerce.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef PARSE_COERCE_H
#define PARSE_COERCE_H

#include "parser/parse_node.h"


/* Type categories (see TYPCATEGORY_xxx symbols in catalog/pg_type.h) */
typedef char TYPCATEGORY;

/* Result codes for find_coercion_pathway */
typedef enum CoercionPathType
{
	COERCION_PATH_NONE,			/* failed to find any coercion pathway */
	COERCION_PATH_FUNC,			/* apply the specified coercion function */
	COERCION_PATH_RELABELTYPE,	/* binary-compatible cast, no function */
	COERCION_PATH_ARRAYCOERCE,	/* need an ArrayCoerceExpr node */
	COERCION_PATH_COERCEVIAIO	/* need a CoerceViaIO node */
} CoercionPathType;


extern bool IsBinaryCoercible(Oid srctype, Oid targettype);
extern bool IsPreferredType(TYPCATEGORY category, Oid type);
extern TYPCATEGORY TypeCategory(Oid type);

extern Node *coerce_to_target_type(ParseState *pstate,
					  Node *expr, Oid exprtype,
					  Oid targettype, int32 targettypmod,
					  CoercionContext ccontext,
					  CoercionForm cformat,
					  int location);
extern bool can_coerce_type(int nargs, Oid *input_typeids, Oid *target_typeids,
				CoercionContext ccontext);
extern Node *coerce_type(ParseState *pstate, Node *node,
			Oid inputTypeId, Oid targetTypeId, int32 targetTypeMod,
			CoercionContext ccontext, CoercionForm cformat, int location);
extern Node *coerce_to_domain(Node *arg, Oid baseTypeId, int32 baseTypeMod,
				 Oid typeId,
				 CoercionForm cformat, int location,
				 bool hideInputCoercion,
				 bool lengthCoercionDone);

extern Node *coerce_to_boolean(ParseState *pstate, Node *node,
				  const char *constructName);
extern Node *coerce_to_specific_type(ParseState *pstate, Node *node,
						Oid targetTypeId,
						const char *constructName);

extern int parser_coercion_errposition(ParseState *pstate,
							int coerce_location,
							Node *input_expr);

extern Oid select_common_type(ParseState *pstate, List *exprs,
				   const char *context, Node **which_expr);
extern Node *coerce_to_common_type(ParseState *pstate, Node *node,
					  Oid targetTypeId,
					  const char *context);

extern bool check_generic_type_consistency(Oid *actual_arg_types,
							   Oid *declared_arg_types,
							   int nargs);
extern Oid enforce_generic_type_consistency(Oid *actual_arg_types,
								 Oid *declared_arg_types,
								 int nargs,
								 Oid rettype,
								 bool allow_poly);
extern Oid resolve_generic_type(Oid declared_type,
					 Oid context_actual_type,
					 Oid context_declared_type);

extern CoercionPathType find_coercion_pathway(Oid targetTypeId,
					  Oid sourceTypeId,
					  CoercionContext ccontext,
					  Oid *funcid);
extern CoercionPathType find_typmod_coercion_function(Oid typeId,
							  Oid *funcid);

#endif   /* PARSE_COERCE_H */
