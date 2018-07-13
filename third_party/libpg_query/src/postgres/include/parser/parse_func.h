/*-------------------------------------------------------------------------
 *
 * parse_func.h
 *
 *
 *
 * Portions Copyright (c) 1996-2015, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/parser/parse_func.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef PARSER_FUNC_H
#define PARSER_FUNC_H

#include "catalog/namespace.h"
#include "parser/parse_node.h"


/* Result codes for func_get_detail */
typedef enum
{
	FUNCDETAIL_NOTFOUND,		/* no matching function */
	FUNCDETAIL_MULTIPLE,		/* too many matching functions */
	FUNCDETAIL_NORMAL,			/* found a matching regular function */
	FUNCDETAIL_AGGREGATE,		/* found a matching aggregate function */
	FUNCDETAIL_WINDOWFUNC,		/* found a matching window function */
	FUNCDETAIL_COERCION			/* it's a type coercion request */
} FuncDetailCode;


extern Node *ParseFuncOrColumn(ParseState *pstate, List *funcname, List *fargs,
				  FuncCall *fn, int location);

extern FuncDetailCode func_get_detail(List *funcname,
				List *fargs, List *fargnames,
				int nargs, Oid *argtypes,
				bool expand_variadic, bool expand_defaults,
				Oid *funcid, Oid *rettype,
				bool *retset, int *nvargs, Oid *vatype,
				Oid **true_typeids, List **argdefaults);

extern int func_match_argtypes(int nargs,
					Oid *input_typeids,
					FuncCandidateList raw_candidates,
					FuncCandidateList *candidates);

extern FuncCandidateList func_select_candidate(int nargs,
					  Oid *input_typeids,
					  FuncCandidateList candidates);

extern void make_fn_arguments(ParseState *pstate,
				  List *fargs,
				  Oid *actual_arg_types,
				  Oid *declared_arg_types);

extern const char *funcname_signature_string(const char *funcname, int nargs,
						  List *argnames, const Oid *argtypes);
extern const char *func_signature_string(List *funcname, int nargs,
					  List *argnames, const Oid *argtypes);

extern Oid LookupFuncName(List *funcname, int nargs, const Oid *argtypes,
			   bool noError);
extern Oid LookupFuncNameTypeNames(List *funcname, List *argtypes,
						bool noError);
extern Oid LookupAggNameTypeNames(List *aggname, List *argtypes,
					   bool noError);

#endif   /* PARSE_FUNC_H */
