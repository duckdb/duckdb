/*-------------------------------------------------------------------------
 *
 * params.h
 *	  Support for finding the values associated with Param nodes.
 *
 *
 * Portions Copyright (c) 1996-2015, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/nodes/params.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef PARAMS_H
#define PARAMS_H

/* To avoid including a pile of parser headers, reference ParseState thus: */
struct ParseState;


/* ----------------
 *	  ParamListInfo
 *
 *	  ParamListInfo arrays are used to pass parameters into the executor
 *	  for parameterized plans.  Each entry in the array defines the value
 *	  to be substituted for a PARAM_EXTERN parameter.  The "paramid"
 *	  of a PARAM_EXTERN Param can range from 1 to numParams.
 *
 *	  Although parameter numbers are normally consecutive, we allow
 *	  ptype == InvalidOid to signal an unused array entry.
 *
 *	  pflags is a flags field.  Currently the only used bit is:
 *	  PARAM_FLAG_CONST signals the planner that it may treat this parameter
 *	  as a constant (i.e., generate a plan that works only for this value
 *	  of the parameter).
 *
 *	  There are two hook functions that can be associated with a ParamListInfo
 *	  array to support dynamic parameter handling.  First, if paramFetch
 *	  isn't null and the executor requires a value for an invalid parameter
 *	  (one with ptype == InvalidOid), the paramFetch hook is called to give
 *	  it a chance to fill in the parameter value.  Second, a parserSetup
 *	  hook can be supplied to re-instantiate the original parsing hooks if
 *	  a query needs to be re-parsed/planned (as a substitute for supposing
 *	  that the current ptype values represent a fixed set of parameter types).

 *	  Although the data structure is really an array, not a list, we keep
 *	  the old typedef name to avoid unnecessary code changes.
 * ----------------
 */

#define PARAM_FLAG_CONST	0x0001		/* parameter is constant */

typedef struct ParamExternData
{
	Datum		value;			/* parameter value */
	bool		isnull;			/* is it NULL? */
	uint16		pflags;			/* flag bits, see above */
	Oid			ptype;			/* parameter's datatype, or 0 */
} ParamExternData;

typedef struct ParamListInfoData *ParamListInfo;

typedef void (*ParamFetchHook) (ParamListInfo params, int paramid);

typedef void (*ParserSetupHook) (struct ParseState *pstate, void *arg);

typedef struct ParamListInfoData
{
	ParamFetchHook paramFetch;	/* parameter fetch hook */
	void	   *paramFetchArg;
	ParserSetupHook parserSetup;	/* parser setup hook */
	void	   *parserSetupArg;
	int			numParams;		/* number of ParamExternDatas following */
	ParamExternData params[FLEXIBLE_ARRAY_MEMBER];
}	ParamListInfoData;


/* ----------------
 *	  ParamExecData
 *
 *	  ParamExecData entries are used for executor internal parameters
 *	  (that is, values being passed into or out of a sub-query).  The
 *	  paramid of a PARAM_EXEC Param is a (zero-based) index into an
 *	  array of ParamExecData records, which is referenced through
 *	  es_param_exec_vals or ecxt_param_exec_vals.
 *
 *	  If execPlan is not NULL, it points to a SubPlanState node that needs
 *	  to be executed to produce the value.  (This is done so that we can have
 *	  lazy evaluation of InitPlans: they aren't executed until/unless a
 *	  result value is needed.)	Otherwise the value is assumed to be valid
 *	  when needed.
 * ----------------
 */

typedef struct ParamExecData
{
	void	   *execPlan;		/* should be "SubPlanState *" */
	Datum		value;
	bool		isnull;
} ParamExecData;


/* Functions found in src/backend/nodes/params.c */
extern ParamListInfo copyParamList(ParamListInfo from);

#endif   /* PARAMS_H */
