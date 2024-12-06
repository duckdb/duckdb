/*#define YYDEBUG 1*/
/*-------------------------------------------------------------------------
 *
 * gram.y
 *	  POSTGRESQL BISON rules/actions
 *
 * Portions Copyright (c) 1996-2017, PostgreSQL Global Development PGGroup
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * IDENTIFICATION
 *	  src/backend/parser/gram.y
 *
 * HISTORY
 *	  AUTHOR			DATE			MAJOR EVENT
 *	  Andrew Yu			Sept, 1994		POSTQUEL to SQL conversion
 *	  Andrew Yu			Oct, 1994		lispy code conversion
 *
 * NOTES
 *	  CAPITALS are used to represent terminal symbols.
 *	  non-capitals are used to represent non-terminals.
 *
 *	  In general, nothing in this file should initiate database accesses
 *	  nor depend on changeable state (such as SET variables).  If you do
 *	  database accesses, your code will fail when we have aborted the
 *	  current transaction and are just parsing commands to find the next
 *	  ROLLBACK or COMMIT.  If you make use of SET variables, then you
 *	  will do the wrong thing in multi-query strings like this:
 *			SET constraint_exclusion TO off; SELECT * FROM foo;
 *	  because the entire string is parsed by gram.y before the SET gets
 *	  executed.  Anything that depends on the database or changeable state
 *	  should be handled during parse analysis so that it happens at the
 *	  right time not the wrong time.
 *
 * WARNINGS
 *	  If you use a list, make sure the datum is a node so that the printing
 *	  routines work.
 *
 *	  Sometimes we assign constants to makeStrings. Make sure we don't free
 *	  those.
 *
 *-------------------------------------------------------------------------
 */
#include "pg_functions.hpp"
#include <string.h>

#include <ctype.h>
#include <limits.h>

#include "nodes/makefuncs.hpp"
#include "nodes/nodeFuncs.hpp"
#include "parser/gramparse.hpp"
#include "parser/parser.hpp"
#include "utils/datetime.hpp"

namespace duckdb_libpgquery {
#define DEFAULT_SCHEMA "main"

/*
 * Location tracking support --- simpler than bison's default, since we only
 * want to track the start position not the end position of each nonterminal.
 */
#define YYLLOC_DEFAULT(Current, Rhs, N) \
	do { \
		if ((N) > 0) \
			(Current) = (Rhs)[1]; \
		else \
			(Current) = (-1); \
	} while (0)

/*
 * The above macro assigns -1 (unknown) as the parse location of any
 * nonterminal that was reduced from an empty rule, or whose leftmost
 * component was reduced from an empty rule.  This is problematic
 * for nonterminals defined like
 *		OptFooList: / * EMPTY * / { ... } | OptFooList Foo { ... } ;
 * because we'll set -1 as the location during the first reduction and then
 * copy it during each subsequent reduction, leaving us with -1 for the
 * location even when the list is not empty.  To fix that, do this in the
 * action for the nonempty rule(s):
 *		if (@$ < 0) @$ = @2;
 * (Although we have many nonterminals that follow this pattern, we only
 * bother with fixing @$ like this when the nonterminal's parse location
 * is actually referenced in some rule.)
 *
 * A cleaner answer would be to make YYLLOC_DEFAULT scan all the Rhs
 * locations until it's found one that's not -1.  Then we'd get a correct
 * location for any nonterminal that isn't entirely empty.  But this way
 * would add overhead to every rule reduction, and so far there's not been
 * a compelling reason to pay that overhead.
 */

/*
 * Bison doesn't allocate anything that needs to live across parser calls,
 * so we can easily have it use palloc instead of malloc.  This prevents
 * memory leaks if we error out during parsing.  Note this only works with
 * bison >= 2.0.  However, in bison 1.875 the default is to use alloca()
 * if possible, so there's not really much problem anyhow, at least if
 * you're building with gcc.
 */
#define YYMALLOC palloc
#define YYFREE   pfree
#define YYINITDEPTH 1000

/* yields an integer bitmask of these flags: */
#define CAS_NOT_DEFERRABLE			0x01
#define CAS_DEFERRABLE				0x02
#define CAS_INITIALLY_IMMEDIATE		0x04
#define CAS_INITIALLY_DEFERRED		0x08
#define CAS_NOT_VALID				0x10
#define CAS_NO_INHERIT				0x20


#define parser_yyerror(msg)  scanner_yyerror(msg, yyscanner)
#define parser_errposition(pos)  scanner_errposition(pos, yyscanner)

#if YYBISON == 1
// explicitly define stack growing support
// yacc cannot handle stack growing by default YYLTYPE is overriden - which the Postgres parser overrides with an `int`
// so we need to copy these definitions here explicitly
/* A type that is properly aligned for any stack member.  */
union yyalloc
{
  short int yyss;
  YYSTYPE yyvs;
  YYLTYPE yyls;
};

/* The size of the maximum gap between one aligned stack and the next.  */
# define YYSTACK_GAP_MAXIMUM (sizeof (union yyalloc) - 1)

/* The size of an array large to enough to hold all stacks, each with
   N elements.  */
# define YYSTACK_BYTES(N) \
     ((N) * (sizeof (yytype_int16) + sizeof (YYSTYPE) + sizeof (YYLTYPE)) \
      + 2 * YYSTACK_GAP_MAXIMUM)

/* Copy COUNT objects from FROM to TO.  The source and destination do
   not overlap.  */
# ifndef YYCOPY
#  if defined __GNUC__ && 1 < __GNUC__
#   define YYCOPY(To, From, Count) \
      __builtin_memcpy (To, From, (Count) * sizeof (*(From)))
#  else
#   define YYCOPY(To, From, Count)		\
      do					\
	{					\
	  YYSIZE_T yyi;				\
	  for (yyi = 0; yyi < (Count); yyi++)	\
	    (To)[yyi] = (From)[yyi];		\
	}					\
      while (YYID (0))
#  endif
# endif

/* Relocate STACK from its old location to the new one.  The
   local variables YYSIZE and YYSTACKSIZE give the old and new number of
   elements in the stack, and YYPTR gives the new location of the
   stack.  Advance YYPTR to a properly aligned location for the next
   stack.  */
# define YYSTACK_RELOCATE(Stack)					\
    do									\
      {									\
	YYSIZE_T yynewbytes;						\
	YYCOPY (&yyptr->Stack, Stack, yysize);				\
	Stack = &yyptr->Stack;						\
	yynewbytes = yystacksize * sizeof (*Stack) + YYSTACK_GAP_MAXIMUM; \
	yyptr += yynewbytes / sizeof (*yyptr);				\
      }									\
    while (YYID (0))
#endif

static void base_yyerror(YYLTYPE *yylloc, core_yyscan_t yyscanner,
						 const char *msg);
static PGRawStmt *makeRawStmt(PGNode *stmt, int stmt_location);
static void updateRawStmtEnd(PGRawStmt *rs, int end_location);
static PGNode *makeColumnRef(char *colname, PGList *indirection,
						   int location, core_yyscan_t yyscanner);
static PGNode *makeTypeCast(PGNode *arg, PGTypeName *tpname, int trycast, int location);
static PGNode *makeStringConst(char *str, int location);
static PGNode *makeStringConstCast(char *str, int location, PGTypeName *tpname);
static PGNode *makeIntervalNode(char *str, int location, PGList *typmods);
static PGNode *makeIntervalNode(int val, int location, PGList *typmods);
static PGNode *makeIntervalNode(PGNode *arg, int location, PGList *typmods);
static PGNode *makeSampleSize(PGValue *sample_size, bool is_percentage);
static PGNode *makeSampleOptions(PGNode *sample_size, char *method, int *seed, int location);
static PGNode *makeIntConst(int val, int location);
static PGNode *makeFloatConst(char *str, int location);
static PGNode *makeBitStringConst(char *str, int location);
static PGNode *makeNullAConst(int location);
static PGNode *makeAConst(PGValue *v, int location);
static PGNode *makeBoolAConst(bool state, int location);
static PGNode *makeParamRef(int number, int location);
static PGNode *makeNamedParamRef(char* name, int location);
static void check_qualified_name(PGList *names, core_yyscan_t yyscanner);
static PGList *check_func_name(PGList *names, core_yyscan_t yyscanner);
static PGList *check_indirection(PGList *indirection, core_yyscan_t yyscanner);
static void insertSelectOptions(PGSelectStmt *stmt,
								PGList *sortClause, PGList *lockingClause,
								PGNode *limitOffset, PGNode *limitCount,
								PGWithClause *withClause,
								core_yyscan_t yyscanner);
static PGNode *makeSetOp(PGSetOperation op, bool all, PGNode *larg, PGNode *rarg);
static PGNode *doNegate(PGNode *n, int location);
static void doNegateFloat(PGValue *v);
static PGNode *makeAndExpr(PGNode *lexpr, PGNode *rexpr, int location);
static PGNode *makeOrExpr(PGNode *lexpr, PGNode *rexpr, int location);
static PGNode *makeNotExpr(PGNode *expr, int location);
static void SplitColQualList(PGList *qualList,
							 PGList **constraintList, PGCollateClause **collClause,
							 core_yyscan_t yyscanner);
static void processCASbits(int cas_bits, int location, const char *constrType,
			   bool *deferrable, bool *initdeferred, bool *not_valid,
			   bool *no_inherit, core_yyscan_t yyscanner);
static PGNode *makeRecursiveViewSelect(char *relname, PGList *aliases, PGNode *query);
static PGNode *makeLimitPercent(PGNode *limit_percent);
