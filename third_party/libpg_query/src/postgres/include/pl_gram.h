/* A Bison parser, made by GNU Bison 3.0.2.  */

/* Bison interface for Yacc-like parsers in C

   Copyright (C) 1984, 1989-1990, 2000-2013 Free Software Foundation, Inc.

   This program is free software: you can redistribute it and/or modify
   it under the terms of the GNU General Public License as published by
   the Free Software Foundation, either version 3 of the License, or
   (at your option) any later version.

   This program is distributed in the hope that it will be useful,
   but WITHOUT ANY WARRANTY; without even the implied warranty of
   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
   GNU General Public License for more details.

   You should have received a copy of the GNU General Public License
   along with this program.  If not, see <http://www.gnu.org/licenses/>.  */

/* As a special exception, you may create a larger work that contains
   part or all of the Bison parser skeleton and distribute that work
   under terms of your choice, so long as that work isn't itself a
   parser generator using the skeleton or a modified version thereof
   as a parser skeleton.  Alternatively, if you modify or redistribute
   the parser skeleton itself, you may (at your option) remove this
   special exception, which will cause the skeleton and the resulting
   Bison output files to be licensed under the GNU General Public
   License without this special exception.

   This special exception was added by the Free Software Foundation in
   version 2.2 of Bison.  */

#ifndef YY_PLPGSQL_YY_PL_GRAM_H_INCLUDED
# define YY_PLPGSQL_YY_PL_GRAM_H_INCLUDED
/* Debug traces.  */
#ifndef YYDEBUG
# define YYDEBUG 0
#endif
#if YYDEBUG
extern int plpgsql_yydebug;
#endif

/* Token type.  */
#ifndef YYTOKENTYPE
# define YYTOKENTYPE
  enum yytokentype
  {
    IDENT = 258,
    FCONST = 259,
    SCONST = 260,
    BCONST = 261,
    XCONST = 262,
    Op = 263,
    ICONST = 264,
    PARAM = 265,
    TYPECAST = 266,
    DOT_DOT = 267,
    COLON_EQUALS = 268,
    EQUALS_GREATER = 269,
    LESS_EQUALS = 270,
    GREATER_EQUALS = 271,
    NOT_EQUALS = 272,
    T_WORD = 273,
    T_CWORD = 274,
    T_DATUM = 275,
    LESS_LESS = 276,
    GREATER_GREATER = 277,
    K_ABSOLUTE = 278,
    K_ALIAS = 279,
    K_ALL = 280,
    K_ARRAY = 281,
    K_ASSERT = 282,
    K_BACKWARD = 283,
    K_BEGIN = 284,
    K_BY = 285,
    K_CASE = 286,
    K_CLOSE = 287,
    K_COLLATE = 288,
    K_COLUMN = 289,
    K_COLUMN_NAME = 290,
    K_CONSTANT = 291,
    K_CONSTRAINT = 292,
    K_CONSTRAINT_NAME = 293,
    K_CONTINUE = 294,
    K_CURRENT = 295,
    K_CURSOR = 296,
    K_DATATYPE = 297,
    K_DEBUG = 298,
    K_DECLARE = 299,
    K_DEFAULT = 300,
    K_DETAIL = 301,
    K_DIAGNOSTICS = 302,
    K_DUMP = 303,
    K_ELSE = 304,
    K_ELSIF = 305,
    K_END = 306,
    K_ERRCODE = 307,
    K_ERROR = 308,
    K_EXCEPTION = 309,
    K_EXECUTE = 310,
    K_EXIT = 311,
    K_FETCH = 312,
    K_FIRST = 313,
    K_FOR = 314,
    K_FOREACH = 315,
    K_FORWARD = 316,
    K_FROM = 317,
    K_GET = 318,
    K_HINT = 319,
    K_IF = 320,
    K_IN = 321,
    K_INFO = 322,
    K_INSERT = 323,
    K_INTO = 324,
    K_IS = 325,
    K_LAST = 326,
    K_LOG = 327,
    K_LOOP = 328,
    K_MESSAGE = 329,
    K_MESSAGE_TEXT = 330,
    K_MOVE = 331,
    K_NEXT = 332,
    K_NO = 333,
    K_NOT = 334,
    K_NOTICE = 335,
    K_NULL = 336,
    K_OPEN = 337,
    K_OPTION = 338,
    K_OR = 339,
    K_PERFORM = 340,
    K_PG_CONTEXT = 341,
    K_PG_DATATYPE_NAME = 342,
    K_PG_EXCEPTION_CONTEXT = 343,
    K_PG_EXCEPTION_DETAIL = 344,
    K_PG_EXCEPTION_HINT = 345,
    K_PRINT_STRICT_PARAMS = 346,
    K_PRIOR = 347,
    K_QUERY = 348,
    K_RAISE = 349,
    K_RELATIVE = 350,
    K_RESULT_OID = 351,
    K_RETURN = 352,
    K_RETURNED_SQLSTATE = 353,
    K_REVERSE = 354,
    K_ROW_COUNT = 355,
    K_ROWTYPE = 356,
    K_SCHEMA = 357,
    K_SCHEMA_NAME = 358,
    K_SCROLL = 359,
    K_SLICE = 360,
    K_SQLSTATE = 361,
    K_STACKED = 362,
    K_STRICT = 363,
    K_TABLE = 364,
    K_TABLE_NAME = 365,
    K_THEN = 366,
    K_TO = 367,
    K_TYPE = 368,
    K_USE_COLUMN = 369,
    K_USE_VARIABLE = 370,
    K_USING = 371,
    K_VARIABLE_CONFLICT = 372,
    K_WARNING = 373,
    K_WHEN = 374,
    K_WHILE = 375
  };
#endif

/* Value type.  */
#if ! defined YYSTYPE && ! defined YYSTYPE_IS_DECLARED
typedef union YYSTYPE YYSTYPE;
union YYSTYPE
{
#line 117 "pl_gram.y" /* yacc.c:1909  */

		core_YYSTYPE			core_yystype;
		/* these fields must match core_YYSTYPE: */
		int						ival;
		char					*str;
		const char				*keyword;

		PLword					word;
		PLcword					cword;
		PLwdatum				wdatum;
		bool					boolean;
		Oid						oid;
		struct
		{
			char *name;
			int  lineno;
		}						varname;
		struct
		{
			char *name;
			int  lineno;
			PLpgSQL_datum   *scalar;
			PLpgSQL_rec		*rec;
			PLpgSQL_row		*row;
		}						forvariable;
		struct
		{
			char *label;
			int  n_initvars;
			int  *initvarnos;
		}						declhdr;
		struct
		{
			List *stmts;
			char *end_label;
			int   end_label_location;
		}						loop_body;
		List					*list;
		PLpgSQL_type			*dtype;
		PLpgSQL_datum			*datum;
		PLpgSQL_var				*var;
		PLpgSQL_expr			*expr;
		PLpgSQL_stmt			*stmt;
		PLpgSQL_condition		*condition;
		PLpgSQL_exception		*exception;
		PLpgSQL_exception_block	*exception_block;
		PLpgSQL_nsitem			*nsitem;
		PLpgSQL_diag_item		*diagitem;
		PLpgSQL_stmt_fetch		*fetch;
		PLpgSQL_case_when		*casewhen;

#line 227 "pl_gram.h" /* yacc.c:1909  */
};
# define YYSTYPE_IS_TRIVIAL 1
# define YYSTYPE_IS_DECLARED 1
#endif

/* Location type.  */
#if ! defined YYLTYPE && ! defined YYLTYPE_IS_DECLARED
typedef struct YYLTYPE YYLTYPE;
struct YYLTYPE
{
  int first_line;
  int first_column;
  int last_line;
  int last_column;
};
# define YYLTYPE_IS_DECLARED 1
# define YYLTYPE_IS_TRIVIAL 1
#endif


extern __thread  YYSTYPE plpgsql_yylval;
extern __thread  YYLTYPE plpgsql_yylloc;
int plpgsql_yyparse (void);

#endif /* !YY_PLPGSQL_YY_PL_GRAM_H_INCLUDED  */
