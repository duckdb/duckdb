/* A Bison parser, made by GNU Bison 3.5.1.  */

/* Bison interface for Yacc-like parsers in C

   Copyright (C) 1984, 1989-1990, 2000-2015, 2018-2020 Free Software Foundation,
   Inc.

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

/* Undocumented macros, especially those whose name start with YY_,
   are private implementation details.  Do not rely on them.  */

#ifndef YY_BASE_YY_THIRD_PARTY_LIBPG_QUERY_GRAMMAR_GRAMMAR_OUT_HPP_INCLUDED
# define YY_BASE_YY_THIRD_PARTY_LIBPG_QUERY_GRAMMAR_GRAMMAR_OUT_HPP_INCLUDED
/* Debug traces.  */
#ifndef YYDEBUG
# define YYDEBUG 0
#endif
#if YYDEBUG
extern int base_yydebug;
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
    LAMBDA_ARROW = 270,
    LESS_EQUALS = 271,
    GREATER_EQUALS = 272,
    NOT_EQUALS = 273,
    ABORT_P = 274,
    ABSOLUTE_P = 275,
    ACCESS = 276,
    ACTION = 277,
    ADD_P = 278,
    ADMIN = 279,
    AFTER = 280,
    AGGREGATE = 281,
    ALL = 282,
    ALSO = 283,
    ALTER = 284,
    ALWAYS = 285,
    ANALYSE = 286,
    ANALYZE = 287,
    AND = 288,
    ANY = 289,
    ARRAY = 290,
    AS = 291,
    ASC_P = 292,
    ASSERTION = 293,
    ASSIGNMENT = 294,
    ASYMMETRIC = 295,
    AT = 296,
    ATTACH = 297,
    ATTRIBUTE = 298,
    AUTHORIZATION = 299,
    BACKWARD = 300,
    BEFORE = 301,
    BEGIN_P = 302,
    BETWEEN = 303,
    BIGINT = 304,
    BINARY = 305,
    BIT = 306,
    BOOLEAN_P = 307,
    BOTH = 308,
    BY = 309,
    CACHE = 310,
    CALL_P = 311,
    CALLED = 312,
    CASCADE = 313,
    CASCADED = 314,
    CASE = 315,
    CAST = 316,
    CATALOG_P = 317,
    CHAIN = 318,
    CHAR_P = 319,
    CHARACTER = 320,
    CHARACTERISTICS = 321,
    CHECK_P = 322,
    CHECKPOINT = 323,
    CLASS = 324,
    CLOSE = 325,
    CLUSTER = 326,
    COALESCE = 327,
    COLLATE = 328,
    COLLATION = 329,
    COLUMN = 330,
    COLUMNS = 331,
    COMMENT = 332,
    COMMENTS = 333,
    COMMIT = 334,
    COMMITTED = 335,
    CONCURRENTLY = 336,
    CONFIGURATION = 337,
    CONFLICT = 338,
    CONNECTION = 339,
    CONSTRAINT = 340,
    CONSTRAINTS = 341,
    CONTENT_P = 342,
    CONTINUE_P = 343,
    CONVERSION_P = 344,
    COPY = 345,
    COST = 346,
    CREATE_P = 347,
    CROSS = 348,
    CSV = 349,
    CUBE = 350,
    CURRENT_P = 351,
    CURRENT_CATALOG = 352,
    CURRENT_DATE = 353,
    CURRENT_ROLE = 354,
    CURRENT_SCHEMA = 355,
    CURRENT_TIME = 356,
    CURRENT_TIMESTAMP = 357,
    CURRENT_USER = 358,
    CURSOR = 359,
    CYCLE = 360,
    DATA_P = 361,
    DATABASE = 362,
    DAY_P = 363,
    DAYS_P = 364,
    DEALLOCATE = 365,
    DEC = 366,
    DECIMAL_P = 367,
    DECLARE = 368,
    DEFAULT = 369,
    DEFAULTS = 370,
    DEFERRABLE = 371,
    DEFERRED = 372,
    DEFINER = 373,
    DELETE_P = 374,
    DELIMITER = 375,
    DELIMITERS = 376,
    DEPENDS = 377,
    DESC_P = 378,
    DESCRIBE = 379,
    DETACH = 380,
    DICTIONARY = 381,
    DISABLE_P = 382,
    DISCARD = 383,
    DISTINCT = 384,
    DO = 385,
    DOCUMENT_P = 386,
    DOMAIN_P = 387,
    DOUBLE_P = 388,
    DROP = 389,
    EACH = 390,
    ELSE = 391,
    ENABLE_P = 392,
    ENCODING = 393,
    ENCRYPTED = 394,
    END_P = 395,
    ENUM_P = 396,
    ESCAPE = 397,
    EVENT = 398,
    EXCEPT = 399,
    EXCLUDE = 400,
    EXCLUDING = 401,
    EXCLUSIVE = 402,
    EXECUTE = 403,
    EXISTS = 404,
    EXPLAIN = 405,
    EXPORT_P = 406,
    EXTENSION = 407,
    EXTERNAL = 408,
    EXTRACT = 409,
    FALSE_P = 410,
    FAMILY = 411,
    FETCH = 412,
    FILTER = 413,
    FIRST_P = 414,
    FLOAT_P = 415,
    FOLLOWING = 416,
    FOR = 417,
    FORCE = 418,
    FOREIGN = 419,
    FORWARD = 420,
    FREEZE = 421,
    FROM = 422,
    FULL = 423,
    FUNCTION = 424,
    FUNCTIONS = 425,
    GENERATED = 426,
    GLOB = 427,
    GLOBAL = 428,
    GRANT = 429,
    GRANTED = 430,
    GROUP_P = 431,
    GROUPING = 432,
    HANDLER = 433,
    HAVING = 434,
    HEADER_P = 435,
    HOLD = 436,
    HOUR_P = 437,
    HOURS_P = 438,
    IDENTITY_P = 439,
    IF_P = 440,
    ILIKE = 441,
    IMMEDIATE = 442,
    IMMUTABLE = 443,
    IMPLICIT_P = 444,
    IMPORT_P = 445,
    IN_P = 446,
    INCLUDING = 447,
    INCREMENT = 448,
    INDEX = 449,
    INDEXES = 450,
    INHERIT = 451,
    INHERITS = 452,
    INITIALLY = 453,
    INLINE_P = 454,
    INNER_P = 455,
    INOUT = 456,
    INPUT_P = 457,
    INSENSITIVE = 458,
    INSERT = 459,
    INSTEAD = 460,
    INT_P = 461,
    INTEGER = 462,
    INTERSECT = 463,
    INTERVAL = 464,
    INTO = 465,
    INVOKER = 466,
    IS = 467,
    ISNULL = 468,
    ISOLATION = 469,
    JOIN = 470,
    KEY = 471,
    LABEL = 472,
    LANGUAGE = 473,
    LARGE_P = 474,
    LAST_P = 475,
    LATERAL_P = 476,
    LEADING = 477,
    LEAKPROOF = 478,
    LEFT = 479,
    LEVEL = 480,
    LIKE = 481,
    LIMIT = 482,
    LISTEN = 483,
    LOAD = 484,
    LOCAL = 485,
    LOCALTIME = 486,
    LOCALTIMESTAMP = 487,
    LOCATION = 488,
    LOCK_P = 489,
    LOCKED = 490,
    LOGGED = 491,
    MACRO = 492,
    MAP = 493,
    MAPPING = 494,
    MATCH = 495,
    MATERIALIZED = 496,
    MAXVALUE = 497,
    METHOD = 498,
    MICROSECOND_P = 499,
    MICROSECONDS_P = 500,
    MILLISECOND_P = 501,
    MILLISECONDS_P = 502,
    MINUTE_P = 503,
    MINUTES_P = 504,
    MINVALUE = 505,
    MODE = 506,
    MONTH_P = 507,
    MONTHS_P = 508,
    MOVE = 509,
    NAME_P = 510,
    NAMES = 511,
    NATIONAL = 512,
    NATURAL = 513,
    NCHAR = 514,
    NEW = 515,
    NEXT = 516,
    NO = 517,
    NONE = 518,
    NOT = 519,
    NOTHING = 520,
    NOTIFY = 521,
    NOTNULL = 522,
    NOWAIT = 523,
    NULL_P = 524,
    NULLIF = 525,
    NULLS_P = 526,
    NUMERIC = 527,
    OBJECT_P = 528,
    OF = 529,
    OFF = 530,
    OFFSET = 531,
    OIDS = 532,
    OLD = 533,
    ON = 534,
    ONLY = 535,
    OPERATOR = 536,
    OPTION = 537,
    OPTIONS = 538,
    OR = 539,
    ORDER = 540,
    ORDINALITY = 541,
    OUT_P = 542,
    OUTER_P = 543,
    OVER = 544,
    OVERLAPS = 545,
    OVERLAY = 546,
    OVERRIDING = 547,
    OWNED = 548,
    OWNER = 549,
    PARALLEL = 550,
    PARSER = 551,
    PARTIAL = 552,
    PARTITION = 553,
    PASSING = 554,
    PASSWORD = 555,
    PERCENT = 556,
    PLACING = 557,
    PLANS = 558,
    POLICY = 559,
    POSITION = 560,
    PRAGMA_P = 561,
    PRECEDING = 562,
    PRECISION = 563,
    PREPARE = 564,
    PREPARED = 565,
    PRESERVE = 566,
    PRIMARY = 567,
    PRIOR = 568,
    PRIVILEGES = 569,
    PROCEDURAL = 570,
    PROCEDURE = 571,
    PROGRAM = 572,
    PUBLICATION = 573,
    QUOTE = 574,
    RANGE = 575,
    READ_P = 576,
    REAL = 577,
    REASSIGN = 578,
    RECHECK = 579,
    RECURSIVE = 580,
    REF = 581,
    REFERENCES = 582,
    REFERENCING = 583,
    REFRESH = 584,
    REINDEX = 585,
    RELATIVE_P = 586,
    RELEASE = 587,
    RENAME = 588,
    REPEATABLE = 589,
    REPLACE = 590,
    REPLICA = 591,
    RESET = 592,
    RESTART = 593,
    RESTRICT = 594,
    RETURNING = 595,
    RETURNS = 596,
    REVOKE = 597,
    RIGHT = 598,
    ROLE = 599,
    ROLLBACK = 600,
    ROLLUP = 601,
    ROW = 602,
    ROWS = 603,
    RULE = 604,
    SAMPLE = 605,
    SAVEPOINT = 606,
    SCHEMA = 607,
    SCHEMAS = 608,
    SCROLL = 609,
    SEARCH = 610,
    SECOND_P = 611,
    SECONDS_P = 612,
    SECURITY = 613,
    SELECT = 614,
    SEQUENCE = 615,
    SEQUENCES = 616,
    SERIALIZABLE = 617,
    SERVER = 618,
    SESSION = 619,
    SESSION_USER = 620,
    SET = 621,
    SETOF = 622,
    SETS = 623,
    SHARE = 624,
    SHOW = 625,
    SIMILAR = 626,
    SIMPLE = 627,
    SKIP = 628,
    SMALLINT = 629,
    SNAPSHOT = 630,
    SOME = 631,
    SQL_P = 632,
    STABLE = 633,
    STANDALONE_P = 634,
    START = 635,
    STATEMENT = 636,
    STATISTICS = 637,
    STDIN = 638,
    STDOUT = 639,
    STORAGE = 640,
    STRICT_P = 641,
    STRIP_P = 642,
    STRUCT = 643,
    SUBSCRIPTION = 644,
    SUBSTRING = 645,
    SUMMARIZE = 646,
    SYMMETRIC = 647,
    SYSID = 648,
    SYSTEM_P = 649,
    TABLE = 650,
    TABLES = 651,
    TABLESAMPLE = 652,
    TABLESPACE = 653,
    TEMP = 654,
    TEMPLATE = 655,
    TEMPORARY = 656,
    TEXT_P = 657,
    THEN = 658,
    TIME = 659,
    TIMESTAMP = 660,
    TO = 661,
    TRAILING = 662,
    TRANSACTION = 663,
    TRANSFORM = 664,
    TREAT = 665,
    TRIGGER = 666,
    TRIM = 667,
    TRUE_P = 668,
    TRUNCATE = 669,
    TRUSTED = 670,
    TRY_CAST = 671,
    TYPE_P = 672,
    TYPES_P = 673,
    UNBOUNDED = 674,
    UNCOMMITTED = 675,
    UNENCRYPTED = 676,
    UNION = 677,
    UNIQUE = 678,
    UNKNOWN = 679,
    UNLISTEN = 680,
    UNLOGGED = 681,
    UNTIL = 682,
    UPDATE = 683,
    USER = 684,
    USING = 685,
    VACUUM = 686,
    VALID = 687,
    VALIDATE = 688,
    VALIDATOR = 689,
    VALUE_P = 690,
    VALUES = 691,
    VARCHAR = 692,
    VARIADIC = 693,
    VARYING = 694,
    VERBOSE = 695,
    VERSION_P = 696,
    VIEW = 697,
    VIEWS = 698,
    VOLATILE = 699,
    WHEN = 700,
    WHERE = 701,
    WHITESPACE_P = 702,
    WINDOW = 703,
    WITH = 704,
    WITHIN = 705,
    WITHOUT = 706,
    WORK = 707,
    WRAPPER = 708,
    WRITE_P = 709,
    XML_P = 710,
    XMLATTRIBUTES = 711,
    XMLCONCAT = 712,
    XMLELEMENT = 713,
    XMLEXISTS = 714,
    XMLFOREST = 715,
    XMLNAMESPACES = 716,
    XMLPARSE = 717,
    XMLPI = 718,
    XMLROOT = 719,
    XMLSERIALIZE = 720,
    XMLTABLE = 721,
    YEAR_P = 722,
    YEARS_P = 723,
    YES_P = 724,
    ZONE = 725,
    NOT_LA = 726,
    NULLS_LA = 727,
    WITH_LA = 728,
    POSTFIXOP = 729,
    UMINUS = 730
  };
#endif

/* Value type.  */
#if ! defined YYSTYPE && ! defined YYSTYPE_IS_DECLARED
union YYSTYPE
{
#line 14 "third_party/libpg_query/grammar/grammar.y"

	core_YYSTYPE		core_yystype;
	/* these fields must match core_YYSTYPE: */
	int					ival;
	char				*str;
	const char			*keyword;
	const char          *conststr;

	char				chr;
	bool				boolean;
	PGJoinType			jtype;
	PGDropBehavior		dbehavior;
	PGOnCommitAction		oncommit;
	PGList				*list;
	PGNode				*node;
	PGValue				*value;
	PGObjectType			objtype;
	PGTypeName			*typnam;
	PGObjectWithArgs		*objwithargs;
	PGDefElem				*defelt;
	PGSortBy				*sortby;
	PGWindowDef			*windef;
	PGJoinExpr			*jexpr;
	PGIndexElem			*ielem;
	PGAlias				*alias;
	PGRangeVar			*range;
	PGIntoClause			*into;
	PGWithClause			*with;
	PGInferClause			*infer;
	PGOnConflictClause	*onconflict;
	PGAIndices			*aind;
	PGResTarget			*target;
	PGInsertStmt			*istmt;
	PGVariableSetStmt		*vsetstmt;
	PGOverridingKind       override;
	PGSortByDir            sortorder;
	PGSortByNulls          nullorder;
	PGLockClauseStrength lockstrength;
	PGLockWaitPolicy lockwaitpolicy;
	PGSubLinkType subquerytype;
	PGViewCheckOption viewcheckoption;

#line 576 "third_party/libpg_query/grammar/grammar_out.hpp"

};
typedef union YYSTYPE YYSTYPE;
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



int base_yyparse (core_yyscan_t yyscanner);

#endif /* !YY_BASE_YY_THIRD_PARTY_LIBPG_QUERY_GRAMMAR_GRAMMAR_OUT_HPP_INCLUDED  */
