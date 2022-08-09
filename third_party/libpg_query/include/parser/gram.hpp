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
    POWER_OF = 270,
    LAMBDA_ARROW = 271,
    DOUBLE_ARROW = 272,
    LESS_EQUALS = 273,
    GREATER_EQUALS = 274,
    NOT_EQUALS = 275,
    ABORT_P = 276,
    ABSOLUTE_P = 277,
    ACCESS = 278,
    ACTION = 279,
    ADD_P = 280,
    ADMIN = 281,
    AFTER = 282,
    AGGREGATE = 283,
    ALL = 284,
    ALSO = 285,
    ALTER = 286,
    ALWAYS = 287,
    ANALYSE = 288,
    ANALYZE = 289,
    AND = 290,
    ANY = 291,
    ARRAY = 292,
    AS = 293,
    ASC_P = 294,
    ASSERTION = 295,
    ASSIGNMENT = 296,
    ASYMMETRIC = 297,
    AT = 298,
    ATTACH = 299,
    ATTRIBUTE = 300,
    AUTHORIZATION = 301,
    BACKWARD = 302,
    BEFORE = 303,
    BEGIN_P = 304,
    BETWEEN = 305,
    BIGINT = 306,
    BINARY = 307,
    BIT = 308,
    BOOLEAN_P = 309,
    BOTH = 310,
    BY = 311,
    CACHE = 312,
    CALL_P = 313,
    CALLED = 314,
    CASCADE = 315,
    CASCADED = 316,
    CASE = 317,
    CAST = 318,
    CATALOG_P = 319,
    CHAIN = 320,
    CHAR_P = 321,
    CHARACTER = 322,
    CHARACTERISTICS = 323,
    CHECK_P = 324,
    CHECKPOINT = 325,
    CLASS = 326,
    CLOSE = 327,
    CLUSTER = 328,
    COALESCE = 329,
    COLLATE = 330,
    COLLATION = 331,
    COLUMN = 332,
    COLUMNS = 333,
    COMMENT = 334,
    COMMENTS = 335,
    COMMIT = 336,
    COMMITTED = 337,
    COMPRESSION = 338,
    CONCURRENTLY = 339,
    CONFIGURATION = 340,
    CONFLICT = 341,
    CONNECTION = 342,
    CONSTRAINT = 343,
    CONSTRAINTS = 344,
    CONTENT_P = 345,
    CONTINUE_P = 346,
    CONVERSION_P = 347,
    COPY = 348,
    COST = 349,
    CREATE_P = 350,
    CROSS = 351,
    CSV = 352,
    CUBE = 353,
    CURRENT_P = 354,
    CURRENT_CATALOG = 355,
    CURRENT_DATE = 356,
    CURRENT_ROLE = 357,
    CURRENT_SCHEMA = 358,
    CURRENT_TIME = 359,
    CURRENT_TIMESTAMP = 360,
    CURRENT_USER = 361,
    CURSOR = 362,
    CYCLE = 363,
    DATA_P = 364,
    DATABASE = 365,
    DAY_P = 366,
    DAYS_P = 367,
    DEALLOCATE = 368,
    DEC = 369,
    DECIMAL_P = 370,
    DECLARE = 371,
    DEFAULT = 372,
    DEFAULTS = 373,
    DEFERRABLE = 374,
    DEFERRED = 375,
    DEFINER = 376,
    DELETE_P = 377,
    DELIMITER = 378,
    DELIMITERS = 379,
    DEPENDS = 380,
    DESC_P = 381,
    DESCRIBE = 382,
    DETACH = 383,
    DICTIONARY = 384,
    DISABLE_P = 385,
    DISCARD = 386,
    DISTINCT = 387,
    DO = 388,
    DOCUMENT_P = 389,
    DOMAIN_P = 390,
    DOUBLE_P = 391,
    DROP = 392,
    EACH = 393,
    ELSE = 394,
    ENABLE_P = 395,
    ENCODING = 396,
    ENCRYPTED = 397,
    END_P = 398,
    ENUM_P = 399,
    ESCAPE = 400,
    EVENT = 401,
    EXCEPT = 402,
    EXCLUDE = 403,
    EXCLUDING = 404,
    EXCLUSIVE = 405,
    EXECUTE = 406,
    EXISTS = 407,
    EXPLAIN = 408,
    EXPORT_P = 409,
    EXPORT_STATE = 410,
    EXTENSION = 411,
    EXTERNAL = 412,
    EXTRACT = 413,
    FALSE_P = 414,
    FAMILY = 415,
    FETCH = 416,
    FILTER = 417,
    FIRST_P = 418,
    FLOAT_P = 419,
    FOLLOWING = 420,
    FOR = 421,
    FORCE = 422,
    FOREIGN = 423,
    FORWARD = 424,
    FREEZE = 425,
    FROM = 426,
    FULL = 427,
    FUNCTION = 428,
    FUNCTIONS = 429,
    GENERATED = 430,
    GLOB = 431,
    GLOBAL = 432,
    GRANT = 433,
    GRANTED = 434,
    GROUP_P = 435,
    GROUPING = 436,
    GROUPING_ID = 437,
    HANDLER = 438,
    HAVING = 439,
    HEADER_P = 440,
    HOLD = 441,
    HOUR_P = 442,
    HOURS_P = 443,
    IDENTITY_P = 444,
    IF_P = 445,
    IGNORE_P = 446,
    ILIKE = 447,
    IMMEDIATE = 448,
    IMMUTABLE = 449,
    IMPLICIT_P = 450,
    IMPORT_P = 451,
    IN_P = 452,
    INCLUDING = 453,
    INCREMENT = 454,
    INDEX = 455,
    INDEXES = 456,
    INHERIT = 457,
    INHERITS = 458,
    INITIALLY = 459,
    INLINE_P = 460,
    INNER_P = 461,
    INOUT = 462,
    INPUT_P = 463,
    INSENSITIVE = 464,
    INSERT = 465,
    INSTALL = 466,
    INSTEAD = 467,
    INT_P = 468,
    INTEGER = 469,
    INTERSECT = 470,
    INTERVAL = 471,
    INTO = 472,
    INVOKER = 473,
    IS = 474,
    ISNULL = 475,
    ISOLATION = 476,
    JOIN = 477,
    JSON = 478,
    KEY = 479,
    LABEL = 480,
    LANGUAGE = 481,
    LARGE_P = 482,
    LAST_P = 483,
    LATERAL_P = 484,
    LEADING = 485,
    LEAKPROOF = 486,
    LEFT = 487,
    LEVEL = 488,
    LIKE = 489,
    LIMIT = 490,
    LISTEN = 491,
    LOAD = 492,
    LOCAL = 493,
    LOCALTIME = 494,
    LOCALTIMESTAMP = 495,
    LOCATION = 496,
    LOCK_P = 497,
    LOCKED = 498,
    LOGGED = 499,
    MACRO = 500,
    MAP = 501,
    MAPPING = 502,
    MATCH = 503,
    MATERIALIZED = 504,
    MAXVALUE = 505,
    METHOD = 506,
    MICROSECOND_P = 507,
    MICROSECONDS_P = 508,
    MILLISECOND_P = 509,
    MILLISECONDS_P = 510,
    MINUTE_P = 511,
    MINUTES_P = 512,
    MINVALUE = 513,
    MODE = 514,
    MONTH_P = 515,
    MONTHS_P = 516,
    MOVE = 517,
    NAME_P = 518,
    NAMES = 519,
    NATIONAL = 520,
    NATURAL = 521,
    NCHAR = 522,
    NEW = 523,
    NEXT = 524,
    NO = 525,
    NONE = 526,
    NOT = 527,
    NOTHING = 528,
    NOTIFY = 529,
    NOTNULL = 530,
    NOWAIT = 531,
    NULL_P = 532,
    NULLIF = 533,
    NULLS_P = 534,
    NUMERIC = 535,
    OBJECT_P = 536,
    OF = 537,
    OFF = 538,
    OFFSET = 539,
    OIDS = 540,
    OLD = 541,
    ON = 542,
    ONLY = 543,
    OPERATOR = 544,
    OPTION = 545,
    OPTIONS = 546,
    OR = 547,
    ORDER = 548,
    ORDINALITY = 549,
    OUT_P = 550,
    OUTER_P = 551,
    OVER = 552,
    OVERLAPS = 553,
    OVERLAY = 554,
    OVERRIDING = 555,
    OWNED = 556,
    OWNER = 557,
    PARALLEL = 558,
    PARSER = 559,
    PARTIAL = 560,
    PARTITION = 561,
    PASSING = 562,
    PASSWORD = 563,
    PERCENT = 564,
    PLACING = 565,
    PLANS = 566,
    POLICY = 567,
    POSITION = 568,
    PRAGMA_P = 569,
    PRECEDING = 570,
    PRECISION = 571,
    PREPARE = 572,
    PREPARED = 573,
    PRESERVE = 574,
    PRIMARY = 575,
    PRIOR = 576,
    PRIVILEGES = 577,
    PROCEDURAL = 578,
    PROCEDURE = 579,
    PROGRAM = 580,
    PUBLICATION = 581,
    QUALIFY = 582,
    QUOTE = 583,
    RANGE = 584,
    READ_P = 585,
    REAL = 586,
    REASSIGN = 587,
    RECHECK = 588,
    RECURSIVE = 589,
    REF = 590,
    REFERENCES = 591,
    REFERENCING = 592,
    REFRESH = 593,
    REINDEX = 594,
    RELATIVE_P = 595,
    RELEASE = 596,
    RENAME = 597,
    REPEATABLE = 598,
    REPLACE = 599,
    REPLICA = 600,
    RESET = 601,
    RESPECT_P = 602,
    RESTART = 603,
    RESTRICT = 604,
    RETURNING = 605,
    RETURNS = 606,
    REVOKE = 607,
    RIGHT = 608,
    ROLE = 609,
    ROLLBACK = 610,
    ROLLUP = 611,
    ROW = 612,
    ROWS = 613,
    RULE = 614,
    SAMPLE = 615,
    SAVEPOINT = 616,
    SCHEMA = 617,
    SCHEMAS = 618,
    SCROLL = 619,
    SEARCH = 620,
    SECOND_P = 621,
    SECONDS_P = 622,
    SECURITY = 623,
    SELECT = 624,
    SEQUENCE = 625,
    SEQUENCES = 626,
    SERIALIZABLE = 627,
    SERVER = 628,
    SESSION = 629,
    SESSION_USER = 630,
    SET = 631,
    SETOF = 632,
    SETS = 633,
    SHARE = 634,
    SHOW = 635,
    SIMILAR = 636,
    SIMPLE = 637,
    SKIP = 638,
    SMALLINT = 639,
    SNAPSHOT = 640,
    SOME = 641,
    SQL_P = 642,
    STABLE = 643,
    STANDALONE_P = 644,
    START = 645,
    STATEMENT = 646,
    STATISTICS = 647,
    STDIN = 648,
    STDOUT = 649,
    STORAGE = 650,
    STORED = 651,
    STRICT_P = 652,
    STRIP_P = 653,
    STRUCT = 654,
    SUBSCRIPTION = 655,
    SUBSTRING = 656,
    SUMMARIZE = 657,
    SYMMETRIC = 658,
    SYSID = 659,
    SYSTEM_P = 660,
    TABLE = 661,
    TABLES = 662,
    TABLESAMPLE = 663,
    TABLESPACE = 664,
    TEMP = 665,
    TEMPLATE = 666,
    TEMPORARY = 667,
    TEXT_P = 668,
    THEN = 669,
    TIME = 670,
    TIMESTAMP = 671,
    TO = 672,
    TRAILING = 673,
    TRANSACTION = 674,
    TRANSFORM = 675,
    TREAT = 676,
    TRIGGER = 677,
    TRIM = 678,
    TRUE_P = 679,
    TRUNCATE = 680,
    TRUSTED = 681,
    TRY_CAST = 682,
    TYPE_P = 683,
    TYPES_P = 684,
    UNBOUNDED = 685,
    UNCOMMITTED = 686,
    UNENCRYPTED = 687,
    UNION = 688,
    UNIQUE = 689,
    UNKNOWN = 690,
    UNLISTEN = 691,
    UNLOGGED = 692,
    UNTIL = 693,
    UPDATE = 694,
    USER = 695,
    USING = 696,
    VACUUM = 697,
    VALID = 698,
    VALIDATE = 699,
    VALIDATOR = 700,
    VALUE_P = 701,
    VALUES = 702,
    VARCHAR = 703,
    VARIADIC = 704,
    VARYING = 705,
    VERBOSE = 706,
    VERSION_P = 707,
    VIEW = 708,
    VIEWS = 709,
    VIRTUAL = 710,
    VOLATILE = 711,
    WHEN = 712,
    WHERE = 713,
    WHITESPACE_P = 714,
    WINDOW = 715,
    WITH = 716,
    WITHIN = 717,
    WITHOUT = 718,
    WORK = 719,
    WRAPPER = 720,
    WRITE_P = 721,
    XML_P = 722,
    XMLATTRIBUTES = 723,
    XMLCONCAT = 724,
    XMLELEMENT = 725,
    XMLEXISTS = 726,
    XMLFOREST = 727,
    XMLNAMESPACES = 728,
    XMLPARSE = 729,
    XMLPI = 730,
    XMLROOT = 731,
    XMLSERIALIZE = 732,
    XMLTABLE = 733,
    YEAR_P = 734,
    YEARS_P = 735,
    YES_P = 736,
    ZONE = 737,
    NOT_LA = 738,
    NULLS_LA = 739,
    WITH_LA = 740,
    POSTFIXOP = 741,
    UMINUS = 742
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
	PGOnCreateConflict		oncreateconflict;
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
	PGConstrType           constr;
	PGLockClauseStrength lockstrength;
	PGLockWaitPolicy lockwaitpolicy;
	PGSubLinkType subquerytype;
	PGViewCheckOption viewcheckoption;

#line 590 "third_party/libpg_query/grammar/grammar_out.hpp"

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
