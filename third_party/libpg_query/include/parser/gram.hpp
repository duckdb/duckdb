/* A Bison parser, made by GNU Bison 3.6.4.  */

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

/* DO NOT RELY ON FEATURES THAT ARE NOT DOCUMENTED in the manual,
   especially those whose name start with YY_ or yy_.  They are
   private implementation details that can be changed or removed.  */

#ifndef YY_BASE_YY_THIRD_PARTY_LIBPG_QUERY_GRAMMAR_GRAMMAR_OUT_HPP_INCLUDED
# define YY_BASE_YY_THIRD_PARTY_LIBPG_QUERY_GRAMMAR_GRAMMAR_OUT_HPP_INCLUDED
/* Debug traces.  */
#ifndef YYDEBUG
# define YYDEBUG 0
#endif
#if YYDEBUG
extern int base_yydebug;
#endif

/* Token kinds.  */
#ifndef YYTOKENTYPE
# define YYTOKENTYPE
  enum yytokentype
  {
    YYEMPTY = -2,
    YYEOF = 0,                     /* "end of file"  */
    YYerror = 256,                 /* error  */
    YYUNDEF = 257,                 /* "invalid token"  */
    IDENT = 258,                   /* IDENT  */
    FCONST = 259,                  /* FCONST  */
    SCONST = 260,                  /* SCONST  */
    BCONST = 261,                  /* BCONST  */
    XCONST = 262,                  /* XCONST  */
    Op = 263,                      /* Op  */
    ICONST = 264,                  /* ICONST  */
    PARAM = 265,                   /* PARAM  */
    TYPECAST = 266,                /* TYPECAST  */
    DOT_DOT = 267,                 /* DOT_DOT  */
    COLON_EQUALS = 268,            /* COLON_EQUALS  */
    EQUALS_GREATER = 269,          /* EQUALS_GREATER  */
    LESS_EQUALS = 270,             /* LESS_EQUALS  */
    GREATER_EQUALS = 271,          /* GREATER_EQUALS  */
    NOT_EQUALS = 272,              /* NOT_EQUALS  */
    ABORT_P = 273,                 /* ABORT_P  */
    ABSOLUTE_P = 274,              /* ABSOLUTE_P  */
    ACCESS = 275,                  /* ACCESS  */
    ACTION = 276,                  /* ACTION  */
    ADD_P = 277,                   /* ADD_P  */
    ADMIN = 278,                   /* ADMIN  */
    AFTER = 279,                   /* AFTER  */
    AGGREGATE = 280,               /* AGGREGATE  */
    ALL = 281,                     /* ALL  */
    ALSO = 282,                    /* ALSO  */
    ALTER = 283,                   /* ALTER  */
    ALWAYS = 284,                  /* ALWAYS  */
    ANALYSE = 285,                 /* ANALYSE  */
    ANALYZE = 286,                 /* ANALYZE  */
    AND = 287,                     /* AND  */
    ANY = 288,                     /* ANY  */
    ARRAY = 289,                   /* ARRAY  */
    AS = 290,                      /* AS  */
    ASC_P = 291,                   /* ASC_P  */
    ASSERTION = 292,               /* ASSERTION  */
    ASSIGNMENT = 293,              /* ASSIGNMENT  */
    ASYMMETRIC = 294,              /* ASYMMETRIC  */
    AT = 295,                      /* AT  */
    ATTACH = 296,                  /* ATTACH  */
    ATTRIBUTE = 297,               /* ATTRIBUTE  */
    AUTHORIZATION = 298,           /* AUTHORIZATION  */
    BACKWARD = 299,                /* BACKWARD  */
    BEFORE = 300,                  /* BEFORE  */
    BEGIN_P = 301,                 /* BEGIN_P  */
    BETWEEN = 302,                 /* BETWEEN  */
    BIGINT = 303,                  /* BIGINT  */
    BINARY = 304,                  /* BINARY  */
    BIT = 305,                     /* BIT  */
    BOOLEAN_P = 306,               /* BOOLEAN_P  */
    BOTH = 307,                    /* BOTH  */
    BY = 308,                      /* BY  */
    CACHE = 309,                   /* CACHE  */
    CALL_P = 310,                  /* CALL_P  */
    CALLED = 311,                  /* CALLED  */
    CASCADE = 312,                 /* CASCADE  */
    CASCADED = 313,                /* CASCADED  */
    CASE = 314,                    /* CASE  */
    CAST = 315,                    /* CAST  */
    CATALOG_P = 316,               /* CATALOG_P  */
    CHAIN = 317,                   /* CHAIN  */
    CHAR_P = 318,                  /* CHAR_P  */
    CHARACTER = 319,               /* CHARACTER  */
    CHARACTERISTICS = 320,         /* CHARACTERISTICS  */
    CHECK_P = 321,                 /* CHECK_P  */
    CHECKPOINT = 322,              /* CHECKPOINT  */
    CLASS = 323,                   /* CLASS  */
    CLOSE = 324,                   /* CLOSE  */
    CLUSTER = 325,                 /* CLUSTER  */
    COALESCE = 326,                /* COALESCE  */
    COLLATE = 327,                 /* COLLATE  */
    COLLATION = 328,               /* COLLATION  */
    COLUMN = 329,                  /* COLUMN  */
    COLUMNS = 330,                 /* COLUMNS  */
    COMMENT = 331,                 /* COMMENT  */
    COMMENTS = 332,                /* COMMENTS  */
    COMMIT = 333,                  /* COMMIT  */
    COMMITTED = 334,               /* COMMITTED  */
    CONCURRENTLY = 335,            /* CONCURRENTLY  */
    CONFIGURATION = 336,           /* CONFIGURATION  */
    CONFLICT = 337,                /* CONFLICT  */
    CONNECTION = 338,              /* CONNECTION  */
    CONSTRAINT = 339,              /* CONSTRAINT  */
    CONSTRAINTS = 340,             /* CONSTRAINTS  */
    CONTENT_P = 341,               /* CONTENT_P  */
    CONTINUE_P = 342,              /* CONTINUE_P  */
    CONVERSION_P = 343,            /* CONVERSION_P  */
    COPY = 344,                    /* COPY  */
    COST = 345,                    /* COST  */
    CREATE_P = 346,                /* CREATE_P  */
    CROSS = 347,                   /* CROSS  */
    CSV = 348,                     /* CSV  */
    CUBE = 349,                    /* CUBE  */
    CURRENT_P = 350,               /* CURRENT_P  */
    CURRENT_CATALOG = 351,         /* CURRENT_CATALOG  */
    CURRENT_DATE = 352,            /* CURRENT_DATE  */
    CURRENT_ROLE = 353,            /* CURRENT_ROLE  */
    CURRENT_SCHEMA = 354,          /* CURRENT_SCHEMA  */
    CURRENT_TIME = 355,            /* CURRENT_TIME  */
    CURRENT_TIMESTAMP = 356,       /* CURRENT_TIMESTAMP  */
    CURRENT_USER = 357,            /* CURRENT_USER  */
    CURSOR = 358,                  /* CURSOR  */
    CYCLE = 359,                   /* CYCLE  */
    DATA_P = 360,                  /* DATA_P  */
    DATABASE = 361,                /* DATABASE  */
    DAY_P = 362,                   /* DAY_P  */
    DEALLOCATE = 363,              /* DEALLOCATE  */
    DEC = 364,                     /* DEC  */
    DECIMAL_P = 365,               /* DECIMAL_P  */
    DECLARE = 366,                 /* DECLARE  */
    DEFAULT = 367,                 /* DEFAULT  */
    DEFAULTS = 368,                /* DEFAULTS  */
    DEFERRABLE = 369,              /* DEFERRABLE  */
    DEFERRED = 370,                /* DEFERRED  */
    DEFINER = 371,                 /* DEFINER  */
    DELETE_P = 372,                /* DELETE_P  */
    DELIMITER = 373,               /* DELIMITER  */
    DELIMITERS = 374,              /* DELIMITERS  */
    DEPENDS = 375,                 /* DEPENDS  */
    DESC_P = 376,                  /* DESC_P  */
    DESCRIBE = 377,                /* DESCRIBE  */
    DETACH = 378,                  /* DETACH  */
    DICTIONARY = 379,              /* DICTIONARY  */
    DISABLE_P = 380,               /* DISABLE_P  */
    DISCARD = 381,                 /* DISCARD  */
    DISTINCT = 382,                /* DISTINCT  */
    DO = 383,                      /* DO  */
    DOCUMENT_P = 384,              /* DOCUMENT_P  */
    DOMAIN_P = 385,                /* DOMAIN_P  */
    DOUBLE_P = 386,                /* DOUBLE_P  */
    DROP = 387,                    /* DROP  */
    EACH = 388,                    /* EACH  */
    ELSE = 389,                    /* ELSE  */
    ENABLE_P = 390,                /* ENABLE_P  */
    ENCODING = 391,                /* ENCODING  */
    ENCRYPTED = 392,               /* ENCRYPTED  */
    END_P = 393,                   /* END_P  */
    ENUM_P = 394,                  /* ENUM_P  */
    ESCAPE = 395,                  /* ESCAPE  */
    EVENT = 396,                   /* EVENT  */
    EXCEPT = 397,                  /* EXCEPT  */
    EXCLUDE = 398,                 /* EXCLUDE  */
    EXCLUDING = 399,               /* EXCLUDING  */
    EXCLUSIVE = 400,               /* EXCLUSIVE  */
    EXECUTE = 401,                 /* EXECUTE  */
    EXISTS = 402,                  /* EXISTS  */
    EXPLAIN = 403,                 /* EXPLAIN  */
    EXPORT_P = 404,                /* EXPORT_P  */
    EXTENSION = 405,               /* EXTENSION  */
    EXTERNAL = 406,                /* EXTERNAL  */
    EXTRACT = 407,                 /* EXTRACT  */
    FALSE_P = 408,                 /* FALSE_P  */
    FAMILY = 409,                  /* FAMILY  */
    FETCH = 410,                   /* FETCH  */
    FILTER = 411,                  /* FILTER  */
    FIRST_P = 412,                 /* FIRST_P  */
    FLOAT_P = 413,                 /* FLOAT_P  */
    FOLLOWING = 414,               /* FOLLOWING  */
    FOR = 415,                     /* FOR  */
    FORCE = 416,                   /* FORCE  */
    FOREIGN = 417,                 /* FOREIGN  */
    FORWARD = 418,                 /* FORWARD  */
    FREEZE = 419,                  /* FREEZE  */
    FROM = 420,                    /* FROM  */
    FULL = 421,                    /* FULL  */
    FUNCTION = 422,                /* FUNCTION  */
    FUNCTIONS = 423,               /* FUNCTIONS  */
    GENERATED = 424,               /* GENERATED  */
    GLOB = 425,                    /* GLOB  */
    GLOBAL = 426,                  /* GLOBAL  */
    GRANT = 427,                   /* GRANT  */
    GRANTED = 428,                 /* GRANTED  */
    GROUP_P = 429,                 /* GROUP_P  */
    GROUPING = 430,                /* GROUPING  */
    HANDLER = 431,                 /* HANDLER  */
    HAVING = 432,                  /* HAVING  */
    HEADER_P = 433,                /* HEADER_P  */
    HOLD = 434,                    /* HOLD  */
    HOUR_P = 435,                  /* HOUR_P  */
    IDENTITY_P = 436,              /* IDENTITY_P  */
    IF_P = 437,                    /* IF_P  */
    ILIKE = 438,                   /* ILIKE  */
    IMMEDIATE = 439,               /* IMMEDIATE  */
    IMMUTABLE = 440,               /* IMMUTABLE  */
    IMPLICIT_P = 441,              /* IMPLICIT_P  */
    IMPORT_P = 442,                /* IMPORT_P  */
    IN_P = 443,                    /* IN_P  */
    INCLUDING = 444,               /* INCLUDING  */
    INCREMENT = 445,               /* INCREMENT  */
    INDEX = 446,                   /* INDEX  */
    INDEXES = 447,                 /* INDEXES  */
    INHERIT = 448,                 /* INHERIT  */
    INHERITS = 449,                /* INHERITS  */
    INITIALLY = 450,               /* INITIALLY  */
    INLINE_P = 451,                /* INLINE_P  */
    INNER_P = 452,                 /* INNER_P  */
    INOUT = 453,                   /* INOUT  */
    INPUT_P = 454,                 /* INPUT_P  */
    INSENSITIVE = 455,             /* INSENSITIVE  */
    INSERT = 456,                  /* INSERT  */
    INSTEAD = 457,                 /* INSTEAD  */
    INT_P = 458,                   /* INT_P  */
    INTEGER = 459,                 /* INTEGER  */
    INTERSECT = 460,               /* INTERSECT  */
    INTERVAL = 461,                /* INTERVAL  */
    INTO = 462,                    /* INTO  */
    INVOKER = 463,                 /* INVOKER  */
    IS = 464,                      /* IS  */
    ISNULL = 465,                  /* ISNULL  */
    ISOLATION = 466,               /* ISOLATION  */
    JOIN = 467,                    /* JOIN  */
    KEY = 468,                     /* KEY  */
    LABEL = 469,                   /* LABEL  */
    LANGUAGE = 470,                /* LANGUAGE  */
    LARGE_P = 471,                 /* LARGE_P  */
    LAST_P = 472,                  /* LAST_P  */
    LATERAL_P = 473,               /* LATERAL_P  */
    LEADING = 474,                 /* LEADING  */
    LEAKPROOF = 475,               /* LEAKPROOF  */
    LEFT = 476,                    /* LEFT  */
    LEVEL = 477,                   /* LEVEL  */
    LIKE = 478,                    /* LIKE  */
    LIMIT = 479,                   /* LIMIT  */
    LISTEN = 480,                  /* LISTEN  */
    LOAD = 481,                    /* LOAD  */
    LOCAL = 482,                   /* LOCAL  */
    LOCALTIME = 483,               /* LOCALTIME  */
    LOCALTIMESTAMP = 484,          /* LOCALTIMESTAMP  */
    LOCATION = 485,                /* LOCATION  */
    LOCK_P = 486,                  /* LOCK_P  */
    LOCKED = 487,                  /* LOCKED  */
    LOGGED = 488,                  /* LOGGED  */
    MACRO = 489,                   /* MACRO  */
    MAPPING = 490,                 /* MAPPING  */
    MATCH = 491,                   /* MATCH  */
    MATERIALIZED = 492,            /* MATERIALIZED  */
    MAXVALUE = 493,                /* MAXVALUE  */
    METHOD = 494,                  /* METHOD  */
    MINUTE_P = 495,                /* MINUTE_P  */
    MINVALUE = 496,                /* MINVALUE  */
    MODE = 497,                    /* MODE  */
    MONTH_P = 498,                 /* MONTH_P  */
    MOVE = 499,                    /* MOVE  */
    NAME_P = 500,                  /* NAME_P  */
    NAMES = 501,                   /* NAMES  */
    NATIONAL = 502,                /* NATIONAL  */
    NATURAL = 503,                 /* NATURAL  */
    NCHAR = 504,                   /* NCHAR  */
    NEW = 505,                     /* NEW  */
    NEXT = 506,                    /* NEXT  */
    NO = 507,                      /* NO  */
    NONE = 508,                    /* NONE  */
    NOT = 509,                     /* NOT  */
    NOTHING = 510,                 /* NOTHING  */
    NOTIFY = 511,                  /* NOTIFY  */
    NOTNULL = 512,                 /* NOTNULL  */
    NOWAIT = 513,                  /* NOWAIT  */
    NULL_P = 514,                  /* NULL_P  */
    NULLIF = 515,                  /* NULLIF  */
    NULLS_P = 516,                 /* NULLS_P  */
    NUMERIC = 517,                 /* NUMERIC  */
    OBJECT_P = 518,                /* OBJECT_P  */
    OF = 519,                      /* OF  */
    OFF = 520,                     /* OFF  */
    OFFSET = 521,                  /* OFFSET  */
    OIDS = 522,                    /* OIDS  */
    OLD = 523,                     /* OLD  */
    ON = 524,                      /* ON  */
    ONLY = 525,                    /* ONLY  */
    OPERATOR = 526,                /* OPERATOR  */
    OPTION = 527,                  /* OPTION  */
    OPTIONS = 528,                 /* OPTIONS  */
    OR = 529,                      /* OR  */
    ORDER = 530,                   /* ORDER  */
    ORDINALITY = 531,              /* ORDINALITY  */
    OUT_P = 532,                   /* OUT_P  */
    OUTER_P = 533,                 /* OUTER_P  */
    OVER = 534,                    /* OVER  */
    OVERLAPS = 535,                /* OVERLAPS  */
    OVERLAY = 536,                 /* OVERLAY  */
    OVERRIDING = 537,              /* OVERRIDING  */
    OWNED = 538,                   /* OWNED  */
    OWNER = 539,                   /* OWNER  */
    PARALLEL = 540,                /* PARALLEL  */
    PARSER = 541,                  /* PARSER  */
    PARTIAL = 542,                 /* PARTIAL  */
    PARTITION = 543,               /* PARTITION  */
    PASSING = 544,                 /* PASSING  */
    PASSWORD = 545,                /* PASSWORD  */
    PLACING = 546,                 /* PLACING  */
    PLANS = 547,                   /* PLANS  */
    POLICY = 548,                  /* POLICY  */
    POSITION = 549,                /* POSITION  */
    PRAGMA_P = 550,                /* PRAGMA_P  */
    PRECEDING = 551,               /* PRECEDING  */
    PRECISION = 552,               /* PRECISION  */
    PREPARE = 553,                 /* PREPARE  */
    PREPARED = 554,                /* PREPARED  */
    PRESERVE = 555,                /* PRESERVE  */
    PRIMARY = 556,                 /* PRIMARY  */
    PRIOR = 557,                   /* PRIOR  */
    PRIVILEGES = 558,              /* PRIVILEGES  */
    PROCEDURAL = 559,              /* PROCEDURAL  */
    PROCEDURE = 560,               /* PROCEDURE  */
    PROGRAM = 561,                 /* PROGRAM  */
    PUBLICATION = 562,             /* PUBLICATION  */
    QUOTE = 563,                   /* QUOTE  */
    RANGE = 564,                   /* RANGE  */
    READ_P = 565,                  /* READ_P  */
    REAL = 566,                    /* REAL  */
    REASSIGN = 567,                /* REASSIGN  */
    RECHECK = 568,                 /* RECHECK  */
    RECURSIVE = 569,               /* RECURSIVE  */
    REF = 570,                     /* REF  */
    REFERENCES = 571,              /* REFERENCES  */
    REFERENCING = 572,             /* REFERENCING  */
    REFRESH = 573,                 /* REFRESH  */
    REINDEX = 574,                 /* REINDEX  */
    RELATIVE_P = 575,              /* RELATIVE_P  */
    RELEASE = 576,                 /* RELEASE  */
    RENAME = 577,                  /* RENAME  */
    REPEATABLE = 578,              /* REPEATABLE  */
    REPLACE = 579,                 /* REPLACE  */
    REPLICA = 580,                 /* REPLICA  */
    RESET = 581,                   /* RESET  */
    RESTART = 582,                 /* RESTART  */
    RESTRICT = 583,                /* RESTRICT  */
    RETURNING = 584,               /* RETURNING  */
    RETURNS = 585,                 /* RETURNS  */
    REVOKE = 586,                  /* REVOKE  */
    RIGHT = 587,                   /* RIGHT  */
    ROLE = 588,                    /* ROLE  */
    ROLLBACK = 589,                /* ROLLBACK  */
    ROLLUP = 590,                  /* ROLLUP  */
    ROW = 591,                     /* ROW  */
    ROWS = 592,                    /* ROWS  */
    RULE = 593,                    /* RULE  */
    SAVEPOINT = 594,               /* SAVEPOINT  */
    SCHEMA = 595,                  /* SCHEMA  */
    SCHEMAS = 596,                 /* SCHEMAS  */
    SCROLL = 597,                  /* SCROLL  */
    SEARCH = 598,                  /* SEARCH  */
    SECOND_P = 599,                /* SECOND_P  */
    SECURITY = 600,                /* SECURITY  */
    SELECT = 601,                  /* SELECT  */
    SEQUENCE = 602,                /* SEQUENCE  */
    SEQUENCES = 603,               /* SEQUENCES  */
    SERIALIZABLE = 604,            /* SERIALIZABLE  */
    SERVER = 605,                  /* SERVER  */
    SESSION = 606,                 /* SESSION  */
    SESSION_USER = 607,            /* SESSION_USER  */
    SET = 608,                     /* SET  */
    SETOF = 609,                   /* SETOF  */
    SETS = 610,                    /* SETS  */
    SHARE = 611,                   /* SHARE  */
    SHOW = 612,                    /* SHOW  */
    SIMILAR = 613,                 /* SIMILAR  */
    SIMPLE = 614,                  /* SIMPLE  */
    SKIP = 615,                    /* SKIP  */
    SMALLINT = 616,                /* SMALLINT  */
    SNAPSHOT = 617,                /* SNAPSHOT  */
    SOME = 618,                    /* SOME  */
    SQL_P = 619,                   /* SQL_P  */
    STABLE = 620,                  /* STABLE  */
    STANDALONE_P = 621,            /* STANDALONE_P  */
    START = 622,                   /* START  */
    STATEMENT = 623,               /* STATEMENT  */
    STATISTICS = 624,              /* STATISTICS  */
    STDIN = 625,                   /* STDIN  */
    STDOUT = 626,                  /* STDOUT  */
    STORAGE = 627,                 /* STORAGE  */
    STRICT_P = 628,                /* STRICT_P  */
    STRIP_P = 629,                 /* STRIP_P  */
    SUBSCRIPTION = 630,            /* SUBSCRIPTION  */
    SUBSTRING = 631,               /* SUBSTRING  */
    SYMMETRIC = 632,               /* SYMMETRIC  */
    SYSID = 633,                   /* SYSID  */
    SYSTEM_P = 634,                /* SYSTEM_P  */
    TABLE = 635,                   /* TABLE  */
    TABLES = 636,                  /* TABLES  */
    TABLESAMPLE = 637,             /* TABLESAMPLE  */
    TABLESPACE = 638,              /* TABLESPACE  */
    TEMP = 639,                    /* TEMP  */
    TEMPLATE = 640,                /* TEMPLATE  */
    TEMPORARY = 641,               /* TEMPORARY  */
    TEXT_P = 642,                  /* TEXT_P  */
    THEN = 643,                    /* THEN  */
    TIME = 644,                    /* TIME  */
    TIMESTAMP = 645,               /* TIMESTAMP  */
    TO = 646,                      /* TO  */
    TRAILING = 647,                /* TRAILING  */
    TRANSACTION = 648,             /* TRANSACTION  */
    TRANSFORM = 649,               /* TRANSFORM  */
    TREAT = 650,                   /* TREAT  */
    TRIGGER = 651,                 /* TRIGGER  */
    TRIM = 652,                    /* TRIM  */
    TRUE_P = 653,                  /* TRUE_P  */
    TRUNCATE = 654,                /* TRUNCATE  */
    TRUSTED = 655,                 /* TRUSTED  */
    TYPE_P = 656,                  /* TYPE_P  */
    TYPES_P = 657,                 /* TYPES_P  */
    UNBOUNDED = 658,               /* UNBOUNDED  */
    UNCOMMITTED = 659,             /* UNCOMMITTED  */
    UNENCRYPTED = 660,             /* UNENCRYPTED  */
    UNION = 661,                   /* UNION  */
    UNIQUE = 662,                  /* UNIQUE  */
    UNKNOWN = 663,                 /* UNKNOWN  */
    UNLISTEN = 664,                /* UNLISTEN  */
    UNLOGGED = 665,                /* UNLOGGED  */
    UNTIL = 666,                   /* UNTIL  */
    UPDATE = 667,                  /* UPDATE  */
    USER = 668,                    /* USER  */
    USING = 669,                   /* USING  */
    VACUUM = 670,                  /* VACUUM  */
    VALID = 671,                   /* VALID  */
    VALIDATE = 672,                /* VALIDATE  */
    VALIDATOR = 673,               /* VALIDATOR  */
    VALUE_P = 674,                 /* VALUE_P  */
    VALUES = 675,                  /* VALUES  */
    VARCHAR = 676,                 /* VARCHAR  */
    VARIADIC = 677,                /* VARIADIC  */
    VARYING = 678,                 /* VARYING  */
    VERBOSE = 679,                 /* VERBOSE  */
    VERSION_P = 680,               /* VERSION_P  */
    VIEW = 681,                    /* VIEW  */
    VIEWS = 682,                   /* VIEWS  */
    VOLATILE = 683,                /* VOLATILE  */
    WHEN = 684,                    /* WHEN  */
    WHERE = 685,                   /* WHERE  */
    WHITESPACE_P = 686,            /* WHITESPACE_P  */
    WINDOW = 687,                  /* WINDOW  */
    WITH = 688,                    /* WITH  */
    WITHIN = 689,                  /* WITHIN  */
    WITHOUT = 690,                 /* WITHOUT  */
    WORK = 691,                    /* WORK  */
    WRAPPER = 692,                 /* WRAPPER  */
    WRITE_P = 693,                 /* WRITE_P  */
    XML_P = 694,                   /* XML_P  */
    XMLATTRIBUTES = 695,           /* XMLATTRIBUTES  */
    XMLCONCAT = 696,               /* XMLCONCAT  */
    XMLELEMENT = 697,              /* XMLELEMENT  */
    XMLEXISTS = 698,               /* XMLEXISTS  */
    XMLFOREST = 699,               /* XMLFOREST  */
    XMLNAMESPACES = 700,           /* XMLNAMESPACES  */
    XMLPARSE = 701,                /* XMLPARSE  */
    XMLPI = 702,                   /* XMLPI  */
    XMLROOT = 703,                 /* XMLROOT  */
    XMLSERIALIZE = 704,            /* XMLSERIALIZE  */
    XMLTABLE = 705,                /* XMLTABLE  */
    YEAR_P = 706,                  /* YEAR_P  */
    YES_P = 707,                   /* YES_P  */
    ZONE = 708,                    /* ZONE  */
    NOT_LA = 709,                  /* NOT_LA  */
    NULLS_LA = 710,                /* NULLS_LA  */
    WITH_LA = 711,                 /* WITH_LA  */
    POSTFIXOP = 712,               /* POSTFIXOP  */
    UMINUS = 713                   /* UMINUS  */
  };
  typedef enum yytokentype yytoken_kind_t;
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

#line 565 "third_party/libpg_query/grammar/grammar_out.hpp"

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
