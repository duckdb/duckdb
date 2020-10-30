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
    MAPPING = 489,                 /* MAPPING  */
    MATCH = 490,                   /* MATCH  */
    MATERIALIZED = 491,            /* MATERIALIZED  */
    MAXVALUE = 492,                /* MAXVALUE  */
    METHOD = 493,                  /* METHOD  */
    MINUTE_P = 494,                /* MINUTE_P  */
    MINVALUE = 495,                /* MINVALUE  */
    MODE = 496,                    /* MODE  */
    MONTH_P = 497,                 /* MONTH_P  */
    MOVE = 498,                    /* MOVE  */
    NAME_P = 499,                  /* NAME_P  */
    NAMES = 500,                   /* NAMES  */
    NATIONAL = 501,                /* NATIONAL  */
    NATURAL = 502,                 /* NATURAL  */
    NCHAR = 503,                   /* NCHAR  */
    NEW = 504,                     /* NEW  */
    NEXT = 505,                    /* NEXT  */
    NO = 506,                      /* NO  */
    NONE = 507,                    /* NONE  */
    NOT = 508,                     /* NOT  */
    NOTHING = 509,                 /* NOTHING  */
    NOTIFY = 510,                  /* NOTIFY  */
    NOTNULL = 511,                 /* NOTNULL  */
    NOWAIT = 512,                  /* NOWAIT  */
    NULL_P = 513,                  /* NULL_P  */
    NULLIF = 514,                  /* NULLIF  */
    NULLS_P = 515,                 /* NULLS_P  */
    NUMERIC = 516,                 /* NUMERIC  */
    OBJECT_P = 517,                /* OBJECT_P  */
    OF = 518,                      /* OF  */
    OFF = 519,                     /* OFF  */
    OFFSET = 520,                  /* OFFSET  */
    OIDS = 521,                    /* OIDS  */
    OLD = 522,                     /* OLD  */
    ON = 523,                      /* ON  */
    ONLY = 524,                    /* ONLY  */
    OPERATOR = 525,                /* OPERATOR  */
    OPTION = 526,                  /* OPTION  */
    OPTIONS = 527,                 /* OPTIONS  */
    OR = 528,                      /* OR  */
    ORDER = 529,                   /* ORDER  */
    ORDINALITY = 530,              /* ORDINALITY  */
    OUT_P = 531,                   /* OUT_P  */
    OUTER_P = 532,                 /* OUTER_P  */
    OVER = 533,                    /* OVER  */
    OVERLAPS = 534,                /* OVERLAPS  */
    OVERLAY = 535,                 /* OVERLAY  */
    OVERRIDING = 536,              /* OVERRIDING  */
    OWNED = 537,                   /* OWNED  */
    OWNER = 538,                   /* OWNER  */
    PARALLEL = 539,                /* PARALLEL  */
    PARSER = 540,                  /* PARSER  */
    PARTIAL = 541,                 /* PARTIAL  */
    PARTITION = 542,               /* PARTITION  */
    PASSING = 543,                 /* PASSING  */
    PASSWORD = 544,                /* PASSWORD  */
    PLACING = 545,                 /* PLACING  */
    PLANS = 546,                   /* PLANS  */
    POLICY = 547,                  /* POLICY  */
    POSITION = 548,                /* POSITION  */
    PRAGMA_P = 549,                /* PRAGMA_P  */
    PRECEDING = 550,               /* PRECEDING  */
    PRECISION = 551,               /* PRECISION  */
    PREPARE = 552,                 /* PREPARE  */
    PREPARED = 553,                /* PREPARED  */
    PRESERVE = 554,                /* PRESERVE  */
    PRIMARY = 555,                 /* PRIMARY  */
    PRIOR = 556,                   /* PRIOR  */
    PRIVILEGES = 557,              /* PRIVILEGES  */
    PROCEDURAL = 558,              /* PROCEDURAL  */
    PROCEDURE = 559,               /* PROCEDURE  */
    PROGRAM = 560,                 /* PROGRAM  */
    PUBLICATION = 561,             /* PUBLICATION  */
    QUOTE = 562,                   /* QUOTE  */
    RANGE = 563,                   /* RANGE  */
    READ_P = 564,                  /* READ_P  */
    REAL = 565,                    /* REAL  */
    REASSIGN = 566,                /* REASSIGN  */
    RECHECK = 567,                 /* RECHECK  */
    RECURSIVE = 568,               /* RECURSIVE  */
    REF = 569,                     /* REF  */
    REFERENCES = 570,              /* REFERENCES  */
    REFERENCING = 571,             /* REFERENCING  */
    REFRESH = 572,                 /* REFRESH  */
    REINDEX = 573,                 /* REINDEX  */
    RELATIVE_P = 574,              /* RELATIVE_P  */
    RELEASE = 575,                 /* RELEASE  */
    RENAME = 576,                  /* RENAME  */
    REPEATABLE = 577,              /* REPEATABLE  */
    REPLACE = 578,                 /* REPLACE  */
    REPLICA = 579,                 /* REPLICA  */
    RESET = 580,                   /* RESET  */
    RESTART = 581,                 /* RESTART  */
    RESTRICT = 582,                /* RESTRICT  */
    RETURNING = 583,               /* RETURNING  */
    RETURNS = 584,                 /* RETURNS  */
    REVOKE = 585,                  /* REVOKE  */
    RIGHT = 586,                   /* RIGHT  */
    ROLE = 587,                    /* ROLE  */
    ROLLBACK = 588,                /* ROLLBACK  */
    ROLLUP = 589,                  /* ROLLUP  */
    ROW = 590,                     /* ROW  */
    ROWS = 591,                    /* ROWS  */
    RULE = 592,                    /* RULE  */
    SAVEPOINT = 593,               /* SAVEPOINT  */
    SCHEMA = 594,                  /* SCHEMA  */
    SCHEMAS = 595,                 /* SCHEMAS  */
    SCROLL = 596,                  /* SCROLL  */
    SEARCH = 597,                  /* SEARCH  */
    SECOND_P = 598,                /* SECOND_P  */
    SECURITY = 599,                /* SECURITY  */
    SELECT = 600,                  /* SELECT  */
    SEQUENCE = 601,                /* SEQUENCE  */
    SEQUENCES = 602,               /* SEQUENCES  */
    SERIALIZABLE = 603,            /* SERIALIZABLE  */
    SERVER = 604,                  /* SERVER  */
    SESSION = 605,                 /* SESSION  */
    SESSION_USER = 606,            /* SESSION_USER  */
    SET = 607,                     /* SET  */
    SETOF = 608,                   /* SETOF  */
    SETS = 609,                    /* SETS  */
    SHARE = 610,                   /* SHARE  */
    SHOW = 611,                    /* SHOW  */
    SIMILAR = 612,                 /* SIMILAR  */
    SIMPLE = 613,                  /* SIMPLE  */
    SKIP = 614,                    /* SKIP  */
    SMALLINT = 615,                /* SMALLINT  */
    SNAPSHOT = 616,                /* SNAPSHOT  */
    SOME = 617,                    /* SOME  */
    SQL_P = 618,                   /* SQL_P  */
    STABLE = 619,                  /* STABLE  */
    STANDALONE_P = 620,            /* STANDALONE_P  */
    START = 621,                   /* START  */
    STATEMENT = 622,               /* STATEMENT  */
    STATISTICS = 623,              /* STATISTICS  */
    STDIN = 624,                   /* STDIN  */
    STDOUT = 625,                  /* STDOUT  */
    STORAGE = 626,                 /* STORAGE  */
    STRICT_P = 627,                /* STRICT_P  */
    STRIP_P = 628,                 /* STRIP_P  */
    SUBSCRIPTION = 629,            /* SUBSCRIPTION  */
    SUBSTRING = 630,               /* SUBSTRING  */
    SYMMETRIC = 631,               /* SYMMETRIC  */
    SYSID = 632,                   /* SYSID  */
    SYSTEM_P = 633,                /* SYSTEM_P  */
    TABLE = 634,                   /* TABLE  */
    TABLES = 635,                  /* TABLES  */
    TABLESAMPLE = 636,             /* TABLESAMPLE  */
    TABLESPACE = 637,              /* TABLESPACE  */
    TEMP = 638,                    /* TEMP  */
    TEMPLATE = 639,                /* TEMPLATE  */
    TEMPORARY = 640,               /* TEMPORARY  */
    TEXT_P = 641,                  /* TEXT_P  */
    THEN = 642,                    /* THEN  */
    TIME = 643,                    /* TIME  */
    TIMESTAMP = 644,               /* TIMESTAMP  */
    TO = 645,                      /* TO  */
    TRAILING = 646,                /* TRAILING  */
    TRANSACTION = 647,             /* TRANSACTION  */
    TRANSFORM = 648,               /* TRANSFORM  */
    TREAT = 649,                   /* TREAT  */
    TRIGGER = 650,                 /* TRIGGER  */
    TRIM = 651,                    /* TRIM  */
    TRUE_P = 652,                  /* TRUE_P  */
    TRUNCATE = 653,                /* TRUNCATE  */
    TRUSTED = 654,                 /* TRUSTED  */
    TYPE_P = 655,                  /* TYPE_P  */
    TYPES_P = 656,                 /* TYPES_P  */
    UNBOUNDED = 657,               /* UNBOUNDED  */
    UNCOMMITTED = 658,             /* UNCOMMITTED  */
    UNENCRYPTED = 659,             /* UNENCRYPTED  */
    UNION = 660,                   /* UNION  */
    UNIQUE = 661,                  /* UNIQUE  */
    UNKNOWN = 662,                 /* UNKNOWN  */
    UNLISTEN = 663,                /* UNLISTEN  */
    UNLOGGED = 664,                /* UNLOGGED  */
    UNTIL = 665,                   /* UNTIL  */
    UPDATE = 666,                  /* UPDATE  */
    USER = 667,                    /* USER  */
    USING = 668,                   /* USING  */
    VACUUM = 669,                  /* VACUUM  */
    VALID = 670,                   /* VALID  */
    VALIDATE = 671,                /* VALIDATE  */
    VALIDATOR = 672,               /* VALIDATOR  */
    VALUE_P = 673,                 /* VALUE_P  */
    VALUES = 674,                  /* VALUES  */
    VARCHAR = 675,                 /* VARCHAR  */
    VARIADIC = 676,                /* VARIADIC  */
    VARYING = 677,                 /* VARYING  */
    VERBOSE = 678,                 /* VERBOSE  */
    VERSION_P = 679,               /* VERSION_P  */
    VIEW = 680,                    /* VIEW  */
    VIEWS = 681,                   /* VIEWS  */
    VOLATILE = 682,                /* VOLATILE  */
    WHEN = 683,                    /* WHEN  */
    WHERE = 684,                   /* WHERE  */
    WHITESPACE_P = 685,            /* WHITESPACE_P  */
    WINDOW = 686,                  /* WINDOW  */
    WITH = 687,                    /* WITH  */
    WITHIN = 688,                  /* WITHIN  */
    WITHOUT = 689,                 /* WITHOUT  */
    WORK = 690,                    /* WORK  */
    WRAPPER = 691,                 /* WRAPPER  */
    WRITE_P = 692,                 /* WRITE_P  */
    XML_P = 693,                   /* XML_P  */
    XMLATTRIBUTES = 694,           /* XMLATTRIBUTES  */
    XMLCONCAT = 695,               /* XMLCONCAT  */
    XMLELEMENT = 696,              /* XMLELEMENT  */
    XMLEXISTS = 697,               /* XMLEXISTS  */
    XMLFOREST = 698,               /* XMLFOREST  */
    XMLNAMESPACES = 699,           /* XMLNAMESPACES  */
    XMLPARSE = 700,                /* XMLPARSE  */
    XMLPI = 701,                   /* XMLPI  */
    XMLROOT = 702,                 /* XMLROOT  */
    XMLSERIALIZE = 703,            /* XMLSERIALIZE  */
    XMLTABLE = 704,                /* XMLTABLE  */
    YEAR_P = 705,                  /* YEAR_P  */
    YES_P = 706,                   /* YES_P  */
    ZONE = 707,                    /* ZONE  */
    NOT_LA = 708,                  /* NOT_LA  */
    NULLS_LA = 709,                /* NULLS_LA  */
    WITH_LA = 710,                 /* WITH_LA  */
    POSTFIXOP = 711,               /* POSTFIXOP  */
    UMINUS = 712                   /* UMINUS  */
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

#line 564 "third_party/libpg_query/grammar/grammar_out.hpp"

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
