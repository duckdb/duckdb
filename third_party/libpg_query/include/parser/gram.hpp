/* A Bison parser, made by GNU Bison 3.8.2.  */

/* Bison interface for Yacc-like parsers in C

   Copyright (C) 1984, 1989-1990, 2000-2015, 2018-2021 Free Software Foundation,
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
   along with this program.  If not, see <https://www.gnu.org/licenses/>.  */

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
    INTEGER_DIVISION = 270,        /* INTEGER_DIVISION  */
    POWER_OF = 271,                /* POWER_OF  */
    SINGLE_ARROW = 272,            /* SINGLE_ARROW  */
    DOUBLE_ARROW = 273,            /* DOUBLE_ARROW  */
    SINGLE_COLON = 274,            /* SINGLE_COLON  */
    LESS_EQUALS = 275,             /* LESS_EQUALS  */
    GREATER_EQUALS = 276,          /* GREATER_EQUALS  */
    NOT_EQUALS = 277,              /* NOT_EQUALS  */
    ABORT_P = 278,                 /* ABORT_P  */
    ABSOLUTE_P = 279,              /* ABSOLUTE_P  */
    ACCESS = 280,                  /* ACCESS  */
    ACTION = 281,                  /* ACTION  */
    ADD_P = 282,                   /* ADD_P  */
    ADMIN = 283,                   /* ADMIN  */
    AFTER = 284,                   /* AFTER  */
    AGGREGATE = 285,               /* AGGREGATE  */
    ALL = 286,                     /* ALL  */
    ALSO = 287,                    /* ALSO  */
    ALTER = 288,                   /* ALTER  */
    ALWAYS = 289,                  /* ALWAYS  */
    ANALYSE = 290,                 /* ANALYSE  */
    ANALYZE = 291,                 /* ANALYZE  */
    AND = 292,                     /* AND  */
    ANTI = 293,                    /* ANTI  */
    ANY = 294,                     /* ANY  */
    ARRAY = 295,                   /* ARRAY  */
    AS = 296,                      /* AS  */
    ASC_P = 297,                   /* ASC_P  */
    ASOF = 298,                    /* ASOF  */
    ASSERTION = 299,               /* ASSERTION  */
    ASSIGNMENT = 300,              /* ASSIGNMENT  */
    ASYMMETRIC = 301,              /* ASYMMETRIC  */
    AT = 302,                      /* AT  */
    ATTACH = 303,                  /* ATTACH  */
    ATTRIBUTE = 304,               /* ATTRIBUTE  */
    AUTHORIZATION = 305,           /* AUTHORIZATION  */
    BACKWARD = 306,                /* BACKWARD  */
    BEFORE = 307,                  /* BEFORE  */
    BEGIN_P = 308,                 /* BEGIN_P  */
    BETWEEN = 309,                 /* BETWEEN  */
    BIGINT = 310,                  /* BIGINT  */
    BINARY = 311,                  /* BINARY  */
    BIT = 312,                     /* BIT  */
    BOOLEAN_P = 313,               /* BOOLEAN_P  */
    BOTH = 314,                    /* BOTH  */
    BY = 315,                      /* BY  */
    CACHE = 316,                   /* CACHE  */
    CALL_P = 317,                  /* CALL_P  */
    CALLED = 318,                  /* CALLED  */
    CASCADE = 319,                 /* CASCADE  */
    CASCADED = 320,                /* CASCADED  */
    CASE = 321,                    /* CASE  */
    CAST = 322,                    /* CAST  */
    CATALOG_P = 323,               /* CATALOG_P  */
    CENTURIES_P = 324,             /* CENTURIES_P  */
    CENTURY_P = 325,               /* CENTURY_P  */
    CHAIN = 326,                   /* CHAIN  */
    CHAR_P = 327,                  /* CHAR_P  */
    CHARACTER = 328,               /* CHARACTER  */
    CHARACTERISTICS = 329,         /* CHARACTERISTICS  */
    CHECK_P = 330,                 /* CHECK_P  */
    CHECKPOINT = 331,              /* CHECKPOINT  */
    CLASS = 332,                   /* CLASS  */
    CLOSE = 333,                   /* CLOSE  */
    CLUSTER = 334,                 /* CLUSTER  */
    COALESCE = 335,                /* COALESCE  */
    COLLATE = 336,                 /* COLLATE  */
    COLLATION = 337,               /* COLLATION  */
    COLUMN = 338,                  /* COLUMN  */
    COLUMNS = 339,                 /* COLUMNS  */
    COMMENT = 340,                 /* COMMENT  */
    COMMENTS = 341,                /* COMMENTS  */
    COMMIT = 342,                  /* COMMIT  */
    COMMITTED = 343,               /* COMMITTED  */
    COMPRESSION = 344,             /* COMPRESSION  */
    CONCURRENTLY = 345,            /* CONCURRENTLY  */
    CONFIGURATION = 346,           /* CONFIGURATION  */
    CONFLICT = 347,                /* CONFLICT  */
    CONNECTION = 348,              /* CONNECTION  */
    CONSTRAINT = 349,              /* CONSTRAINT  */
    CONSTRAINTS = 350,             /* CONSTRAINTS  */
    CONTENT_P = 351,               /* CONTENT_P  */
    CONTINUE_P = 352,              /* CONTINUE_P  */
    CONVERSION_P = 353,            /* CONVERSION_P  */
    COPY = 354,                    /* COPY  */
    COST = 355,                    /* COST  */
    CREATE_P = 356,                /* CREATE_P  */
    CROSS = 357,                   /* CROSS  */
    CSV = 358,                     /* CSV  */
    CUBE = 359,                    /* CUBE  */
    CURRENT_P = 360,               /* CURRENT_P  */
    CURSOR = 361,                  /* CURSOR  */
    CYCLE = 362,                   /* CYCLE  */
    DATA_P = 363,                  /* DATA_P  */
    DATABASE = 364,                /* DATABASE  */
    DAY_P = 365,                   /* DAY_P  */
    DAYS_P = 366,                  /* DAYS_P  */
    DEALLOCATE = 367,              /* DEALLOCATE  */
    DEC = 368,                     /* DEC  */
    DECADE_P = 369,                /* DECADE_P  */
    DECADES_P = 370,               /* DECADES_P  */
    DECIMAL_P = 371,               /* DECIMAL_P  */
    DECLARE = 372,                 /* DECLARE  */
    DEFAULT = 373,                 /* DEFAULT  */
    DEFAULTS = 374,                /* DEFAULTS  */
    DEFERRABLE = 375,              /* DEFERRABLE  */
    DEFERRED = 376,                /* DEFERRED  */
    DEFINER = 377,                 /* DEFINER  */
    DELETE_P = 378,                /* DELETE_P  */
    DELIMITER = 379,               /* DELIMITER  */
    DELIMITERS = 380,              /* DELIMITERS  */
    DEPENDS = 381,                 /* DEPENDS  */
    DESC_P = 382,                  /* DESC_P  */
    DESCRIBE = 383,                /* DESCRIBE  */
    DETACH = 384,                  /* DETACH  */
    DICTIONARY = 385,              /* DICTIONARY  */
    DISABLE_P = 386,               /* DISABLE_P  */
    DISCARD = 387,                 /* DISCARD  */
    DISTINCT = 388,                /* DISTINCT  */
    DO = 389,                      /* DO  */
    DOCUMENT_P = 390,              /* DOCUMENT_P  */
    DOMAIN_P = 391,                /* DOMAIN_P  */
    DOUBLE_P = 392,                /* DOUBLE_P  */
    DROP = 393,                    /* DROP  */
    EACH = 394,                    /* EACH  */
    ELSE = 395,                    /* ELSE  */
    ENABLE_P = 396,                /* ENABLE_P  */
    ENCODING = 397,                /* ENCODING  */
    ENCRYPTED = 398,               /* ENCRYPTED  */
    END_P = 399,                   /* END_P  */
    ENUM_P = 400,                  /* ENUM_P  */
    ERROR_P = 401,                 /* ERROR_P  */
    ESCAPE = 402,                  /* ESCAPE  */
    EVENT = 403,                   /* EVENT  */
    EXCEPT = 404,                  /* EXCEPT  */
    EXCLUDE = 405,                 /* EXCLUDE  */
    EXCLUDING = 406,               /* EXCLUDING  */
    EXCLUSIVE = 407,               /* EXCLUSIVE  */
    EXECUTE = 408,                 /* EXECUTE  */
    EXISTS = 409,                  /* EXISTS  */
    EXPLAIN = 410,                 /* EXPLAIN  */
    EXPORT_P = 411,                /* EXPORT_P  */
    EXPORT_STATE = 412,            /* EXPORT_STATE  */
    EXTENSION = 413,               /* EXTENSION  */
    EXTENSIONS = 414,              /* EXTENSIONS  */
    EXTERNAL = 415,                /* EXTERNAL  */
    EXTRACT = 416,                 /* EXTRACT  */
    FALSE_P = 417,                 /* FALSE_P  */
    FAMILY = 418,                  /* FAMILY  */
    FETCH = 419,                   /* FETCH  */
    FILTER = 420,                  /* FILTER  */
    FIRST_P = 421,                 /* FIRST_P  */
    FLOAT_P = 422,                 /* FLOAT_P  */
    FOLLOWING = 423,               /* FOLLOWING  */
    FOR = 424,                     /* FOR  */
    FORCE = 425,                   /* FORCE  */
    FOREIGN = 426,                 /* FOREIGN  */
    FORWARD = 427,                 /* FORWARD  */
    FREEZE = 428,                  /* FREEZE  */
    FROM = 429,                    /* FROM  */
    FULL = 430,                    /* FULL  */
    FUNCTION = 431,                /* FUNCTION  */
    FUNCTIONS = 432,               /* FUNCTIONS  */
    GENERATED = 433,               /* GENERATED  */
    GLOB = 434,                    /* GLOB  */
    GLOBAL = 435,                  /* GLOBAL  */
    GRANT = 436,                   /* GRANT  */
    GRANTED = 437,                 /* GRANTED  */
    GROUP_P = 438,                 /* GROUP_P  */
    GROUPING = 439,                /* GROUPING  */
    GROUPING_ID = 440,             /* GROUPING_ID  */
    GROUPS = 441,                  /* GROUPS  */
    HANDLER = 442,                 /* HANDLER  */
    HAVING = 443,                  /* HAVING  */
    HEADER_P = 444,                /* HEADER_P  */
    HOLD = 445,                    /* HOLD  */
    HOUR_P = 446,                  /* HOUR_P  */
    HOURS_P = 447,                 /* HOURS_P  */
    IDENTITY_P = 448,              /* IDENTITY_P  */
    IF_P = 449,                    /* IF_P  */
    IGNORE_P = 450,                /* IGNORE_P  */
    ILIKE = 451,                   /* ILIKE  */
    IMMEDIATE = 452,               /* IMMEDIATE  */
    IMMUTABLE = 453,               /* IMMUTABLE  */
    IMPLICIT_P = 454,              /* IMPLICIT_P  */
    IMPORT_P = 455,                /* IMPORT_P  */
    IN_P = 456,                    /* IN_P  */
    INCLUDE_P = 457,               /* INCLUDE_P  */
    INCLUDING = 458,               /* INCLUDING  */
    INCREMENT = 459,               /* INCREMENT  */
    INDEX = 460,                   /* INDEX  */
    INDEXES = 461,                 /* INDEXES  */
    INHERIT = 462,                 /* INHERIT  */
    INHERITS = 463,                /* INHERITS  */
    INITIALLY = 464,               /* INITIALLY  */
    INLINE_P = 465,                /* INLINE_P  */
    INNER_P = 466,                 /* INNER_P  */
    INOUT = 467,                   /* INOUT  */
    INPUT_P = 468,                 /* INPUT_P  */
    INSENSITIVE = 469,             /* INSENSITIVE  */
    INSERT = 470,                  /* INSERT  */
    INSTALL = 471,                 /* INSTALL  */
    INSTEAD = 472,                 /* INSTEAD  */
    INT_P = 473,                   /* INT_P  */
    INTEGER = 474,                 /* INTEGER  */
    INTERSECT = 475,               /* INTERSECT  */
    INTERVAL = 476,                /* INTERVAL  */
    INTO = 477,                    /* INTO  */
    INVOKER = 478,                 /* INVOKER  */
    IS = 479,                      /* IS  */
    ISNULL = 480,                  /* ISNULL  */
    ISOLATION = 481,               /* ISOLATION  */
    JOIN = 482,                    /* JOIN  */
    JSON = 483,                    /* JSON  */
    KEY = 484,                     /* KEY  */
    LABEL = 485,                   /* LABEL  */
    LAMBDA = 486,                  /* LAMBDA  */
    LANGUAGE = 487,                /* LANGUAGE  */
    LARGE_P = 488,                 /* LARGE_P  */
    LAST_P = 489,                  /* LAST_P  */
    LATERAL_P = 490,               /* LATERAL_P  */
    LEADING = 491,                 /* LEADING  */
    LEAKPROOF = 492,               /* LEAKPROOF  */
    LEFT = 493,                    /* LEFT  */
    LEVEL = 494,                   /* LEVEL  */
    LIKE = 495,                    /* LIKE  */
    LIMIT = 496,                   /* LIMIT  */
    LISTEN = 497,                  /* LISTEN  */
    LOAD = 498,                    /* LOAD  */
    LOCAL = 499,                   /* LOCAL  */
    LOCATION = 500,                /* LOCATION  */
    LOCK_P = 501,                  /* LOCK_P  */
    LOCKED = 502,                  /* LOCKED  */
    LOGGED = 503,                  /* LOGGED  */
    MACRO = 504,                   /* MACRO  */
    MAP = 505,                     /* MAP  */
    MAPPING = 506,                 /* MAPPING  */
    MATCH = 507,                   /* MATCH  */
    MATCHED = 508,                 /* MATCHED  */
    MATERIALIZED = 509,            /* MATERIALIZED  */
    MAXVALUE = 510,                /* MAXVALUE  */
    MERGE = 511,                   /* MERGE  */
    METHOD = 512,                  /* METHOD  */
    MICROSECOND_P = 513,           /* MICROSECOND_P  */
    MICROSECONDS_P = 514,          /* MICROSECONDS_P  */
    MILLENNIA_P = 515,             /* MILLENNIA_P  */
    MILLENNIUM_P = 516,            /* MILLENNIUM_P  */
    MILLISECOND_P = 517,           /* MILLISECOND_P  */
    MILLISECONDS_P = 518,          /* MILLISECONDS_P  */
    MINUTE_P = 519,                /* MINUTE_P  */
    MINUTES_P = 520,               /* MINUTES_P  */
    MINVALUE = 521,                /* MINVALUE  */
    MODE = 522,                    /* MODE  */
    MONTH_P = 523,                 /* MONTH_P  */
    MONTHS_P = 524,                /* MONTHS_P  */
    MOVE = 525,                    /* MOVE  */
    NAME_P = 526,                  /* NAME_P  */
    NAMES = 527,                   /* NAMES  */
    NATIONAL = 528,                /* NATIONAL  */
    NATURAL = 529,                 /* NATURAL  */
    NCHAR = 530,                   /* NCHAR  */
    NEW = 531,                     /* NEW  */
    NEXT = 532,                    /* NEXT  */
    NO = 533,                      /* NO  */
    NONE = 534,                    /* NONE  */
    NOT = 535,                     /* NOT  */
    NOTHING = 536,                 /* NOTHING  */
    NOTIFY = 537,                  /* NOTIFY  */
    NOTNULL = 538,                 /* NOTNULL  */
    NOWAIT = 539,                  /* NOWAIT  */
    NULL_P = 540,                  /* NULL_P  */
    NULLIF = 541,                  /* NULLIF  */
    NULLS_P = 542,                 /* NULLS_P  */
    NUMERIC = 543,                 /* NUMERIC  */
    OBJECT_P = 544,                /* OBJECT_P  */
    OF = 545,                      /* OF  */
    OFF = 546,                     /* OFF  */
    OFFSET = 547,                  /* OFFSET  */
    OIDS = 548,                    /* OIDS  */
    OLD = 549,                     /* OLD  */
    ON = 550,                      /* ON  */
    ONLY = 551,                    /* ONLY  */
    OPERATOR = 552,                /* OPERATOR  */
    OPTION = 553,                  /* OPTION  */
    OPTIONS = 554,                 /* OPTIONS  */
    OR = 555,                      /* OR  */
    ORDER = 556,                   /* ORDER  */
    ORDINALITY = 557,              /* ORDINALITY  */
    OTHERS = 558,                  /* OTHERS  */
    OUT_P = 559,                   /* OUT_P  */
    OUTER_P = 560,                 /* OUTER_P  */
    OVER = 561,                    /* OVER  */
    OVERLAPS = 562,                /* OVERLAPS  */
    OVERLAY = 563,                 /* OVERLAY  */
    OVERRIDING = 564,              /* OVERRIDING  */
    OWNED = 565,                   /* OWNED  */
    OWNER = 566,                   /* OWNER  */
    PARALLEL = 567,                /* PARALLEL  */
    PARSER = 568,                  /* PARSER  */
    PARTIAL = 569,                 /* PARTIAL  */
    PARTITION = 570,               /* PARTITION  */
    PARTITIONED = 571,             /* PARTITIONED  */
    PASSING = 572,                 /* PASSING  */
    PASSWORD = 573,                /* PASSWORD  */
    PERCENT = 574,                 /* PERCENT  */
    PERSISTENT = 575,              /* PERSISTENT  */
    PIVOT = 576,                   /* PIVOT  */
    PIVOT_LONGER = 577,            /* PIVOT_LONGER  */
    PIVOT_WIDER = 578,             /* PIVOT_WIDER  */
    PLACING = 579,                 /* PLACING  */
    PLANS = 580,                   /* PLANS  */
    POLICY = 581,                  /* POLICY  */
    POSITION = 582,                /* POSITION  */
    POSITIONAL = 583,              /* POSITIONAL  */
    PRAGMA_P = 584,                /* PRAGMA_P  */
    PRECEDING = 585,               /* PRECEDING  */
    PRECISION = 586,               /* PRECISION  */
    PREPARE = 587,                 /* PREPARE  */
    PREPARED = 588,                /* PREPARED  */
    PRESERVE = 589,                /* PRESERVE  */
    PRIMARY = 590,                 /* PRIMARY  */
    PRIOR = 591,                   /* PRIOR  */
    PRIVILEGES = 592,              /* PRIVILEGES  */
    PROCEDURAL = 593,              /* PROCEDURAL  */
    PROCEDURE = 594,               /* PROCEDURE  */
    PROGRAM = 595,                 /* PROGRAM  */
    PUBLICATION = 596,             /* PUBLICATION  */
    QUALIFY = 597,                 /* QUALIFY  */
    QUARTER_P = 598,               /* QUARTER_P  */
    QUARTERS_P = 599,              /* QUARTERS_P  */
    QUOTE = 600,                   /* QUOTE  */
    RANGE = 601,                   /* RANGE  */
    READ_P = 602,                  /* READ_P  */
    REAL = 603,                    /* REAL  */
    REASSIGN = 604,                /* REASSIGN  */
    RECHECK = 605,                 /* RECHECK  */
    RECURSIVE = 606,               /* RECURSIVE  */
    REF = 607,                     /* REF  */
    REFERENCES = 608,              /* REFERENCES  */
    REFERENCING = 609,             /* REFERENCING  */
    REFRESH = 610,                 /* REFRESH  */
    REINDEX = 611,                 /* REINDEX  */
    RELATIVE_P = 612,              /* RELATIVE_P  */
    RELEASE = 613,                 /* RELEASE  */
    RENAME = 614,                  /* RENAME  */
    REPEATABLE = 615,              /* REPEATABLE  */
    REPLACE = 616,                 /* REPLACE  */
    REPLICA = 617,                 /* REPLICA  */
    RESET = 618,                   /* RESET  */
    RESPECT_P = 619,               /* RESPECT_P  */
    RESTART = 620,                 /* RESTART  */
    RESTRICT = 621,                /* RESTRICT  */
    RETURNING = 622,               /* RETURNING  */
    RETURNS = 623,                 /* RETURNS  */
    REVOKE = 624,                  /* REVOKE  */
    RIGHT = 625,                   /* RIGHT  */
    ROLE = 626,                    /* ROLE  */
    ROLLBACK = 627,                /* ROLLBACK  */
    ROLLUP = 628,                  /* ROLLUP  */
    ROW = 629,                     /* ROW  */
    ROWS = 630,                    /* ROWS  */
    RULE = 631,                    /* RULE  */
    SAMPLE = 632,                  /* SAMPLE  */
    SAVEPOINT = 633,               /* SAVEPOINT  */
    SCHEMA = 634,                  /* SCHEMA  */
    SCHEMAS = 635,                 /* SCHEMAS  */
    SCOPE = 636,                   /* SCOPE  */
    SCROLL = 637,                  /* SCROLL  */
    SEARCH = 638,                  /* SEARCH  */
    SECOND_P = 639,                /* SECOND_P  */
    SECONDS_P = 640,               /* SECONDS_P  */
    SECRET = 641,                  /* SECRET  */
    SECURITY = 642,                /* SECURITY  */
    SELECT = 643,                  /* SELECT  */
    SEMI = 644,                    /* SEMI  */
    SEQUENCE = 645,                /* SEQUENCE  */
    SEQUENCES = 646,               /* SEQUENCES  */
    SERIALIZABLE = 647,            /* SERIALIZABLE  */
    SERVER = 648,                  /* SERVER  */
    SESSION = 649,                 /* SESSION  */
    SET = 650,                     /* SET  */
    SETOF = 651,                   /* SETOF  */
    SETS = 652,                    /* SETS  */
    SHARE = 653,                   /* SHARE  */
    SHOW = 654,                    /* SHOW  */
    SIMILAR = 655,                 /* SIMILAR  */
    SIMPLE = 656,                  /* SIMPLE  */
    SKIP = 657,                    /* SKIP  */
    SMALLINT = 658,                /* SMALLINT  */
    SNAPSHOT = 659,                /* SNAPSHOT  */
    SOME = 660,                    /* SOME  */
    SORTED = 661,                  /* SORTED  */
    SOURCE_P = 662,                /* SOURCE_P  */
    SQL_P = 663,                   /* SQL_P  */
    STABLE = 664,                  /* STABLE  */
    STANDALONE_P = 665,            /* STANDALONE_P  */
    START = 666,                   /* START  */
    STATEMENT = 667,               /* STATEMENT  */
    STATISTICS = 668,              /* STATISTICS  */
    STDIN = 669,                   /* STDIN  */
    STDOUT = 670,                  /* STDOUT  */
    STORAGE = 671,                 /* STORAGE  */
    STORED = 672,                  /* STORED  */
    STRICT_P = 673,                /* STRICT_P  */
    STRIP_P = 674,                 /* STRIP_P  */
    STRUCT = 675,                  /* STRUCT  */
    SUBSCRIPTION = 676,            /* SUBSCRIPTION  */
    SUBSTRING = 677,               /* SUBSTRING  */
    SUMMARIZE = 678,               /* SUMMARIZE  */
    SWITCH = 679,                  /* SWITCH  */
    SYMMETRIC = 680,               /* SYMMETRIC  */
    SYSID = 681,                   /* SYSID  */
    SYSTEM_P = 682,                /* SYSTEM_P  */
    TABLE = 683,                   /* TABLE  */
    TABLES = 684,                  /* TABLES  */
    TABLESAMPLE = 685,             /* TABLESAMPLE  */
    TABLESPACE = 686,              /* TABLESPACE  */
    TARGET_P = 687,                /* TARGET_P  */
    TEMP = 688,                    /* TEMP  */
    TEMPLATE = 689,                /* TEMPLATE  */
    TEMPORARY = 690,               /* TEMPORARY  */
    TEXT_P = 691,                  /* TEXT_P  */
    THEN = 692,                    /* THEN  */
    TIES = 693,                    /* TIES  */
    TIME = 694,                    /* TIME  */
    TIMESTAMP = 695,               /* TIMESTAMP  */
    TO = 696,                      /* TO  */
    TRAILING = 697,                /* TRAILING  */
    TRANSACTION = 698,             /* TRANSACTION  */
    TRANSFORM = 699,               /* TRANSFORM  */
    TREAT = 700,                   /* TREAT  */
    TRIGGER = 701,                 /* TRIGGER  */
    TRIM = 702,                    /* TRIM  */
    TRUE_P = 703,                  /* TRUE_P  */
    TRUNCATE = 704,                /* TRUNCATE  */
    TRUSTED = 705,                 /* TRUSTED  */
    TRY_CAST = 706,                /* TRY_CAST  */
    TYPE_P = 707,                  /* TYPE_P  */
    TYPES_P = 708,                 /* TYPES_P  */
    UNBOUNDED = 709,               /* UNBOUNDED  */
    UNCOMMITTED = 710,             /* UNCOMMITTED  */
    UNENCRYPTED = 711,             /* UNENCRYPTED  */
    UNION = 712,                   /* UNION  */
    UNIQUE = 713,                  /* UNIQUE  */
    UNKNOWN = 714,                 /* UNKNOWN  */
    UNLISTEN = 715,                /* UNLISTEN  */
    UNLOGGED = 716,                /* UNLOGGED  */
    UNPACK = 717,                  /* UNPACK  */
    UNPIVOT = 718,                 /* UNPIVOT  */
    UNTIL = 719,                   /* UNTIL  */
    UPDATE = 720,                  /* UPDATE  */
    USE_P = 721,                   /* USE_P  */
    USER = 722,                    /* USER  */
    USING = 723,                   /* USING  */
    VACUUM = 724,                  /* VACUUM  */
    VALID = 725,                   /* VALID  */
    VALIDATE = 726,                /* VALIDATE  */
    VALIDATOR = 727,               /* VALIDATOR  */
    VALUE_P = 728,                 /* VALUE_P  */
    VALUES = 729,                  /* VALUES  */
    VARCHAR = 730,                 /* VARCHAR  */
    VARIABLE_P = 731,              /* VARIABLE_P  */
    VARIADIC = 732,                /* VARIADIC  */
    VARYING = 733,                 /* VARYING  */
    VERBOSE = 734,                 /* VERBOSE  */
    VERSION_P = 735,               /* VERSION_P  */
    VIEW = 736,                    /* VIEW  */
    VIEWS = 737,                   /* VIEWS  */
    VIRTUAL = 738,                 /* VIRTUAL  */
    VOLATILE = 739,                /* VOLATILE  */
    WEEK_P = 740,                  /* WEEK_P  */
    WEEKS_P = 741,                 /* WEEKS_P  */
    WHEN = 742,                    /* WHEN  */
    WHERE = 743,                   /* WHERE  */
    WHITESPACE_P = 744,            /* WHITESPACE_P  */
    WINDOW = 745,                  /* WINDOW  */
    WITH = 746,                    /* WITH  */
    WITHIN = 747,                  /* WITHIN  */
    WITHOUT = 748,                 /* WITHOUT  */
    WORK = 749,                    /* WORK  */
    WRAPPER = 750,                 /* WRAPPER  */
    WRITE_P = 751,                 /* WRITE_P  */
    XML_P = 752,                   /* XML_P  */
    XMLATTRIBUTES = 753,           /* XMLATTRIBUTES  */
    XMLCONCAT = 754,               /* XMLCONCAT  */
    XMLELEMENT = 755,              /* XMLELEMENT  */
    XMLEXISTS = 756,               /* XMLEXISTS  */
    XMLFOREST = 757,               /* XMLFOREST  */
    XMLNAMESPACES = 758,           /* XMLNAMESPACES  */
    XMLPARSE = 759,                /* XMLPARSE  */
    XMLPI = 760,                   /* XMLPI  */
    XMLROOT = 761,                 /* XMLROOT  */
    XMLSERIALIZE = 762,            /* XMLSERIALIZE  */
    XMLTABLE = 763,                /* XMLTABLE  */
    YEAR_P = 764,                  /* YEAR_P  */
    YEARS_P = 765,                 /* YEARS_P  */
    YES_P = 766,                   /* YES_P  */
    ZONE = 767,                    /* ZONE  */
    NOT_LA = 768,                  /* NOT_LA  */
    NULLS_LA = 769,                /* NULLS_LA  */
    WITH_LA = 770,                 /* WITH_LA  */
    POSTFIXOP = 771,               /* POSTFIXOP  */
    UMINUS = 772                   /* UMINUS  */
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
	PGCTEMaterialize			ctematerialize;
	PGWithClause			*with;
	PGInferClause			*infer;
	PGOnConflictClause	*onconflict;
	PGOnConflictActionAlias onconflictshorthand;
	PGAIndices			*aind;
	PGResTarget			*target;
	PGInsertStmt			*istmt;
	PGVariableSetStmt		*vsetstmt;
	PGOverridingKind       override;
	PGSortByDir            sortorder;
	PGSortByNulls          nullorder;
	PGIgnoreNulls          ignorenulls;
	PGConstrType           constr;
	PGLockClauseStrength lockstrength;
	PGLockWaitPolicy lockwaitpolicy;
	PGSubLinkType subquerytype;
	PGViewCheckOption viewcheckoption;
	PGInsertColumnOrder bynameorposition;
	PGLoadInstallType loadinstalltype;
	PGTransactionStmtType transactiontype;
	PGMergeAction mergeaction;

#line 633 "third_party/libpg_query/grammar/grammar_out.hpp"

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
