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
    LAMBDA_ARROW = 272,            /* LAMBDA_ARROW  */
    DOUBLE_ARROW = 273,            /* DOUBLE_ARROW  */
    LESS_EQUALS = 274,             /* LESS_EQUALS  */
    GREATER_EQUALS = 275,          /* GREATER_EQUALS  */
    NOT_EQUALS = 276,              /* NOT_EQUALS  */
    ABORT_P = 277,                 /* ABORT_P  */
    ABSOLUTE_P = 278,              /* ABSOLUTE_P  */
    ACCESS = 279,                  /* ACCESS  */
    ACTION = 280,                  /* ACTION  */
    ADD_P = 281,                   /* ADD_P  */
    ADMIN = 282,                   /* ADMIN  */
    AFTER = 283,                   /* AFTER  */
    AGGREGATE = 284,               /* AGGREGATE  */
    ALL = 285,                     /* ALL  */
    ALSO = 286,                    /* ALSO  */
    ALTER = 287,                   /* ALTER  */
    ALWAYS = 288,                  /* ALWAYS  */
    ANALYSE = 289,                 /* ANALYSE  */
    ANALYZE = 290,                 /* ANALYZE  */
    AND = 291,                     /* AND  */
    ANTI = 292,                    /* ANTI  */
    ANY = 293,                     /* ANY  */
    ARRAY = 294,                   /* ARRAY  */
    AS = 295,                      /* AS  */
    ASC_P = 296,                   /* ASC_P  */
    ASOF = 297,                    /* ASOF  */
    ASSERTION = 298,               /* ASSERTION  */
    ASSIGNMENT = 299,              /* ASSIGNMENT  */
    ASYMMETRIC = 300,              /* ASYMMETRIC  */
    AT = 301,                      /* AT  */
    ATTACH = 302,                  /* ATTACH  */
    ATTRIBUTE = 303,               /* ATTRIBUTE  */
    AUTHORIZATION = 304,           /* AUTHORIZATION  */
    BACKWARD = 305,                /* BACKWARD  */
    BEFORE = 306,                  /* BEFORE  */
    BEGIN_P = 307,                 /* BEGIN_P  */
    BETWEEN = 308,                 /* BETWEEN  */
    BIGINT = 309,                  /* BIGINT  */
    BINARY = 310,                  /* BINARY  */
    BIT = 311,                     /* BIT  */
    BOOLEAN_P = 312,               /* BOOLEAN_P  */
    BOTH = 313,                    /* BOTH  */
    BY = 314,                      /* BY  */
    CACHE = 315,                   /* CACHE  */
    CALL_P = 316,                  /* CALL_P  */
    CALLED = 317,                  /* CALLED  */
    CASCADE = 318,                 /* CASCADE  */
    CASCADED = 319,                /* CASCADED  */
    CASE = 320,                    /* CASE  */
    CAST = 321,                    /* CAST  */
    CATALOG_P = 322,               /* CATALOG_P  */
    CENTURIES_P = 323,             /* CENTURIES_P  */
    CENTURY_P = 324,               /* CENTURY_P  */
    CHAIN = 325,                   /* CHAIN  */
    CHAR_P = 326,                  /* CHAR_P  */
    CHARACTER = 327,               /* CHARACTER  */
    CHARACTERISTICS = 328,         /* CHARACTERISTICS  */
    CHECK_P = 329,                 /* CHECK_P  */
    CHECKPOINT = 330,              /* CHECKPOINT  */
    CLASS = 331,                   /* CLASS  */
    CLOSE = 332,                   /* CLOSE  */
    CLUSTER = 333,                 /* CLUSTER  */
    COALESCE = 334,                /* COALESCE  */
    COLLATE = 335,                 /* COLLATE  */
    COLLATION = 336,               /* COLLATION  */
    COLUMN = 337,                  /* COLUMN  */
    COLUMNS = 338,                 /* COLUMNS  */
    COMMENT = 339,                 /* COMMENT  */
    COMMENTS = 340,                /* COMMENTS  */
    COMMIT = 341,                  /* COMMIT  */
    COMMITTED = 342,               /* COMMITTED  */
    COMPRESSION = 343,             /* COMPRESSION  */
    CONCURRENTLY = 344,            /* CONCURRENTLY  */
    CONFIGURATION = 345,           /* CONFIGURATION  */
    CONFLICT = 346,                /* CONFLICT  */
    CONNECTION = 347,              /* CONNECTION  */
    CONSTRAINT = 348,              /* CONSTRAINT  */
    CONSTRAINTS = 349,             /* CONSTRAINTS  */
    CONTENT_P = 350,               /* CONTENT_P  */
    CONTINUE_P = 351,              /* CONTINUE_P  */
    CONVERSION_P = 352,            /* CONVERSION_P  */
    COPY = 353,                    /* COPY  */
    COST = 354,                    /* COST  */
    CREATE_P = 355,                /* CREATE_P  */
    CROSS = 356,                   /* CROSS  */
    CSV = 357,                     /* CSV  */
    CUBE = 358,                    /* CUBE  */
    CURRENT_P = 359,               /* CURRENT_P  */
    CURSOR = 360,                  /* CURSOR  */
    CYCLE = 361,                   /* CYCLE  */
    DATA_P = 362,                  /* DATA_P  */
    DATABASE = 363,                /* DATABASE  */
    DAY_P = 364,                   /* DAY_P  */
    DAYS_P = 365,                  /* DAYS_P  */
    DEALLOCATE = 366,              /* DEALLOCATE  */
    DEC = 367,                     /* DEC  */
    DECADE_P = 368,                /* DECADE_P  */
    DECADES_P = 369,               /* DECADES_P  */
    DECIMAL_P = 370,               /* DECIMAL_P  */
    DECLARE = 371,                 /* DECLARE  */
    DEFAULT = 372,                 /* DEFAULT  */
    DEFAULTS = 373,                /* DEFAULTS  */
    DEFERRABLE = 374,              /* DEFERRABLE  */
    DEFERRED = 375,                /* DEFERRED  */
    DEFINER = 376,                 /* DEFINER  */
    DELETE_P = 377,                /* DELETE_P  */
    DELIMITER = 378,               /* DELIMITER  */
    DELIMITERS = 379,              /* DELIMITERS  */
    DEPENDS = 380,                 /* DEPENDS  */
    DESC_P = 381,                  /* DESC_P  */
    DESCRIBE = 382,                /* DESCRIBE  */
    DETACH = 383,                  /* DETACH  */
    DICTIONARY = 384,              /* DICTIONARY  */
    DISABLE_P = 385,               /* DISABLE_P  */
    DISCARD = 386,                 /* DISCARD  */
    DISTINCT = 387,                /* DISTINCT  */
    DO = 388,                      /* DO  */
    DOCUMENT_P = 389,              /* DOCUMENT_P  */
    DOMAIN_P = 390,                /* DOMAIN_P  */
    DOUBLE_P = 391,                /* DOUBLE_P  */
    DROP = 392,                    /* DROP  */
    EACH = 393,                    /* EACH  */
    ELSE = 394,                    /* ELSE  */
    ENABLE_P = 395,                /* ENABLE_P  */
    ENCODING = 396,                /* ENCODING  */
    ENCRYPTED = 397,               /* ENCRYPTED  */
    END_P = 398,                   /* END_P  */
    ENUM_P = 399,                  /* ENUM_P  */
    ESCAPE = 400,                  /* ESCAPE  */
    EVENT = 401,                   /* EVENT  */
    EXCEPT = 402,                  /* EXCEPT  */
    EXCLUDE = 403,                 /* EXCLUDE  */
    EXCLUDING = 404,               /* EXCLUDING  */
    EXCLUSIVE = 405,               /* EXCLUSIVE  */
    EXECUTE = 406,                 /* EXECUTE  */
    EXISTS = 407,                  /* EXISTS  */
    EXPLAIN = 408,                 /* EXPLAIN  */
    EXPORT_P = 409,                /* EXPORT_P  */
    EXPORT_STATE = 410,            /* EXPORT_STATE  */
    EXTENSION = 411,               /* EXTENSION  */
    EXTENSIONS = 412,              /* EXTENSIONS  */
    EXTERNAL = 413,                /* EXTERNAL  */
    EXTRACT = 414,                 /* EXTRACT  */
    FALSE_P = 415,                 /* FALSE_P  */
    FAMILY = 416,                  /* FAMILY  */
    FETCH = 417,                   /* FETCH  */
    FILTER = 418,                  /* FILTER  */
    FIRST_P = 419,                 /* FIRST_P  */
    FLOAT_P = 420,                 /* FLOAT_P  */
    FOLLOWING = 421,               /* FOLLOWING  */
    FOR = 422,                     /* FOR  */
    FORCE = 423,                   /* FORCE  */
    FOREIGN = 424,                 /* FOREIGN  */
    FORWARD = 425,                 /* FORWARD  */
    FREEZE = 426,                  /* FREEZE  */
    FROM = 427,                    /* FROM  */
    FULL = 428,                    /* FULL  */
    FUNCTION = 429,                /* FUNCTION  */
    FUNCTIONS = 430,               /* FUNCTIONS  */
    GENERATED = 431,               /* GENERATED  */
    GLOB = 432,                    /* GLOB  */
    GLOBAL = 433,                  /* GLOBAL  */
    GRANT = 434,                   /* GRANT  */
    GRANTED = 435,                 /* GRANTED  */
    GROUP_P = 436,                 /* GROUP_P  */
    GROUPING = 437,                /* GROUPING  */
    GROUPING_ID = 438,             /* GROUPING_ID  */
    GROUPS = 439,                  /* GROUPS  */
    HANDLER = 440,                 /* HANDLER  */
    HAVING = 441,                  /* HAVING  */
    HEADER_P = 442,                /* HEADER_P  */
    HOLD = 443,                    /* HOLD  */
    HOUR_P = 444,                  /* HOUR_P  */
    HOURS_P = 445,                 /* HOURS_P  */
    IDENTITY_P = 446,              /* IDENTITY_P  */
    IF_P = 447,                    /* IF_P  */
    IGNORE_P = 448,                /* IGNORE_P  */
    ILIKE = 449,                   /* ILIKE  */
    IMMEDIATE = 450,               /* IMMEDIATE  */
    IMMUTABLE = 451,               /* IMMUTABLE  */
    IMPLICIT_P = 452,              /* IMPLICIT_P  */
    IMPORT_P = 453,                /* IMPORT_P  */
    IN_P = 454,                    /* IN_P  */
    INCLUDE_P = 455,               /* INCLUDE_P  */
    INCLUDING = 456,               /* INCLUDING  */
    INCREMENT = 457,               /* INCREMENT  */
    INDEX = 458,                   /* INDEX  */
    INDEXES = 459,                 /* INDEXES  */
    INHERIT = 460,                 /* INHERIT  */
    INHERITS = 461,                /* INHERITS  */
    INITIALLY = 462,               /* INITIALLY  */
    INLINE_P = 463,                /* INLINE_P  */
    INNER_P = 464,                 /* INNER_P  */
    INOUT = 465,                   /* INOUT  */
    INPUT_P = 466,                 /* INPUT_P  */
    INSENSITIVE = 467,             /* INSENSITIVE  */
    INSERT = 468,                  /* INSERT  */
    INSTALL = 469,                 /* INSTALL  */
    INSTEAD = 470,                 /* INSTEAD  */
    INT_P = 471,                   /* INT_P  */
    INTEGER = 472,                 /* INTEGER  */
    INTERSECT = 473,               /* INTERSECT  */
    INTERVAL = 474,                /* INTERVAL  */
    INTO = 475,                    /* INTO  */
    INVOKER = 476,                 /* INVOKER  */
    IS = 477,                      /* IS  */
    ISNULL = 478,                  /* ISNULL  */
    ISOLATION = 479,               /* ISOLATION  */
    JOIN = 480,                    /* JOIN  */
    JSON = 481,                    /* JSON  */
    KEY = 482,                     /* KEY  */
    LABEL = 483,                   /* LABEL  */
    LANGUAGE = 484,                /* LANGUAGE  */
    LARGE_P = 485,                 /* LARGE_P  */
    LAST_P = 486,                  /* LAST_P  */
    LATERAL_P = 487,               /* LATERAL_P  */
    LEADING = 488,                 /* LEADING  */
    LEAKPROOF = 489,               /* LEAKPROOF  */
    LEFT = 490,                    /* LEFT  */
    LEVEL = 491,                   /* LEVEL  */
    LIKE = 492,                    /* LIKE  */
    LIMIT = 493,                   /* LIMIT  */
    LISTEN = 494,                  /* LISTEN  */
    LOAD = 495,                    /* LOAD  */
    LOCAL = 496,                   /* LOCAL  */
    LOCATION = 497,                /* LOCATION  */
    LOCK_P = 498,                  /* LOCK_P  */
    LOCKED = 499,                  /* LOCKED  */
    LOGGED = 500,                  /* LOGGED  */
    MACRO = 501,                   /* MACRO  */
    MAP = 502,                     /* MAP  */
    MAPPING = 503,                 /* MAPPING  */
    MATCH = 504,                   /* MATCH  */
    MATERIALIZED = 505,            /* MATERIALIZED  */
    MAXVALUE = 506,                /* MAXVALUE  */
    METHOD = 507,                  /* METHOD  */
    MICROSECOND_P = 508,           /* MICROSECOND_P  */
    MICROSECONDS_P = 509,          /* MICROSECONDS_P  */
    MILLENNIA_P = 510,             /* MILLENNIA_P  */
    MILLENNIUM_P = 511,            /* MILLENNIUM_P  */
    MILLISECOND_P = 512,           /* MILLISECOND_P  */
    MILLISECONDS_P = 513,          /* MILLISECONDS_P  */
    MINUTE_P = 514,                /* MINUTE_P  */
    MINUTES_P = 515,               /* MINUTES_P  */
    MINVALUE = 516,                /* MINVALUE  */
    MODE = 517,                    /* MODE  */
    MONTH_P = 518,                 /* MONTH_P  */
    MONTHS_P = 519,                /* MONTHS_P  */
    MOVE = 520,                    /* MOVE  */
    MYSQL_P = 521,                 /* MYSQL_P  */
    NAME_P = 522,                  /* NAME_P  */
    NAMES = 523,                   /* NAMES  */
    NATIONAL = 524,                /* NATIONAL  */
    NATURAL = 525,                 /* NATURAL  */
    NCHAR = 526,                   /* NCHAR  */
    NEW = 527,                     /* NEW  */
    NEXT = 528,                    /* NEXT  */
    NO = 529,                      /* NO  */
    NONE = 530,                    /* NONE  */
    NOT = 531,                     /* NOT  */
    NOTHING = 532,                 /* NOTHING  */
    NOTIFY = 533,                  /* NOTIFY  */
    NOTNULL = 534,                 /* NOTNULL  */
    NOWAIT = 535,                  /* NOWAIT  */
    NULL_P = 536,                  /* NULL_P  */
    NULLIF = 537,                  /* NULLIF  */
    NULLS_P = 538,                 /* NULLS_P  */
    NUMERIC = 539,                 /* NUMERIC  */
    OBJECT_P = 540,                /* OBJECT_P  */
    OF = 541,                      /* OF  */
    OFF = 542,                     /* OFF  */
    OFFSET = 543,                  /* OFFSET  */
    OIDS = 544,                    /* OIDS  */
    OLD = 545,                     /* OLD  */
    ON = 546,                      /* ON  */
    ONLY = 547,                    /* ONLY  */
    OPERATOR = 548,                /* OPERATOR  */
    OPTION = 549,                  /* OPTION  */
    OPTIONS = 550,                 /* OPTIONS  */
    OR = 551,                      /* OR  */
    ORDER = 552,                   /* ORDER  */
    ORDINALITY = 553,              /* ORDINALITY  */
    OTHERS = 554,                  /* OTHERS  */
    OUT_P = 555,                   /* OUT_P  */
    OUTER_P = 556,                 /* OUTER_P  */
    OVER = 557,                    /* OVER  */
    OVERLAPS = 558,                /* OVERLAPS  */
    OVERLAY = 559,                 /* OVERLAY  */
    OVERRIDING = 560,              /* OVERRIDING  */
    OWNED = 561,                   /* OWNED  */
    OWNER = 562,                   /* OWNER  */
    PARALLEL = 563,                /* PARALLEL  */
    PARSER = 564,                  /* PARSER  */
    PARTIAL = 565,                 /* PARTIAL  */
    PARTITION = 566,               /* PARTITION  */
    PASSING = 567,                 /* PASSING  */
    PASSWORD = 568,                /* PASSWORD  */
    PERCENT = 569,                 /* PERCENT  */
    PERSISTENT = 570,              /* PERSISTENT  */
    PIVOT = 571,                   /* PIVOT  */
    PIVOT_LONGER = 572,            /* PIVOT_LONGER  */
    PIVOT_WIDER = 573,             /* PIVOT_WIDER  */
    PLACING = 574,                 /* PLACING  */
    PLANS = 575,                   /* PLANS  */
    POLICY = 576,                  /* POLICY  */
    POSITION = 577,                /* POSITION  */
    POSITIONAL = 578,              /* POSITIONAL  */
    PRAGMA_P = 579,                /* PRAGMA_P  */
    PRECEDING = 580,               /* PRECEDING  */
    PRECISION = 581,               /* PRECISION  */
    PREPARE = 582,                 /* PREPARE  */
    PREPARED = 583,                /* PREPARED  */
    PRESERVE = 584,                /* PRESERVE  */
    PRIMARY = 585,                 /* PRIMARY  */
    PRIOR = 586,                   /* PRIOR  */
    PRIVILEGES = 587,              /* PRIVILEGES  */
    PROCEDURAL = 588,              /* PROCEDURAL  */
    PROCEDURE = 589,               /* PROCEDURE  */
    PROGRAM = 590,                 /* PROGRAM  */
    PUBLICATION = 591,             /* PUBLICATION  */
    QUALIFY = 592,                 /* QUALIFY  */
    QUARTER_P = 593,               /* QUARTER_P  */
    QUARTERS_P = 594,              /* QUARTERS_P  */
    QUOTE = 595,                   /* QUOTE  */
    RANGE = 596,                   /* RANGE  */
    READ_P = 597,                  /* READ_P  */
    REAL = 598,                    /* REAL  */
    REASSIGN = 599,                /* REASSIGN  */
    RECHECK = 600,                 /* RECHECK  */
    RECURSIVE = 601,               /* RECURSIVE  */
    REF = 602,                     /* REF  */
    REFERENCES = 603,              /* REFERENCES  */
    REFERENCING = 604,             /* REFERENCING  */
    REFRESH = 605,                 /* REFRESH  */
    REINDEX = 606,                 /* REINDEX  */
    RELATIVE_P = 607,              /* RELATIVE_P  */
    RELEASE = 608,                 /* RELEASE  */
    RENAME = 609,                  /* RENAME  */
    REPEATABLE = 610,              /* REPEATABLE  */
    REPLACE = 611,                 /* REPLACE  */
    REPLICA = 612,                 /* REPLICA  */
    RESET = 613,                   /* RESET  */
    RESPECT_P = 614,               /* RESPECT_P  */
    RESTART = 615,                 /* RESTART  */
    RESTRICT = 616,                /* RESTRICT  */
    RETURNING = 617,               /* RETURNING  */
    RETURNS = 618,                 /* RETURNS  */
    REVOKE = 619,                  /* REVOKE  */
    RIGHT = 620,                   /* RIGHT  */
    ROLE = 621,                    /* ROLE  */
    ROLLBACK = 622,                /* ROLLBACK  */
    ROLLUP = 623,                  /* ROLLUP  */
    ROW = 624,                     /* ROW  */
    ROWS = 625,                    /* ROWS  */
    RULE = 626,                    /* RULE  */
    SAMPLE = 627,                  /* SAMPLE  */
    SAVEPOINT = 628,               /* SAVEPOINT  */
    SCHEMA = 629,                  /* SCHEMA  */
    SCHEMAS = 630,                 /* SCHEMAS  */
    SCOPE = 631,                   /* SCOPE  */
    SCROLL = 632,                  /* SCROLL  */
    SEARCH = 633,                  /* SEARCH  */
    SECOND_P = 634,                /* SECOND_P  */
    SECONDS_P = 635,               /* SECONDS_P  */
    SECRET = 636,                  /* SECRET  */
    SECURITY = 637,                /* SECURITY  */
    SELECT = 638,                  /* SELECT  */
    SEMI = 639,                    /* SEMI  */
    SEQUENCE = 640,                /* SEQUENCE  */
    SEQUENCES = 641,               /* SEQUENCES  */
    SERIALIZABLE = 642,            /* SERIALIZABLE  */
    SERVER = 643,                  /* SERVER  */
    SESSION = 644,                 /* SESSION  */
    SET = 645,                     /* SET  */
    SETOF = 646,                   /* SETOF  */
    SETS = 647,                    /* SETS  */
    SHARE = 648,                   /* SHARE  */
    SHOW = 649,                    /* SHOW  */
    SIMILAR = 650,                 /* SIMILAR  */
    SIMPLE = 651,                  /* SIMPLE  */
    SKIP = 652,                    /* SKIP  */
    SMALLINT = 653,                /* SMALLINT  */
    SNAPSHOT = 654,                /* SNAPSHOT  */
    SOME = 655,                    /* SOME  */
    SQL_P = 656,                   /* SQL_P  */
    STABLE = 657,                  /* STABLE  */
    STANDALONE_P = 658,            /* STANDALONE_P  */
    START = 659,                   /* START  */
    STATEMENT = 660,               /* STATEMENT  */
    STATISTICS = 661,              /* STATISTICS  */
    STDIN = 662,                   /* STDIN  */
    STDOUT = 663,                  /* STDOUT  */
    STORAGE = 664,                 /* STORAGE  */
    STORED = 665,                  /* STORED  */
    STRICT_P = 666,                /* STRICT_P  */
    STRIP_P = 667,                 /* STRIP_P  */
    STRUCT = 668,                  /* STRUCT  */
    SUBSCRIPTION = 669,            /* SUBSCRIPTION  */
    SUBSTRING = 670,               /* SUBSTRING  */
    SUMMARIZE = 671,               /* SUMMARIZE  */
    SYMMETRIC = 672,               /* SYMMETRIC  */
    SYSID = 673,                   /* SYSID  */
    SYSTEM_P = 674,                /* SYSTEM_P  */
    TABLE = 675,                   /* TABLE  */
    TABLES = 676,                  /* TABLES  */
    TABLESAMPLE = 677,             /* TABLESAMPLE  */
    TABLESPACE = 678,              /* TABLESPACE  */
    TEMP = 679,                    /* TEMP  */
    TEMPLATE = 680,                /* TEMPLATE  */
    TEMPORARY = 681,               /* TEMPORARY  */
    TEXT_P = 682,                  /* TEXT_P  */
    THEN = 683,                    /* THEN  */
    TIES = 684,                    /* TIES  */
    TIME = 685,                    /* TIME  */
    TIMESTAMP = 686,               /* TIMESTAMP  */
    TO = 687,                      /* TO  */
    TRAILING = 688,                /* TRAILING  */
    TRANSACTION = 689,             /* TRANSACTION  */
    TRANSFORM = 690,               /* TRANSFORM  */
    TREAT = 691,                   /* TREAT  */
    TRIGGER = 692,                 /* TRIGGER  */
    TRIM = 693,                    /* TRIM  */
    TRUE_P = 694,                  /* TRUE_P  */
    TRUNCATE = 695,                /* TRUNCATE  */
    TRUSTED = 696,                 /* TRUSTED  */
    TRY_CAST = 697,                /* TRY_CAST  */
    TYPE_P = 698,                  /* TYPE_P  */
    TYPES_P = 699,                 /* TYPES_P  */
    UNBOUNDED = 700,               /* UNBOUNDED  */
    UNCOMMITTED = 701,             /* UNCOMMITTED  */
    UNENCRYPTED = 702,             /* UNENCRYPTED  */
    UNION = 703,                   /* UNION  */
    UNIQUE = 704,                  /* UNIQUE  */
    UNKNOWN = 705,                 /* UNKNOWN  */
    UNLISTEN = 706,                /* UNLISTEN  */
    UNLOGGED = 707,                /* UNLOGGED  */
    UNPIVOT = 708,                 /* UNPIVOT  */
    UNTIL = 709,                   /* UNTIL  */
    UPDATE = 710,                  /* UPDATE  */
    USE_P = 711,                   /* USE_P  */
    USER = 712,                    /* USER  */
    USING = 713,                   /* USING  */
    VACUUM = 714,                  /* VACUUM  */
    VALID = 715,                   /* VALID  */
    VALIDATE = 716,                /* VALIDATE  */
    VALIDATOR = 717,               /* VALIDATOR  */
    VALUE_P = 718,                 /* VALUE_P  */
    VALUES = 719,                  /* VALUES  */
    VARCHAR = 720,                 /* VARCHAR  */
    VARIABLE_P = 721,              /* VARIABLE_P  */
    VARIADIC = 722,                /* VARIADIC  */
    VARYING = 723,                 /* VARYING  */
    VERBOSE = 724,                 /* VERBOSE  */
    VERSION_P = 725,               /* VERSION_P  */
    VIEW = 726,                    /* VIEW  */
    VIEWS = 727,                   /* VIEWS  */
    VIRTUAL = 728,                 /* VIRTUAL  */
    VOLATILE = 729,                /* VOLATILE  */
    WEEK_P = 730,                  /* WEEK_P  */
    WEEKS_P = 731,                 /* WEEKS_P  */
    WHEN = 732,                    /* WHEN  */
    WHERE = 733,                   /* WHERE  */
    WHITESPACE_P = 734,            /* WHITESPACE_P  */
    WINDOW = 735,                  /* WINDOW  */
    WITH = 736,                    /* WITH  */
    WITHIN = 737,                  /* WITHIN  */
    WITHOUT = 738,                 /* WITHOUT  */
    WORK = 739,                    /* WORK  */
    WRAPPER = 740,                 /* WRAPPER  */
    WRITE_P = 741,                 /* WRITE_P  */
    XML_P = 742,                   /* XML_P  */
    XMLATTRIBUTES = 743,           /* XMLATTRIBUTES  */
    XMLCONCAT = 744,               /* XMLCONCAT  */
    XMLELEMENT = 745,              /* XMLELEMENT  */
    XMLEXISTS = 746,               /* XMLEXISTS  */
    XMLFOREST = 747,               /* XMLFOREST  */
    XMLNAMESPACES = 748,           /* XMLNAMESPACES  */
    XMLPARSE = 749,                /* XMLPARSE  */
    XMLPI = 750,                   /* XMLPI  */
    XMLROOT = 751,                 /* XMLROOT  */
    XMLSERIALIZE = 752,            /* XMLSERIALIZE  */
    XMLTABLE = 753,                /* XMLTABLE  */
    YEAR_P = 754,                  /* YEAR_P  */
    YEARS_P = 755,                 /* YEARS_P  */
    YES_P = 756,                   /* YES_P  */
    ZONE = 757,                    /* ZONE  */
    NOT_LA = 758,                  /* NOT_LA  */
    NULLS_LA = 759,                /* NULLS_LA  */
    WITH_LA = 760,                 /* WITH_LA  */
    POSTFIXOP = 761,               /* POSTFIXOP  */
    UMINUS = 762                   /* UMINUS  */
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

#line 622 "third_party/libpg_query/grammar/grammar_out.hpp"

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
