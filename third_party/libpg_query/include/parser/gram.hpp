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
    EXTERNAL = 412,                /* EXTERNAL  */
    EXTRACT = 413,                 /* EXTRACT  */
    FALSE_P = 414,                 /* FALSE_P  */
    FAMILY = 415,                  /* FAMILY  */
    FETCH = 416,                   /* FETCH  */
    FILTER = 417,                  /* FILTER  */
    FIRST_P = 418,                 /* FIRST_P  */
    FLOAT_P = 419,                 /* FLOAT_P  */
    FOLLOWING = 420,               /* FOLLOWING  */
    FOR = 421,                     /* FOR  */
    FORCE = 422,                   /* FORCE  */
    FOREIGN = 423,                 /* FOREIGN  */
    FORWARD = 424,                 /* FORWARD  */
    FREEZE = 425,                  /* FREEZE  */
    FROM = 426,                    /* FROM  */
    FULL = 427,                    /* FULL  */
    FUNCTION = 428,                /* FUNCTION  */
    FUNCTIONS = 429,               /* FUNCTIONS  */
    GENERATED = 430,               /* GENERATED  */
    GLOB = 431,                    /* GLOB  */
    GLOBAL = 432,                  /* GLOBAL  */
    GRANT = 433,                   /* GRANT  */
    GRANTED = 434,                 /* GRANTED  */
    GROUP_P = 435,                 /* GROUP_P  */
    GROUPING = 436,                /* GROUPING  */
    GROUPING_ID = 437,             /* GROUPING_ID  */
    GROUPS = 438,                  /* GROUPS  */
    HANDLER = 439,                 /* HANDLER  */
    HAVING = 440,                  /* HAVING  */
    HEADER_P = 441,                /* HEADER_P  */
    HOLD = 442,                    /* HOLD  */
    HOUR_P = 443,                  /* HOUR_P  */
    HOURS_P = 444,                 /* HOURS_P  */
    IDENTITY_P = 445,              /* IDENTITY_P  */
    IF_P = 446,                    /* IF_P  */
    IGNORE_P = 447,                /* IGNORE_P  */
    ILIKE = 448,                   /* ILIKE  */
    IMMEDIATE = 449,               /* IMMEDIATE  */
    IMMUTABLE = 450,               /* IMMUTABLE  */
    IMPLICIT_P = 451,              /* IMPLICIT_P  */
    IMPORT_P = 452,                /* IMPORT_P  */
    IN_P = 453,                    /* IN_P  */
    INCLUDE_P = 454,               /* INCLUDE_P  */
    INCLUDING = 455,               /* INCLUDING  */
    INCREMENT = 456,               /* INCREMENT  */
    INDEX = 457,                   /* INDEX  */
    INDEXES = 458,                 /* INDEXES  */
    INHERIT = 459,                 /* INHERIT  */
    INHERITS = 460,                /* INHERITS  */
    INITIALLY = 461,               /* INITIALLY  */
    INLINE_P = 462,                /* INLINE_P  */
    INNER_P = 463,                 /* INNER_P  */
    INOUT = 464,                   /* INOUT  */
    INPUT_P = 465,                 /* INPUT_P  */
    INSENSITIVE = 466,             /* INSENSITIVE  */
    INSERT = 467,                  /* INSERT  */
    INSTALL = 468,                 /* INSTALL  */
    INSTEAD = 469,                 /* INSTEAD  */
    INT_P = 470,                   /* INT_P  */
    INTEGER = 471,                 /* INTEGER  */
    INTERSECT = 472,               /* INTERSECT  */
    INTERVAL = 473,                /* INTERVAL  */
    INTO = 474,                    /* INTO  */
    INVOKER = 475,                 /* INVOKER  */
    IS = 476,                      /* IS  */
    ISNULL = 477,                  /* ISNULL  */
    ISOLATION = 478,               /* ISOLATION  */
    JOIN = 479,                    /* JOIN  */
    JSON = 480,                    /* JSON  */
    KEY = 481,                     /* KEY  */
    LABEL = 482,                   /* LABEL  */
    LANGUAGE = 483,                /* LANGUAGE  */
    LARGE_P = 484,                 /* LARGE_P  */
    LAST_P = 485,                  /* LAST_P  */
    LATERAL_P = 486,               /* LATERAL_P  */
    LEADING = 487,                 /* LEADING  */
    LEAKPROOF = 488,               /* LEAKPROOF  */
    LEFT = 489,                    /* LEFT  */
    LEVEL = 490,                   /* LEVEL  */
    LIKE = 491,                    /* LIKE  */
    LIMIT = 492,                   /* LIMIT  */
    LISTEN = 493,                  /* LISTEN  */
    LOAD = 494,                    /* LOAD  */
    LOCAL = 495,                   /* LOCAL  */
    LOCATION = 496,                /* LOCATION  */
    LOCK_P = 497,                  /* LOCK_P  */
    LOCKED = 498,                  /* LOCKED  */
    LOGGED = 499,                  /* LOGGED  */
    MACRO = 500,                   /* MACRO  */
    MAP = 501,                     /* MAP  */
    MAPPING = 502,                 /* MAPPING  */
    MATCH = 503,                   /* MATCH  */
    MATERIALIZED = 504,            /* MATERIALIZED  */
    MAXVALUE = 505,                /* MAXVALUE  */
    METHOD = 506,                  /* METHOD  */
    MICROSECOND_P = 507,           /* MICROSECOND_P  */
    MICROSECONDS_P = 508,          /* MICROSECONDS_P  */
    MILLENNIA_P = 509,             /* MILLENNIA_P  */
    MILLENNIUM_P = 510,            /* MILLENNIUM_P  */
    MILLISECOND_P = 511,           /* MILLISECOND_P  */
    MILLISECONDS_P = 512,          /* MILLISECONDS_P  */
    MINUTE_P = 513,                /* MINUTE_P  */
    MINUTES_P = 514,               /* MINUTES_P  */
    MINVALUE = 515,                /* MINVALUE  */
    MODE = 516,                    /* MODE  */
    MONTH_P = 517,                 /* MONTH_P  */
    MONTHS_P = 518,                /* MONTHS_P  */
    MOVE = 519,                    /* MOVE  */
    NAME_P = 520,                  /* NAME_P  */
    NAMES = 521,                   /* NAMES  */
    NATIONAL = 522,                /* NATIONAL  */
    NATURAL = 523,                 /* NATURAL  */
    NCHAR = 524,                   /* NCHAR  */
    NEW = 525,                     /* NEW  */
    NEXT = 526,                    /* NEXT  */
    NO = 527,                      /* NO  */
    NONE = 528,                    /* NONE  */
    NOT = 529,                     /* NOT  */
    NOTHING = 530,                 /* NOTHING  */
    NOTIFY = 531,                  /* NOTIFY  */
    NOTNULL = 532,                 /* NOTNULL  */
    NOWAIT = 533,                  /* NOWAIT  */
    NULL_P = 534,                  /* NULL_P  */
    NULLIF = 535,                  /* NULLIF  */
    NULLS_P = 536,                 /* NULLS_P  */
    NUMERIC = 537,                 /* NUMERIC  */
    OBJECT_P = 538,                /* OBJECT_P  */
    OF = 539,                      /* OF  */
    OFF = 540,                     /* OFF  */
    OFFSET = 541,                  /* OFFSET  */
    OIDS = 542,                    /* OIDS  */
    OLD = 543,                     /* OLD  */
    ON = 544,                      /* ON  */
    ONLY = 545,                    /* ONLY  */
    OPERATOR = 546,                /* OPERATOR  */
    OPTION = 547,                  /* OPTION  */
    OPTIONS = 548,                 /* OPTIONS  */
    OR = 549,                      /* OR  */
    ORDER = 550,                   /* ORDER  */
    ORDINALITY = 551,              /* ORDINALITY  */
    OTHERS = 552,                  /* OTHERS  */
    OUT_P = 553,                   /* OUT_P  */
    OUTER_P = 554,                 /* OUTER_P  */
    OVER = 555,                    /* OVER  */
    OVERLAPS = 556,                /* OVERLAPS  */
    OVERLAY = 557,                 /* OVERLAY  */
    OVERRIDING = 558,              /* OVERRIDING  */
    OWNED = 559,                   /* OWNED  */
    OWNER = 560,                   /* OWNER  */
    PARALLEL = 561,                /* PARALLEL  */
    PARSER = 562,                  /* PARSER  */
    PARTIAL = 563,                 /* PARTIAL  */
    PARTITION = 564,               /* PARTITION  */
    PASSING = 565,                 /* PASSING  */
    PASSWORD = 566,                /* PASSWORD  */
    PERCENT = 567,                 /* PERCENT  */
    PERSISTENT = 568,              /* PERSISTENT  */
    PIVOT = 569,                   /* PIVOT  */
    PIVOT_LONGER = 570,            /* PIVOT_LONGER  */
    PIVOT_WIDER = 571,             /* PIVOT_WIDER  */
    PLACING = 572,                 /* PLACING  */
    PLANS = 573,                   /* PLANS  */
    POLICY = 574,                  /* POLICY  */
    POSITION = 575,                /* POSITION  */
    POSITIONAL = 576,              /* POSITIONAL  */
    PRAGMA_P = 577,                /* PRAGMA_P  */
    PRECEDING = 578,               /* PRECEDING  */
    PRECISION = 579,               /* PRECISION  */
    PREPARE = 580,                 /* PREPARE  */
    PREPARED = 581,                /* PREPARED  */
    PRESERVE = 582,                /* PRESERVE  */
    PRIMARY = 583,                 /* PRIMARY  */
    PRIOR = 584,                   /* PRIOR  */
    PRIVILEGES = 585,              /* PRIVILEGES  */
    PROCEDURAL = 586,              /* PROCEDURAL  */
    PROCEDURE = 587,               /* PROCEDURE  */
    PROGRAM = 588,                 /* PROGRAM  */
    PUBLICATION = 589,             /* PUBLICATION  */
    QUALIFY = 590,                 /* QUALIFY  */
    QUOTE = 591,                   /* QUOTE  */
    RANGE = 592,                   /* RANGE  */
    READ_P = 593,                  /* READ_P  */
    REAL = 594,                    /* REAL  */
    REASSIGN = 595,                /* REASSIGN  */
    RECHECK = 596,                 /* RECHECK  */
    RECURSIVE = 597,               /* RECURSIVE  */
    REF = 598,                     /* REF  */
    REFERENCES = 599,              /* REFERENCES  */
    REFERENCING = 600,             /* REFERENCING  */
    REFRESH = 601,                 /* REFRESH  */
    REINDEX = 602,                 /* REINDEX  */
    RELATIVE_P = 603,              /* RELATIVE_P  */
    RELEASE = 604,                 /* RELEASE  */
    RENAME = 605,                  /* RENAME  */
    REPEATABLE = 606,              /* REPEATABLE  */
    REPLACE = 607,                 /* REPLACE  */
    REPLICA = 608,                 /* REPLICA  */
    RESET = 609,                   /* RESET  */
    RESPECT_P = 610,               /* RESPECT_P  */
    RESTART = 611,                 /* RESTART  */
    RESTRICT = 612,                /* RESTRICT  */
    RETURNING = 613,               /* RETURNING  */
    RETURNS = 614,                 /* RETURNS  */
    REVOKE = 615,                  /* REVOKE  */
    RIGHT = 616,                   /* RIGHT  */
    ROLE = 617,                    /* ROLE  */
    ROLLBACK = 618,                /* ROLLBACK  */
    ROLLUP = 619,                  /* ROLLUP  */
    ROW = 620,                     /* ROW  */
    ROWS = 621,                    /* ROWS  */
    RULE = 622,                    /* RULE  */
    SAMPLE = 623,                  /* SAMPLE  */
    SAVEPOINT = 624,               /* SAVEPOINT  */
    SCHEMA = 625,                  /* SCHEMA  */
    SCHEMAS = 626,                 /* SCHEMAS  */
    SCOPE = 627,                   /* SCOPE  */
    SCROLL = 628,                  /* SCROLL  */
    SEARCH = 629,                  /* SEARCH  */
    SECOND_P = 630,                /* SECOND_P  */
    SECONDS_P = 631,               /* SECONDS_P  */
    SECRET = 632,                  /* SECRET  */
    SECURITY = 633,                /* SECURITY  */
    SELECT = 634,                  /* SELECT  */
    SEMI = 635,                    /* SEMI  */
    SEQUENCE = 636,                /* SEQUENCE  */
    SEQUENCES = 637,               /* SEQUENCES  */
    SERIALIZABLE = 638,            /* SERIALIZABLE  */
    SERVER = 639,                  /* SERVER  */
    SESSION = 640,                 /* SESSION  */
    SET = 641,                     /* SET  */
    SETOF = 642,                   /* SETOF  */
    SETS = 643,                    /* SETS  */
    SHARE = 644,                   /* SHARE  */
    SHOW = 645,                    /* SHOW  */
    SIMILAR = 646,                 /* SIMILAR  */
    SIMPLE = 647,                  /* SIMPLE  */
    SKIP = 648,                    /* SKIP  */
    SMALLINT = 649,                /* SMALLINT  */
    SNAPSHOT = 650,                /* SNAPSHOT  */
    SOME = 651,                    /* SOME  */
    SQL_P = 652,                   /* SQL_P  */
    STABLE = 653,                  /* STABLE  */
    STANDALONE_P = 654,            /* STANDALONE_P  */
    START = 655,                   /* START  */
    STATEMENT = 656,               /* STATEMENT  */
    STATISTICS = 657,              /* STATISTICS  */
    STDIN = 658,                   /* STDIN  */
    STDOUT = 659,                  /* STDOUT  */
    STORAGE = 660,                 /* STORAGE  */
    STORED = 661,                  /* STORED  */
    STRICT_P = 662,                /* STRICT_P  */
    STRIP_P = 663,                 /* STRIP_P  */
    STRUCT = 664,                  /* STRUCT  */
    SUBSCRIPTION = 665,            /* SUBSCRIPTION  */
    SUBSTRING = 666,               /* SUBSTRING  */
    SUMMARIZE = 667,               /* SUMMARIZE  */
    SYMMETRIC = 668,               /* SYMMETRIC  */
    SYSID = 669,                   /* SYSID  */
    SYSTEM_P = 670,                /* SYSTEM_P  */
    TABLE = 671,                   /* TABLE  */
    TABLES = 672,                  /* TABLES  */
    TABLESAMPLE = 673,             /* TABLESAMPLE  */
    TABLESPACE = 674,              /* TABLESPACE  */
    TEMP = 675,                    /* TEMP  */
    TEMPLATE = 676,                /* TEMPLATE  */
    TEMPORARY = 677,               /* TEMPORARY  */
    TEXT_P = 678,                  /* TEXT_P  */
    THEN = 679,                    /* THEN  */
    TIES = 680,                    /* TIES  */
    TIME = 681,                    /* TIME  */
    TIMESTAMP = 682,               /* TIMESTAMP  */
    TO = 683,                      /* TO  */
    TRAILING = 684,                /* TRAILING  */
    TRANSACTION = 685,             /* TRANSACTION  */
    TRANSFORM = 686,               /* TRANSFORM  */
    TREAT = 687,                   /* TREAT  */
    TRIGGER = 688,                 /* TRIGGER  */
    TRIM = 689,                    /* TRIM  */
    TRUE_P = 690,                  /* TRUE_P  */
    TRUNCATE = 691,                /* TRUNCATE  */
    TRUSTED = 692,                 /* TRUSTED  */
    TRY_CAST = 693,                /* TRY_CAST  */
    TYPE_P = 694,                  /* TYPE_P  */
    TYPES_P = 695,                 /* TYPES_P  */
    UNBOUNDED = 696,               /* UNBOUNDED  */
    UNCOMMITTED = 697,             /* UNCOMMITTED  */
    UNENCRYPTED = 698,             /* UNENCRYPTED  */
    UNION = 699,                   /* UNION  */
    UNIQUE = 700,                  /* UNIQUE  */
    UNKNOWN = 701,                 /* UNKNOWN  */
    UNLISTEN = 702,                /* UNLISTEN  */
    UNLOGGED = 703,                /* UNLOGGED  */
    UNPIVOT = 704,                 /* UNPIVOT  */
    UNTIL = 705,                   /* UNTIL  */
    UPDATE = 706,                  /* UPDATE  */
    USE_P = 707,                   /* USE_P  */
    USER = 708,                    /* USER  */
    USING = 709,                   /* USING  */
    VACUUM = 710,                  /* VACUUM  */
    VALID = 711,                   /* VALID  */
    VALIDATE = 712,                /* VALIDATE  */
    VALIDATOR = 713,               /* VALIDATOR  */
    VALUE_P = 714,                 /* VALUE_P  */
    VALUES = 715,                  /* VALUES  */
    VARCHAR = 716,                 /* VARCHAR  */
    VARIADIC = 717,                /* VARIADIC  */
    VARYING = 718,                 /* VARYING  */
    VERBOSE = 719,                 /* VERBOSE  */
    VERSION_P = 720,               /* VERSION_P  */
    VIEW = 721,                    /* VIEW  */
    VIEWS = 722,                   /* VIEWS  */
    VIRTUAL = 723,                 /* VIRTUAL  */
    VOLATILE = 724,                /* VOLATILE  */
    WEEK_P = 725,                  /* WEEK_P  */
    WEEKS_P = 726,                 /* WEEKS_P  */
    WHEN = 727,                    /* WHEN  */
    WHERE = 728,                   /* WHERE  */
    WHITESPACE_P = 729,            /* WHITESPACE_P  */
    WINDOW = 730,                  /* WINDOW  */
    WITH = 731,                    /* WITH  */
    WITHIN = 732,                  /* WITHIN  */
    WITHOUT = 733,                 /* WITHOUT  */
    WORK = 734,                    /* WORK  */
    WRAPPER = 735,                 /* WRAPPER  */
    WRITE_P = 736,                 /* WRITE_P  */
    XML_P = 737,                   /* XML_P  */
    XMLATTRIBUTES = 738,           /* XMLATTRIBUTES  */
    XMLCONCAT = 739,               /* XMLCONCAT  */
    XMLELEMENT = 740,              /* XMLELEMENT  */
    XMLEXISTS = 741,               /* XMLEXISTS  */
    XMLFOREST = 742,               /* XMLFOREST  */
    XMLNAMESPACES = 743,           /* XMLNAMESPACES  */
    XMLPARSE = 744,                /* XMLPARSE  */
    XMLPI = 745,                   /* XMLPI  */
    XMLROOT = 746,                 /* XMLROOT  */
    XMLSERIALIZE = 747,            /* XMLSERIALIZE  */
    XMLTABLE = 748,                /* XMLTABLE  */
    YEAR_P = 749,                  /* YEAR_P  */
    YEARS_P = 750,                 /* YEARS_P  */
    YES_P = 751,                   /* YES_P  */
    ZONE = 752,                    /* ZONE  */
    NOT_LA = 753,                  /* NOT_LA  */
    NULLS_LA = 754,                /* NULLS_LA  */
    WITH_LA = 755,                 /* WITH_LA  */
    POSTFIXOP = 756,               /* POSTFIXOP  */
    UMINUS = 757                   /* UMINUS  */
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
	PGConstrType           constr;
	PGLockClauseStrength lockstrength;
	PGLockWaitPolicy lockwaitpolicy;
	PGSubLinkType subquerytype;
	PGViewCheckOption viewcheckoption;
	PGInsertColumnOrder bynameorposition;

#line 614 "third_party/libpg_query/grammar/grammar_out.hpp"

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
