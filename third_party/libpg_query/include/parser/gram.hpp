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
    CHAIN = 323,                   /* CHAIN  */
    CHAR_P = 324,                  /* CHAR_P  */
    CHARACTER = 325,               /* CHARACTER  */
    CHARACTERISTICS = 326,         /* CHARACTERISTICS  */
    CHECK_P = 327,                 /* CHECK_P  */
    CHECKPOINT = 328,              /* CHECKPOINT  */
    CLASS = 329,                   /* CLASS  */
    CLOSE = 330,                   /* CLOSE  */
    CLUSTER = 331,                 /* CLUSTER  */
    COALESCE = 332,                /* COALESCE  */
    COLLATE = 333,                 /* COLLATE  */
    COLLATION = 334,               /* COLLATION  */
    COLUMN = 335,                  /* COLUMN  */
    COLUMNS = 336,                 /* COLUMNS  */
    COMMENT = 337,                 /* COMMENT  */
    COMMENTS = 338,                /* COMMENTS  */
    COMMIT = 339,                  /* COMMIT  */
    COMMITTED = 340,               /* COMMITTED  */
    COMPRESSION = 341,             /* COMPRESSION  */
    CONCURRENTLY = 342,            /* CONCURRENTLY  */
    CONFIGURATION = 343,           /* CONFIGURATION  */
    CONFLICT = 344,                /* CONFLICT  */
    CONNECTION = 345,              /* CONNECTION  */
    CONSTRAINT = 346,              /* CONSTRAINT  */
    CONSTRAINTS = 347,             /* CONSTRAINTS  */
    CONTENT_P = 348,               /* CONTENT_P  */
    CONTINUE_P = 349,              /* CONTINUE_P  */
    CONVERSION_P = 350,            /* CONVERSION_P  */
    COPY = 351,                    /* COPY  */
    COST = 352,                    /* COST  */
    CREATE_P = 353,                /* CREATE_P  */
    CROSS = 354,                   /* CROSS  */
    CSV = 355,                     /* CSV  */
    CUBE = 356,                    /* CUBE  */
    CURRENT_P = 357,               /* CURRENT_P  */
    CURSOR = 358,                  /* CURSOR  */
    CYCLE = 359,                   /* CYCLE  */
    DATA_P = 360,                  /* DATA_P  */
    DATABASE = 361,                /* DATABASE  */
    DAY_P = 362,                   /* DAY_P  */
    DAYS_P = 363,                  /* DAYS_P  */
    DEALLOCATE = 364,              /* DEALLOCATE  */
    DEC = 365,                     /* DEC  */
    DECIMAL_P = 366,               /* DECIMAL_P  */
    DECLARE = 367,                 /* DECLARE  */
    DEFAULT = 368,                 /* DEFAULT  */
    DEFAULTS = 369,                /* DEFAULTS  */
    DEFERRABLE = 370,              /* DEFERRABLE  */
    DEFERRED = 371,                /* DEFERRED  */
    DEFINER = 372,                 /* DEFINER  */
    DELETE_P = 373,                /* DELETE_P  */
    DELIMITER = 374,               /* DELIMITER  */
    DELIMITERS = 375,              /* DELIMITERS  */
    DEPENDS = 376,                 /* DEPENDS  */
    DESC_P = 377,                  /* DESC_P  */
    DESCRIBE = 378,                /* DESCRIBE  */
    DETACH = 379,                  /* DETACH  */
    DICTIONARY = 380,              /* DICTIONARY  */
    DISABLE_P = 381,               /* DISABLE_P  */
    DISCARD = 382,                 /* DISCARD  */
    DISTINCT = 383,                /* DISTINCT  */
    DO = 384,                      /* DO  */
    DOCUMENT_P = 385,              /* DOCUMENT_P  */
    DOMAIN_P = 386,                /* DOMAIN_P  */
    DOUBLE_P = 387,                /* DOUBLE_P  */
    DROP = 388,                    /* DROP  */
    EACH = 389,                    /* EACH  */
    ELSE = 390,                    /* ELSE  */
    ENABLE_P = 391,                /* ENABLE_P  */
    ENCODING = 392,                /* ENCODING  */
    ENCRYPTED = 393,               /* ENCRYPTED  */
    END_P = 394,                   /* END_P  */
    ENUM_P = 395,                  /* ENUM_P  */
    ESCAPE = 396,                  /* ESCAPE  */
    EVENT = 397,                   /* EVENT  */
    EXCEPT = 398,                  /* EXCEPT  */
    EXCLUDE = 399,                 /* EXCLUDE  */
    EXCLUDING = 400,               /* EXCLUDING  */
    EXCLUSIVE = 401,               /* EXCLUSIVE  */
    EXECUTE = 402,                 /* EXECUTE  */
    EXISTS = 403,                  /* EXISTS  */
    EXPLAIN = 404,                 /* EXPLAIN  */
    EXPORT_P = 405,                /* EXPORT_P  */
    EXPORT_STATE = 406,            /* EXPORT_STATE  */
    EXTENSION = 407,               /* EXTENSION  */
    EXTERNAL = 408,                /* EXTERNAL  */
    EXTRACT = 409,                 /* EXTRACT  */
    FALSE_P = 410,                 /* FALSE_P  */
    FAMILY = 411,                  /* FAMILY  */
    FETCH = 412,                   /* FETCH  */
    FILTER = 413,                  /* FILTER  */
    FIRST_P = 414,                 /* FIRST_P  */
    FLOAT_P = 415,                 /* FLOAT_P  */
    FOLLOWING = 416,               /* FOLLOWING  */
    FOR = 417,                     /* FOR  */
    FORCE = 418,                   /* FORCE  */
    FOREIGN = 419,                 /* FOREIGN  */
    FORWARD = 420,                 /* FORWARD  */
    FREEZE = 421,                  /* FREEZE  */
    FROM = 422,                    /* FROM  */
    FULL = 423,                    /* FULL  */
    FUNCTION = 424,                /* FUNCTION  */
    FUNCTIONS = 425,               /* FUNCTIONS  */
    GENERATED = 426,               /* GENERATED  */
    GLOB = 427,                    /* GLOB  */
    GLOBAL = 428,                  /* GLOBAL  */
    GRANT = 429,                   /* GRANT  */
    GRANTED = 430,                 /* GRANTED  */
    GROUP_P = 431,                 /* GROUP_P  */
    GROUPING = 432,                /* GROUPING  */
    GROUPING_ID = 433,             /* GROUPING_ID  */
    GROUPS = 434,                  /* GROUPS  */
    HANDLER = 435,                 /* HANDLER  */
    HAVING = 436,                  /* HAVING  */
    HEADER_P = 437,                /* HEADER_P  */
    HOLD = 438,                    /* HOLD  */
    HOUR_P = 439,                  /* HOUR_P  */
    HOURS_P = 440,                 /* HOURS_P  */
    IDENTITY_P = 441,              /* IDENTITY_P  */
    IF_P = 442,                    /* IF_P  */
    IGNORE_P = 443,                /* IGNORE_P  */
    ILIKE = 444,                   /* ILIKE  */
    IMMEDIATE = 445,               /* IMMEDIATE  */
    IMMUTABLE = 446,               /* IMMUTABLE  */
    IMPLICIT_P = 447,              /* IMPLICIT_P  */
    IMPORT_P = 448,                /* IMPORT_P  */
    IN_P = 449,                    /* IN_P  */
    INCLUDE_P = 450,               /* INCLUDE_P  */
    INCLUDING = 451,               /* INCLUDING  */
    INCREMENT = 452,               /* INCREMENT  */
    INDEX = 453,                   /* INDEX  */
    INDEXES = 454,                 /* INDEXES  */
    INHERIT = 455,                 /* INHERIT  */
    INHERITS = 456,                /* INHERITS  */
    INITIALLY = 457,               /* INITIALLY  */
    INLINE_P = 458,                /* INLINE_P  */
    INNER_P = 459,                 /* INNER_P  */
    INOUT = 460,                   /* INOUT  */
    INPUT_P = 461,                 /* INPUT_P  */
    INSENSITIVE = 462,             /* INSENSITIVE  */
    INSERT = 463,                  /* INSERT  */
    INSTALL = 464,                 /* INSTALL  */
    INSTEAD = 465,                 /* INSTEAD  */
    INT_P = 466,                   /* INT_P  */
    INTEGER = 467,                 /* INTEGER  */
    INTERSECT = 468,               /* INTERSECT  */
    INTERVAL = 469,                /* INTERVAL  */
    INTO = 470,                    /* INTO  */
    INVOKER = 471,                 /* INVOKER  */
    IS = 472,                      /* IS  */
    ISNULL = 473,                  /* ISNULL  */
    ISOLATION = 474,               /* ISOLATION  */
    JOIN = 475,                    /* JOIN  */
    JSON = 476,                    /* JSON  */
    KEY = 477,                     /* KEY  */
    LABEL = 478,                   /* LABEL  */
    LANGUAGE = 479,                /* LANGUAGE  */
    LARGE_P = 480,                 /* LARGE_P  */
    LAST_P = 481,                  /* LAST_P  */
    LATERAL_P = 482,               /* LATERAL_P  */
    LEADING = 483,                 /* LEADING  */
    LEAKPROOF = 484,               /* LEAKPROOF  */
    LEFT = 485,                    /* LEFT  */
    LEVEL = 486,                   /* LEVEL  */
    LIKE = 487,                    /* LIKE  */
    LIMIT = 488,                   /* LIMIT  */
    LISTEN = 489,                  /* LISTEN  */
    LOAD = 490,                    /* LOAD  */
    LOCAL = 491,                   /* LOCAL  */
    LOCATION = 492,                /* LOCATION  */
    LOCK_P = 493,                  /* LOCK_P  */
    LOCKED = 494,                  /* LOCKED  */
    LOGGED = 495,                  /* LOGGED  */
    MACRO = 496,                   /* MACRO  */
    MAP = 497,                     /* MAP  */
    MAPPING = 498,                 /* MAPPING  */
    MATCH = 499,                   /* MATCH  */
    MATERIALIZED = 500,            /* MATERIALIZED  */
    MAXVALUE = 501,                /* MAXVALUE  */
    METHOD = 502,                  /* METHOD  */
    MICROSECOND_P = 503,           /* MICROSECOND_P  */
    MICROSECONDS_P = 504,          /* MICROSECONDS_P  */
    MILLISECOND_P = 505,           /* MILLISECOND_P  */
    MILLISECONDS_P = 506,          /* MILLISECONDS_P  */
    MINUTE_P = 507,                /* MINUTE_P  */
    MINUTES_P = 508,               /* MINUTES_P  */
    MINVALUE = 509,                /* MINVALUE  */
    MODE = 510,                    /* MODE  */
    MONTH_P = 511,                 /* MONTH_P  */
    MONTHS_P = 512,                /* MONTHS_P  */
    MOVE = 513,                    /* MOVE  */
    NAME_P = 514,                  /* NAME_P  */
    NAMES = 515,                   /* NAMES  */
    NATIONAL = 516,                /* NATIONAL  */
    NATURAL = 517,                 /* NATURAL  */
    NCHAR = 518,                   /* NCHAR  */
    NEW = 519,                     /* NEW  */
    NEXT = 520,                    /* NEXT  */
    NO = 521,                      /* NO  */
    NONE = 522,                    /* NONE  */
    NOT = 523,                     /* NOT  */
    NOTHING = 524,                 /* NOTHING  */
    NOTIFY = 525,                  /* NOTIFY  */
    NOTNULL = 526,                 /* NOTNULL  */
    NOWAIT = 527,                  /* NOWAIT  */
    NULL_P = 528,                  /* NULL_P  */
    NULLIF = 529,                  /* NULLIF  */
    NULLS_P = 530,                 /* NULLS_P  */
    NUMERIC = 531,                 /* NUMERIC  */
    OBJECT_P = 532,                /* OBJECT_P  */
    OF = 533,                      /* OF  */
    OFF = 534,                     /* OFF  */
    OFFSET = 535,                  /* OFFSET  */
    OIDS = 536,                    /* OIDS  */
    OLD = 537,                     /* OLD  */
    ON = 538,                      /* ON  */
    ONLY = 539,                    /* ONLY  */
    OPERATOR = 540,                /* OPERATOR  */
    OPTION = 541,                  /* OPTION  */
    OPTIONS = 542,                 /* OPTIONS  */
    OR = 543,                      /* OR  */
    ORDER = 544,                   /* ORDER  */
    ORDINALITY = 545,              /* ORDINALITY  */
    OTHERS = 546,                  /* OTHERS  */
    OUT_P = 547,                   /* OUT_P  */
    OUTER_P = 548,                 /* OUTER_P  */
    OVER = 549,                    /* OVER  */
    OVERLAPS = 550,                /* OVERLAPS  */
    OVERLAY = 551,                 /* OVERLAY  */
    OVERRIDING = 552,              /* OVERRIDING  */
    OWNED = 553,                   /* OWNED  */
    OWNER = 554,                   /* OWNER  */
    PARALLEL = 555,                /* PARALLEL  */
    PARSER = 556,                  /* PARSER  */
    PARTIAL = 557,                 /* PARTIAL  */
    PARTITION = 558,               /* PARTITION  */
    PASSING = 559,                 /* PASSING  */
    PASSWORD = 560,                /* PASSWORD  */
    PERCENT = 561,                 /* PERCENT  */
    PIVOT = 562,                   /* PIVOT  */
    PIVOT_LONGER = 563,            /* PIVOT_LONGER  */
    PIVOT_WIDER = 564,             /* PIVOT_WIDER  */
    PLACING = 565,                 /* PLACING  */
    PLANS = 566,                   /* PLANS  */
    POLICY = 567,                  /* POLICY  */
    POSITION = 568,                /* POSITION  */
    POSITIONAL = 569,              /* POSITIONAL  */
    PRAGMA_P = 570,                /* PRAGMA_P  */
    PRECEDING = 571,               /* PRECEDING  */
    PRECISION = 572,               /* PRECISION  */
    PREPARE = 573,                 /* PREPARE  */
    PREPARED = 574,                /* PREPARED  */
    PRESERVE = 575,                /* PRESERVE  */
    PRIMARY = 576,                 /* PRIMARY  */
    PRIOR = 577,                   /* PRIOR  */
    PRIVILEGES = 578,              /* PRIVILEGES  */
    PROCEDURAL = 579,              /* PROCEDURAL  */
    PROCEDURE = 580,               /* PROCEDURE  */
    PROGRAM = 581,                 /* PROGRAM  */
    PUBLICATION = 582,             /* PUBLICATION  */
    QUALIFY = 583,                 /* QUALIFY  */
    QUOTE = 584,                   /* QUOTE  */
    RANGE = 585,                   /* RANGE  */
    READ_P = 586,                  /* READ_P  */
    REAL = 587,                    /* REAL  */
    REASSIGN = 588,                /* REASSIGN  */
    RECHECK = 589,                 /* RECHECK  */
    RECURSIVE = 590,               /* RECURSIVE  */
    REF = 591,                     /* REF  */
    REFERENCES = 592,              /* REFERENCES  */
    REFERENCING = 593,             /* REFERENCING  */
    REFRESH = 594,                 /* REFRESH  */
    REINDEX = 595,                 /* REINDEX  */
    RELATIVE_P = 596,              /* RELATIVE_P  */
    RELEASE = 597,                 /* RELEASE  */
    RENAME = 598,                  /* RENAME  */
    REPEATABLE = 599,              /* REPEATABLE  */
    REPLACE = 600,                 /* REPLACE  */
    REPLICA = 601,                 /* REPLICA  */
    RESET = 602,                   /* RESET  */
    RESPECT_P = 603,               /* RESPECT_P  */
    RESTART = 604,                 /* RESTART  */
    RESTRICT = 605,                /* RESTRICT  */
    RETURNING = 606,               /* RETURNING  */
    RETURNS = 607,                 /* RETURNS  */
    REVOKE = 608,                  /* REVOKE  */
    RIGHT = 609,                   /* RIGHT  */
    ROLE = 610,                    /* ROLE  */
    ROLLBACK = 611,                /* ROLLBACK  */
    ROLLUP = 612,                  /* ROLLUP  */
    ROW = 613,                     /* ROW  */
    ROWS = 614,                    /* ROWS  */
    RULE = 615,                    /* RULE  */
    SAMPLE = 616,                  /* SAMPLE  */
    SAVEPOINT = 617,               /* SAVEPOINT  */
    SCHEMA = 618,                  /* SCHEMA  */
    SCHEMAS = 619,                 /* SCHEMAS  */
    SCROLL = 620,                  /* SCROLL  */
    SEARCH = 621,                  /* SEARCH  */
    SECOND_P = 622,                /* SECOND_P  */
    SECONDS_P = 623,               /* SECONDS_P  */
    SECURITY = 624,                /* SECURITY  */
    SELECT = 625,                  /* SELECT  */
    SEMI = 626,                    /* SEMI  */
    SEQUENCE = 627,                /* SEQUENCE  */
    SEQUENCES = 628,               /* SEQUENCES  */
    SERIALIZABLE = 629,            /* SERIALIZABLE  */
    SERVER = 630,                  /* SERVER  */
    SESSION = 631,                 /* SESSION  */
    SET = 632,                     /* SET  */
    SETOF = 633,                   /* SETOF  */
    SETS = 634,                    /* SETS  */
    SHARE = 635,                   /* SHARE  */
    SHOW = 636,                    /* SHOW  */
    SIMILAR = 637,                 /* SIMILAR  */
    SIMPLE = 638,                  /* SIMPLE  */
    SKIP = 639,                    /* SKIP  */
    SMALLINT = 640,                /* SMALLINT  */
    SNAPSHOT = 641,                /* SNAPSHOT  */
    SOME = 642,                    /* SOME  */
    SQL_P = 643,                   /* SQL_P  */
    STABLE = 644,                  /* STABLE  */
    STANDALONE_P = 645,            /* STANDALONE_P  */
    START = 646,                   /* START  */
    STATEMENT = 647,               /* STATEMENT  */
    STATISTICS = 648,              /* STATISTICS  */
    STDIN = 649,                   /* STDIN  */
    STDOUT = 650,                  /* STDOUT  */
    STORAGE = 651,                 /* STORAGE  */
    STORED = 652,                  /* STORED  */
    STRICT_P = 653,                /* STRICT_P  */
    STRIP_P = 654,                 /* STRIP_P  */
    STRUCT = 655,                  /* STRUCT  */
    SUBSCRIPTION = 656,            /* SUBSCRIPTION  */
    SUBSTRING = 657,               /* SUBSTRING  */
    SUMMARIZE = 658,               /* SUMMARIZE  */
    SYMMETRIC = 659,               /* SYMMETRIC  */
    SYSID = 660,                   /* SYSID  */
    SYSTEM_P = 661,                /* SYSTEM_P  */
    TABLE = 662,                   /* TABLE  */
    TABLES = 663,                  /* TABLES  */
    TABLESAMPLE = 664,             /* TABLESAMPLE  */
    TABLESPACE = 665,              /* TABLESPACE  */
    TEMP = 666,                    /* TEMP  */
    TEMPLATE = 667,                /* TEMPLATE  */
    TEMPORARY = 668,               /* TEMPORARY  */
    TEXT_P = 669,                  /* TEXT_P  */
    THEN = 670,                    /* THEN  */
    TIES = 671,                    /* TIES  */
    TIME = 672,                    /* TIME  */
    TIMESTAMP = 673,               /* TIMESTAMP  */
    TO = 674,                      /* TO  */
    TRAILING = 675,                /* TRAILING  */
    TRANSACTION = 676,             /* TRANSACTION  */
    TRANSFORM = 677,               /* TRANSFORM  */
    TREAT = 678,                   /* TREAT  */
    TRIGGER = 679,                 /* TRIGGER  */
    TRIM = 680,                    /* TRIM  */
    TRUE_P = 681,                  /* TRUE_P  */
    TRUNCATE = 682,                /* TRUNCATE  */
    TRUSTED = 683,                 /* TRUSTED  */
    TRY_CAST = 684,                /* TRY_CAST  */
    TYPE_P = 685,                  /* TYPE_P  */
    TYPES_P = 686,                 /* TYPES_P  */
    UNBOUNDED = 687,               /* UNBOUNDED  */
    UNCOMMITTED = 688,             /* UNCOMMITTED  */
    UNENCRYPTED = 689,             /* UNENCRYPTED  */
    UNION = 690,                   /* UNION  */
    UNIQUE = 691,                  /* UNIQUE  */
    UNKNOWN = 692,                 /* UNKNOWN  */
    UNLISTEN = 693,                /* UNLISTEN  */
    UNLOGGED = 694,                /* UNLOGGED  */
    UNPIVOT = 695,                 /* UNPIVOT  */
    UNTIL = 696,                   /* UNTIL  */
    UPDATE = 697,                  /* UPDATE  */
    USE_P = 698,                   /* USE_P  */
    USER = 699,                    /* USER  */
    USING = 700,                   /* USING  */
    VACUUM = 701,                  /* VACUUM  */
    VALID = 702,                   /* VALID  */
    VALIDATE = 703,                /* VALIDATE  */
    VALIDATOR = 704,               /* VALIDATOR  */
    VALUE_P = 705,                 /* VALUE_P  */
    VALUES = 706,                  /* VALUES  */
    VARCHAR = 707,                 /* VARCHAR  */
    VARIADIC = 708,                /* VARIADIC  */
    VARYING = 709,                 /* VARYING  */
    VERBOSE = 710,                 /* VERBOSE  */
    VERSION_P = 711,               /* VERSION_P  */
    VIEW = 712,                    /* VIEW  */
    VIEWS = 713,                   /* VIEWS  */
    VIRTUAL = 714,                 /* VIRTUAL  */
    VOLATILE = 715,                /* VOLATILE  */
    WHEN = 716,                    /* WHEN  */
    WHERE = 717,                   /* WHERE  */
    WHITESPACE_P = 718,            /* WHITESPACE_P  */
    WINDOW = 719,                  /* WINDOW  */
    WITH = 720,                    /* WITH  */
    WITHIN = 721,                  /* WITHIN  */
    WITHOUT = 722,                 /* WITHOUT  */
    WORK = 723,                    /* WORK  */
    WRAPPER = 724,                 /* WRAPPER  */
    WRITE_P = 725,                 /* WRITE_P  */
    XML_P = 726,                   /* XML_P  */
    XMLATTRIBUTES = 727,           /* XMLATTRIBUTES  */
    XMLCONCAT = 728,               /* XMLCONCAT  */
    XMLELEMENT = 729,              /* XMLELEMENT  */
    XMLEXISTS = 730,               /* XMLEXISTS  */
    XMLFOREST = 731,               /* XMLFOREST  */
    XMLNAMESPACES = 732,           /* XMLNAMESPACES  */
    XMLPARSE = 733,                /* XMLPARSE  */
    XMLPI = 734,                   /* XMLPI  */
    XMLROOT = 735,                 /* XMLROOT  */
    XMLSERIALIZE = 736,            /* XMLSERIALIZE  */
    XMLTABLE = 737,                /* XMLTABLE  */
    YEAR_P = 738,                  /* YEAR_P  */
    YEARS_P = 739,                 /* YEARS_P  */
    YES_P = 740,                   /* YES_P  */
    ZONE = 741,                    /* ZONE  */
    NOT_LA = 742,                  /* NOT_LA  */
    NULLS_LA = 743,                /* NULLS_LA  */
    WITH_LA = 744,                 /* WITH_LA  */
    POSTFIXOP = 745,               /* POSTFIXOP  */
    UMINUS = 746                   /* UMINUS  */
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

#line 603 "third_party/libpg_query/grammar/grammar_out.hpp"

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
