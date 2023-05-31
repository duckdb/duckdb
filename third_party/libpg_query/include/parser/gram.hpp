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
    ACYCLIC = 281,                 /* ACYCLIC  */
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
    ARE = 295,                     /* ARE  */
    ARRAY = 296,                   /* ARRAY  */
    AS = 297,                      /* AS  */
    ASC_P = 298,                   /* ASC_P  */
    ASOF = 299,                    /* ASOF  */
    ASSERTION = 300,               /* ASSERTION  */
    ASSIGNMENT = 301,              /* ASSIGNMENT  */
    ASYMMETRIC = 302,              /* ASYMMETRIC  */
    AT = 303,                      /* AT  */
    ATTACH = 304,                  /* ATTACH  */
    ATTRIBUTE = 305,               /* ATTRIBUTE  */
    AUTHORIZATION = 306,           /* AUTHORIZATION  */
    BACKWARD = 307,                /* BACKWARD  */
    BEFORE = 308,                  /* BEFORE  */
    BEGIN_P = 309,                 /* BEGIN_P  */
    BETWEEN = 310,                 /* BETWEEN  */
    BIGINT = 311,                  /* BIGINT  */
    BINARY = 312,                  /* BINARY  */
    BIT = 313,                     /* BIT  */
    BOOLEAN_P = 314,               /* BOOLEAN_P  */
    BOTH = 315,                    /* BOTH  */
    BY = 316,                      /* BY  */
    CACHE = 317,                   /* CACHE  */
    CALL_P = 318,                  /* CALL_P  */
    CALLED = 319,                  /* CALLED  */
    CASCADE = 320,                 /* CASCADE  */
    CASCADED = 321,                /* CASCADED  */
    CASE = 322,                    /* CASE  */
    CAST = 323,                    /* CAST  */
    CATALOG_P = 324,               /* CATALOG_P  */
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
    DECIMAL_P = 368,               /* DECIMAL_P  */
    DECLARE = 369,                 /* DECLARE  */
    DEFAULT = 370,                 /* DEFAULT  */
    DEFAULTS = 371,                /* DEFAULTS  */
    DEFERRABLE = 372,              /* DEFERRABLE  */
    DEFERRED = 373,                /* DEFERRED  */
    DEFINER = 374,                 /* DEFINER  */
    DELETE_P = 375,                /* DELETE_P  */
    DELIMITER = 376,               /* DELIMITER  */
    DELIMITERS = 377,              /* DELIMITERS  */
    DEPENDS = 378,                 /* DEPENDS  */
    DESC_P = 379,                  /* DESC_P  */
    DESCRIBE = 380,                /* DESCRIBE  */
    DESTINATION = 381,             /* DESTINATION  */
    DETACH = 382,                  /* DETACH  */
    DICTIONARY = 383,              /* DICTIONARY  */
    DISABLE_P = 384,               /* DISABLE_P  */
    DISCARD = 385,                 /* DISCARD  */
    DISTINCT = 386,                /* DISTINCT  */
    DO = 387,                      /* DO  */
    DOCUMENT_P = 388,              /* DOCUMENT_P  */
    DOMAIN_P = 389,                /* DOMAIN_P  */
    DOUBLE_P = 390,                /* DOUBLE_P  */
    DROP = 391,                    /* DROP  */
    EACH = 392,                    /* EACH  */
    EDGE = 393,                    /* EDGE  */
    ELEMENT_ID = 394,              /* ELEMENT_ID  */
    ELSE = 395,                    /* ELSE  */
    ENABLE_P = 396,                /* ENABLE_P  */
    ENCODING = 397,                /* ENCODING  */
    ENCRYPTED = 398,               /* ENCRYPTED  */
    END_P = 399,                   /* END_P  */
    ENUM_P = 400,                  /* ENUM_P  */
    ESCAPE = 401,                  /* ESCAPE  */
    EVENT = 402,                   /* EVENT  */
    EXCEPT = 403,                  /* EXCEPT  */
    EXCLUDE = 404,                 /* EXCLUDE  */
    EXCLUDING = 405,               /* EXCLUDING  */
    EXCLUSIVE = 406,               /* EXCLUSIVE  */
    EXECUTE = 407,                 /* EXECUTE  */
    EXISTS = 408,                  /* EXISTS  */
    EXPLAIN = 409,                 /* EXPLAIN  */
    EXPORT_P = 410,                /* EXPORT_P  */
    EXPORT_STATE = 411,            /* EXPORT_STATE  */
    EXTENSION = 412,               /* EXTENSION  */
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
    GRAPH = 436,                   /* GRAPH  */
    GRAPH_TABLE = 437,             /* GRAPH_TABLE  */
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
    KEEP = 484,                    /* KEEP  */
    KEY = 485,                     /* KEY  */
    LABEL = 486,                   /* LABEL  */
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
    MATERIALIZED = 508,            /* MATERIALIZED  */
    MAXVALUE = 509,                /* MAXVALUE  */
    METHOD = 510,                  /* METHOD  */
    MICROSECOND_P = 511,           /* MICROSECOND_P  */
    MICROSECONDS_P = 512,          /* MICROSECONDS_P  */
    MILLISECOND_P = 513,           /* MILLISECOND_P  */
    MILLISECONDS_P = 514,          /* MILLISECONDS_P  */
    MINUTE_P = 515,                /* MINUTE_P  */
    MINUTES_P = 516,               /* MINUTES_P  */
    MINVALUE = 517,                /* MINVALUE  */
    MODE = 518,                    /* MODE  */
    MONTH_P = 519,                 /* MONTH_P  */
    MONTHS_P = 520,                /* MONTHS_P  */
    MOVE = 521,                    /* MOVE  */
    NAME_P = 522,                  /* NAME_P  */
    NAMES = 523,                   /* NAMES  */
    NATIONAL = 524,                /* NATIONAL  */
    NATURAL = 525,                 /* NATURAL  */
    NCHAR = 526,                   /* NCHAR  */
    NEW = 527,                     /* NEW  */
    NEXT = 528,                    /* NEXT  */
    NO = 529,                      /* NO  */
    NODE = 530,                    /* NODE  */
    NONE = 531,                    /* NONE  */
    NOT = 532,                     /* NOT  */
    NOTHING = 533,                 /* NOTHING  */
    NOTIFY = 534,                  /* NOTIFY  */
    NOTNULL = 535,                 /* NOTNULL  */
    NOWAIT = 536,                  /* NOWAIT  */
    NULL_P = 537,                  /* NULL_P  */
    NULLIF = 538,                  /* NULLIF  */
    NULLS_P = 539,                 /* NULLS_P  */
    NUMERIC = 540,                 /* NUMERIC  */
    OBJECT_P = 541,                /* OBJECT_P  */
    OF = 542,                      /* OF  */
    OFF = 543,                     /* OFF  */
    OFFSET = 544,                  /* OFFSET  */
    OIDS = 545,                    /* OIDS  */
    OLD = 546,                     /* OLD  */
    ON = 547,                      /* ON  */
    ONLY = 548,                    /* ONLY  */
    OPERATOR = 549,                /* OPERATOR  */
    OPTION = 550,                  /* OPTION  */
    OPTIONS = 551,                 /* OPTIONS  */
    OR = 552,                      /* OR  */
    ORDER = 553,                   /* ORDER  */
    ORDINALITY = 554,              /* ORDINALITY  */
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
    PATH = 569,                    /* PATH  */
    PATHS = 570,                   /* PATHS  */
    PERCENT = 571,                 /* PERCENT  */
    PIVOT = 572,                   /* PIVOT  */
    PIVOT_LONGER = 573,            /* PIVOT_LONGER  */
    PIVOT_WIDER = 574,             /* PIVOT_WIDER  */
    PLACING = 575,                 /* PLACING  */
    PLANS = 576,                   /* PLANS  */
    POLICY = 577,                  /* POLICY  */
    POSITION = 578,                /* POSITION  */
    POSITIONAL = 579,              /* POSITIONAL  */
    PRAGMA_P = 580,                /* PRAGMA_P  */
    PRECEDING = 581,               /* PRECEDING  */
    PRECISION = 582,               /* PRECISION  */
    PREPARE = 583,                 /* PREPARE  */
    PREPARED = 584,                /* PREPARED  */
    PRESERVE = 585,                /* PRESERVE  */
    PRIMARY = 586,                 /* PRIMARY  */
    PRIOR = 587,                   /* PRIOR  */
    PRIVILEGES = 588,              /* PRIVILEGES  */
    PROCEDURAL = 589,              /* PROCEDURAL  */
    PROCEDURE = 590,               /* PROCEDURE  */
    PROGRAM = 591,                 /* PROGRAM  */
    PROPERTIES = 592,              /* PROPERTIES  */
    PROPERTY = 593,                /* PROPERTY  */
    PUBLICATION = 594,             /* PUBLICATION  */
    QUALIFY = 595,                 /* QUALIFY  */
    QUOTE = 596,                   /* QUOTE  */
    RANGE = 597,                   /* RANGE  */
    READ_P = 598,                  /* READ_P  */
    REAL = 599,                    /* REAL  */
    REASSIGN = 600,                /* REASSIGN  */
    RECHECK = 601,                 /* RECHECK  */
    RECURSIVE = 602,               /* RECURSIVE  */
    REF = 603,                     /* REF  */
    REFERENCES = 604,              /* REFERENCES  */
    REFERENCING = 605,             /* REFERENCING  */
    REFRESH = 606,                 /* REFRESH  */
    REINDEX = 607,                 /* REINDEX  */
    RELATIONSHIP = 608,            /* RELATIONSHIP  */
    RELATIVE_P = 609,              /* RELATIVE_P  */
    RELEASE = 610,                 /* RELEASE  */
    RENAME = 611,                  /* RENAME  */
    REPEATABLE = 612,              /* REPEATABLE  */
    REPLACE = 613,                 /* REPLACE  */
    REPLICA = 614,                 /* REPLICA  */
    RESET = 615,                   /* RESET  */
    RESPECT_P = 616,               /* RESPECT_P  */
    RESTART = 617,                 /* RESTART  */
    RESTRICT = 618,                /* RESTRICT  */
    RETURNING = 619,               /* RETURNING  */
    RETURNS = 620,                 /* RETURNS  */
    REVOKE = 621,                  /* REVOKE  */
    RIGHT = 622,                   /* RIGHT  */
    ROLE = 623,                    /* ROLE  */
    ROLLBACK = 624,                /* ROLLBACK  */
    ROLLUP = 625,                  /* ROLLUP  */
    ROW = 626,                     /* ROW  */
    ROWS = 627,                    /* ROWS  */
    RULE = 628,                    /* RULE  */
    SAMPLE = 629,                  /* SAMPLE  */
    SAVEPOINT = 630,               /* SAVEPOINT  */
    SCHEMA = 631,                  /* SCHEMA  */
    SCHEMAS = 632,                 /* SCHEMAS  */
    SCROLL = 633,                  /* SCROLL  */
    SEARCH = 634,                  /* SEARCH  */
    SECOND_P = 635,                /* SECOND_P  */
    SECONDS_P = 636,               /* SECONDS_P  */
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
    SHORTEST = 649,                /* SHORTEST  */
    SHOW = 650,                    /* SHOW  */
    SIMILAR = 651,                 /* SIMILAR  */
    SIMPLE = 652,                  /* SIMPLE  */
    SKIP = 653,                    /* SKIP  */
    SMALLINT = 654,                /* SMALLINT  */
    SNAPSHOT = 655,                /* SNAPSHOT  */
    SOME = 656,                    /* SOME  */
    SOURCE = 657,                  /* SOURCE  */
    SQL_P = 658,                   /* SQL_P  */
    STABLE = 659,                  /* STABLE  */
    STANDALONE_P = 660,            /* STANDALONE_P  */
    START = 661,                   /* START  */
    STATEMENT = 662,               /* STATEMENT  */
    STATISTICS = 663,              /* STATISTICS  */
    STDIN = 664,                   /* STDIN  */
    STDOUT = 665,                  /* STDOUT  */
    STORAGE = 666,                 /* STORAGE  */
    STORED = 667,                  /* STORED  */
    STRICT_P = 668,                /* STRICT_P  */
    STRIP_P = 669,                 /* STRIP_P  */
    STRUCT = 670,                  /* STRUCT  */
    SUBSCRIPTION = 671,            /* SUBSCRIPTION  */
    SUBSTRING = 672,               /* SUBSTRING  */
    SUMMARIZE = 673,               /* SUMMARIZE  */
    SYMMETRIC = 674,               /* SYMMETRIC  */
    SYSID = 675,                   /* SYSID  */
    SYSTEM_P = 676,                /* SYSTEM_P  */
    TABLE = 677,                   /* TABLE  */
    TABLES = 678,                  /* TABLES  */
    TABLESAMPLE = 679,             /* TABLESAMPLE  */
    TABLESPACE = 680,              /* TABLESPACE  */
    TEMP = 681,                    /* TEMP  */
    TEMPLATE = 682,                /* TEMPLATE  */
    TEMPORARY = 683,               /* TEMPORARY  */
    TEXT_P = 684,                  /* TEXT_P  */
    THEN = 685,                    /* THEN  */
    TIME = 686,                    /* TIME  */
    TIMESTAMP = 687,               /* TIMESTAMP  */
    TO = 688,                      /* TO  */
    TRAIL = 689,                   /* TRAIL  */
    TRAILING = 690,                /* TRAILING  */
    TRANSACTION = 691,             /* TRANSACTION  */
    TRANSFORM = 692,               /* TRANSFORM  */
    TREAT = 693,                   /* TREAT  */
    TRIGGER = 694,                 /* TRIGGER  */
    TRIM = 695,                    /* TRIM  */
    TRUE_P = 696,                  /* TRUE_P  */
    TRUNCATE = 697,                /* TRUNCATE  */
    TRUSTED = 698,                 /* TRUSTED  */
    TRY_CAST = 699,                /* TRY_CAST  */
    TYPE_P = 700,                  /* TYPE_P  */
    TYPES_P = 701,                 /* TYPES_P  */
    UNBOUNDED = 702,               /* UNBOUNDED  */
    UNCOMMITTED = 703,             /* UNCOMMITTED  */
    UNENCRYPTED = 704,             /* UNENCRYPTED  */
    UNION = 705,                   /* UNION  */
    UNIQUE = 706,                  /* UNIQUE  */
    UNKNOWN = 707,                 /* UNKNOWN  */
    UNLISTEN = 708,                /* UNLISTEN  */
    UNLOGGED = 709,                /* UNLOGGED  */
    UNPIVOT = 710,                 /* UNPIVOT  */
    UNTIL = 711,                   /* UNTIL  */
    UPDATE = 712,                  /* UPDATE  */
    USE_P = 713,                   /* USE_P  */
    USER = 714,                    /* USER  */
    USING = 715,                   /* USING  */
    VACUUM = 716,                  /* VACUUM  */
    VALID = 717,                   /* VALID  */
    VALIDATE = 718,                /* VALIDATE  */
    VALIDATOR = 719,               /* VALIDATOR  */
    VALUE_P = 720,                 /* VALUE_P  */
    VALUES = 721,                  /* VALUES  */
    VARCHAR = 722,                 /* VARCHAR  */
    VARIADIC = 723,                /* VARIADIC  */
    VARYING = 724,                 /* VARYING  */
    VERBOSE = 725,                 /* VERBOSE  */
    VERSION_P = 726,               /* VERSION_P  */
    VERTEX = 727,                  /* VERTEX  */
    VIEW = 728,                    /* VIEW  */
    VIEWS = 729,                   /* VIEWS  */
    VIRTUAL = 730,                 /* VIRTUAL  */
    VOLATILE = 731,                /* VOLATILE  */
    WALK = 732,                    /* WALK  */
    WHEN = 733,                    /* WHEN  */
    WHERE = 734,                   /* WHERE  */
    WHITESPACE_P = 735,            /* WHITESPACE_P  */
    WINDOW = 736,                  /* WINDOW  */
    WITH = 737,                    /* WITH  */
    WITHIN = 738,                  /* WITHIN  */
    WITHOUT = 739,                 /* WITHOUT  */
    WORK = 740,                    /* WORK  */
    WRAPPER = 741,                 /* WRAPPER  */
    WRITE_P = 742,                 /* WRITE_P  */
    XML_P = 743,                   /* XML_P  */
    XMLATTRIBUTES = 744,           /* XMLATTRIBUTES  */
    XMLCONCAT = 745,               /* XMLCONCAT  */
    XMLELEMENT = 746,              /* XMLELEMENT  */
    XMLEXISTS = 747,               /* XMLEXISTS  */
    XMLFOREST = 748,               /* XMLFOREST  */
    XMLNAMESPACES = 749,           /* XMLNAMESPACES  */
    XMLPARSE = 750,                /* XMLPARSE  */
    XMLPI = 751,                   /* XMLPI  */
    XMLROOT = 752,                 /* XMLROOT  */
    XMLSERIALIZE = 753,            /* XMLSERIALIZE  */
    XMLTABLE = 754,                /* XMLTABLE  */
    YEAR_P = 755,                  /* YEAR_P  */
    YEARS_P = 756,                 /* YEARS_P  */
    YES_P = 757,                   /* YES_P  */
    ZONE = 758,                    /* ZONE  */
    NOT_LA = 759,                  /* NOT_LA  */
    NULLS_LA = 760,                /* NULLS_LA  */
    WITH_LA = 761,                 /* WITH_LA  */
    POSTFIXOP = 762,               /* POSTFIXOP  */
    UMINUS = 763                   /* UMINUS  */
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

#line 619 "third_party/libpg_query/grammar/grammar_out.hpp"

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
