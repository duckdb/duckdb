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
    CENTURIES_P = 325,             /* CENTURIES_P  */
    CENTURY_P = 326,               /* CENTURY_P  */
    CHAIN = 327,                   /* CHAIN  */
    CHAR_P = 328,                  /* CHAR_P  */
    CHARACTER = 329,               /* CHARACTER  */
    CHARACTERISTICS = 330,         /* CHARACTERISTICS  */
    CHECK_P = 331,                 /* CHECK_P  */
    CHECKPOINT = 332,              /* CHECKPOINT  */
    CLASS = 333,                   /* CLASS  */
    CLOSE = 334,                   /* CLOSE  */
    CLUSTER = 335,                 /* CLUSTER  */
    COALESCE = 336,                /* COALESCE  */
    COLLATE = 337,                 /* COLLATE  */
    COLLATION = 338,               /* COLLATION  */
    COLUMN = 339,                  /* COLUMN  */
    COLUMNS = 340,                 /* COLUMNS  */
    COMMENT = 341,                 /* COMMENT  */
    COMMENTS = 342,                /* COMMENTS  */
    COMMIT = 343,                  /* COMMIT  */
    COMMITTED = 344,               /* COMMITTED  */
    COMPRESSION = 345,             /* COMPRESSION  */
    CONCURRENTLY = 346,            /* CONCURRENTLY  */
    CONFIGURATION = 347,           /* CONFIGURATION  */
    CONFLICT = 348,                /* CONFLICT  */
    CONNECTION = 349,              /* CONNECTION  */
    CONSTRAINT = 350,              /* CONSTRAINT  */
    CONSTRAINTS = 351,             /* CONSTRAINTS  */
    CONTENT_P = 352,               /* CONTENT_P  */
    CONTINUE_P = 353,              /* CONTINUE_P  */
    CONVERSION_P = 354,            /* CONVERSION_P  */
    COPY = 355,                    /* COPY  */
    COST = 356,                    /* COST  */
    CREATE_P = 357,                /* CREATE_P  */
    CROSS = 358,                   /* CROSS  */
    CSV = 359,                     /* CSV  */
    CUBE = 360,                    /* CUBE  */
    CURRENT_P = 361,               /* CURRENT_P  */
    CURSOR = 362,                  /* CURSOR  */
    CYCLE = 363,                   /* CYCLE  */
    DATA_P = 364,                  /* DATA_P  */
    DATABASE = 365,                /* DATABASE  */
    DAY_P = 366,                   /* DAY_P  */
    DAYS_P = 367,                  /* DAYS_P  */
    DEALLOCATE = 368,              /* DEALLOCATE  */
    DEC = 369,                     /* DEC  */
    DECADE_P = 370,                /* DECADE_P  */
    DECADES_P = 371,               /* DECADES_P  */
    DECIMAL_P = 372,               /* DECIMAL_P  */
    DECLARE = 373,                 /* DECLARE  */
    DEFAULT = 374,                 /* DEFAULT  */
    DEFAULTS = 375,                /* DEFAULTS  */
    DEFERRABLE = 376,              /* DEFERRABLE  */
    DEFERRED = 377,                /* DEFERRED  */
    DEFINER = 378,                 /* DEFINER  */
    DELETE_P = 379,                /* DELETE_P  */
    DELIMITER = 380,               /* DELIMITER  */
    DELIMITERS = 381,              /* DELIMITERS  */
    DEPENDS = 382,                 /* DEPENDS  */
    DESC_P = 383,                  /* DESC_P  */
    DESCRIBE = 384,                /* DESCRIBE  */
    DESTINATION = 385,             /* DESTINATION  */
    DETACH = 386,                  /* DETACH  */
    DICTIONARY = 387,              /* DICTIONARY  */
    DISABLE_P = 388,               /* DISABLE_P  */
    DISCARD = 389,                 /* DISCARD  */
    DISTINCT = 390,                /* DISTINCT  */
    DO = 391,                      /* DO  */
    DOCUMENT_P = 392,              /* DOCUMENT_P  */
    DOMAIN_P = 393,                /* DOMAIN_P  */
    DOUBLE_P = 394,                /* DOUBLE_P  */
    DROP = 395,                    /* DROP  */
    EACH = 396,                    /* EACH  */
    EDGE = 397,                    /* EDGE  */
    ELEMENT_ID = 398,              /* ELEMENT_ID  */
    ELSE = 399,                    /* ELSE  */
    ENABLE_P = 400,                /* ENABLE_P  */
    ENCODING = 401,                /* ENCODING  */
    ENCRYPTED = 402,               /* ENCRYPTED  */
    END_P = 403,                   /* END_P  */
    ENUM_P = 404,                  /* ENUM_P  */
    ESCAPE = 405,                  /* ESCAPE  */
    EVENT = 406,                   /* EVENT  */
    EXCEPT = 407,                  /* EXCEPT  */
    EXCLUDE = 408,                 /* EXCLUDE  */
    EXCLUDING = 409,               /* EXCLUDING  */
    EXCLUSIVE = 410,               /* EXCLUSIVE  */
    EXECUTE = 411,                 /* EXECUTE  */
    EXISTS = 412,                  /* EXISTS  */
    EXPLAIN = 413,                 /* EXPLAIN  */
    EXPORT_P = 414,                /* EXPORT_P  */
    EXPORT_STATE = 415,            /* EXPORT_STATE  */
    EXTENSION = 416,               /* EXTENSION  */
    EXTENSIONS = 417,              /* EXTENSIONS  */
    EXTERNAL = 418,                /* EXTERNAL  */
    EXTRACT = 419,                 /* EXTRACT  */
    FALSE_P = 420,                 /* FALSE_P  */
    FAMILY = 421,                  /* FAMILY  */
    FETCH = 422,                   /* FETCH  */
    FILTER = 423,                  /* FILTER  */
    FIRST_P = 424,                 /* FIRST_P  */
    FLOAT_P = 425,                 /* FLOAT_P  */
    FOLLOWING = 426,               /* FOLLOWING  */
    FOR = 427,                     /* FOR  */
    FORCE = 428,                   /* FORCE  */
    FOREIGN = 429,                 /* FOREIGN  */
    FORWARD = 430,                 /* FORWARD  */
    FREEZE = 431,                  /* FREEZE  */
    FROM = 432,                    /* FROM  */
    FULL = 433,                    /* FULL  */
    FUNCTION = 434,                /* FUNCTION  */
    FUNCTIONS = 435,               /* FUNCTIONS  */
    GENERATED = 436,               /* GENERATED  */
    GLOB = 437,                    /* GLOB  */
    GLOBAL = 438,                  /* GLOBAL  */
    GRANT = 439,                   /* GRANT  */
    GRANTED = 440,                 /* GRANTED  */
    GRAPH = 441,                   /* GRAPH  */
    GRAPH_TABLE = 442,             /* GRAPH_TABLE  */
    GROUP_P = 443,                 /* GROUP_P  */
    GROUPING = 444,                /* GROUPING  */
    GROUPING_ID = 445,             /* GROUPING_ID  */
    GROUPS = 446,                  /* GROUPS  */
    HANDLER = 447,                 /* HANDLER  */
    HAVING = 448,                  /* HAVING  */
    HEADER_P = 449,                /* HEADER_P  */
    HOLD = 450,                    /* HOLD  */
    HOUR_P = 451,                  /* HOUR_P  */
    HOURS_P = 452,                 /* HOURS_P  */
    IDENTITY_P = 453,              /* IDENTITY_P  */
    IF_P = 454,                    /* IF_P  */
    IGNORE_P = 455,                /* IGNORE_P  */
    ILIKE = 456,                   /* ILIKE  */
    IMMEDIATE = 457,               /* IMMEDIATE  */
    IMMUTABLE = 458,               /* IMMUTABLE  */
    IMPLICIT_P = 459,              /* IMPLICIT_P  */
    IMPORT_P = 460,                /* IMPORT_P  */
    IN_P = 461,                    /* IN_P  */
    INCLUDE_P = 462,               /* INCLUDE_P  */
    INCLUDING = 463,               /* INCLUDING  */
    INCREMENT = 464,               /* INCREMENT  */
    INDEX = 465,                   /* INDEX  */
    INDEXES = 466,                 /* INDEXES  */
    INHERIT = 467,                 /* INHERIT  */
    INHERITS = 468,                /* INHERITS  */
    INITIALLY = 469,               /* INITIALLY  */
    INLINE_P = 470,                /* INLINE_P  */
    INNER_P = 471,                 /* INNER_P  */
    INOUT = 472,                   /* INOUT  */
    INPUT_P = 473,                 /* INPUT_P  */
    INSENSITIVE = 474,             /* INSENSITIVE  */
    INSERT = 475,                  /* INSERT  */
    INSTALL = 476,                 /* INSTALL  */
    INSTEAD = 477,                 /* INSTEAD  */
    INT_P = 478,                   /* INT_P  */
    INTEGER = 479,                 /* INTEGER  */
    INTERSECT = 480,               /* INTERSECT  */
    INTERVAL = 481,                /* INTERVAL  */
    INTO = 482,                    /* INTO  */
    INVOKER = 483,                 /* INVOKER  */
    IS = 484,                      /* IS  */
    ISNULL = 485,                  /* ISNULL  */
    ISOLATION = 486,               /* ISOLATION  */
    JOIN = 487,                    /* JOIN  */
    JSON = 488,                    /* JSON  */
    KEEP = 489,                    /* KEEP  */
    KEY = 490,                     /* KEY  */
    LABEL = 491,                   /* LABEL  */
    LANGUAGE = 492,                /* LANGUAGE  */
    LARGE_P = 493,                 /* LARGE_P  */
    LAST_P = 494,                  /* LAST_P  */
    LATERAL_P = 495,               /* LATERAL_P  */
    LEADING = 496,                 /* LEADING  */
    LEAKPROOF = 497,               /* LEAKPROOF  */
    LEFT = 498,                    /* LEFT  */
    LEVEL = 499,                   /* LEVEL  */
    LIKE = 500,                    /* LIKE  */
    LIMIT = 501,                   /* LIMIT  */
    LISTEN = 502,                  /* LISTEN  */
    LOAD = 503,                    /* LOAD  */
    LOCAL = 504,                   /* LOCAL  */
    LOCATION = 505,                /* LOCATION  */
    LOCK_P = 506,                  /* LOCK_P  */
    LOCKED = 507,                  /* LOCKED  */
    LOGGED = 508,                  /* LOGGED  */
    MACRO = 509,                   /* MACRO  */
    MAP = 510,                     /* MAP  */
    MAPPING = 511,                 /* MAPPING  */
    MATCH = 512,                   /* MATCH  */
    MATERIALIZED = 513,            /* MATERIALIZED  */
    MAXVALUE = 514,                /* MAXVALUE  */
    METHOD = 515,                  /* METHOD  */
    MICROSECOND_P = 516,           /* MICROSECOND_P  */
    MICROSECONDS_P = 517,          /* MICROSECONDS_P  */
    MILLENNIA_P = 518,             /* MILLENNIA_P  */
    MILLENNIUM_P = 519,            /* MILLENNIUM_P  */
    MILLISECOND_P = 520,           /* MILLISECOND_P  */
    MILLISECONDS_P = 521,          /* MILLISECONDS_P  */
    MINUTE_P = 522,                /* MINUTE_P  */
    MINUTES_P = 523,               /* MINUTES_P  */
    MINVALUE = 524,                /* MINVALUE  */
    MODE = 525,                    /* MODE  */
    MONTH_P = 526,                 /* MONTH_P  */
    MONTHS_P = 527,                /* MONTHS_P  */
    MOVE = 528,                    /* MOVE  */
    NAME_P = 529,                  /* NAME_P  */
    NAMES = 530,                   /* NAMES  */
    NATIONAL = 531,                /* NATIONAL  */
    NATURAL = 532,                 /* NATURAL  */
    NCHAR = 533,                   /* NCHAR  */
    NEW = 534,                     /* NEW  */
    NEXT = 535,                    /* NEXT  */
    NO = 536,                      /* NO  */
    NODE = 537,                    /* NODE  */
    NONE = 538,                    /* NONE  */
    NOT = 539,                     /* NOT  */
    NOTHING = 540,                 /* NOTHING  */
    NOTIFY = 541,                  /* NOTIFY  */
    NOTNULL = 542,                 /* NOTNULL  */
    NOWAIT = 543,                  /* NOWAIT  */
    NULL_P = 544,                  /* NULL_P  */
    NULLIF = 545,                  /* NULLIF  */
    NULLS_P = 546,                 /* NULLS_P  */
    NUMERIC = 547,                 /* NUMERIC  */
    OBJECT_P = 548,                /* OBJECT_P  */
    OF = 549,                      /* OF  */
    OFF = 550,                     /* OFF  */
    OFFSET = 551,                  /* OFFSET  */
    OIDS = 552,                    /* OIDS  */
    OLD = 553,                     /* OLD  */
    ON = 554,                      /* ON  */
    ONLY = 555,                    /* ONLY  */
    OPERATOR = 556,                /* OPERATOR  */
    OPTION = 557,                  /* OPTION  */
    OPTIONS = 558,                 /* OPTIONS  */
    OR = 559,                      /* OR  */
    ORDER = 560,                   /* ORDER  */
    ORDINALITY = 561,              /* ORDINALITY  */
    OTHERS = 562,                  /* OTHERS  */
    OUT_P = 563,                   /* OUT_P  */
    OUTER_P = 564,                 /* OUTER_P  */
    OVER = 565,                    /* OVER  */
    OVERLAPS = 566,                /* OVERLAPS  */
    OVERLAY = 567,                 /* OVERLAY  */
    OVERRIDING = 568,              /* OVERRIDING  */
    OWNED = 569,                   /* OWNED  */
    OWNER = 570,                   /* OWNER  */
    PARALLEL = 571,                /* PARALLEL  */
    PARSER = 572,                  /* PARSER  */
    PARTIAL = 573,                 /* PARTIAL  */
    PARTITION = 574,               /* PARTITION  */
    PASSING = 575,                 /* PASSING  */
    PASSWORD = 576,                /* PASSWORD  */
    PATH = 577,                    /* PATH  */
    PATHS = 578,                   /* PATHS  */
    PERCENT = 579,                 /* PERCENT  */
    PERSISTENT = 580,              /* PERSISTENT  */
    PIVOT = 581,                   /* PIVOT  */
    PIVOT_LONGER = 582,            /* PIVOT_LONGER  */
    PIVOT_WIDER = 583,             /* PIVOT_WIDER  */
    PLACING = 584,                 /* PLACING  */
    PLANS = 585,                   /* PLANS  */
    POLICY = 586,                  /* POLICY  */
    POSITION = 587,                /* POSITION  */
    POSITIONAL = 588,              /* POSITIONAL  */
    PRAGMA_P = 589,                /* PRAGMA_P  */
    PRECEDING = 590,               /* PRECEDING  */
    PRECISION = 591,               /* PRECISION  */
    PREPARE = 592,                 /* PREPARE  */
    PREPARED = 593,                /* PREPARED  */
    PRESERVE = 594,                /* PRESERVE  */
    PRIMARY = 595,                 /* PRIMARY  */
    PRIOR = 596,                   /* PRIOR  */
    PRIVILEGES = 597,              /* PRIVILEGES  */
    PROCEDURAL = 598,              /* PROCEDURAL  */
    PROCEDURE = 599,               /* PROCEDURE  */
    PROGRAM = 600,                 /* PROGRAM  */
    PROPERTIES = 601,              /* PROPERTIES  */
    PROPERTY = 602,                /* PROPERTY  */
    PUBLICATION = 603,             /* PUBLICATION  */
    QUALIFY = 604,                 /* QUALIFY  */
    QUARTER_P = 605,               /* QUARTER_P  */
    QUARTERS_P = 606,              /* QUARTERS_P  */
    QUOTE = 607,                   /* QUOTE  */
    RANGE = 608,                   /* RANGE  */
    READ_P = 609,                  /* READ_P  */
    REAL = 610,                    /* REAL  */
    REASSIGN = 611,                /* REASSIGN  */
    RECHECK = 612,                 /* RECHECK  */
    RECURSIVE = 613,               /* RECURSIVE  */
    REF = 614,                     /* REF  */
    REFERENCES = 615,              /* REFERENCES  */
    REFERENCING = 616,             /* REFERENCING  */
    REFRESH = 617,                 /* REFRESH  */
    REINDEX = 618,                 /* REINDEX  */
    RELATIONSHIP = 619,            /* RELATIONSHIP  */
    RELATIVE_P = 620,              /* RELATIVE_P  */
    RELEASE = 621,                 /* RELEASE  */
    RENAME = 622,                  /* RENAME  */
    REPEATABLE = 623,              /* REPEATABLE  */
    REPLACE = 624,                 /* REPLACE  */
    REPLICA = 625,                 /* REPLICA  */
    RESET = 626,                   /* RESET  */
    RESPECT_P = 627,               /* RESPECT_P  */
    RESTART = 628,                 /* RESTART  */
    RESTRICT = 629,                /* RESTRICT  */
    RETURNING = 630,               /* RETURNING  */
    RETURNS = 631,                 /* RETURNS  */
    REVOKE = 632,                  /* REVOKE  */
    RIGHT = 633,                   /* RIGHT  */
    ROLE = 634,                    /* ROLE  */
    ROLLBACK = 635,                /* ROLLBACK  */
    ROLLUP = 636,                  /* ROLLUP  */
    ROW = 637,                     /* ROW  */
    ROWS = 638,                    /* ROWS  */
    RULE = 639,                    /* RULE  */
    SAMPLE = 640,                  /* SAMPLE  */
    SAVEPOINT = 641,               /* SAVEPOINT  */
    SCHEMA = 642,                  /* SCHEMA  */
    SCHEMAS = 643,                 /* SCHEMAS  */
    SCOPE = 644,                   /* SCOPE  */
    SCROLL = 645,                  /* SCROLL  */
    SEARCH = 646,                  /* SEARCH  */
    SECOND_P = 647,                /* SECOND_P  */
    SECONDS_P = 648,               /* SECONDS_P  */
    SECRET = 649,                  /* SECRET  */
    SECURITY = 650,                /* SECURITY  */
    SELECT = 651,                  /* SELECT  */
    SEMI = 652,                    /* SEMI  */
    SEQUENCE = 653,                /* SEQUENCE  */
    SEQUENCES = 654,               /* SEQUENCES  */
    SERIALIZABLE = 655,            /* SERIALIZABLE  */
    SERVER = 656,                  /* SERVER  */
    SESSION = 657,                 /* SESSION  */
    SET = 658,                     /* SET  */
    SETOF = 659,                   /* SETOF  */
    SETS = 660,                    /* SETS  */
    SHARE = 661,                   /* SHARE  */
    SHORTEST = 662,                /* SHORTEST  */
    SHOW = 663,                    /* SHOW  */
    SIMILAR = 664,                 /* SIMILAR  */
    SIMPLE = 665,                  /* SIMPLE  */
    SKIP = 666,                    /* SKIP  */
    SMALLINT = 667,                /* SMALLINT  */
    SNAPSHOT = 668,                /* SNAPSHOT  */
    SOME = 669,                    /* SOME  */
    SOURCE = 670,                  /* SOURCE  */
    SQL_P = 671,                   /* SQL_P  */
    STABLE = 672,                  /* STABLE  */
    STANDALONE_P = 673,            /* STANDALONE_P  */
    START = 674,                   /* START  */
    STATEMENT = 675,               /* STATEMENT  */
    STATISTICS = 676,              /* STATISTICS  */
    STDIN = 677,                   /* STDIN  */
    STDOUT = 678,                  /* STDOUT  */
    STORAGE = 679,                 /* STORAGE  */
    STORED = 680,                  /* STORED  */
    STRICT_P = 681,                /* STRICT_P  */
    STRIP_P = 682,                 /* STRIP_P  */
    STRUCT = 683,                  /* STRUCT  */
    SUBSCRIPTION = 684,            /* SUBSCRIPTION  */
    SUBSTRING = 685,               /* SUBSTRING  */
    SUMMARIZE = 686,               /* SUMMARIZE  */
    SYMMETRIC = 687,               /* SYMMETRIC  */
    SYSID = 688,                   /* SYSID  */
    SYSTEM_P = 689,                /* SYSTEM_P  */
    TABLE = 690,                   /* TABLE  */
    TABLES = 691,                  /* TABLES  */
    TABLESAMPLE = 692,             /* TABLESAMPLE  */
    TABLESPACE = 693,              /* TABLESPACE  */
    TEMP = 694,                    /* TEMP  */
    TEMPLATE = 695,                /* TEMPLATE  */
    TEMPORARY = 696,               /* TEMPORARY  */
    TEXT_P = 697,                  /* TEXT_P  */
    THEN = 698,                    /* THEN  */
    TIES = 699,                    /* TIES  */
    TIME = 700,                    /* TIME  */
    TIMESTAMP = 701,               /* TIMESTAMP  */
    TO = 702,                      /* TO  */
    TRAIL = 703,                   /* TRAIL  */
    TRAILING = 704,                /* TRAILING  */
    TRANSACTION = 705,             /* TRANSACTION  */
    TRANSFORM = 706,               /* TRANSFORM  */
    TREAT = 707,                   /* TREAT  */
    TRIGGER = 708,                 /* TRIGGER  */
    TRIM = 709,                    /* TRIM  */
    TRUE_P = 710,                  /* TRUE_P  */
    TRUNCATE = 711,                /* TRUNCATE  */
    TRUSTED = 712,                 /* TRUSTED  */
    TRY_CAST = 713,                /* TRY_CAST  */
    TYPE_P = 714,                  /* TYPE_P  */
    TYPES_P = 715,                 /* TYPES_P  */
    UNBOUNDED = 716,               /* UNBOUNDED  */
    UNCOMMITTED = 717,             /* UNCOMMITTED  */
    UNENCRYPTED = 718,             /* UNENCRYPTED  */
    UNION = 719,                   /* UNION  */
    UNIQUE = 720,                  /* UNIQUE  */
    UNKNOWN = 721,                 /* UNKNOWN  */
    UNLISTEN = 722,                /* UNLISTEN  */
    UNLOGGED = 723,                /* UNLOGGED  */
    UNPIVOT = 724,                 /* UNPIVOT  */
    UNTIL = 725,                   /* UNTIL  */
    UPDATE = 726,                  /* UPDATE  */
    USE_P = 727,                   /* USE_P  */
    USER = 728,                    /* USER  */
    USING = 729,                   /* USING  */
    VACUUM = 730,                  /* VACUUM  */
    VALID = 731,                   /* VALID  */
    VALIDATE = 732,                /* VALIDATE  */
    VALIDATOR = 733,               /* VALIDATOR  */
    VALUE_P = 734,                 /* VALUE_P  */
    VALUES = 735,                  /* VALUES  */
    VARCHAR = 736,                 /* VARCHAR  */
    VARIADIC = 737,                /* VARIADIC  */
    VARYING = 738,                 /* VARYING  */
    VERBOSE = 739,                 /* VERBOSE  */
    VERSION_P = 740,               /* VERSION_P  */
    VERTEX = 741,                  /* VERTEX  */
    VIEW = 742,                    /* VIEW  */
    VIEWS = 743,                   /* VIEWS  */
    VIRTUAL = 744,                 /* VIRTUAL  */
    VOLATILE = 745,                /* VOLATILE  */
    WALK = 746,                    /* WALK  */
    WEEK_P = 747,                  /* WEEK_P  */
    WEEKS_P = 748,                 /* WEEKS_P  */
    WHEN = 749,                    /* WHEN  */
    WHERE = 750,                   /* WHERE  */
    WHITESPACE_P = 751,            /* WHITESPACE_P  */
    WINDOW = 752,                  /* WINDOW  */
    WITH = 753,                    /* WITH  */
    WITHIN = 754,                  /* WITHIN  */
    WITHOUT = 755,                 /* WITHOUT  */
    WORK = 756,                    /* WORK  */
    WRAPPER = 757,                 /* WRAPPER  */
    WRITE_P = 758,                 /* WRITE_P  */
    XML_P = 759,                   /* XML_P  */
    XMLATTRIBUTES = 760,           /* XMLATTRIBUTES  */
    XMLCONCAT = 761,               /* XMLCONCAT  */
    XMLELEMENT = 762,              /* XMLELEMENT  */
    XMLEXISTS = 763,               /* XMLEXISTS  */
    XMLFOREST = 764,               /* XMLFOREST  */
    XMLNAMESPACES = 765,           /* XMLNAMESPACES  */
    XMLPARSE = 766,                /* XMLPARSE  */
    XMLPI = 767,                   /* XMLPI  */
    XMLROOT = 768,                 /* XMLROOT  */
    XMLSERIALIZE = 769,            /* XMLSERIALIZE  */
    XMLTABLE = 770,                /* XMLTABLE  */
    YEAR_P = 771,                  /* YEAR_P  */
    YEARS_P = 772,                 /* YEARS_P  */
    YES_P = 773,                   /* YES_P  */
    ZONE = 774,                    /* ZONE  */
    NOT_LA = 775,                  /* NOT_LA  */
    NULLS_LA = 776,                /* NULLS_LA  */
    WITH_LA = 777,                 /* WITH_LA  */
    POSTFIXOP = 778,               /* POSTFIXOP  */
    UMINUS = 779                   /* UMINUS  */
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

#line 638 "third_party/libpg_query/grammar/grammar_out.hpp"

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
