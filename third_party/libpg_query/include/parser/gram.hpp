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
    EXTERNAL = 417,                /* EXTERNAL  */
    EXTRACT = 418,                 /* EXTRACT  */
    FALSE_P = 419,                 /* FALSE_P  */
    FAMILY = 420,                  /* FAMILY  */
    FETCH = 421,                   /* FETCH  */
    FILTER = 422,                  /* FILTER  */
    FIRST_P = 423,                 /* FIRST_P  */
    FLOAT_P = 424,                 /* FLOAT_P  */
    FOLLOWING = 425,               /* FOLLOWING  */
    FOR = 426,                     /* FOR  */
    FORCE = 427,                   /* FORCE  */
    FOREIGN = 428,                 /* FOREIGN  */
    FORWARD = 429,                 /* FORWARD  */
    FREEZE = 430,                  /* FREEZE  */
    FROM = 431,                    /* FROM  */
    FULL = 432,                    /* FULL  */
    FUNCTION = 433,                /* FUNCTION  */
    FUNCTIONS = 434,               /* FUNCTIONS  */
    GENERATED = 435,               /* GENERATED  */
    GLOB = 436,                    /* GLOB  */
    GLOBAL = 437,                  /* GLOBAL  */
    GRANT = 438,                   /* GRANT  */
    GRANTED = 439,                 /* GRANTED  */
    GRAPH = 440,                   /* GRAPH  */
    GRAPH_TABLE = 441,             /* GRAPH_TABLE  */
    GROUP_P = 442,                 /* GROUP_P  */
    GROUPING = 443,                /* GROUPING  */
    GROUPING_ID = 444,             /* GROUPING_ID  */
    GROUPS = 445,                  /* GROUPS  */
    HANDLER = 446,                 /* HANDLER  */
    HAVING = 447,                  /* HAVING  */
    HEADER_P = 448,                /* HEADER_P  */
    HOLD = 449,                    /* HOLD  */
    HOUR_P = 450,                  /* HOUR_P  */
    HOURS_P = 451,                 /* HOURS_P  */
    IDENTITY_P = 452,              /* IDENTITY_P  */
    IF_P = 453,                    /* IF_P  */
    IGNORE_P = 454,                /* IGNORE_P  */
    ILIKE = 455,                   /* ILIKE  */
    IMMEDIATE = 456,               /* IMMEDIATE  */
    IMMUTABLE = 457,               /* IMMUTABLE  */
    IMPLICIT_P = 458,              /* IMPLICIT_P  */
    IMPORT_P = 459,                /* IMPORT_P  */
    IN_P = 460,                    /* IN_P  */
    INCLUDE_P = 461,               /* INCLUDE_P  */
    INCLUDING = 462,               /* INCLUDING  */
    INCREMENT = 463,               /* INCREMENT  */
    INDEX = 464,                   /* INDEX  */
    INDEXES = 465,                 /* INDEXES  */
    INHERIT = 466,                 /* INHERIT  */
    INHERITS = 467,                /* INHERITS  */
    INITIALLY = 468,               /* INITIALLY  */
    INLINE_P = 469,                /* INLINE_P  */
    INNER_P = 470,                 /* INNER_P  */
    INOUT = 471,                   /* INOUT  */
    INPUT_P = 472,                 /* INPUT_P  */
    INSENSITIVE = 473,             /* INSENSITIVE  */
    INSERT = 474,                  /* INSERT  */
    INSTALL = 475,                 /* INSTALL  */
    INSTEAD = 476,                 /* INSTEAD  */
    INT_P = 477,                   /* INT_P  */
    INTEGER = 478,                 /* INTEGER  */
    INTERSECT = 479,               /* INTERSECT  */
    INTERVAL = 480,                /* INTERVAL  */
    INTO = 481,                    /* INTO  */
    INVOKER = 482,                 /* INVOKER  */
    IS = 483,                      /* IS  */
    ISNULL = 484,                  /* ISNULL  */
    ISOLATION = 485,               /* ISOLATION  */
    JOIN = 486,                    /* JOIN  */
    JSON = 487,                    /* JSON  */
    KEEP = 488,                    /* KEEP  */
    KEY = 489,                     /* KEY  */
    LABEL = 490,                   /* LABEL  */
    LANGUAGE = 491,                /* LANGUAGE  */
    LARGE_P = 492,                 /* LARGE_P  */
    LAST_P = 493,                  /* LAST_P  */
    LATERAL_P = 494,               /* LATERAL_P  */
    LEADING = 495,                 /* LEADING  */
    LEAKPROOF = 496,               /* LEAKPROOF  */
    LEFT = 497,                    /* LEFT  */
    LEVEL = 498,                   /* LEVEL  */
    LIKE = 499,                    /* LIKE  */
    LIMIT = 500,                   /* LIMIT  */
    LISTEN = 501,                  /* LISTEN  */
    LOAD = 502,                    /* LOAD  */
    LOCAL = 503,                   /* LOCAL  */
    LOCATION = 504,                /* LOCATION  */
    LOCK_P = 505,                  /* LOCK_P  */
    LOCKED = 506,                  /* LOCKED  */
    LOGGED = 507,                  /* LOGGED  */
    MACRO = 508,                   /* MACRO  */
    MAP = 509,                     /* MAP  */
    MAPPING = 510,                 /* MAPPING  */
    MATCH = 511,                   /* MATCH  */
    MATERIALIZED = 512,            /* MATERIALIZED  */
    MAXVALUE = 513,                /* MAXVALUE  */
    METHOD = 514,                  /* METHOD  */
    MICROSECOND_P = 515,           /* MICROSECOND_P  */
    MICROSECONDS_P = 516,          /* MICROSECONDS_P  */
    MILLENNIA_P = 517,             /* MILLENNIA_P  */
    MILLENNIUM_P = 518,            /* MILLENNIUM_P  */
    MILLISECOND_P = 519,           /* MILLISECOND_P  */
    MILLISECONDS_P = 520,          /* MILLISECONDS_P  */
    MINUTE_P = 521,                /* MINUTE_P  */
    MINUTES_P = 522,               /* MINUTES_P  */
    MINVALUE = 523,                /* MINVALUE  */
    MODE = 524,                    /* MODE  */
    MONTH_P = 525,                 /* MONTH_P  */
    MONTHS_P = 526,                /* MONTHS_P  */
    MOVE = 527,                    /* MOVE  */
    NAME_P = 528,                  /* NAME_P  */
    NAMES = 529,                   /* NAMES  */
    NATIONAL = 530,                /* NATIONAL  */
    NATURAL = 531,                 /* NATURAL  */
    NCHAR = 532,                   /* NCHAR  */
    NEW = 533,                     /* NEW  */
    NEXT = 534,                    /* NEXT  */
    NO = 535,                      /* NO  */
    NODE = 536,                    /* NODE  */
    NONE = 537,                    /* NONE  */
    NOT = 538,                     /* NOT  */
    NOTHING = 539,                 /* NOTHING  */
    NOTIFY = 540,                  /* NOTIFY  */
    NOTNULL = 541,                 /* NOTNULL  */
    NOWAIT = 542,                  /* NOWAIT  */
    NULL_P = 543,                  /* NULL_P  */
    NULLIF = 544,                  /* NULLIF  */
    NULLS_P = 545,                 /* NULLS_P  */
    NUMERIC = 546,                 /* NUMERIC  */
    OBJECT_P = 547,                /* OBJECT_P  */
    OF = 548,                      /* OF  */
    OFF = 549,                     /* OFF  */
    OFFSET = 550,                  /* OFFSET  */
    OIDS = 551,                    /* OIDS  */
    OLD = 552,                     /* OLD  */
    ON = 553,                      /* ON  */
    ONLY = 554,                    /* ONLY  */
    OPERATOR = 555,                /* OPERATOR  */
    OPTION = 556,                  /* OPTION  */
    OPTIONS = 557,                 /* OPTIONS  */
    OR = 558,                      /* OR  */
    ORDER = 559,                   /* ORDER  */
    ORDINALITY = 560,              /* ORDINALITY  */
    OTHERS = 561,                  /* OTHERS  */
    OUT_P = 562,                   /* OUT_P  */
    OUTER_P = 563,                 /* OUTER_P  */
    OVER = 564,                    /* OVER  */
    OVERLAPS = 565,                /* OVERLAPS  */
    OVERLAY = 566,                 /* OVERLAY  */
    OVERRIDING = 567,              /* OVERRIDING  */
    OWNED = 568,                   /* OWNED  */
    OWNER = 569,                   /* OWNER  */
    PARALLEL = 570,                /* PARALLEL  */
    PARSER = 571,                  /* PARSER  */
    PARTIAL = 572,                 /* PARTIAL  */
    PARTITION = 573,               /* PARTITION  */
    PASSING = 574,                 /* PASSING  */
    PASSWORD = 575,                /* PASSWORD  */
    PATH = 576,                    /* PATH  */
    PATHS = 577,                   /* PATHS  */
    PERCENT = 578,                 /* PERCENT  */
    PERSISTENT = 579,              /* PERSISTENT  */
    PIVOT = 580,                   /* PIVOT  */
    PIVOT_LONGER = 581,            /* PIVOT_LONGER  */
    PIVOT_WIDER = 582,             /* PIVOT_WIDER  */
    PLACING = 583,                 /* PLACING  */
    PLANS = 584,                   /* PLANS  */
    POLICY = 585,                  /* POLICY  */
    POSITION = 586,                /* POSITION  */
    POSITIONAL = 587,              /* POSITIONAL  */
    PRAGMA_P = 588,                /* PRAGMA_P  */
    PRECEDING = 589,               /* PRECEDING  */
    PRECISION = 590,               /* PRECISION  */
    PREPARE = 591,                 /* PREPARE  */
    PREPARED = 592,                /* PREPARED  */
    PRESERVE = 593,                /* PRESERVE  */
    PRIMARY = 594,                 /* PRIMARY  */
    PRIOR = 595,                   /* PRIOR  */
    PRIVILEGES = 596,              /* PRIVILEGES  */
    PROCEDURAL = 597,              /* PROCEDURAL  */
    PROCEDURE = 598,               /* PROCEDURE  */
    PROGRAM = 599,                 /* PROGRAM  */
    PROPERTIES = 600,              /* PROPERTIES  */
    PROPERTY = 601,                /* PROPERTY  */
    PUBLICATION = 602,             /* PUBLICATION  */
    QUALIFY = 603,                 /* QUALIFY  */
    QUOTE = 604,                   /* QUOTE  */
    RANGE = 605,                   /* RANGE  */
    READ_P = 606,                  /* READ_P  */
    REAL = 607,                    /* REAL  */
    REASSIGN = 608,                /* REASSIGN  */
    RECHECK = 609,                 /* RECHECK  */
    RECURSIVE = 610,               /* RECURSIVE  */
    REF = 611,                     /* REF  */
    REFERENCES = 612,              /* REFERENCES  */
    REFERENCING = 613,             /* REFERENCING  */
    REFRESH = 614,                 /* REFRESH  */
    REINDEX = 615,                 /* REINDEX  */
    RELATIONSHIP = 616,            /* RELATIONSHIP  */
    RELATIVE_P = 617,              /* RELATIVE_P  */
    RELEASE = 618,                 /* RELEASE  */
    RENAME = 619,                  /* RENAME  */
    REPEATABLE = 620,              /* REPEATABLE  */
    REPLACE = 621,                 /* REPLACE  */
    REPLICA = 622,                 /* REPLICA  */
    RESET = 623,                   /* RESET  */
    RESPECT_P = 624,               /* RESPECT_P  */
    RESTART = 625,                 /* RESTART  */
    RESTRICT = 626,                /* RESTRICT  */
    RETURNING = 627,               /* RETURNING  */
    RETURNS = 628,                 /* RETURNS  */
    REVOKE = 629,                  /* REVOKE  */
    RIGHT = 630,                   /* RIGHT  */
    ROLE = 631,                    /* ROLE  */
    ROLLBACK = 632,                /* ROLLBACK  */
    ROLLUP = 633,                  /* ROLLUP  */
    ROW = 634,                     /* ROW  */
    ROWS = 635,                    /* ROWS  */
    RULE = 636,                    /* RULE  */
    SAMPLE = 637,                  /* SAMPLE  */
    SAVEPOINT = 638,               /* SAVEPOINT  */
    SCHEMA = 639,                  /* SCHEMA  */
    SCHEMAS = 640,                 /* SCHEMAS  */
    SCOPE = 641,                   /* SCOPE  */
    SCROLL = 642,                  /* SCROLL  */
    SEARCH = 643,                  /* SEARCH  */
    SECOND_P = 644,                /* SECOND_P  */
    SECONDS_P = 645,               /* SECONDS_P  */
    SECRET = 646,                  /* SECRET  */
    SECURITY = 647,                /* SECURITY  */
    SELECT = 648,                  /* SELECT  */
    SEMI = 649,                    /* SEMI  */
    SEQUENCE = 650,                /* SEQUENCE  */
    SEQUENCES = 651,               /* SEQUENCES  */
    SERIALIZABLE = 652,            /* SERIALIZABLE  */
    SERVER = 653,                  /* SERVER  */
    SESSION = 654,                 /* SESSION  */
    SET = 655,                     /* SET  */
    SETOF = 656,                   /* SETOF  */
    SETS = 657,                    /* SETS  */
    SHARE = 658,                   /* SHARE  */
    SHORTEST = 659,                /* SHORTEST  */
    SHOW = 660,                    /* SHOW  */
    SIMILAR = 661,                 /* SIMILAR  */
    SIMPLE = 662,                  /* SIMPLE  */
    SKIP = 663,                    /* SKIP  */
    SMALLINT = 664,                /* SMALLINT  */
    SNAPSHOT = 665,                /* SNAPSHOT  */
    SOME = 666,                    /* SOME  */
    SOURCE = 667,                  /* SOURCE  */
    SQL_P = 668,                   /* SQL_P  */
    STABLE = 669,                  /* STABLE  */
    STANDALONE_P = 670,            /* STANDALONE_P  */
    START = 671,                   /* START  */
    STATEMENT = 672,               /* STATEMENT  */
    STATISTICS = 673,              /* STATISTICS  */
    STDIN = 674,                   /* STDIN  */
    STDOUT = 675,                  /* STDOUT  */
    STORAGE = 676,                 /* STORAGE  */
    STORED = 677,                  /* STORED  */
    STRICT_P = 678,                /* STRICT_P  */
    STRIP_P = 679,                 /* STRIP_P  */
    STRUCT = 680,                  /* STRUCT  */
    SUBSCRIPTION = 681,            /* SUBSCRIPTION  */
    SUBSTRING = 682,               /* SUBSTRING  */
    SUMMARIZE = 683,               /* SUMMARIZE  */
    SYMMETRIC = 684,               /* SYMMETRIC  */
    SYSID = 685,                   /* SYSID  */
    SYSTEM_P = 686,                /* SYSTEM_P  */
    TABLE = 687,                   /* TABLE  */
    TABLES = 688,                  /* TABLES  */
    TABLESAMPLE = 689,             /* TABLESAMPLE  */
    TABLESPACE = 690,              /* TABLESPACE  */
    TEMP = 691,                    /* TEMP  */
    TEMPLATE = 692,                /* TEMPLATE  */
    TEMPORARY = 693,               /* TEMPORARY  */
    TEXT_P = 694,                  /* TEXT_P  */
    THEN = 695,                    /* THEN  */
    TIES = 696,                    /* TIES  */
    TIME = 697,                    /* TIME  */
    TIMESTAMP = 698,               /* TIMESTAMP  */
    TO = 699,                      /* TO  */
    TRAIL = 700,                   /* TRAIL  */
    TRAILING = 701,                /* TRAILING  */
    TRANSACTION = 702,             /* TRANSACTION  */
    TRANSFORM = 703,               /* TRANSFORM  */
    TREAT = 704,                   /* TREAT  */
    TRIGGER = 705,                 /* TRIGGER  */
    TRIM = 706,                    /* TRIM  */
    TRUE_P = 707,                  /* TRUE_P  */
    TRUNCATE = 708,                /* TRUNCATE  */
    TRUSTED = 709,                 /* TRUSTED  */
    TRY_CAST = 710,                /* TRY_CAST  */
    TYPE_P = 711,                  /* TYPE_P  */
    TYPES_P = 712,                 /* TYPES_P  */
    UNBOUNDED = 713,               /* UNBOUNDED  */
    UNCOMMITTED = 714,             /* UNCOMMITTED  */
    UNENCRYPTED = 715,             /* UNENCRYPTED  */
    UNION = 716,                   /* UNION  */
    UNIQUE = 717,                  /* UNIQUE  */
    UNKNOWN = 718,                 /* UNKNOWN  */
    UNLISTEN = 719,                /* UNLISTEN  */
    UNLOGGED = 720,                /* UNLOGGED  */
    UNPIVOT = 721,                 /* UNPIVOT  */
    UNTIL = 722,                   /* UNTIL  */
    UPDATE = 723,                  /* UPDATE  */
    USE_P = 724,                   /* USE_P  */
    USER = 725,                    /* USER  */
    USING = 726,                   /* USING  */
    VACUUM = 727,                  /* VACUUM  */
    VALID = 728,                   /* VALID  */
    VALIDATE = 729,                /* VALIDATE  */
    VALIDATOR = 730,               /* VALIDATOR  */
    VALUE_P = 731,                 /* VALUE_P  */
    VALUES = 732,                  /* VALUES  */
    VARCHAR = 733,                 /* VARCHAR  */
    VARIADIC = 734,                /* VARIADIC  */
    VARYING = 735,                 /* VARYING  */
    VERBOSE = 736,                 /* VERBOSE  */
    VERSION_P = 737,               /* VERSION_P  */
    VERTEX = 738,                  /* VERTEX  */
    VIEW = 739,                    /* VIEW  */
    VIEWS = 740,                   /* VIEWS  */
    VIRTUAL = 741,                 /* VIRTUAL  */
    VOLATILE = 742,                /* VOLATILE  */
    WALK = 743,                    /* WALK  */
    WEEK_P = 744,                  /* WEEK_P  */
    WEEKS_P = 745,                 /* WEEKS_P  */
    WHEN = 746,                    /* WHEN  */
    WHERE = 747,                   /* WHERE  */
    WHITESPACE_P = 748,            /* WHITESPACE_P  */
    WINDOW = 749,                  /* WINDOW  */
    WITH = 750,                    /* WITH  */
    WITHIN = 751,                  /* WITHIN  */
    WITHOUT = 752,                 /* WITHOUT  */
    WORK = 753,                    /* WORK  */
    WRAPPER = 754,                 /* WRAPPER  */
    WRITE_P = 755,                 /* WRITE_P  */
    XML_P = 756,                   /* XML_P  */
    XMLATTRIBUTES = 757,           /* XMLATTRIBUTES  */
    XMLCONCAT = 758,               /* XMLCONCAT  */
    XMLELEMENT = 759,              /* XMLELEMENT  */
    XMLEXISTS = 760,               /* XMLEXISTS  */
    XMLFOREST = 761,               /* XMLFOREST  */
    XMLNAMESPACES = 762,           /* XMLNAMESPACES  */
    XMLPARSE = 763,                /* XMLPARSE  */
    XMLPI = 764,                   /* XMLPI  */
    XMLROOT = 765,                 /* XMLROOT  */
    XMLSERIALIZE = 766,            /* XMLSERIALIZE  */
    XMLTABLE = 767,                /* XMLTABLE  */
    YEAR_P = 768,                  /* YEAR_P  */
    YEARS_P = 769,                 /* YEARS_P  */
    YES_P = 770,                   /* YES_P  */
    ZONE = 771,                    /* ZONE  */
    NOT_LA = 772,                  /* NOT_LA  */
    NULLS_LA = 773,                /* NULLS_LA  */
    WITH_LA = 774,                 /* WITH_LA  */
    POSTFIXOP = 775,               /* POSTFIXOP  */
    UMINUS = 776                   /* UMINUS  */
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

#line 634 "third_party/libpg_query/grammar/grammar_out.hpp"

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
