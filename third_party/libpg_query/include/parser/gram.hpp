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
    ARROW_LEFT = 277,              /* ARROW_LEFT  */
    ARROW_BOTH = 278,              /* ARROW_BOTH  */
    ABORT_P = 279,                 /* ABORT_P  */
    ABSOLUTE_P = 280,              /* ABSOLUTE_P  */
    ACCESS = 281,                  /* ACCESS  */
    ACTION = 282,                  /* ACTION  */
    ACYCLIC = 283,                 /* ACYCLIC  */
    ADD_P = 284,                   /* ADD_P  */
    ADMIN = 285,                   /* ADMIN  */
    AFTER = 286,                   /* AFTER  */
    AGGREGATE = 287,               /* AGGREGATE  */
    ALL = 288,                     /* ALL  */
    ALSO = 289,                    /* ALSO  */
    ALTER = 290,                   /* ALTER  */
    ALWAYS = 291,                  /* ALWAYS  */
    ANALYSE = 292,                 /* ANALYSE  */
    ANALYZE = 293,                 /* ANALYZE  */
    AND = 294,                     /* AND  */
    ANTI = 295,                    /* ANTI  */
    ANY = 296,                     /* ANY  */
    ARE = 297,                     /* ARE  */
    ARRAY = 298,                   /* ARRAY  */
    AS = 299,                      /* AS  */
    ASC_P = 300,                   /* ASC_P  */
    ASOF = 301,                    /* ASOF  */
    ASSERTION = 302,               /* ASSERTION  */
    ASSIGNMENT = 303,              /* ASSIGNMENT  */
    ASYMMETRIC = 304,              /* ASYMMETRIC  */
    AT = 305,                      /* AT  */
    ATTACH = 306,                  /* ATTACH  */
    ATTRIBUTE = 307,               /* ATTRIBUTE  */
    AUTHORIZATION = 308,           /* AUTHORIZATION  */
    BACKWARD = 309,                /* BACKWARD  */
    BEFORE = 310,                  /* BEFORE  */
    BEGIN_P = 311,                 /* BEGIN_P  */
    BETWEEN = 312,                 /* BETWEEN  */
    BIGINT = 313,                  /* BIGINT  */
    BINARY = 314,                  /* BINARY  */
    BIT = 315,                     /* BIT  */
    BOOLEAN_P = 316,               /* BOOLEAN_P  */
    BOTH = 317,                    /* BOTH  */
    BY = 318,                      /* BY  */
    CACHE = 319,                   /* CACHE  */
    CALL_P = 320,                  /* CALL_P  */
    CALLED = 321,                  /* CALLED  */
    CASCADE = 322,                 /* CASCADE  */
    CASCADED = 323,                /* CASCADED  */
    CASE = 324,                    /* CASE  */
    CAST = 325,                    /* CAST  */
    CATALOG_P = 326,               /* CATALOG_P  */
    CENTURIES_P = 327,             /* CENTURIES_P  */
    CENTURY_P = 328,               /* CENTURY_P  */
    CHAIN = 329,                   /* CHAIN  */
    CHAR_P = 330,                  /* CHAR_P  */
    CHARACTER = 331,               /* CHARACTER  */
    CHARACTERISTICS = 332,         /* CHARACTERISTICS  */
    CHECK_P = 333,                 /* CHECK_P  */
    CHECKPOINT = 334,              /* CHECKPOINT  */
    CLASS = 335,                   /* CLASS  */
    CLOSE = 336,                   /* CLOSE  */
    CLUSTER = 337,                 /* CLUSTER  */
    COALESCE = 338,                /* COALESCE  */
    COLLATE = 339,                 /* COLLATE  */
    COLLATION = 340,               /* COLLATION  */
    COLUMN = 341,                  /* COLUMN  */
    COLUMNS = 342,                 /* COLUMNS  */
    COMMENT = 343,                 /* COMMENT  */
    COMMENTS = 344,                /* COMMENTS  */
    COMMIT = 345,                  /* COMMIT  */
    COMMITTED = 346,               /* COMMITTED  */
    COMPRESSION = 347,             /* COMPRESSION  */
    CONCURRENTLY = 348,            /* CONCURRENTLY  */
    CONFIGURATION = 349,           /* CONFIGURATION  */
    CONFLICT = 350,                /* CONFLICT  */
    CONNECTION = 351,              /* CONNECTION  */
    CONSTRAINT = 352,              /* CONSTRAINT  */
    CONSTRAINTS = 353,             /* CONSTRAINTS  */
    CONTENT_P = 354,               /* CONTENT_P  */
    CONTINUE_P = 355,              /* CONTINUE_P  */
    CONVERSION_P = 356,            /* CONVERSION_P  */
    COPY = 357,                    /* COPY  */
    COST = 358,                    /* COST  */
    CREATE_P = 359,                /* CREATE_P  */
    CROSS = 360,                   /* CROSS  */
    CSV = 361,                     /* CSV  */
    CUBE = 362,                    /* CUBE  */
    CURRENT_P = 363,               /* CURRENT_P  */
    CURSOR = 364,                  /* CURSOR  */
    CYCLE = 365,                   /* CYCLE  */
    DATA_P = 366,                  /* DATA_P  */
    DATABASE = 367,                /* DATABASE  */
    DAY_P = 368,                   /* DAY_P  */
    DAYS_P = 369,                  /* DAYS_P  */
    DEALLOCATE = 370,              /* DEALLOCATE  */
    DEC = 371,                     /* DEC  */
    DECADE_P = 372,                /* DECADE_P  */
    DECADES_P = 373,               /* DECADES_P  */
    DECIMAL_P = 374,               /* DECIMAL_P  */
    DECLARE = 375,                 /* DECLARE  */
    DEFAULT = 376,                 /* DEFAULT  */
    DEFAULTS = 377,                /* DEFAULTS  */
    DEFERRABLE = 378,              /* DEFERRABLE  */
    DEFERRED = 379,                /* DEFERRED  */
    DEFINER = 380,                 /* DEFINER  */
    DELETE_P = 381,                /* DELETE_P  */
    DELIMITER = 382,               /* DELIMITER  */
    DELIMITERS = 383,              /* DELIMITERS  */
    DEPENDS = 384,                 /* DEPENDS  */
    DESC_P = 385,                  /* DESC_P  */
    DESCRIBE = 386,                /* DESCRIBE  */
    DESTINATION = 387,             /* DESTINATION  */
    DETACH = 388,                  /* DETACH  */
    DICTIONARY = 389,              /* DICTIONARY  */
    DISABLE_P = 390,               /* DISABLE_P  */
    DISCARD = 391,                 /* DISCARD  */
    DISTINCT = 392,                /* DISTINCT  */
    DO = 393,                      /* DO  */
    DOCUMENT_P = 394,              /* DOCUMENT_P  */
    DOMAIN_P = 395,                /* DOMAIN_P  */
    DOUBLE_P = 396,                /* DOUBLE_P  */
    DROP = 397,                    /* DROP  */
    EACH = 398,                    /* EACH  */
    EDGE = 399,                    /* EDGE  */
    ELEMENT_ID = 400,              /* ELEMENT_ID  */
    ELSE = 401,                    /* ELSE  */
    ENABLE_P = 402,                /* ENABLE_P  */
    ENCODING = 403,                /* ENCODING  */
    ENCRYPTED = 404,               /* ENCRYPTED  */
    END_P = 405,                   /* END_P  */
    ENUM_P = 406,                  /* ENUM_P  */
    ESCAPE = 407,                  /* ESCAPE  */
    EVENT = 408,                   /* EVENT  */
    EXCEPT = 409,                  /* EXCEPT  */
    EXCLUDE = 410,                 /* EXCLUDE  */
    EXCLUDING = 411,               /* EXCLUDING  */
    EXCLUSIVE = 412,               /* EXCLUSIVE  */
    EXECUTE = 413,                 /* EXECUTE  */
    EXISTS = 414,                  /* EXISTS  */
    EXPLAIN = 415,                 /* EXPLAIN  */
    EXPORT_P = 416,                /* EXPORT_P  */
    EXPORT_STATE = 417,            /* EXPORT_STATE  */
    EXTENSION = 418,               /* EXTENSION  */
    EXTENSIONS = 419,              /* EXTENSIONS  */
    EXTERNAL = 420,                /* EXTERNAL  */
    EXTRACT = 421,                 /* EXTRACT  */
    FALSE_P = 422,                 /* FALSE_P  */
    FAMILY = 423,                  /* FAMILY  */
    FETCH = 424,                   /* FETCH  */
    FILTER = 425,                  /* FILTER  */
    FIRST_P = 426,                 /* FIRST_P  */
    FLOAT_P = 427,                 /* FLOAT_P  */
    FOLLOWING = 428,               /* FOLLOWING  */
    FOR = 429,                     /* FOR  */
    FORCE = 430,                   /* FORCE  */
    FOREIGN = 431,                 /* FOREIGN  */
    FORWARD = 432,                 /* FORWARD  */
    FREEZE = 433,                  /* FREEZE  */
    FROM = 434,                    /* FROM  */
    FULL = 435,                    /* FULL  */
    FUNCTION = 436,                /* FUNCTION  */
    FUNCTIONS = 437,               /* FUNCTIONS  */
    GENERATED = 438,               /* GENERATED  */
    GLOB = 439,                    /* GLOB  */
    GLOBAL = 440,                  /* GLOBAL  */
    GRANT = 441,                   /* GRANT  */
    GRANTED = 442,                 /* GRANTED  */
    GRAPH = 443,                   /* GRAPH  */
    GRAPH_TABLE = 444,             /* GRAPH_TABLE  */
    GROUP_P = 445,                 /* GROUP_P  */
    GROUPING = 446,                /* GROUPING  */
    GROUPING_ID = 447,             /* GROUPING_ID  */
    GROUPS = 448,                  /* GROUPS  */
    HANDLER = 449,                 /* HANDLER  */
    HAVING = 450,                  /* HAVING  */
    HEADER_P = 451,                /* HEADER_P  */
    HOLD = 452,                    /* HOLD  */
    HOUR_P = 453,                  /* HOUR_P  */
    HOURS_P = 454,                 /* HOURS_P  */
    IDENTITY_P = 455,              /* IDENTITY_P  */
    IF_P = 456,                    /* IF_P  */
    IGNORE_P = 457,                /* IGNORE_P  */
    ILIKE = 458,                   /* ILIKE  */
    IMMEDIATE = 459,               /* IMMEDIATE  */
    IMMUTABLE = 460,               /* IMMUTABLE  */
    IMPLICIT_P = 461,              /* IMPLICIT_P  */
    IMPORT_P = 462,                /* IMPORT_P  */
    IN_P = 463,                    /* IN_P  */
    INCLUDE_P = 464,               /* INCLUDE_P  */
    INCLUDING = 465,               /* INCLUDING  */
    INCREMENT = 466,               /* INCREMENT  */
    INDEX = 467,                   /* INDEX  */
    INDEXES = 468,                 /* INDEXES  */
    INHERIT = 469,                 /* INHERIT  */
    INHERITS = 470,                /* INHERITS  */
    INITIALLY = 471,               /* INITIALLY  */
    INLINE_P = 472,                /* INLINE_P  */
    INNER_P = 473,                 /* INNER_P  */
    INOUT = 474,                   /* INOUT  */
    INPUT_P = 475,                 /* INPUT_P  */
    INSENSITIVE = 476,             /* INSENSITIVE  */
    INSERT = 477,                  /* INSERT  */
    INSTALL = 478,                 /* INSTALL  */
    INSTEAD = 479,                 /* INSTEAD  */
    INT_P = 480,                   /* INT_P  */
    INTEGER = 481,                 /* INTEGER  */
    INTERSECT = 482,               /* INTERSECT  */
    INTERVAL = 483,                /* INTERVAL  */
    INTO = 484,                    /* INTO  */
    INVOKER = 485,                 /* INVOKER  */
    IS = 486,                      /* IS  */
    ISNULL = 487,                  /* ISNULL  */
    ISOLATION = 488,               /* ISOLATION  */
    JOIN = 489,                    /* JOIN  */
    JSON = 490,                    /* JSON  */
    KEEP = 491,                    /* KEEP  */
    KEY = 492,                     /* KEY  */
    LABEL = 493,                   /* LABEL  */
    LANGUAGE = 494,                /* LANGUAGE  */
    LARGE_P = 495,                 /* LARGE_P  */
    LAST_P = 496,                  /* LAST_P  */
    LATERAL_P = 497,               /* LATERAL_P  */
    LEADING = 498,                 /* LEADING  */
    LEAKPROOF = 499,               /* LEAKPROOF  */
    LEFT = 500,                    /* LEFT  */
    LEVEL = 501,                   /* LEVEL  */
    LIKE = 502,                    /* LIKE  */
    LIMIT = 503,                   /* LIMIT  */
    LISTEN = 504,                  /* LISTEN  */
    LOAD = 505,                    /* LOAD  */
    LOCAL = 506,                   /* LOCAL  */
    LOCATION = 507,                /* LOCATION  */
    LOCK_P = 508,                  /* LOCK_P  */
    LOCKED = 509,                  /* LOCKED  */
    LOGGED = 510,                  /* LOGGED  */
    MACRO = 511,                   /* MACRO  */
    MAP = 512,                     /* MAP  */
    MAPPING = 513,                 /* MAPPING  */
    MATCH = 514,                   /* MATCH  */
    MATERIALIZED = 515,            /* MATERIALIZED  */
    MAXVALUE = 516,                /* MAXVALUE  */
    METHOD = 517,                  /* METHOD  */
    MICROSECOND_P = 518,           /* MICROSECOND_P  */
    MICROSECONDS_P = 519,          /* MICROSECONDS_P  */
    MILLENNIA_P = 520,             /* MILLENNIA_P  */
    MILLENNIUM_P = 521,            /* MILLENNIUM_P  */
    MILLISECOND_P = 522,           /* MILLISECOND_P  */
    MILLISECONDS_P = 523,          /* MILLISECONDS_P  */
    MINUTE_P = 524,                /* MINUTE_P  */
    MINUTES_P = 525,               /* MINUTES_P  */
    MINVALUE = 526,                /* MINVALUE  */
    MODE = 527,                    /* MODE  */
    MONTH_P = 528,                 /* MONTH_P  */
    MONTHS_P = 529,                /* MONTHS_P  */
    MOVE = 530,                    /* MOVE  */
    NAME_P = 531,                  /* NAME_P  */
    NAMES = 532,                   /* NAMES  */
    NATIONAL = 533,                /* NATIONAL  */
    NATURAL = 534,                 /* NATURAL  */
    NCHAR = 535,                   /* NCHAR  */
    NEW = 536,                     /* NEW  */
    NEXT = 537,                    /* NEXT  */
    NO = 538,                      /* NO  */
    NODE = 539,                    /* NODE  */
    NONE = 540,                    /* NONE  */
    NOT = 541,                     /* NOT  */
    NOTHING = 542,                 /* NOTHING  */
    NOTIFY = 543,                  /* NOTIFY  */
    NOTNULL = 544,                 /* NOTNULL  */
    NOWAIT = 545,                  /* NOWAIT  */
    NULL_P = 546,                  /* NULL_P  */
    NULLIF = 547,                  /* NULLIF  */
    NULLS_P = 548,                 /* NULLS_P  */
    NUMERIC = 549,                 /* NUMERIC  */
    OBJECT_P = 550,                /* OBJECT_P  */
    OF = 551,                      /* OF  */
    OFF = 552,                     /* OFF  */
    OFFSET = 553,                  /* OFFSET  */
    OIDS = 554,                    /* OIDS  */
    OLD = 555,                     /* OLD  */
    ON = 556,                      /* ON  */
    ONLY = 557,                    /* ONLY  */
    OPERATOR = 558,                /* OPERATOR  */
    OPTION = 559,                  /* OPTION  */
    OPTIONS = 560,                 /* OPTIONS  */
    OR = 561,                      /* OR  */
    ORDER = 562,                   /* ORDER  */
    ORDINALITY = 563,              /* ORDINALITY  */
    OTHERS = 564,                  /* OTHERS  */
    OUT_P = 565,                   /* OUT_P  */
    OUTER_P = 566,                 /* OUTER_P  */
    OVER = 567,                    /* OVER  */
    OVERLAPS = 568,                /* OVERLAPS  */
    OVERLAY = 569,                 /* OVERLAY  */
    OVERRIDING = 570,              /* OVERRIDING  */
    OWNED = 571,                   /* OWNED  */
    OWNER = 572,                   /* OWNER  */
    PARALLEL = 573,                /* PARALLEL  */
    PARSER = 574,                  /* PARSER  */
    PARTIAL = 575,                 /* PARTIAL  */
    PARTITION = 576,               /* PARTITION  */
    PASSING = 577,                 /* PASSING  */
    PASSWORD = 578,                /* PASSWORD  */
    PATH = 579,                    /* PATH  */
    PATHS = 580,                   /* PATHS  */
    PERCENT = 581,                 /* PERCENT  */
    PERSISTENT = 582,              /* PERSISTENT  */
    PIVOT = 583,                   /* PIVOT  */
    PIVOT_LONGER = 584,            /* PIVOT_LONGER  */
    PIVOT_WIDER = 585,             /* PIVOT_WIDER  */
    PLACING = 586,                 /* PLACING  */
    PLANS = 587,                   /* PLANS  */
    POLICY = 588,                  /* POLICY  */
    POSITION = 589,                /* POSITION  */
    POSITIONAL = 590,              /* POSITIONAL  */
    PRAGMA_P = 591,                /* PRAGMA_P  */
    PRECEDING = 592,               /* PRECEDING  */
    PRECISION = 593,               /* PRECISION  */
    PREPARE = 594,                 /* PREPARE  */
    PREPARED = 595,                /* PREPARED  */
    PRESERVE = 596,                /* PRESERVE  */
    PRIMARY = 597,                 /* PRIMARY  */
    PRIOR = 598,                   /* PRIOR  */
    PRIVILEGES = 599,              /* PRIVILEGES  */
    PROCEDURAL = 600,              /* PROCEDURAL  */
    PROCEDURE = 601,               /* PROCEDURE  */
    PROGRAM = 602,                 /* PROGRAM  */
    PROPERTIES = 603,              /* PROPERTIES  */
    PROPERTY = 604,                /* PROPERTY  */
    PUBLICATION = 605,             /* PUBLICATION  */
    QUALIFY = 606,                 /* QUALIFY  */
    QUARTER_P = 607,               /* QUARTER_P  */
    QUARTERS_P = 608,              /* QUARTERS_P  */
    QUOTE = 609,                   /* QUOTE  */
    RANGE = 610,                   /* RANGE  */
    READ_P = 611,                  /* READ_P  */
    REAL = 612,                    /* REAL  */
    REASSIGN = 613,                /* REASSIGN  */
    RECHECK = 614,                 /* RECHECK  */
    RECURSIVE = 615,               /* RECURSIVE  */
    REF = 616,                     /* REF  */
    REFERENCES = 617,              /* REFERENCES  */
    REFERENCING = 618,             /* REFERENCING  */
    REFRESH = 619,                 /* REFRESH  */
    REINDEX = 620,                 /* REINDEX  */
    RELATIONSHIP = 621,            /* RELATIONSHIP  */
    RELATIVE_P = 622,              /* RELATIVE_P  */
    RELEASE = 623,                 /* RELEASE  */
    RENAME = 624,                  /* RENAME  */
    REPEATABLE = 625,              /* REPEATABLE  */
    REPLACE = 626,                 /* REPLACE  */
    REPLICA = 627,                 /* REPLICA  */
    RESET = 628,                   /* RESET  */
    RESPECT_P = 629,               /* RESPECT_P  */
    RESTART = 630,                 /* RESTART  */
    RESTRICT = 631,                /* RESTRICT  */
    RETURNING = 632,               /* RETURNING  */
    RETURNS = 633,                 /* RETURNS  */
    REVOKE = 634,                  /* REVOKE  */
    RIGHT = 635,                   /* RIGHT  */
    ROLE = 636,                    /* ROLE  */
    ROLLBACK = 637,                /* ROLLBACK  */
    ROLLUP = 638,                  /* ROLLUP  */
    ROW = 639,                     /* ROW  */
    ROWS = 640,                    /* ROWS  */
    RULE = 641,                    /* RULE  */
    SAMPLE = 642,                  /* SAMPLE  */
    SAVEPOINT = 643,               /* SAVEPOINT  */
    SCHEMA = 644,                  /* SCHEMA  */
    SCHEMAS = 645,                 /* SCHEMAS  */
    SCOPE = 646,                   /* SCOPE  */
    SCROLL = 647,                  /* SCROLL  */
    SEARCH = 648,                  /* SEARCH  */
    SECOND_P = 649,                /* SECOND_P  */
    SECONDS_P = 650,               /* SECONDS_P  */
    SECRET = 651,                  /* SECRET  */
    SECURITY = 652,                /* SECURITY  */
    SELECT = 653,                  /* SELECT  */
    SEMI = 654,                    /* SEMI  */
    SEQUENCE = 655,                /* SEQUENCE  */
    SEQUENCES = 656,               /* SEQUENCES  */
    SERIALIZABLE = 657,            /* SERIALIZABLE  */
    SERVER = 658,                  /* SERVER  */
    SESSION = 659,                 /* SESSION  */
    SET = 660,                     /* SET  */
    SETOF = 661,                   /* SETOF  */
    SETS = 662,                    /* SETS  */
    SHARE = 663,                   /* SHARE  */
    SHORTEST = 664,                /* SHORTEST  */
    SHOW = 665,                    /* SHOW  */
    SIMILAR = 666,                 /* SIMILAR  */
    SIMPLE = 667,                  /* SIMPLE  */
    SKIP = 668,                    /* SKIP  */
    SMALLINT = 669,                /* SMALLINT  */
    SNAPSHOT = 670,                /* SNAPSHOT  */
    SOME = 671,                    /* SOME  */
    SOURCE = 672,                  /* SOURCE  */
    SQL_P = 673,                   /* SQL_P  */
    STABLE = 674,                  /* STABLE  */
    STANDALONE_P = 675,            /* STANDALONE_P  */
    START = 676,                   /* START  */
    STATEMENT = 677,               /* STATEMENT  */
    STATISTICS = 678,              /* STATISTICS  */
    STDIN = 679,                   /* STDIN  */
    STDOUT = 680,                  /* STDOUT  */
    STORAGE = 681,                 /* STORAGE  */
    STORED = 682,                  /* STORED  */
    STRICT_P = 683,                /* STRICT_P  */
    STRIP_P = 684,                 /* STRIP_P  */
    STRUCT = 685,                  /* STRUCT  */
    SUBSCRIPTION = 686,            /* SUBSCRIPTION  */
    SUBSTRING = 687,               /* SUBSTRING  */
    SUMMARIZE = 688,               /* SUMMARIZE  */
    SYMMETRIC = 689,               /* SYMMETRIC  */
    SYSID = 690,                   /* SYSID  */
    SYSTEM_P = 691,                /* SYSTEM_P  */
    TABLE = 692,                   /* TABLE  */
    TABLES = 693,                  /* TABLES  */
    TABLESAMPLE = 694,             /* TABLESAMPLE  */
    TABLESPACE = 695,              /* TABLESPACE  */
    TEMP = 696,                    /* TEMP  */
    TEMPLATE = 697,                /* TEMPLATE  */
    TEMPORARY = 698,               /* TEMPORARY  */
    TEXT_P = 699,                  /* TEXT_P  */
    THEN = 700,                    /* THEN  */
    TIES = 701,                    /* TIES  */
    TIME = 702,                    /* TIME  */
    TIMESTAMP = 703,               /* TIMESTAMP  */
    TO = 704,                      /* TO  */
    TRAIL = 705,                   /* TRAIL  */
    TRAILING = 706,                /* TRAILING  */
    TRANSACTION = 707,             /* TRANSACTION  */
    TRANSFORM = 708,               /* TRANSFORM  */
    TREAT = 709,                   /* TREAT  */
    TRIGGER = 710,                 /* TRIGGER  */
    TRIM = 711,                    /* TRIM  */
    TRUE_P = 712,                  /* TRUE_P  */
    TRUNCATE = 713,                /* TRUNCATE  */
    TRUSTED = 714,                 /* TRUSTED  */
    TRY_CAST = 715,                /* TRY_CAST  */
    TYPE_P = 716,                  /* TYPE_P  */
    TYPES_P = 717,                 /* TYPES_P  */
    UNBOUNDED = 718,               /* UNBOUNDED  */
    UNCOMMITTED = 719,             /* UNCOMMITTED  */
    UNENCRYPTED = 720,             /* UNENCRYPTED  */
    UNION = 721,                   /* UNION  */
    UNIQUE = 722,                  /* UNIQUE  */
    UNKNOWN = 723,                 /* UNKNOWN  */
    UNLISTEN = 724,                /* UNLISTEN  */
    UNLOGGED = 725,                /* UNLOGGED  */
    UNPIVOT = 726,                 /* UNPIVOT  */
    UNTIL = 727,                   /* UNTIL  */
    UPDATE = 728,                  /* UPDATE  */
    USE_P = 729,                   /* USE_P  */
    USER = 730,                    /* USER  */
    USING = 731,                   /* USING  */
    VACUUM = 732,                  /* VACUUM  */
    VALID = 733,                   /* VALID  */
    VALIDATE = 734,                /* VALIDATE  */
    VALIDATOR = 735,               /* VALIDATOR  */
    VALUE_P = 736,                 /* VALUE_P  */
    VALUES = 737,                  /* VALUES  */
    VARCHAR = 738,                 /* VARCHAR  */
    VARIABLE_P = 739,              /* VARIABLE_P  */
    VARIADIC = 740,                /* VARIADIC  */
    VARYING = 741,                 /* VARYING  */
    VERBOSE = 742,                 /* VERBOSE  */
    VERSION_P = 743,               /* VERSION_P  */
    VERTEX = 744,                  /* VERTEX  */
    VIEW = 745,                    /* VIEW  */
    VIEWS = 746,                   /* VIEWS  */
    VIRTUAL = 747,                 /* VIRTUAL  */
    VOLATILE = 748,                /* VOLATILE  */
    WALK = 749,                    /* WALK  */
    WEEK_P = 750,                  /* WEEK_P  */
    WEEKS_P = 751,                 /* WEEKS_P  */
    WHEN = 752,                    /* WHEN  */
    WHERE = 753,                   /* WHERE  */
    WHITESPACE_P = 754,            /* WHITESPACE_P  */
    WINDOW = 755,                  /* WINDOW  */
    WITH = 756,                    /* WITH  */
    WITHIN = 757,                  /* WITHIN  */
    WITHOUT = 758,                 /* WITHOUT  */
    WORK = 759,                    /* WORK  */
    WRAPPER = 760,                 /* WRAPPER  */
    WRITE_P = 761,                 /* WRITE_P  */
    XML_P = 762,                   /* XML_P  */
    XMLATTRIBUTES = 763,           /* XMLATTRIBUTES  */
    XMLCONCAT = 764,               /* XMLCONCAT  */
    XMLELEMENT = 765,              /* XMLELEMENT  */
    XMLEXISTS = 766,               /* XMLEXISTS  */
    XMLFOREST = 767,               /* XMLFOREST  */
    XMLNAMESPACES = 768,           /* XMLNAMESPACES  */
    XMLPARSE = 769,                /* XMLPARSE  */
    XMLPI = 770,                   /* XMLPI  */
    XMLROOT = 771,                 /* XMLROOT  */
    XMLSERIALIZE = 772,            /* XMLSERIALIZE  */
    XMLTABLE = 773,                /* XMLTABLE  */
    YEAR_P = 774,                  /* YEAR_P  */
    YEARS_P = 775,                 /* YEARS_P  */
    YES_P = 776,                   /* YES_P  */
    ZONE = 777,                    /* ZONE  */
    NOT_LA = 778,                  /* NOT_LA  */
    NULLS_LA = 779,                /* NULLS_LA  */
    WITH_LA = 780,                 /* WITH_LA  */
    POSTFIXOP = 781,               /* POSTFIXOP  */
    UMINUS = 782                   /* UMINUS  */
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

#line 642 "third_party/libpg_query/grammar/grammar_out.hpp"

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
