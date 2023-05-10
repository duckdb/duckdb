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
    POWER_OF = 270,                /* POWER_OF  */
    LAMBDA_ARROW = 271,            /* LAMBDA_ARROW  */
    DOUBLE_ARROW = 272,            /* DOUBLE_ARROW  */
    LESS_EQUALS = 273,             /* LESS_EQUALS  */
    GREATER_EQUALS = 274,          /* GREATER_EQUALS  */
    NOT_EQUALS = 275,              /* NOT_EQUALS  */
    ABORT_P = 276,                 /* ABORT_P  */
    ABSOLUTE_P = 277,              /* ABSOLUTE_P  */
    ACCESS = 278,                  /* ACCESS  */
    ACTION = 279,                  /* ACTION  */
    ACYCLIC = 280,                 /* ACYCLIC  */
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
    ANY = 292,                     /* ANY  */
    ARE = 293,                     /* ARE  */
    ARRAY = 294,                   /* ARRAY  */
    AS = 295,                      /* AS  */
    ASC_P = 296,                   /* ASC_P  */
    ASSERTION = 297,               /* ASSERTION  */
    ASSIGNMENT = 298,              /* ASSIGNMENT  */
    ASYMMETRIC = 299,              /* ASYMMETRIC  */
    AT = 300,                      /* AT  */
    ATTACH = 301,                  /* ATTACH  */
    ATTRIBUTE = 302,               /* ATTRIBUTE  */
    AUTHORIZATION = 303,           /* AUTHORIZATION  */
    BACKWARD = 304,                /* BACKWARD  */
    BEFORE = 305,                  /* BEFORE  */
    BEGIN_P = 306,                 /* BEGIN_P  */
    BETWEEN = 307,                 /* BETWEEN  */
    BIGINT = 308,                  /* BIGINT  */
    BINARY = 309,                  /* BINARY  */
    BIT = 310,                     /* BIT  */
    BOOLEAN_P = 311,               /* BOOLEAN_P  */
    BOTH = 312,                    /* BOTH  */
    BY = 313,                      /* BY  */
    CACHE = 314,                   /* CACHE  */
    CALL_P = 315,                  /* CALL_P  */
    CALLED = 316,                  /* CALLED  */
    CASCADE = 317,                 /* CASCADE  */
    CASCADED = 318,                /* CASCADED  */
    CASE = 319,                    /* CASE  */
    CAST = 320,                    /* CAST  */
    CATALOG_P = 321,               /* CATALOG_P  */
    CHAIN = 322,                   /* CHAIN  */
    CHAR_P = 323,                  /* CHAR_P  */
    CHARACTER = 324,               /* CHARACTER  */
    CHARACTERISTICS = 325,         /* CHARACTERISTICS  */
    CHECK_P = 326,                 /* CHECK_P  */
    CHECKPOINT = 327,              /* CHECKPOINT  */
    CLASS = 328,                   /* CLASS  */
    CLOSE = 329,                   /* CLOSE  */
    CLUSTER = 330,                 /* CLUSTER  */
    COALESCE = 331,                /* COALESCE  */
    COLLATE = 332,                 /* COLLATE  */
    COLLATION = 333,               /* COLLATION  */
    COLUMN = 334,                  /* COLUMN  */
    COLUMNS = 335,                 /* COLUMNS  */
    COMMENT = 336,                 /* COMMENT  */
    COMMENTS = 337,                /* COMMENTS  */
    COMMIT = 338,                  /* COMMIT  */
    COMMITTED = 339,               /* COMMITTED  */
    COMPRESSION = 340,             /* COMPRESSION  */
    CONCURRENTLY = 341,            /* CONCURRENTLY  */
    CONFIGURATION = 342,           /* CONFIGURATION  */
    CONFLICT = 343,                /* CONFLICT  */
    CONNECTION = 344,              /* CONNECTION  */
    CONSTRAINT = 345,              /* CONSTRAINT  */
    CONSTRAINTS = 346,             /* CONSTRAINTS  */
    CONTENT_P = 347,               /* CONTENT_P  */
    CONTINUE_P = 348,              /* CONTINUE_P  */
    CONVERSION_P = 349,            /* CONVERSION_P  */
    COPY = 350,                    /* COPY  */
    COST = 351,                    /* COST  */
    CREATE_P = 352,                /* CREATE_P  */
    CROSS = 353,                   /* CROSS  */
    CSV = 354,                     /* CSV  */
    CUBE = 355,                    /* CUBE  */
    CURRENT_P = 356,               /* CURRENT_P  */
    CURRENT_CATALOG = 357,         /* CURRENT_CATALOG  */
    CURRENT_DATE = 358,            /* CURRENT_DATE  */
    CURRENT_ROLE = 359,            /* CURRENT_ROLE  */
    CURRENT_SCHEMA = 360,          /* CURRENT_SCHEMA  */
    CURRENT_TIME = 361,            /* CURRENT_TIME  */
    CURRENT_TIMESTAMP = 362,       /* CURRENT_TIMESTAMP  */
    CURRENT_USER = 363,            /* CURRENT_USER  */
    CURSOR = 364,                  /* CURSOR  */
    CYCLE = 365,                   /* CYCLE  */
    DATA_P = 366,                  /* DATA_P  */
    DATABASE = 367,                /* DATABASE  */
    DAY_P = 368,                   /* DAY_P  */
    DAYS_P = 369,                  /* DAYS_P  */
    DEALLOCATE = 370,              /* DEALLOCATE  */
    DEC = 371,                     /* DEC  */
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
    INCLUDING = 461,               /* INCLUDING  */
    INCREMENT = 462,               /* INCREMENT  */
    INDEX = 463,                   /* INDEX  */
    INDEXES = 464,                 /* INDEXES  */
    INHERIT = 465,                 /* INHERIT  */
    INHERITS = 466,                /* INHERITS  */
    INITIALLY = 467,               /* INITIALLY  */
    INLINE_P = 468,                /* INLINE_P  */
    INNER_P = 469,                 /* INNER_P  */
    INOUT = 470,                   /* INOUT  */
    INPUT_P = 471,                 /* INPUT_P  */
    INSENSITIVE = 472,             /* INSENSITIVE  */
    INSERT = 473,                  /* INSERT  */
    INSTALL = 474,                 /* INSTALL  */
    INSTEAD = 475,                 /* INSTEAD  */
    INT_P = 476,                   /* INT_P  */
    INTEGER = 477,                 /* INTEGER  */
    INTERSECT = 478,               /* INTERSECT  */
    INTERVAL = 479,                /* INTERVAL  */
    INTO = 480,                    /* INTO  */
    INVOKER = 481,                 /* INVOKER  */
    IS = 482,                      /* IS  */
    ISNULL = 483,                  /* ISNULL  */
    ISOLATION = 484,               /* ISOLATION  */
    JOIN = 485,                    /* JOIN  */
    JSON = 486,                    /* JSON  */
    KEEP = 487,                    /* KEEP  */
    KEY = 488,                     /* KEY  */
    LABEL = 489,                   /* LABEL  */
    LANGUAGE = 490,                /* LANGUAGE  */
    LARGE_P = 491,                 /* LARGE_P  */
    LAST_P = 492,                  /* LAST_P  */
    LATERAL_P = 493,               /* LATERAL_P  */
    LEADING = 494,                 /* LEADING  */
    LEAKPROOF = 495,               /* LEAKPROOF  */
    LEFT = 496,                    /* LEFT  */
    LEVEL = 497,                   /* LEVEL  */
    LIKE = 498,                    /* LIKE  */
    LIMIT = 499,                   /* LIMIT  */
    LISTEN = 500,                  /* LISTEN  */
    LOAD = 501,                    /* LOAD  */
    LOCAL = 502,                   /* LOCAL  */
    LOCALTIME = 503,               /* LOCALTIME  */
    LOCALTIMESTAMP = 504,          /* LOCALTIMESTAMP  */
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
    MILLISECOND_P = 518,           /* MILLISECOND_P  */
    MILLISECONDS_P = 519,          /* MILLISECONDS_P  */
    MINUTE_P = 520,                /* MINUTE_P  */
    MINUTES_P = 521,               /* MINUTES_P  */
    MINVALUE = 522,                /* MINVALUE  */
    MODE = 523,                    /* MODE  */
    MONTH_P = 524,                 /* MONTH_P  */
    MONTHS_P = 525,                /* MONTHS_P  */
    MOVE = 526,                    /* MOVE  */
    NAME_P = 527,                  /* NAME_P  */
    NAMES = 528,                   /* NAMES  */
    NATIONAL = 529,                /* NATIONAL  */
    NATURAL = 530,                 /* NATURAL  */
    NCHAR = 531,                   /* NCHAR  */
    NEW = 532,                     /* NEW  */
    NEXT = 533,                    /* NEXT  */
    NO = 534,                      /* NO  */
    NODE = 535,                    /* NODE  */
    NONE = 536,                    /* NONE  */
    NOT = 537,                     /* NOT  */
    NOTHING = 538,                 /* NOTHING  */
    NOTIFY = 539,                  /* NOTIFY  */
    NOTNULL = 540,                 /* NOTNULL  */
    NOWAIT = 541,                  /* NOWAIT  */
    NULL_P = 542,                  /* NULL_P  */
    NULLIF = 543,                  /* NULLIF  */
    NULLS_P = 544,                 /* NULLS_P  */
    NUMERIC = 545,                 /* NUMERIC  */
    OBJECT_P = 546,                /* OBJECT_P  */
    OF = 547,                      /* OF  */
    OFF = 548,                     /* OFF  */
    OFFSET = 549,                  /* OFFSET  */
    OIDS = 550,                    /* OIDS  */
    OLD = 551,                     /* OLD  */
    ON = 552,                      /* ON  */
    ONLY = 553,                    /* ONLY  */
    OPERATOR = 554,                /* OPERATOR  */
    OPTION = 555,                  /* OPTION  */
    OPTIONS = 556,                 /* OPTIONS  */
    OR = 557,                      /* OR  */
    ORDER = 558,                   /* ORDER  */
    ORDINALITY = 559,              /* ORDINALITY  */
    OUT_P = 560,                   /* OUT_P  */
    OUTER_P = 561,                 /* OUTER_P  */
    OVER = 562,                    /* OVER  */
    OVERLAPS = 563,                /* OVERLAPS  */
    OVERLAY = 564,                 /* OVERLAY  */
    OVERRIDING = 565,              /* OVERRIDING  */
    OWNED = 566,                   /* OWNED  */
    OWNER = 567,                   /* OWNER  */
    PARALLEL = 568,                /* PARALLEL  */
    PARSER = 569,                  /* PARSER  */
    PARTIAL = 570,                 /* PARTIAL  */
    PARTITION = 571,               /* PARTITION  */
    PASSING = 572,                 /* PASSING  */
    PASSWORD = 573,                /* PASSWORD  */
    PATH = 574,                    /* PATH  */
    PATHS = 575,                   /* PATHS  */
    PERCENT = 576,                 /* PERCENT  */
    PLACING = 577,                 /* PLACING  */
    PLANS = 578,                   /* PLANS  */
    POLICY = 579,                  /* POLICY  */
    POSITION = 580,                /* POSITION  */
    POSITIONAL = 581,              /* POSITIONAL  */
    PRAGMA_P = 582,                /* PRAGMA_P  */
    PRECEDING = 583,               /* PRECEDING  */
    PRECISION = 584,               /* PRECISION  */
    PREPARE = 585,                 /* PREPARE  */
    PREPARED = 586,                /* PREPARED  */
    PRESERVE = 587,                /* PRESERVE  */
    PRIMARY = 588,                 /* PRIMARY  */
    PRIOR = 589,                   /* PRIOR  */
    PRIVILEGES = 590,              /* PRIVILEGES  */
    PROCEDURAL = 591,              /* PROCEDURAL  */
    PROCEDURE = 592,               /* PROCEDURE  */
    PROGRAM = 593,                 /* PROGRAM  */
    PROPERTIES = 594,              /* PROPERTIES  */
    PROPERTY = 595,                /* PROPERTY  */
    PUBLICATION = 596,             /* PUBLICATION  */
    QUALIFY = 597,                 /* QUALIFY  */
    QUOTE = 598,                   /* QUOTE  */
    RANGE = 599,                   /* RANGE  */
    READ_P = 600,                  /* READ_P  */
    REAL = 601,                    /* REAL  */
    REASSIGN = 602,                /* REASSIGN  */
    RECHECK = 603,                 /* RECHECK  */
    RECURSIVE = 604,               /* RECURSIVE  */
    REF = 605,                     /* REF  */
    REFERENCES = 606,              /* REFERENCES  */
    REFERENCING = 607,             /* REFERENCING  */
    REFRESH = 608,                 /* REFRESH  */
    REINDEX = 609,                 /* REINDEX  */
    RELATIONSHIP = 610,            /* RELATIONSHIP  */
    RELATIVE_P = 611,              /* RELATIVE_P  */
    RELEASE = 612,                 /* RELEASE  */
    RENAME = 613,                  /* RENAME  */
    REPEATABLE = 614,              /* REPEATABLE  */
    REPLACE = 615,                 /* REPLACE  */
    REPLICA = 616,                 /* REPLICA  */
    RESET = 617,                   /* RESET  */
    RESPECT_P = 618,               /* RESPECT_P  */
    RESTART = 619,                 /* RESTART  */
    RESTRICT = 620,                /* RESTRICT  */
    RETURNING = 621,               /* RETURNING  */
    RETURNS = 622,                 /* RETURNS  */
    REVOKE = 623,                  /* REVOKE  */
    RIGHT = 624,                   /* RIGHT  */
    ROLE = 625,                    /* ROLE  */
    ROLLBACK = 626,                /* ROLLBACK  */
    ROLLUP = 627,                  /* ROLLUP  */
    ROW = 628,                     /* ROW  */
    ROWS = 629,                    /* ROWS  */
    RULE = 630,                    /* RULE  */
    SAMPLE = 631,                  /* SAMPLE  */
    SAVEPOINT = 632,               /* SAVEPOINT  */
    SCHEMA = 633,                  /* SCHEMA  */
    SCHEMAS = 634,                 /* SCHEMAS  */
    SCROLL = 635,                  /* SCROLL  */
    SEARCH = 636,                  /* SEARCH  */
    SECOND_P = 637,                /* SECOND_P  */
    SECONDS_P = 638,               /* SECONDS_P  */
    SECURITY = 639,                /* SECURITY  */
    SELECT = 640,                  /* SELECT  */
    SEQUENCE = 641,                /* SEQUENCE  */
    SEQUENCES = 642,               /* SEQUENCES  */
    SERIALIZABLE = 643,            /* SERIALIZABLE  */
    SERVER = 644,                  /* SERVER  */
    SESSION = 645,                 /* SESSION  */
    SESSION_USER = 646,            /* SESSION_USER  */
    SET = 647,                     /* SET  */
    SETOF = 648,                   /* SETOF  */
    SETS = 649,                    /* SETS  */
    SHARE = 650,                   /* SHARE  */
    SHORTEST = 651,                /* SHORTEST  */
    SHOW = 652,                    /* SHOW  */
    SIMILAR = 653,                 /* SIMILAR  */
    SIMPLE = 654,                  /* SIMPLE  */
    SKIP = 655,                    /* SKIP  */
    SMALLINT = 656,                /* SMALLINT  */
    SNAPSHOT = 657,                /* SNAPSHOT  */
    SOME = 658,                    /* SOME  */
    SOURCE = 659,                  /* SOURCE  */
    SQL_P = 660,                   /* SQL_P  */
    STABLE = 661,                  /* STABLE  */
    STANDALONE_P = 662,            /* STANDALONE_P  */
    START = 663,                   /* START  */
    STATEMENT = 664,               /* STATEMENT  */
    STATISTICS = 665,              /* STATISTICS  */
    STDIN = 666,                   /* STDIN  */
    STDOUT = 667,                  /* STDOUT  */
    STORAGE = 668,                 /* STORAGE  */
    STORED = 669,                  /* STORED  */
    STRICT_P = 670,                /* STRICT_P  */
    STRIP_P = 671,                 /* STRIP_P  */
    STRUCT = 672,                  /* STRUCT  */
    SUBSCRIPTION = 673,            /* SUBSCRIPTION  */
    SUBSTRING = 674,               /* SUBSTRING  */
    SUMMARIZE = 675,               /* SUMMARIZE  */
    SYMMETRIC = 676,               /* SYMMETRIC  */
    SYSID = 677,                   /* SYSID  */
    SYSTEM_P = 678,                /* SYSTEM_P  */
    TABLE = 679,                   /* TABLE  */
    TABLES = 680,                  /* TABLES  */
    TABLESAMPLE = 681,             /* TABLESAMPLE  */
    TABLESPACE = 682,              /* TABLESPACE  */
    TEMP = 683,                    /* TEMP  */
    TEMPLATE = 684,                /* TEMPLATE  */
    TEMPORARY = 685,               /* TEMPORARY  */
    TEXT_P = 686,                  /* TEXT_P  */
    THEN = 687,                    /* THEN  */
    TIME = 688,                    /* TIME  */
    TIMESTAMP = 689,               /* TIMESTAMP  */
    TO = 690,                      /* TO  */
    TRAIL = 691,                   /* TRAIL  */
    TRAILING = 692,                /* TRAILING  */
    TRANSACTION = 693,             /* TRANSACTION  */
    TRANSFORM = 694,               /* TRANSFORM  */
    TREAT = 695,                   /* TREAT  */
    TRIGGER = 696,                 /* TRIGGER  */
    TRIM = 697,                    /* TRIM  */
    TRUE_P = 698,                  /* TRUE_P  */
    TRUNCATE = 699,                /* TRUNCATE  */
    TRUSTED = 700,                 /* TRUSTED  */
    TRY_CAST = 701,                /* TRY_CAST  */
    TYPE_P = 702,                  /* TYPE_P  */
    TYPES_P = 703,                 /* TYPES_P  */
    UNBOUNDED = 704,               /* UNBOUNDED  */
    UNCOMMITTED = 705,             /* UNCOMMITTED  */
    UNENCRYPTED = 706,             /* UNENCRYPTED  */
    UNION = 707,                   /* UNION  */
    UNIQUE = 708,                  /* UNIQUE  */
    UNKNOWN = 709,                 /* UNKNOWN  */
    UNLISTEN = 710,                /* UNLISTEN  */
    UNLOGGED = 711,                /* UNLOGGED  */
    UNTIL = 712,                   /* UNTIL  */
    UPDATE = 713,                  /* UPDATE  */
    USE_P = 714,                   /* USE_P  */
    USER = 715,                    /* USER  */
    USING = 716,                   /* USING  */
    VACUUM = 717,                  /* VACUUM  */
    VALID = 718,                   /* VALID  */
    VALIDATE = 719,                /* VALIDATE  */
    VALIDATOR = 720,               /* VALIDATOR  */
    VALUE_P = 721,                 /* VALUE_P  */
    VALUES = 722,                  /* VALUES  */
    VARCHAR = 723,                 /* VARCHAR  */
    VARIADIC = 724,                /* VARIADIC  */
    VARYING = 725,                 /* VARYING  */
    VERBOSE = 726,                 /* VERBOSE  */
    VERSION_P = 727,               /* VERSION_P  */
    VERTEX = 728,                  /* VERTEX  */
    VIEW = 729,                    /* VIEW  */
    VIEWS = 730,                   /* VIEWS  */
    VIRTUAL = 731,                 /* VIRTUAL  */
    VOLATILE = 732,                /* VOLATILE  */
    WALK = 733,                    /* WALK  */
    WHEN = 734,                    /* WHEN  */
    WHERE = 735,                   /* WHERE  */
    WHITESPACE_P = 736,            /* WHITESPACE_P  */
    WINDOW = 737,                  /* WINDOW  */
    WITH = 738,                    /* WITH  */
    WITHIN = 739,                  /* WITHIN  */
    WITHOUT = 740,                 /* WITHOUT  */
    WORK = 741,                    /* WORK  */
    WRAPPER = 742,                 /* WRAPPER  */
    WRITE_P = 743,                 /* WRITE_P  */
    XML_P = 744,                   /* XML_P  */
    XMLATTRIBUTES = 745,           /* XMLATTRIBUTES  */
    XMLCONCAT = 746,               /* XMLCONCAT  */
    XMLELEMENT = 747,              /* XMLELEMENT  */
    XMLEXISTS = 748,               /* XMLEXISTS  */
    XMLFOREST = 749,               /* XMLFOREST  */
    XMLNAMESPACES = 750,           /* XMLNAMESPACES  */
    XMLPARSE = 751,                /* XMLPARSE  */
    XMLPI = 752,                   /* XMLPI  */
    XMLROOT = 753,                 /* XMLROOT  */
    XMLSERIALIZE = 754,            /* XMLSERIALIZE  */
    XMLTABLE = 755,                /* XMLTABLE  */
    YEAR_P = 756,                  /* YEAR_P  */
    YEARS_P = 757,                 /* YEARS_P  */
    YES_P = 758,                   /* YES_P  */
    ZONE = 759,                    /* ZONE  */
    NOT_LA = 760,                  /* NOT_LA  */
    NULLS_LA = 761,                /* NULLS_LA  */
    WITH_LA = 762,                 /* WITH_LA  */
    POSTFIXOP = 763,               /* POSTFIXOP  */
    UMINUS = 764                   /* UMINUS  */
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
