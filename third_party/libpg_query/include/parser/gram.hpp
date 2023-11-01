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
    SCOPE = 620,                   /* SCOPE  */
    SCROLL = 621,                  /* SCROLL  */
    SEARCH = 622,                  /* SEARCH  */
    SECOND_P = 623,                /* SECOND_P  */
    SECONDS_P = 624,               /* SECONDS_P  */
    SECRET = 625,                  /* SECRET  */
    SECURITY = 626,                /* SECURITY  */
    SELECT = 627,                  /* SELECT  */
    SEMI = 628,                    /* SEMI  */
    SEQUENCE = 629,                /* SEQUENCE  */
    SEQUENCES = 630,               /* SEQUENCES  */
    SERIALIZABLE = 631,            /* SERIALIZABLE  */
    SERVER = 632,                  /* SERVER  */
    SESSION = 633,                 /* SESSION  */
    SET = 634,                     /* SET  */
    SETOF = 635,                   /* SETOF  */
    SETS = 636,                    /* SETS  */
    SHARE = 637,                   /* SHARE  */
    SHOW = 638,                    /* SHOW  */
    SIMILAR = 639,                 /* SIMILAR  */
    SIMPLE = 640,                  /* SIMPLE  */
    SKIP = 641,                    /* SKIP  */
    SMALLINT = 642,                /* SMALLINT  */
    SNAPSHOT = 643,                /* SNAPSHOT  */
    SOME = 644,                    /* SOME  */
    SQL_P = 645,                   /* SQL_P  */
    STABLE = 646,                  /* STABLE  */
    STANDALONE_P = 647,            /* STANDALONE_P  */
    START = 648,                   /* START  */
    STATEMENT = 649,               /* STATEMENT  */
    STATISTICS = 650,              /* STATISTICS  */
    STDIN = 651,                   /* STDIN  */
    STDOUT = 652,                  /* STDOUT  */
    STORAGE = 653,                 /* STORAGE  */
    STORED = 654,                  /* STORED  */
    STRICT_P = 655,                /* STRICT_P  */
    STRIP_P = 656,                 /* STRIP_P  */
    STRUCT = 657,                  /* STRUCT  */
    SUBSCRIPTION = 658,            /* SUBSCRIPTION  */
    SUBSTRING = 659,               /* SUBSTRING  */
    SUMMARIZE = 660,               /* SUMMARIZE  */
    SYMMETRIC = 661,               /* SYMMETRIC  */
    SYSID = 662,                   /* SYSID  */
    SYSTEM_P = 663,                /* SYSTEM_P  */
    TABLE = 664,                   /* TABLE  */
    TABLES = 665,                  /* TABLES  */
    TABLESAMPLE = 666,             /* TABLESAMPLE  */
    TABLESPACE = 667,              /* TABLESPACE  */
    TEMP = 668,                    /* TEMP  */
    TEMPLATE = 669,                /* TEMPLATE  */
    TEMPORARY = 670,               /* TEMPORARY  */
    TEXT_P = 671,                  /* TEXT_P  */
    THEN = 672,                    /* THEN  */
    TIES = 673,                    /* TIES  */
    TIME = 674,                    /* TIME  */
    TIMESTAMP = 675,               /* TIMESTAMP  */
    TO = 676,                      /* TO  */
    TRAILING = 677,                /* TRAILING  */
    TRANSACTION = 678,             /* TRANSACTION  */
    TRANSFORM = 679,               /* TRANSFORM  */
    TREAT = 680,                   /* TREAT  */
    TRIGGER = 681,                 /* TRIGGER  */
    TRIM = 682,                    /* TRIM  */
    TRUE_P = 683,                  /* TRUE_P  */
    TRUNCATE = 684,                /* TRUNCATE  */
    TRUSTED = 685,                 /* TRUSTED  */
    TRY_CAST = 686,                /* TRY_CAST  */
    TYPE_P = 687,                  /* TYPE_P  */
    TYPES_P = 688,                 /* TYPES_P  */
    UNBOUNDED = 689,               /* UNBOUNDED  */
    UNCOMMITTED = 690,             /* UNCOMMITTED  */
    UNENCRYPTED = 691,             /* UNENCRYPTED  */
    UNION = 692,                   /* UNION  */
    UNIQUE = 693,                  /* UNIQUE  */
    UNKNOWN = 694,                 /* UNKNOWN  */
    UNLISTEN = 695,                /* UNLISTEN  */
    UNLOGGED = 696,                /* UNLOGGED  */
    UNPIVOT = 697,                 /* UNPIVOT  */
    UNTIL = 698,                   /* UNTIL  */
    UPDATE = 699,                  /* UPDATE  */
    USE_P = 700,                   /* USE_P  */
    USER = 701,                    /* USER  */
    USING = 702,                   /* USING  */
    VACUUM = 703,                  /* VACUUM  */
    VALID = 704,                   /* VALID  */
    VALIDATE = 705,                /* VALIDATE  */
    VALIDATOR = 706,               /* VALIDATOR  */
    VALUE_P = 707,                 /* VALUE_P  */
    VALUES = 708,                  /* VALUES  */
    VARCHAR = 709,                 /* VARCHAR  */
    VARIADIC = 710,                /* VARIADIC  */
    VARYING = 711,                 /* VARYING  */
    VERBOSE = 712,                 /* VERBOSE  */
    VERSION_P = 713,               /* VERSION_P  */
    VIEW = 714,                    /* VIEW  */
    VIEWS = 715,                   /* VIEWS  */
    VIRTUAL = 716,                 /* VIRTUAL  */
    VOLATILE = 717,                /* VOLATILE  */
    WHEN = 718,                    /* WHEN  */
    WHERE = 719,                   /* WHERE  */
    WHITESPACE_P = 720,            /* WHITESPACE_P  */
    WINDOW = 721,                  /* WINDOW  */
    WITH = 722,                    /* WITH  */
    WITHIN = 723,                  /* WITHIN  */
    WITHOUT = 724,                 /* WITHOUT  */
    WORK = 725,                    /* WORK  */
    WRAPPER = 726,                 /* WRAPPER  */
    WRITE_P = 727,                 /* WRITE_P  */
    XML_P = 728,                   /* XML_P  */
    XMLATTRIBUTES = 729,           /* XMLATTRIBUTES  */
    XMLCONCAT = 730,               /* XMLCONCAT  */
    XMLELEMENT = 731,              /* XMLELEMENT  */
    XMLEXISTS = 732,               /* XMLEXISTS  */
    XMLFOREST = 733,               /* XMLFOREST  */
    XMLNAMESPACES = 734,           /* XMLNAMESPACES  */
    XMLPARSE = 735,                /* XMLPARSE  */
    XMLPI = 736,                   /* XMLPI  */
    XMLROOT = 737,                 /* XMLROOT  */
    XMLSERIALIZE = 738,            /* XMLSERIALIZE  */
    XMLTABLE = 739,                /* XMLTABLE  */
    YEAR_P = 740,                  /* YEAR_P  */
    YEARS_P = 741,                 /* YEARS_P  */
    YES_P = 742,                   /* YES_P  */
    ZONE = 743,                    /* ZONE  */
    NOT_LA = 744,                  /* NOT_LA  */
    NULLS_LA = 745,                /* NULLS_LA  */
    WITH_LA = 746,                 /* WITH_LA  */
    POSTFIXOP = 747,               /* POSTFIXOP  */
    UMINUS = 748                   /* UMINUS  */
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

#line 605 "third_party/libpg_query/grammar/grammar_out.hpp"

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
