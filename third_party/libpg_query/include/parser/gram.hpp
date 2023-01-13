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
    ADD_P = 280,                   /* ADD_P  */
    ADMIN = 281,                   /* ADMIN  */
    AFTER = 282,                   /* AFTER  */
    AGGREGATE = 283,               /* AGGREGATE  */
    ALL = 284,                     /* ALL  */
    ALSO = 285,                    /* ALSO  */
    ALTER = 286,                   /* ALTER  */
    ALWAYS = 287,                  /* ALWAYS  */
    ANALYSE = 288,                 /* ANALYSE  */
    ANALYZE = 289,                 /* ANALYZE  */
    AND = 290,                     /* AND  */
    ANY = 291,                     /* ANY  */
    ARRAY = 292,                   /* ARRAY  */
    AS = 293,                      /* AS  */
    ASC_P = 294,                   /* ASC_P  */
    ASSERTION = 295,               /* ASSERTION  */
    ASSIGNMENT = 296,              /* ASSIGNMENT  */
    ASYMMETRIC = 297,              /* ASYMMETRIC  */
    AT = 298,                      /* AT  */
    ATTACH = 299,                  /* ATTACH  */
    ATTRIBUTE = 300,               /* ATTRIBUTE  */
    AUTHORIZATION = 301,           /* AUTHORIZATION  */
    BACKWARD = 302,                /* BACKWARD  */
    BEFORE = 303,                  /* BEFORE  */
    BEGIN_P = 304,                 /* BEGIN_P  */
    BETWEEN = 305,                 /* BETWEEN  */
    BIGINT = 306,                  /* BIGINT  */
    BINARY = 307,                  /* BINARY  */
    BIT = 308,                     /* BIT  */
    BOOLEAN_P = 309,               /* BOOLEAN_P  */
    BOTH = 310,                    /* BOTH  */
    BY = 311,                      /* BY  */
    CACHE = 312,                   /* CACHE  */
    CALL_P = 313,                  /* CALL_P  */
    CALLED = 314,                  /* CALLED  */
    CASCADE = 315,                 /* CASCADE  */
    CASCADED = 316,                /* CASCADED  */
    CASE = 317,                    /* CASE  */
    CAST = 318,                    /* CAST  */
    CATALOG_P = 319,               /* CATALOG_P  */
    CHAIN = 320,                   /* CHAIN  */
    CHAR_P = 321,                  /* CHAR_P  */
    CHARACTER = 322,               /* CHARACTER  */
    CHARACTERISTICS = 323,         /* CHARACTERISTICS  */
    CHECK_P = 324,                 /* CHECK_P  */
    CHECKPOINT = 325,              /* CHECKPOINT  */
    CLASS = 326,                   /* CLASS  */
    CLOSE = 327,                   /* CLOSE  */
    CLUSTER = 328,                 /* CLUSTER  */
    COALESCE = 329,                /* COALESCE  */
    COLLATE = 330,                 /* COLLATE  */
    COLLATION = 331,               /* COLLATION  */
    COLUMN = 332,                  /* COLUMN  */
    COLUMNS = 333,                 /* COLUMNS  */
    COMMENT = 334,                 /* COMMENT  */
    COMMENTS = 335,                /* COMMENTS  */
    COMMIT = 336,                  /* COMMIT  */
    COMMITTED = 337,               /* COMMITTED  */
    COMPRESSION = 338,             /* COMPRESSION  */
    CONCURRENTLY = 339,            /* CONCURRENTLY  */
    CONFIGURATION = 340,           /* CONFIGURATION  */
    CONFLICT = 341,                /* CONFLICT  */
    CONNECTION = 342,              /* CONNECTION  */
    CONSTRAINT = 343,              /* CONSTRAINT  */
    CONSTRAINTS = 344,             /* CONSTRAINTS  */
    CONTENT_P = 345,               /* CONTENT_P  */
    CONTINUE_P = 346,              /* CONTINUE_P  */
    CONVERSION_P = 347,            /* CONVERSION_P  */
    COPY = 348,                    /* COPY  */
    COST = 349,                    /* COST  */
    CREATE_P = 350,                /* CREATE_P  */
    CROSS = 351,                   /* CROSS  */
    CSV = 352,                     /* CSV  */
    CUBE = 353,                    /* CUBE  */
    CURRENT_P = 354,               /* CURRENT_P  */
    CURRENT_CATALOG = 355,         /* CURRENT_CATALOG  */
    CURRENT_DATE = 356,            /* CURRENT_DATE  */
    CURRENT_ROLE = 357,            /* CURRENT_ROLE  */
    CURRENT_SCHEMA = 358,          /* CURRENT_SCHEMA  */
    CURRENT_TIME = 359,            /* CURRENT_TIME  */
    CURRENT_TIMESTAMP = 360,       /* CURRENT_TIMESTAMP  */
    CURRENT_USER = 361,            /* CURRENT_USER  */
    CURSOR = 362,                  /* CURSOR  */
    CYCLE = 363,                   /* CYCLE  */
    DATA_P = 364,                  /* DATA_P  */
    DATABASE = 365,                /* DATABASE  */
    DAY_P = 366,                   /* DAY_P  */
    DAYS_P = 367,                  /* DAYS_P  */
    DEALLOCATE = 368,              /* DEALLOCATE  */
    DEC = 369,                     /* DEC  */
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
    HANDLER = 438,                 /* HANDLER  */
    HAVING = 439,                  /* HAVING  */
    HEADER_P = 440,                /* HEADER_P  */
    HOLD = 441,                    /* HOLD  */
    HOUR_P = 442,                  /* HOUR_P  */
    HOURS_P = 443,                 /* HOURS_P  */
    IDENTITY_P = 444,              /* IDENTITY_P  */
    IF_P = 445,                    /* IF_P  */
    IGNORE_P = 446,                /* IGNORE_P  */
    ILIKE = 447,                   /* ILIKE  */
    IMMEDIATE = 448,               /* IMMEDIATE  */
    IMMUTABLE = 449,               /* IMMUTABLE  */
    IMPLICIT_P = 450,              /* IMPLICIT_P  */
    IMPORT_P = 451,                /* IMPORT_P  */
    IN_P = 452,                    /* IN_P  */
    INCLUDING = 453,               /* INCLUDING  */
    INCREMENT = 454,               /* INCREMENT  */
    INDEX = 455,                   /* INDEX  */
    INDEXES = 456,                 /* INDEXES  */
    INHERIT = 457,                 /* INHERIT  */
    INHERITS = 458,                /* INHERITS  */
    INITIALLY = 459,               /* INITIALLY  */
    INLINE_P = 460,                /* INLINE_P  */
    INNER_P = 461,                 /* INNER_P  */
    INOUT = 462,                   /* INOUT  */
    INPUT_P = 463,                 /* INPUT_P  */
    INSENSITIVE = 464,             /* INSENSITIVE  */
    INSERT = 465,                  /* INSERT  */
    INSTALL = 466,                 /* INSTALL  */
    INSTEAD = 467,                 /* INSTEAD  */
    INT_P = 468,                   /* INT_P  */
    INTEGER = 469,                 /* INTEGER  */
    INTERSECT = 470,               /* INTERSECT  */
    INTERVAL = 471,                /* INTERVAL  */
    INTO = 472,                    /* INTO  */
    INVOKER = 473,                 /* INVOKER  */
    IS = 474,                      /* IS  */
    ISNULL = 475,                  /* ISNULL  */
    ISOLATION = 476,               /* ISOLATION  */
    JOIN = 477,                    /* JOIN  */
    JSON = 478,                    /* JSON  */
    KEY = 479,                     /* KEY  */
    LABEL = 480,                   /* LABEL  */
    LANGUAGE = 481,                /* LANGUAGE  */
    LARGE_P = 482,                 /* LARGE_P  */
    LAST_P = 483,                  /* LAST_P  */
    LATERAL_P = 484,               /* LATERAL_P  */
    LEADING = 485,                 /* LEADING  */
    LEAKPROOF = 486,               /* LEAKPROOF  */
    LEFT = 487,                    /* LEFT  */
    LEVEL = 488,                   /* LEVEL  */
    LIKE = 489,                    /* LIKE  */
    LIMIT = 490,                   /* LIMIT  */
    LISTEN = 491,                  /* LISTEN  */
    LOAD = 492,                    /* LOAD  */
    LOCAL = 493,                   /* LOCAL  */
    LOCALTIME = 494,               /* LOCALTIME  */
    LOCALTIMESTAMP = 495,          /* LOCALTIMESTAMP  */
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
    MILLISECOND_P = 509,           /* MILLISECOND_P  */
    MILLISECONDS_P = 510,          /* MILLISECONDS_P  */
    MINUTE_P = 511,                /* MINUTE_P  */
    MINUTES_P = 512,               /* MINUTES_P  */
    MINVALUE = 513,                /* MINVALUE  */
    MODE = 514,                    /* MODE  */
    MONTH_P = 515,                 /* MONTH_P  */
    MONTHS_P = 516,                /* MONTHS_P  */
    MOVE = 517,                    /* MOVE  */
    NAME_P = 518,                  /* NAME_P  */
    NAMES = 519,                   /* NAMES  */
    NATIONAL = 520,                /* NATIONAL  */
    NATURAL = 521,                 /* NATURAL  */
    NCHAR = 522,                   /* NCHAR  */
    NEW = 523,                     /* NEW  */
    NEXT = 524,                    /* NEXT  */
    NO = 525,                      /* NO  */
    NONE = 526,                    /* NONE  */
    NOT = 527,                     /* NOT  */
    NOTHING = 528,                 /* NOTHING  */
    NOTIFY = 529,                  /* NOTIFY  */
    NOTNULL = 530,                 /* NOTNULL  */
    NOWAIT = 531,                  /* NOWAIT  */
    NULL_P = 532,                  /* NULL_P  */
    NULLIF = 533,                  /* NULLIF  */
    NULLS_P = 534,                 /* NULLS_P  */
    NUMERIC = 535,                 /* NUMERIC  */
    OBJECT_P = 536,                /* OBJECT_P  */
    OF = 537,                      /* OF  */
    OFF = 538,                     /* OFF  */
    OFFSET = 539,                  /* OFFSET  */
    OIDS = 540,                    /* OIDS  */
    OLD = 541,                     /* OLD  */
    ON = 542,                      /* ON  */
    ONLY = 543,                    /* ONLY  */
    OPERATOR = 544,                /* OPERATOR  */
    OPTION = 545,                  /* OPTION  */
    OPTIONS = 546,                 /* OPTIONS  */
    OR = 547,                      /* OR  */
    ORDER = 548,                   /* ORDER  */
    ORDINALITY = 549,              /* ORDINALITY  */
    OUT_P = 550,                   /* OUT_P  */
    OUTER_P = 551,                 /* OUTER_P  */
    OVER = 552,                    /* OVER  */
    OVERLAPS = 553,                /* OVERLAPS  */
    OVERLAY = 554,                 /* OVERLAY  */
    OVERRIDING = 555,              /* OVERRIDING  */
    OWNED = 556,                   /* OWNED  */
    OWNER = 557,                   /* OWNER  */
    PARALLEL = 558,                /* PARALLEL  */
    PARSER = 559,                  /* PARSER  */
    PARTIAL = 560,                 /* PARTIAL  */
    PARTITION = 561,               /* PARTITION  */
    PASSING = 562,                 /* PASSING  */
    PASSWORD = 563,                /* PASSWORD  */
    PERCENT = 564,                 /* PERCENT  */
    PLACING = 565,                 /* PLACING  */
    PLANS = 566,                   /* PLANS  */
    POLICY = 567,                  /* POLICY  */
    POSITION = 568,                /* POSITION  */
    PRAGMA_P = 569,                /* PRAGMA_P  */
    PRECEDING = 570,               /* PRECEDING  */
    PRECISION = 571,               /* PRECISION  */
    PREPARE = 572,                 /* PREPARE  */
    PREPARED = 573,                /* PREPARED  */
    PRESERVE = 574,                /* PRESERVE  */
    PRIMARY = 575,                 /* PRIMARY  */
    PRIOR = 576,                   /* PRIOR  */
    PRIVILEGES = 577,              /* PRIVILEGES  */
    PROCEDURAL = 578,              /* PROCEDURAL  */
    PROCEDURE = 579,               /* PROCEDURE  */
    PROGRAM = 580,                 /* PROGRAM  */
    PUBLICATION = 581,             /* PUBLICATION  */
    QUALIFY = 582,                 /* QUALIFY  */
    QUOTE = 583,                   /* QUOTE  */
    RANGE = 584,                   /* RANGE  */
    READ_P = 585,                  /* READ_P  */
    REAL = 586,                    /* REAL  */
    REASSIGN = 587,                /* REASSIGN  */
    RECHECK = 588,                 /* RECHECK  */
    RECURSIVE = 589,               /* RECURSIVE  */
    REF = 590,                     /* REF  */
    REFERENCES = 591,              /* REFERENCES  */
    REFERENCING = 592,             /* REFERENCING  */
    REFRESH = 593,                 /* REFRESH  */
    REINDEX = 594,                 /* REINDEX  */
    RELATIVE_P = 595,              /* RELATIVE_P  */
    RELEASE = 596,                 /* RELEASE  */
    RENAME = 597,                  /* RENAME  */
    REPEATABLE = 598,              /* REPEATABLE  */
    REPLACE = 599,                 /* REPLACE  */
    REPLICA = 600,                 /* REPLICA  */
    RESET = 601,                   /* RESET  */
    RESPECT_P = 602,               /* RESPECT_P  */
    RESTART = 603,                 /* RESTART  */
    RESTRICT = 604,                /* RESTRICT  */
    RETURNING = 605,               /* RETURNING  */
    RETURNS = 606,                 /* RETURNS  */
    REVOKE = 607,                  /* REVOKE  */
    RIGHT = 608,                   /* RIGHT  */
    ROLE = 609,                    /* ROLE  */
    ROLLBACK = 610,                /* ROLLBACK  */
    ROLLUP = 611,                  /* ROLLUP  */
    ROW = 612,                     /* ROW  */
    ROWS = 613,                    /* ROWS  */
    RULE = 614,                    /* RULE  */
    SAMPLE = 615,                  /* SAMPLE  */
    SAVEPOINT = 616,               /* SAVEPOINT  */
    SCHEMA = 617,                  /* SCHEMA  */
    SCHEMAS = 618,                 /* SCHEMAS  */
    SCROLL = 619,                  /* SCROLL  */
    SEARCH = 620,                  /* SEARCH  */
    SECOND_P = 621,                /* SECOND_P  */
    SECONDS_P = 622,               /* SECONDS_P  */
    SECURITY = 623,                /* SECURITY  */
    SELECT = 624,                  /* SELECT  */
    SEQUENCE = 625,                /* SEQUENCE  */
    SEQUENCES = 626,               /* SEQUENCES  */
    SERIALIZABLE = 627,            /* SERIALIZABLE  */
    SERVER = 628,                  /* SERVER  */
    SESSION = 629,                 /* SESSION  */
    SESSION_USER = 630,            /* SESSION_USER  */
    SET = 631,                     /* SET  */
    SETOF = 632,                   /* SETOF  */
    SETS = 633,                    /* SETS  */
    SHARE = 634,                   /* SHARE  */
    SHOW = 635,                    /* SHOW  */
    SIMILAR = 636,                 /* SIMILAR  */
    SIMPLE = 637,                  /* SIMPLE  */
    SKIP = 638,                    /* SKIP  */
    SMALLINT = 639,                /* SMALLINT  */
    SNAPSHOT = 640,                /* SNAPSHOT  */
    SOME = 641,                    /* SOME  */
    SQL_P = 642,                   /* SQL_P  */
    STABLE = 643,                  /* STABLE  */
    STANDALONE_P = 644,            /* STANDALONE_P  */
    START = 645,                   /* START  */
    STATEMENT = 646,               /* STATEMENT  */
    STATISTICS = 647,              /* STATISTICS  */
    STDIN = 648,                   /* STDIN  */
    STDOUT = 649,                  /* STDOUT  */
    STORAGE = 650,                 /* STORAGE  */
    STORED = 651,                  /* STORED  */
    STRICT_P = 652,                /* STRICT_P  */
    STRIP_P = 653,                 /* STRIP_P  */
    STRUCT = 654,                  /* STRUCT  */
    SUBSCRIPTION = 655,            /* SUBSCRIPTION  */
    SUBSTRING = 656,               /* SUBSTRING  */
    SUMMARIZE = 657,               /* SUMMARIZE  */
    SYMMETRIC = 658,               /* SYMMETRIC  */
    SYSID = 659,                   /* SYSID  */
    SYSTEM_P = 660,                /* SYSTEM_P  */
    TABLE = 661,                   /* TABLE  */
    TABLES = 662,                  /* TABLES  */
    TABLESAMPLE = 663,             /* TABLESAMPLE  */
    TABLESPACE = 664,              /* TABLESPACE  */
    TEMP = 665,                    /* TEMP  */
    TEMPLATE = 666,                /* TEMPLATE  */
    TEMPORARY = 667,               /* TEMPORARY  */
    TEXT_P = 668,                  /* TEXT_P  */
    THEN = 669,                    /* THEN  */
    TIME = 670,                    /* TIME  */
    TIMESTAMP = 671,               /* TIMESTAMP  */
    TO = 672,                      /* TO  */
    TRAILING = 673,                /* TRAILING  */
    TRANSACTION = 674,             /* TRANSACTION  */
    TRANSFORM = 675,               /* TRANSFORM  */
    TREAT = 676,                   /* TREAT  */
    TRIGGER = 677,                 /* TRIGGER  */
    TRIM = 678,                    /* TRIM  */
    TRUE_P = 679,                  /* TRUE_P  */
    TRUNCATE = 680,                /* TRUNCATE  */
    TRUSTED = 681,                 /* TRUSTED  */
    TRY_CAST = 682,                /* TRY_CAST  */
    TYPE_P = 683,                  /* TYPE_P  */
    TYPES_P = 684,                 /* TYPES_P  */
    UNBOUNDED = 685,               /* UNBOUNDED  */
    UNCOMMITTED = 686,             /* UNCOMMITTED  */
    UNENCRYPTED = 687,             /* UNENCRYPTED  */
    UNION = 688,                   /* UNION  */
    UNIQUE = 689,                  /* UNIQUE  */
    UNKNOWN = 690,                 /* UNKNOWN  */
    UNLISTEN = 691,                /* UNLISTEN  */
    UNLOGGED = 692,                /* UNLOGGED  */
    UNTIL = 693,                   /* UNTIL  */
    UPDATE = 694,                  /* UPDATE  */
    USE_P = 695,                   /* USE_P  */
    USER = 696,                    /* USER  */
    USING = 697,                   /* USING  */
    VACUUM = 698,                  /* VACUUM  */
    VALID = 699,                   /* VALID  */
    VALIDATE = 700,                /* VALIDATE  */
    VALIDATOR = 701,               /* VALIDATOR  */
    VALUE_P = 702,                 /* VALUE_P  */
    VALUES = 703,                  /* VALUES  */
    VARCHAR = 704,                 /* VARCHAR  */
    VARIADIC = 705,                /* VARIADIC  */
    VARYING = 706,                 /* VARYING  */
    VERBOSE = 707,                 /* VERBOSE  */
    VERSION_P = 708,               /* VERSION_P  */
    VIEW = 709,                    /* VIEW  */
    VIEWS = 710,                   /* VIEWS  */
    VIRTUAL = 711,                 /* VIRTUAL  */
    VOLATILE = 712,                /* VOLATILE  */
    WHEN = 713,                    /* WHEN  */
    WHERE = 714,                   /* WHERE  */
    WHITESPACE_P = 715,            /* WHITESPACE_P  */
    WINDOW = 716,                  /* WINDOW  */
    WITH = 717,                    /* WITH  */
    WITHIN = 718,                  /* WITHIN  */
    WITHOUT = 719,                 /* WITHOUT  */
    WORK = 720,                    /* WORK  */
    WRAPPER = 721,                 /* WRAPPER  */
    WRITE_P = 722,                 /* WRITE_P  */
    XML_P = 723,                   /* XML_P  */
    XMLATTRIBUTES = 724,           /* XMLATTRIBUTES  */
    XMLCONCAT = 725,               /* XMLCONCAT  */
    XMLELEMENT = 726,              /* XMLELEMENT  */
    XMLEXISTS = 727,               /* XMLEXISTS  */
    XMLFOREST = 728,               /* XMLFOREST  */
    XMLNAMESPACES = 729,           /* XMLNAMESPACES  */
    XMLPARSE = 730,                /* XMLPARSE  */
    XMLPI = 731,                   /* XMLPI  */
    XMLROOT = 732,                 /* XMLROOT  */
    XMLSERIALIZE = 733,            /* XMLSERIALIZE  */
    XMLTABLE = 734,                /* XMLTABLE  */
    YEAR_P = 735,                  /* YEAR_P  */
    YEARS_P = 736,                 /* YEARS_P  */
    YES_P = 737,                   /* YES_P  */
    ZONE = 738,                    /* ZONE  */
    NOT_LA = 739,                  /* NOT_LA  */
    NULLS_LA = 740,                /* NULLS_LA  */
    WITH_LA = 741,                 /* WITH_LA  */
    POSTFIXOP = 742,               /* POSTFIXOP  */
    UMINUS = 743                   /* UMINUS  */
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

#line 597 "third_party/libpg_query/grammar/grammar_out.hpp"

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
