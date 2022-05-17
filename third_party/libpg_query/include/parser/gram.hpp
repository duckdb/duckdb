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
    LAMBDA_ARROW = 270,            /* LAMBDA_ARROW  */
    POWER_OF = 271,                /* POWER_OF  */
    LESS_EQUALS = 272,             /* LESS_EQUALS  */
    GREATER_EQUALS = 273,          /* GREATER_EQUALS  */
    NOT_EQUALS = 274,              /* NOT_EQUALS  */
    ABORT_P = 275,                 /* ABORT_P  */
    ABSOLUTE_P = 276,              /* ABSOLUTE_P  */
    ACCESS = 277,                  /* ACCESS  */
    ACTION = 278,                  /* ACTION  */
    ADD_P = 279,                   /* ADD_P  */
    ADMIN = 280,                   /* ADMIN  */
    AFTER = 281,                   /* AFTER  */
    AGGREGATE = 282,               /* AGGREGATE  */
    ALL = 283,                     /* ALL  */
    ALSO = 284,                    /* ALSO  */
    ALTER = 285,                   /* ALTER  */
    ALWAYS = 286,                  /* ALWAYS  */
    ANALYSE = 287,                 /* ANALYSE  */
    ANALYZE = 288,                 /* ANALYZE  */
    AND = 289,                     /* AND  */
    ANY = 290,                     /* ANY  */
    ARRAY = 291,                   /* ARRAY  */
    AS = 292,                      /* AS  */
    ASC_P = 293,                   /* ASC_P  */
    ASSERTION = 294,               /* ASSERTION  */
    ASSIGNMENT = 295,              /* ASSIGNMENT  */
    ASYMMETRIC = 296,              /* ASYMMETRIC  */
    AT = 297,                      /* AT  */
    ATTACH = 298,                  /* ATTACH  */
    ATTRIBUTE = 299,               /* ATTRIBUTE  */
    AUTHORIZATION = 300,           /* AUTHORIZATION  */
    BACKWARD = 301,                /* BACKWARD  */
    BEFORE = 302,                  /* BEFORE  */
    BEGIN_P = 303,                 /* BEGIN_P  */
    BETWEEN = 304,                 /* BETWEEN  */
    BIGINT = 305,                  /* BIGINT  */
    BINARY = 306,                  /* BINARY  */
    BIT = 307,                     /* BIT  */
    BOOLEAN_P = 308,               /* BOOLEAN_P  */
    BOTH = 309,                    /* BOTH  */
    BY = 310,                      /* BY  */
    CACHE = 311,                   /* CACHE  */
    CALL_P = 312,                  /* CALL_P  */
    CALLED = 313,                  /* CALLED  */
    CASCADE = 314,                 /* CASCADE  */
    CASCADED = 315,                /* CASCADED  */
    CASE = 316,                    /* CASE  */
    CAST = 317,                    /* CAST  */
    CATALOG_P = 318,               /* CATALOG_P  */
    CHAIN = 319,                   /* CHAIN  */
    CHAR_P = 320,                  /* CHAR_P  */
    CHARACTER = 321,               /* CHARACTER  */
    CHARACTERISTICS = 322,         /* CHARACTERISTICS  */
    CHECK_P = 323,                 /* CHECK_P  */
    CHECKPOINT = 324,              /* CHECKPOINT  */
    CLASS = 325,                   /* CLASS  */
    CLOSE = 326,                   /* CLOSE  */
    CLUSTER = 327,                 /* CLUSTER  */
    COALESCE = 328,                /* COALESCE  */
    COLLATE = 329,                 /* COLLATE  */
    COLLATION = 330,               /* COLLATION  */
    COLUMN = 331,                  /* COLUMN  */
    COLUMNS = 332,                 /* COLUMNS  */
    COMMENT = 333,                 /* COMMENT  */
    COMMENTS = 334,                /* COMMENTS  */
    COMMIT = 335,                  /* COMMIT  */
    COMMITTED = 336,               /* COMMITTED  */
    COMPRESSION = 337,             /* COMPRESSION  */
    CONCURRENTLY = 338,            /* CONCURRENTLY  */
    CONFIGURATION = 339,           /* CONFIGURATION  */
    CONFLICT = 340,                /* CONFLICT  */
    CONNECTION = 341,              /* CONNECTION  */
    CONSTRAINT = 342,              /* CONSTRAINT  */
    CONSTRAINTS = 343,             /* CONSTRAINTS  */
    CONTENT_P = 344,               /* CONTENT_P  */
    CONTINUE_P = 345,              /* CONTINUE_P  */
    CONVERSION_P = 346,            /* CONVERSION_P  */
    COPY = 347,                    /* COPY  */
    COST = 348,                    /* COST  */
    CREATE_P = 349,                /* CREATE_P  */
    CROSS = 350,                   /* CROSS  */
    CSV = 351,                     /* CSV  */
    CUBE = 352,                    /* CUBE  */
    CURRENT_P = 353,               /* CURRENT_P  */
    CURRENT_CATALOG = 354,         /* CURRENT_CATALOG  */
    CURRENT_DATE = 355,            /* CURRENT_DATE  */
    CURRENT_ROLE = 356,            /* CURRENT_ROLE  */
    CURRENT_SCHEMA = 357,          /* CURRENT_SCHEMA  */
    CURRENT_TIME = 358,            /* CURRENT_TIME  */
    CURRENT_TIMESTAMP = 359,       /* CURRENT_TIMESTAMP  */
    CURRENT_USER = 360,            /* CURRENT_USER  */
    CURSOR = 361,                  /* CURSOR  */
    CYCLE = 362,                   /* CYCLE  */
    DATA_P = 363,                  /* DATA_P  */
    DATABASE = 364,                /* DATABASE  */
    DAY_P = 365,                   /* DAY_P  */
    DAYS_P = 366,                  /* DAYS_P  */
    DEALLOCATE = 367,              /* DEALLOCATE  */
    DEC = 368,                     /* DEC  */
    DECIMAL_P = 369,               /* DECIMAL_P  */
    DECLARE = 370,                 /* DECLARE  */
    DEFAULT = 371,                 /* DEFAULT  */
    DEFAULTS = 372,                /* DEFAULTS  */
    DEFERRABLE = 373,              /* DEFERRABLE  */
    DEFERRED = 374,                /* DEFERRED  */
    DEFINER = 375,                 /* DEFINER  */
    DELETE_P = 376,                /* DELETE_P  */
    DELIMITER = 377,               /* DELIMITER  */
    DELIMITERS = 378,              /* DELIMITERS  */
    DEPENDS = 379,                 /* DEPENDS  */
    DESC_P = 380,                  /* DESC_P  */
    DESCRIBE = 381,                /* DESCRIBE  */
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
    ELSE = 393,                    /* ELSE  */
    ENABLE_P = 394,                /* ENABLE_P  */
    ENCODING = 395,                /* ENCODING  */
    ENCRYPTED = 396,               /* ENCRYPTED  */
    END_P = 397,                   /* END_P  */
    ENUM_P = 398,                  /* ENUM_P  */
    ESCAPE = 399,                  /* ESCAPE  */
    EVENT = 400,                   /* EVENT  */
    EXCEPT = 401,                  /* EXCEPT  */
    EXCLUDE = 402,                 /* EXCLUDE  */
    EXCLUDING = 403,               /* EXCLUDING  */
    EXCLUSIVE = 404,               /* EXCLUSIVE  */
    EXECUTE = 405,                 /* EXECUTE  */
    EXISTS = 406,                  /* EXISTS  */
    EXPLAIN = 407,                 /* EXPLAIN  */
    EXPORT_P = 408,                /* EXPORT_P  */
    EXPORT_STATE = 409,            /* EXPORT_STATE  */
    EXTENSION = 410,               /* EXTENSION  */
    EXTERNAL = 411,                /* EXTERNAL  */
    EXTRACT = 412,                 /* EXTRACT  */
    FALSE_P = 413,                 /* FALSE_P  */
    FAMILY = 414,                  /* FAMILY  */
    FETCH = 415,                   /* FETCH  */
    FILTER = 416,                  /* FILTER  */
    FIRST_P = 417,                 /* FIRST_P  */
    FLOAT_P = 418,                 /* FLOAT_P  */
    FOLLOWING = 419,               /* FOLLOWING  */
    FOR = 420,                     /* FOR  */
    FORCE = 421,                   /* FORCE  */
    FOREIGN = 422,                 /* FOREIGN  */
    FORWARD = 423,                 /* FORWARD  */
    FREEZE = 424,                  /* FREEZE  */
    FROM = 425,                    /* FROM  */
    FULL = 426,                    /* FULL  */
    FUNCTION = 427,                /* FUNCTION  */
    FUNCTIONS = 428,               /* FUNCTIONS  */
    GENERATED = 429,               /* GENERATED  */
    GLOB = 430,                    /* GLOB  */
    GLOBAL = 431,                  /* GLOBAL  */
    GRANT = 432,                   /* GRANT  */
    GRANTED = 433,                 /* GRANTED  */
    GROUP_P = 434,                 /* GROUP_P  */
    GROUPING = 435,                /* GROUPING  */
    GROUPING_ID = 436,             /* GROUPING_ID  */
    HANDLER = 437,                 /* HANDLER  */
    HAVING = 438,                  /* HAVING  */
    HEADER_P = 439,                /* HEADER_P  */
    HOLD = 440,                    /* HOLD  */
    HOUR_P = 441,                  /* HOUR_P  */
    HOURS_P = 442,                 /* HOURS_P  */
    IDENTITY_P = 443,              /* IDENTITY_P  */
    IF_P = 444,                    /* IF_P  */
    IGNORE_P = 445,                /* IGNORE_P  */
    ILIKE = 446,                   /* ILIKE  */
    IMMEDIATE = 447,               /* IMMEDIATE  */
    IMMUTABLE = 448,               /* IMMUTABLE  */
    IMPLICIT_P = 449,              /* IMPLICIT_P  */
    IMPORT_P = 450,                /* IMPORT_P  */
    IN_P = 451,                    /* IN_P  */
    INCLUDING = 452,               /* INCLUDING  */
    INCREMENT = 453,               /* INCREMENT  */
    INDEX = 454,                   /* INDEX  */
    INDEXES = 455,                 /* INDEXES  */
    INHERIT = 456,                 /* INHERIT  */
    INHERITS = 457,                /* INHERITS  */
    INITIALLY = 458,               /* INITIALLY  */
    INLINE_P = 459,                /* INLINE_P  */
    INNER_P = 460,                 /* INNER_P  */
    INOUT = 461,                   /* INOUT  */
    INPUT_P = 462,                 /* INPUT_P  */
    INSENSITIVE = 463,             /* INSENSITIVE  */
    INSERT = 464,                  /* INSERT  */
    INSTALL = 465,                 /* INSTALL  */
    INSTEAD = 466,                 /* INSTEAD  */
    INT_P = 467,                   /* INT_P  */
    INTEGER = 468,                 /* INTEGER  */
    INTERSECT = 469,               /* INTERSECT  */
    INTERVAL = 470,                /* INTERVAL  */
    INTO = 471,                    /* INTO  */
    INVOKER = 472,                 /* INVOKER  */
    IS = 473,                      /* IS  */
    ISNULL = 474,                  /* ISNULL  */
    ISOLATION = 475,               /* ISOLATION  */
    JOIN = 476,                    /* JOIN  */
    JSON = 477,                    /* JSON  */
    KEY = 478,                     /* KEY  */
    LABEL = 479,                   /* LABEL  */
    LANGUAGE = 480,                /* LANGUAGE  */
    LARGE_P = 481,                 /* LARGE_P  */
    LAST_P = 482,                  /* LAST_P  */
    LATERAL_P = 483,               /* LATERAL_P  */
    LEADING = 484,                 /* LEADING  */
    LEAKPROOF = 485,               /* LEAKPROOF  */
    LEFT = 486,                    /* LEFT  */
    LEVEL = 487,                   /* LEVEL  */
    LIKE = 488,                    /* LIKE  */
    LIMIT = 489,                   /* LIMIT  */
    LISTEN = 490,                  /* LISTEN  */
    LOAD = 491,                    /* LOAD  */
    LOCAL = 492,                   /* LOCAL  */
    LOCALTIME = 493,               /* LOCALTIME  */
    LOCALTIMESTAMP = 494,          /* LOCALTIMESTAMP  */
    LOCATION = 495,                /* LOCATION  */
    LOCK_P = 496,                  /* LOCK_P  */
    LOCKED = 497,                  /* LOCKED  */
    LOGGED = 498,                  /* LOGGED  */
    MACRO = 499,                   /* MACRO  */
    MAP = 500,                     /* MAP  */
    MAPPING = 501,                 /* MAPPING  */
    MATCH = 502,                   /* MATCH  */
    MATERIALIZED = 503,            /* MATERIALIZED  */
    MAXVALUE = 504,                /* MAXVALUE  */
    METHOD = 505,                  /* METHOD  */
    MICROSECOND_P = 506,           /* MICROSECOND_P  */
    MICROSECONDS_P = 507,          /* MICROSECONDS_P  */
    MILLISECOND_P = 508,           /* MILLISECOND_P  */
    MILLISECONDS_P = 509,          /* MILLISECONDS_P  */
    MINUTE_P = 510,                /* MINUTE_P  */
    MINUTES_P = 511,               /* MINUTES_P  */
    MINVALUE = 512,                /* MINVALUE  */
    MODE = 513,                    /* MODE  */
    MONTH_P = 514,                 /* MONTH_P  */
    MONTHS_P = 515,                /* MONTHS_P  */
    MOVE = 516,                    /* MOVE  */
    NAME_P = 517,                  /* NAME_P  */
    NAMES = 518,                   /* NAMES  */
    NATIONAL = 519,                /* NATIONAL  */
    NATURAL = 520,                 /* NATURAL  */
    NCHAR = 521,                   /* NCHAR  */
    NEW = 522,                     /* NEW  */
    NEXT = 523,                    /* NEXT  */
    NO = 524,                      /* NO  */
    NONE = 525,                    /* NONE  */
    NOT = 526,                     /* NOT  */
    NOTHING = 527,                 /* NOTHING  */
    NOTIFY = 528,                  /* NOTIFY  */
    NOTNULL = 529,                 /* NOTNULL  */
    NOWAIT = 530,                  /* NOWAIT  */
    NULL_P = 531,                  /* NULL_P  */
    NULLIF = 532,                  /* NULLIF  */
    NULLS_P = 533,                 /* NULLS_P  */
    NUMERIC = 534,                 /* NUMERIC  */
    OBJECT_P = 535,                /* OBJECT_P  */
    OF = 536,                      /* OF  */
    OFF = 537,                     /* OFF  */
    OFFSET = 538,                  /* OFFSET  */
    OIDS = 539,                    /* OIDS  */
    OLD = 540,                     /* OLD  */
    ON = 541,                      /* ON  */
    ONLY = 542,                    /* ONLY  */
    OPERATOR = 543,                /* OPERATOR  */
    OPTION = 544,                  /* OPTION  */
    OPTIONS = 545,                 /* OPTIONS  */
    OR = 546,                      /* OR  */
    ORDER = 547,                   /* ORDER  */
    ORDINALITY = 548,              /* ORDINALITY  */
    OUT_P = 549,                   /* OUT_P  */
    OUTER_P = 550,                 /* OUTER_P  */
    OVER = 551,                    /* OVER  */
    OVERLAPS = 552,                /* OVERLAPS  */
    OVERLAY = 553,                 /* OVERLAY  */
    OVERRIDING = 554,              /* OVERRIDING  */
    OWNED = 555,                   /* OWNED  */
    OWNER = 556,                   /* OWNER  */
    PARALLEL = 557,                /* PARALLEL  */
    PARSER = 558,                  /* PARSER  */
    PARTIAL = 559,                 /* PARTIAL  */
    PARTITION = 560,               /* PARTITION  */
    PASSING = 561,                 /* PASSING  */
    PASSWORD = 562,                /* PASSWORD  */
    PERCENT = 563,                 /* PERCENT  */
    PLACING = 564,                 /* PLACING  */
    PLANS = 565,                   /* PLANS  */
    POLICY = 566,                  /* POLICY  */
    POSITION = 567,                /* POSITION  */
    PRAGMA_P = 568,                /* PRAGMA_P  */
    PRECEDING = 569,               /* PRECEDING  */
    PRECISION = 570,               /* PRECISION  */
    PREPARE = 571,                 /* PREPARE  */
    PREPARED = 572,                /* PREPARED  */
    PRESERVE = 573,                /* PRESERVE  */
    PRIMARY = 574,                 /* PRIMARY  */
    PRIOR = 575,                   /* PRIOR  */
    PRIVILEGES = 576,              /* PRIVILEGES  */
    PROCEDURAL = 577,              /* PROCEDURAL  */
    PROCEDURE = 578,               /* PROCEDURE  */
    PROGRAM = 579,                 /* PROGRAM  */
    PUBLICATION = 580,             /* PUBLICATION  */
    QUALIFY = 581,                 /* QUALIFY  */
    QUOTE = 582,                   /* QUOTE  */
    RANGE = 583,                   /* RANGE  */
    READ_P = 584,                  /* READ_P  */
    REAL = 585,                    /* REAL  */
    REASSIGN = 586,                /* REASSIGN  */
    RECHECK = 587,                 /* RECHECK  */
    RECURSIVE = 588,               /* RECURSIVE  */
    REF = 589,                     /* REF  */
    REFERENCES = 590,              /* REFERENCES  */
    REFERENCING = 591,             /* REFERENCING  */
    REFRESH = 592,                 /* REFRESH  */
    REINDEX = 593,                 /* REINDEX  */
    RELATIVE_P = 594,              /* RELATIVE_P  */
    RELEASE = 595,                 /* RELEASE  */
    RENAME = 596,                  /* RENAME  */
    REPEATABLE = 597,              /* REPEATABLE  */
    REPLACE = 598,                 /* REPLACE  */
    REPLICA = 599,                 /* REPLICA  */
    RESET = 600,                   /* RESET  */
    RESPECT_P = 601,               /* RESPECT_P  */
    RESTART = 602,                 /* RESTART  */
    RESTRICT = 603,                /* RESTRICT  */
    RETURNING = 604,               /* RETURNING  */
    RETURNS = 605,                 /* RETURNS  */
    REVOKE = 606,                  /* REVOKE  */
    RIGHT = 607,                   /* RIGHT  */
    ROLE = 608,                    /* ROLE  */
    ROLLBACK = 609,                /* ROLLBACK  */
    ROLLUP = 610,                  /* ROLLUP  */
    ROW = 611,                     /* ROW  */
    ROWS = 612,                    /* ROWS  */
    RULE = 613,                    /* RULE  */
    SAMPLE = 614,                  /* SAMPLE  */
    SAVEPOINT = 615,               /* SAVEPOINT  */
    SCHEMA = 616,                  /* SCHEMA  */
    SCHEMAS = 617,                 /* SCHEMAS  */
    SCROLL = 618,                  /* SCROLL  */
    SEARCH = 619,                  /* SEARCH  */
    SECOND_P = 620,                /* SECOND_P  */
    SECONDS_P = 621,               /* SECONDS_P  */
    SECURITY = 622,                /* SECURITY  */
    SELECT = 623,                  /* SELECT  */
    SEQUENCE = 624,                /* SEQUENCE  */
    SEQUENCES = 625,               /* SEQUENCES  */
    SERIALIZABLE = 626,            /* SERIALIZABLE  */
    SERVER = 627,                  /* SERVER  */
    SESSION = 628,                 /* SESSION  */
    SESSION_USER = 629,            /* SESSION_USER  */
    SET = 630,                     /* SET  */
    SETOF = 631,                   /* SETOF  */
    SETS = 632,                    /* SETS  */
    SHARE = 633,                   /* SHARE  */
    SHOW = 634,                    /* SHOW  */
    SIMILAR = 635,                 /* SIMILAR  */
    SIMPLE = 636,                  /* SIMPLE  */
    SKIP = 637,                    /* SKIP  */
    SMALLINT = 638,                /* SMALLINT  */
    SNAPSHOT = 639,                /* SNAPSHOT  */
    SOME = 640,                    /* SOME  */
    SQL_P = 641,                   /* SQL_P  */
    STABLE = 642,                  /* STABLE  */
    STANDALONE_P = 643,            /* STANDALONE_P  */
    START = 644,                   /* START  */
    STATEMENT = 645,               /* STATEMENT  */
    STATISTICS = 646,              /* STATISTICS  */
    STDIN = 647,                   /* STDIN  */
    STDOUT = 648,                  /* STDOUT  */
    STORAGE = 649,                 /* STORAGE  */
    STORED = 650,                  /* STORED  */
    STRICT_P = 651,                /* STRICT_P  */
    STRIP_P = 652,                 /* STRIP_P  */
    STRUCT = 653,                  /* STRUCT  */
    SUBSCRIPTION = 654,            /* SUBSCRIPTION  */
    SUBSTRING = 655,               /* SUBSTRING  */
    SUMMARIZE = 656,               /* SUMMARIZE  */
    SYMMETRIC = 657,               /* SYMMETRIC  */
    SYSID = 658,                   /* SYSID  */
    SYSTEM_P = 659,                /* SYSTEM_P  */
    TABLE = 660,                   /* TABLE  */
    TABLES = 661,                  /* TABLES  */
    TABLESAMPLE = 662,             /* TABLESAMPLE  */
    TABLESPACE = 663,              /* TABLESPACE  */
    TEMP = 664,                    /* TEMP  */
    TEMPLATE = 665,                /* TEMPLATE  */
    TEMPORARY = 666,               /* TEMPORARY  */
    TEXT_P = 667,                  /* TEXT_P  */
    THEN = 668,                    /* THEN  */
    TIME = 669,                    /* TIME  */
    TIMESTAMP = 670,               /* TIMESTAMP  */
    TO = 671,                      /* TO  */
    TRAILING = 672,                /* TRAILING  */
    TRANSACTION = 673,             /* TRANSACTION  */
    TRANSFORM = 674,               /* TRANSFORM  */
    TREAT = 675,                   /* TREAT  */
    TRIGGER = 676,                 /* TRIGGER  */
    TRIM = 677,                    /* TRIM  */
    TRUE_P = 678,                  /* TRUE_P  */
    TRUNCATE = 679,                /* TRUNCATE  */
    TRUSTED = 680,                 /* TRUSTED  */
    TRY_CAST = 681,                /* TRY_CAST  */
    TYPE_P = 682,                  /* TYPE_P  */
    TYPES_P = 683,                 /* TYPES_P  */
    UNBOUNDED = 684,               /* UNBOUNDED  */
    UNCOMMITTED = 685,             /* UNCOMMITTED  */
    UNENCRYPTED = 686,             /* UNENCRYPTED  */
    UNION = 687,                   /* UNION  */
    UNIQUE = 688,                  /* UNIQUE  */
    UNKNOWN = 689,                 /* UNKNOWN  */
    UNLISTEN = 690,                /* UNLISTEN  */
    UNLOGGED = 691,                /* UNLOGGED  */
    UNTIL = 692,                   /* UNTIL  */
    UPDATE = 693,                  /* UPDATE  */
    USER = 694,                    /* USER  */
    USING = 695,                   /* USING  */
    VACUUM = 696,                  /* VACUUM  */
    VALID = 697,                   /* VALID  */
    VALIDATE = 698,                /* VALIDATE  */
    VALIDATOR = 699,               /* VALIDATOR  */
    VALUE_P = 700,                 /* VALUE_P  */
    VALUES = 701,                  /* VALUES  */
    VARCHAR = 702,                 /* VARCHAR  */
    VARIADIC = 703,                /* VARIADIC  */
    VARYING = 704,                 /* VARYING  */
    VERBOSE = 705,                 /* VERBOSE  */
    VERSION_P = 706,               /* VERSION_P  */
    VIEW = 707,                    /* VIEW  */
    VIEWS = 708,                   /* VIEWS  */
    VIRTUAL = 709,                 /* VIRTUAL  */
    VOLATILE = 710,                /* VOLATILE  */
    WHEN = 711,                    /* WHEN  */
    WHERE = 712,                   /* WHERE  */
    WHITESPACE_P = 713,            /* WHITESPACE_P  */
    WINDOW = 714,                  /* WINDOW  */
    WITH = 715,                    /* WITH  */
    WITHIN = 716,                  /* WITHIN  */
    WITHOUT = 717,                 /* WITHOUT  */
    WORK = 718,                    /* WORK  */
    WRAPPER = 719,                 /* WRAPPER  */
    WRITE_P = 720,                 /* WRITE_P  */
    XML_P = 721,                   /* XML_P  */
    XMLATTRIBUTES = 722,           /* XMLATTRIBUTES  */
    XMLCONCAT = 723,               /* XMLCONCAT  */
    XMLELEMENT = 724,              /* XMLELEMENT  */
    XMLEXISTS = 725,               /* XMLEXISTS  */
    XMLFOREST = 726,               /* XMLFOREST  */
    XMLNAMESPACES = 727,           /* XMLNAMESPACES  */
    XMLPARSE = 728,                /* XMLPARSE  */
    XMLPI = 729,                   /* XMLPI  */
    XMLROOT = 730,                 /* XMLROOT  */
    XMLSERIALIZE = 731,            /* XMLSERIALIZE  */
    XMLTABLE = 732,                /* XMLTABLE  */
    YEAR_P = 733,                  /* YEAR_P  */
    YEARS_P = 734,                 /* YEARS_P  */
    YES_P = 735,                   /* YES_P  */
    ZONE = 736,                    /* ZONE  */
    NOT_LA = 737,                  /* NOT_LA  */
    NULLS_LA = 738,                /* NULLS_LA  */
    WITH_LA = 739,                 /* WITH_LA  */
    POSTFIXOP = 740,               /* POSTFIXOP  */
    UMINUS = 741                   /* UMINUS  */
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
	PGLockClauseStrength lockstrength;
	PGLockWaitPolicy lockwaitpolicy;
	PGSubLinkType subquerytype;
	PGViewCheckOption viewcheckoption;

#line 594 "third_party/libpg_query/grammar/grammar_out.hpp"

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
