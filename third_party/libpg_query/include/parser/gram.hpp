/* A Bison parser, made by GNU Bison 2.3.  */

/* Skeleton interface for Bison's Yacc-like parsers in C

   Copyright (C) 1984, 1989, 1990, 2000, 2001, 2002, 2003, 2004, 2005, 2006
   Free Software Foundation, Inc.

   This program is free software; you can redistribute it and/or modify
   it under the terms of the GNU General Public License as published by
   the Free Software Foundation; either version 2, or (at your option)
   any later version.

   This program is distributed in the hope that it will be useful,
   but WITHOUT ANY WARRANTY; without even the implied warranty of
   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
   GNU General Public License for more details.

   You should have received a copy of the GNU General Public License
   along with this program; if not, write to the Free Software
   Foundation, Inc., 51 Franklin Street, Fifth Floor,
   Boston, MA 02110-1301, USA.  */

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

/* Tokens.  */
#ifndef YYTOKENTYPE
# define YYTOKENTYPE
   /* Put the tokens into the symbol table, so that GDB and other debuggers
      know about them.  */
   enum yytokentype {
     IDENT = 258,
     FCONST = 259,
     SCONST = 260,
     BCONST = 261,
     XCONST = 262,
     Op = 263,
     ICONST = 264,
     PARAM = 265,
     TYPECAST = 266,
     DOT_DOT = 267,
     COLON_EQUALS = 268,
     EQUALS_GREATER = 269,
     INTEGER_DIVISION = 270,
     POWER_OF = 271,
     SINGLE_ARROW = 272,
     DOUBLE_ARROW = 273,
     SINGLE_COLON = 274,
     LESS_EQUALS = 275,
     GREATER_EQUALS = 276,
     NOT_EQUALS = 277,
     ABORT_P = 278,
     ABSOLUTE_P = 279,
     ACCESS = 280,
     ACTION = 281,
     ADD_P = 282,
     ADMIN = 283,
     AFTER = 284,
     AGGREGATE = 285,
     ALL = 286,
     ALSO = 287,
     ALTER = 288,
     ALWAYS = 289,
     ANALYSE = 290,
     ANALYZE = 291,
     AND = 292,
     ANTI = 293,
     ANY = 294,
     ARRAY = 295,
     AS = 296,
     ASC_P = 297,
     ASOF = 298,
     ASSERTION = 299,
     ASSIGNMENT = 300,
     ASYMMETRIC = 301,
     AT = 302,
     ATTACH = 303,
     ATTRIBUTE = 304,
     AUTHORIZATION = 305,
     BACKWARD = 306,
     BEFORE = 307,
     BEGIN_P = 308,
     BETWEEN = 309,
     BIGINT = 310,
     BINARY = 311,
     BIT = 312,
     BOOLEAN_P = 313,
     BOTH = 314,
     BY = 315,
     CACHE = 316,
     CALL_P = 317,
     CALLED = 318,
     CASCADE = 319,
     CASCADED = 320,
     CASE = 321,
     CAST = 322,
     CATALOG_P = 323,
     CENTURIES_P = 324,
     CENTURY_P = 325,
     CHAIN = 326,
     CHAR_P = 327,
     CHARACTER = 328,
     CHARACTERISTICS = 329,
     CHECK_P = 330,
     CHECKPOINT = 331,
     CLASS = 332,
     CLOSE = 333,
     CLUSTER = 334,
     COALESCE = 335,
     COLLATE = 336,
     COLLATION = 337,
     COLUMN = 338,
     COLUMNS = 339,
     COMMENT = 340,
     COMMENTS = 341,
     COMMIT = 342,
     COMMITTED = 343,
     COMPRESSION = 344,
     CONCURRENTLY = 345,
     CONFIGURATION = 346,
     CONFLICT = 347,
     CONNECTION = 348,
     CONSTRAINT = 349,
     CONSTRAINTS = 350,
     CONTENT_P = 351,
     CONTINUE_P = 352,
     CONVERSION_P = 353,
     COPY = 354,
     COST = 355,
     CREATE_P = 356,
     CROSS = 357,
     CSV = 358,
     CUBE = 359,
     CURRENT_P = 360,
     CURSOR = 361,
     CYCLE = 362,
     DATA_P = 363,
     DATABASE = 364,
     DAY_P = 365,
     DAYS_P = 366,
     DEALLOCATE = 367,
     DEC = 368,
     DECADE_P = 369,
     DECADES_P = 370,
     DECIMAL_P = 371,
     DECLARE = 372,
     DEFAULT = 373,
     DEFAULTS = 374,
     DEFERRABLE = 375,
     DEFERRED = 376,
     DEFINER = 377,
     DELETE_P = 378,
     DELIMITER = 379,
     DELIMITERS = 380,
     DEPENDS = 381,
     DESC_P = 382,
     DESCRIBE = 383,
     DETACH = 384,
     DICTIONARY = 385,
     DISABLE_P = 386,
     DISCARD = 387,
     DISTINCT = 388,
     DO = 389,
     DOCUMENT_P = 390,
     DOMAIN_P = 391,
     DOUBLE_P = 392,
     DROP = 393,
     EACH = 394,
     ELSE = 395,
     ENABLE_P = 396,
     ENCODING = 397,
     ENCRYPTED = 398,
     END_P = 399,
     ENUM_P = 400,
     ERROR_P = 401,
     ESCAPE = 402,
     EVENT = 403,
     EXCEPT = 404,
     EXCLUDE = 405,
     EXCLUDING = 406,
     EXCLUSIVE = 407,
     EXECUTE = 408,
     EXISTS = 409,
     EXPLAIN = 410,
     EXPORT_P = 411,
     EXPORT_STATE = 412,
     EXTENSION = 413,
     EXTENSIONS = 414,
     EXTERNAL = 415,
     EXTRACT = 416,
     FALSE_P = 417,
     FAMILY = 418,
     FETCH = 419,
     FILTER = 420,
     FIRST_P = 421,
     FLOAT_P = 422,
     FOLLOWING = 423,
     FOR = 424,
     FORCE = 425,
     FOREIGN = 426,
     FORWARD = 427,
     FREEZE = 428,
     FROM = 429,
     FULL = 430,
     FUNCTION = 431,
     FUNCTIONS = 432,
     GENERATED = 433,
     GLOB = 434,
     GLOBAL = 435,
     GRANT = 436,
     GRANTED = 437,
     GROUP_P = 438,
     GROUPING = 439,
     GROUPING_ID = 440,
     GROUPS = 441,
     HANDLER = 442,
     HAVING = 443,
     HEADER_P = 444,
     HOLD = 445,
     HOUR_P = 446,
     HOURS_P = 447,
     IDENTITY_P = 448,
     IF_P = 449,
     IGNORE_P = 450,
     ILIKE = 451,
     IMMEDIATE = 452,
     IMMUTABLE = 453,
     IMPLICIT_P = 454,
     IMPORT_P = 455,
     IN_P = 456,
     INCLUDE_P = 457,
     INCLUDING = 458,
     INCREMENT = 459,
     INDEX = 460,
     INDEXES = 461,
     INHERIT = 462,
     INHERITS = 463,
     INITIALLY = 464,
     INLINE_P = 465,
     INNER_P = 466,
     INOUT = 467,
     INPUT_P = 468,
     INSENSITIVE = 469,
     INSERT = 470,
     INSTALL = 471,
     INSTEAD = 472,
     INT_P = 473,
     INTEGER = 474,
     INTERSECT = 475,
     INTERVAL = 476,
     INTO = 477,
     INVOKER = 478,
     IS = 479,
     ISNULL = 480,
     ISOLATION = 481,
     JOIN = 482,
     JSON = 483,
     KEY = 484,
     LABEL = 485,
     LAMBDA = 486,
     LANGUAGE = 487,
     LARGE_P = 488,
     LAST_P = 489,
     LATERAL_P = 490,
     LEADING = 491,
     LEAKPROOF = 492,
     LEFT = 493,
     LEVEL = 494,
     LIKE = 495,
     LIMIT = 496,
     LISTEN = 497,
     LOAD = 498,
     LOCAL = 499,
     LOCATION = 500,
     LOCK_P = 501,
     LOCKED = 502,
     LOGGED = 503,
     MACRO = 504,
     MAP = 505,
     MAPPING = 506,
     MATCH = 507,
     MATCHED = 508,
     MATERIALIZED = 509,
     MAX = 510,
     MAXVALUE = 511,
     MERGE = 512,
     METHOD = 513,
     MICROSECOND_P = 514,
     MICROSECONDS_P = 515,
     MILLENNIA_P = 516,
     MILLENNIUM_P = 517,
     MILLISECOND_P = 518,
     MILLISECONDS_P = 519,
     MIN = 520,
     MINUTE_P = 521,
     MINUTES_P = 522,
     MINVALUE = 523,
     MODE = 524,
     MONTH_P = 525,
     MONTHS_P = 526,
     MOVE = 527,
     NAME_P = 528,
     NAMES = 529,
     NATIONAL = 530,
     NATURAL = 531,
     NCHAR = 532,
     NEW = 533,
     NEXT = 534,
     NO = 535,
     NONE = 536,
     NOT = 537,
     NOTHING = 538,
     NOTIFY = 539,
     NOTNULL = 540,
     NOWAIT = 541,
     NULL_P = 542,
     NULLIF = 543,
     NULLS_P = 544,
     NUMERIC = 545,
     OBJECT_P = 546,
     OF = 547,
     OFF = 548,
     OFFSET = 549,
     OIDS = 550,
     OLD = 551,
     ON = 552,
     ONLY = 553,
     OPERATOR = 554,
     OPTION = 555,
     OPTIONS = 556,
     OR = 557,
     ORDER = 558,
     ORDINALITY = 559,
     OTHERS = 560,
     OUT_P = 561,
     OUTER_P = 562,
     OVER = 563,
     OVERLAPS = 564,
     OVERLAY = 565,
     OVERRIDING = 566,
     OWNED = 567,
     OWNER = 568,
     PARALLEL = 569,
     PARSER = 570,
     PARTIAL = 571,
     PARTITION = 572,
     PARTITIONED = 573,
     PASSING = 574,
     PASSWORD = 575,
     PERCENT = 576,
     PERSISTENT = 577,
     PIVOT = 578,
     PIVOT_LONGER = 579,
     PIVOT_WIDER = 580,
     PLACING = 581,
     PLANS = 582,
     POLICY = 583,
     POSITION = 584,
     POSITIONAL = 585,
     PRAGMA_P = 586,
     PRECEDING = 587,
     PRECISION = 588,
     PREPARE = 589,
     PREPARED = 590,
     PRESERVE = 591,
     PRIMARY = 592,
     PRIOR = 593,
     PRIVILEGES = 594,
     PROCEDURAL = 595,
     PROCEDURE = 596,
     PROGRAM = 597,
     PUBLICATION = 598,
     QUALIFY = 599,
     QUARTER_P = 600,
     QUARTERS_P = 601,
     QUOTE = 602,
     RANGE = 603,
     READ_P = 604,
     REAL = 605,
     REASSIGN = 606,
     RECHECK = 607,
     RECURSIVE = 608,
     REF = 609,
     REFERENCES = 610,
     REFERENCING = 611,
     REFRESH = 612,
     REINDEX = 613,
     RELATIVE_P = 614,
     RELEASE = 615,
     RENAME = 616,
     REPEATABLE = 617,
     REPLACE = 618,
     REPLICA = 619,
     RESET = 620,
     RESPECT_P = 621,
     RESTART = 622,
     RESTRICT = 623,
     RETURNING = 624,
     RETURNS = 625,
     REVOKE = 626,
     RIGHT = 627,
     ROLE = 628,
     ROLLBACK = 629,
     ROLLUP = 630,
     ROW = 631,
     ROWS = 632,
     RULE = 633,
     SAMPLE = 634,
     SAVEPOINT = 635,
     SCHEMA = 636,
     SCHEMAS = 637,
     SCOPE = 638,
     SCROLL = 639,
     SEARCH = 640,
     SECOND_P = 641,
     SECONDS_P = 642,
     SECRET = 643,
     SECURITY = 644,
     SELECT = 645,
     SEMI = 646,
     SEQUENCE = 647,
     SEQUENCES = 648,
     SERIALIZABLE = 649,
     SERVER = 650,
     SESSION = 651,
     SET = 652,
     SETOF = 653,
     SETS = 654,
     SHARE = 655,
     SHOW = 656,
     SIMILAR = 657,
     SIMPLE = 658,
     SKIP = 659,
     SMALLINT = 660,
     SNAPSHOT = 661,
     SOME = 662,
     SORTED = 663,
     SOURCE_P = 664,
     SQL_P = 665,
     STABLE = 666,
     STANDALONE_P = 667,
     START = 668,
     STATEMENT = 669,
     STATISTICS = 670,
     STDIN = 671,
     STDOUT = 672,
     STORAGE = 673,
     STORED = 674,
     STRICT_P = 675,
     STRIP_P = 676,
     STRUCT = 677,
     SUBSCRIPTION = 678,
     SUBSTRING = 679,
     SUMMARIZE = 680,
     SYMMETRIC = 681,
     SYSID = 682,
     SYSTEM_P = 683,
     TABLE = 684,
     TABLES = 685,
     TABLESAMPLE = 686,
     TABLESPACE = 687,
     TARGET_P = 688,
     TEMP = 689,
     TEMPLATE = 690,
     TEMPORARY = 691,
     TEXT_P = 692,
     THEN = 693,
     TIES = 694,
     TIME = 695,
     TIMESTAMP = 696,
     TO = 697,
     TRAILING = 698,
     TRANSACTION = 699,
     TRANSFORM = 700,
     TREAT = 701,
     TRIGGER = 702,
     TRIM = 703,
     TRUE_P = 704,
     TRUNCATE = 705,
     TRUSTED = 706,
     TRY_CAST = 707,
     TYPE_P = 708,
     TYPES_P = 709,
     UNBOUNDED = 710,
     UNCOMMITTED = 711,
     UNENCRYPTED = 712,
     UNION = 713,
     UNIQUE = 714,
     UNKNOWN = 715,
     UNLISTEN = 716,
     UNLOGGED = 717,
     UNPACK = 718,
     UNPIVOT = 719,
     UNTIL = 720,
     UPDATE = 721,
     USE_P = 722,
     USER = 723,
     USING = 724,
     VACUUM = 725,
     VALID = 726,
     VALIDATE = 727,
     VALIDATOR = 728,
     VALUE_P = 729,
     VALUES = 730,
     VARCHAR = 731,
     VARIABLE_P = 732,
     VARIADIC = 733,
     VARYING = 734,
     VERBOSE = 735,
     VERSION_P = 736,
     VIEW = 737,
     VIEWS = 738,
     VIRTUAL = 739,
     VOLATILE = 740,
     WEEK_P = 741,
     WEEKS_P = 742,
     WHEN = 743,
     WHERE = 744,
     WHITESPACE_P = 745,
     WINDOW = 746,
     WITH = 747,
     WITHIN = 748,
     WITHOUT = 749,
     WORK = 750,
     WRAPPER = 751,
     WRITE_P = 752,
     XML_P = 753,
     XMLATTRIBUTES = 754,
     XMLCONCAT = 755,
     XMLELEMENT = 756,
     XMLEXISTS = 757,
     XMLFOREST = 758,
     XMLNAMESPACES = 759,
     XMLPARSE = 760,
     XMLPI = 761,
     XMLROOT = 762,
     XMLSERIALIZE = 763,
     XMLTABLE = 764,
     YEAR_P = 765,
     YEARS_P = 766,
     YES_P = 767,
     ZONE = 768,
     NOT_LA = 769,
     NULLS_LA = 770,
     WITH_LA = 771,
     POSTFIXOP = 772,
     UMINUS = 773
   };
#endif
/* Tokens.  */
#define IDENT 258
#define FCONST 259
#define SCONST 260
#define BCONST 261
#define XCONST 262
#define Op 263
#define ICONST 264
#define PARAM 265
#define TYPECAST 266
#define DOT_DOT 267
#define COLON_EQUALS 268
#define EQUALS_GREATER 269
#define INTEGER_DIVISION 270
#define POWER_OF 271
#define SINGLE_ARROW 272
#define DOUBLE_ARROW 273
#define SINGLE_COLON 274
#define LESS_EQUALS 275
#define GREATER_EQUALS 276
#define NOT_EQUALS 277
#define ABORT_P 278
#define ABSOLUTE_P 279
#define ACCESS 280
#define ACTION 281
#define ADD_P 282
#define ADMIN 283
#define AFTER 284
#define AGGREGATE 285
#define ALL 286
#define ALSO 287
#define ALTER 288
#define ALWAYS 289
#define ANALYSE 290
#define ANALYZE 291
#define AND 292
#define ANTI 293
#define ANY 294
#define ARRAY 295
#define AS 296
#define ASC_P 297
#define ASOF 298
#define ASSERTION 299
#define ASSIGNMENT 300
#define ASYMMETRIC 301
#define AT 302
#define ATTACH 303
#define ATTRIBUTE 304
#define AUTHORIZATION 305
#define BACKWARD 306
#define BEFORE 307
#define BEGIN_P 308
#define BETWEEN 309
#define BIGINT 310
#define BINARY 311
#define BIT 312
#define BOOLEAN_P 313
#define BOTH 314
#define BY 315
#define CACHE 316
#define CALL_P 317
#define CALLED 318
#define CASCADE 319
#define CASCADED 320
#define CASE 321
#define CAST 322
#define CATALOG_P 323
#define CENTURIES_P 324
#define CENTURY_P 325
#define CHAIN 326
#define CHAR_P 327
#define CHARACTER 328
#define CHARACTERISTICS 329
#define CHECK_P 330
#define CHECKPOINT 331
#define CLASS 332
#define CLOSE 333
#define CLUSTER 334
#define COALESCE 335
#define COLLATE 336
#define COLLATION 337
#define COLUMN 338
#define COLUMNS 339
#define COMMENT 340
#define COMMENTS 341
#define COMMIT 342
#define COMMITTED 343
#define COMPRESSION 344
#define CONCURRENTLY 345
#define CONFIGURATION 346
#define CONFLICT 347
#define CONNECTION 348
#define CONSTRAINT 349
#define CONSTRAINTS 350
#define CONTENT_P 351
#define CONTINUE_P 352
#define CONVERSION_P 353
#define COPY 354
#define COST 355
#define CREATE_P 356
#define CROSS 357
#define CSV 358
#define CUBE 359
#define CURRENT_P 360
#define CURSOR 361
#define CYCLE 362
#define DATA_P 363
#define DATABASE 364
#define DAY_P 365
#define DAYS_P 366
#define DEALLOCATE 367
#define DEC 368
#define DECADE_P 369
#define DECADES_P 370
#define DECIMAL_P 371
#define DECLARE 372
#define DEFAULT 373
#define DEFAULTS 374
#define DEFERRABLE 375
#define DEFERRED 376
#define DEFINER 377
#define DELETE_P 378
#define DELIMITER 379
#define DELIMITERS 380
#define DEPENDS 381
#define DESC_P 382
#define DESCRIBE 383
#define DETACH 384
#define DICTIONARY 385
#define DISABLE_P 386
#define DISCARD 387
#define DISTINCT 388
#define DO 389
#define DOCUMENT_P 390
#define DOMAIN_P 391
#define DOUBLE_P 392
#define DROP 393
#define EACH 394
#define ELSE 395
#define ENABLE_P 396
#define ENCODING 397
#define ENCRYPTED 398
#define END_P 399
#define ENUM_P 400
#define ERROR_P 401
#define ESCAPE 402
#define EVENT 403
#define EXCEPT 404
#define EXCLUDE 405
#define EXCLUDING 406
#define EXCLUSIVE 407
#define EXECUTE 408
#define EXISTS 409
#define EXPLAIN 410
#define EXPORT_P 411
#define EXPORT_STATE 412
#define EXTENSION 413
#define EXTENSIONS 414
#define EXTERNAL 415
#define EXTRACT 416
#define FALSE_P 417
#define FAMILY 418
#define FETCH 419
#define FILTER 420
#define FIRST_P 421
#define FLOAT_P 422
#define FOLLOWING 423
#define FOR 424
#define FORCE 425
#define FOREIGN 426
#define FORWARD 427
#define FREEZE 428
#define FROM 429
#define FULL 430
#define FUNCTION 431
#define FUNCTIONS 432
#define GENERATED 433
#define GLOB 434
#define GLOBAL 435
#define GRANT 436
#define GRANTED 437
#define GROUP_P 438
#define GROUPING 439
#define GROUPING_ID 440
#define GROUPS 441
#define HANDLER 442
#define HAVING 443
#define HEADER_P 444
#define HOLD 445
#define HOUR_P 446
#define HOURS_P 447
#define IDENTITY_P 448
#define IF_P 449
#define IGNORE_P 450
#define ILIKE 451
#define IMMEDIATE 452
#define IMMUTABLE 453
#define IMPLICIT_P 454
#define IMPORT_P 455
#define IN_P 456
#define INCLUDE_P 457
#define INCLUDING 458
#define INCREMENT 459
#define INDEX 460
#define INDEXES 461
#define INHERIT 462
#define INHERITS 463
#define INITIALLY 464
#define INLINE_P 465
#define INNER_P 466
#define INOUT 467
#define INPUT_P 468
#define INSENSITIVE 469
#define INSERT 470
#define INSTALL 471
#define INSTEAD 472
#define INT_P 473
#define INTEGER 474
#define INTERSECT 475
#define INTERVAL 476
#define INTO 477
#define INVOKER 478
#define IS 479
#define ISNULL 480
#define ISOLATION 481
#define JOIN 482
#define JSON 483
#define KEY 484
#define LABEL 485
#define LAMBDA 486
#define LANGUAGE 487
#define LARGE_P 488
#define LAST_P 489
#define LATERAL_P 490
#define LEADING 491
#define LEAKPROOF 492
#define LEFT 493
#define LEVEL 494
#define LIKE 495
#define LIMIT 496
#define LISTEN 497
#define LOAD 498
#define LOCAL 499
#define LOCATION 500
#define LOCK_P 501
#define LOCKED 502
#define LOGGED 503
#define MACRO 504
#define MAP 505
#define MAPPING 506
#define MATCH 507
#define MATCHED 508
#define MATERIALIZED 509
#define MAX 510
#define MAXVALUE 511
#define MERGE 512
#define METHOD 513
#define MICROSECOND_P 514
#define MICROSECONDS_P 515
#define MILLENNIA_P 516
#define MILLENNIUM_P 517
#define MILLISECOND_P 518
#define MILLISECONDS_P 519
#define MIN 520
#define MINUTE_P 521
#define MINUTES_P 522
#define MINVALUE 523
#define MODE 524
#define MONTH_P 525
#define MONTHS_P 526
#define MOVE 527
#define NAME_P 528
#define NAMES 529
#define NATIONAL 530
#define NATURAL 531
#define NCHAR 532
#define NEW 533
#define NEXT 534
#define NO 535
#define NONE 536
#define NOT 537
#define NOTHING 538
#define NOTIFY 539
#define NOTNULL 540
#define NOWAIT 541
#define NULL_P 542
#define NULLIF 543
#define NULLS_P 544
#define NUMERIC 545
#define OBJECT_P 546
#define OF 547
#define OFF 548
#define OFFSET 549
#define OIDS 550
#define OLD 551
#define ON 552
#define ONLY 553
#define OPERATOR 554
#define OPTION 555
#define OPTIONS 556
#define OR 557
#define ORDER 558
#define ORDINALITY 559
#define OTHERS 560
#define OUT_P 561
#define OUTER_P 562
#define OVER 563
#define OVERLAPS 564
#define OVERLAY 565
#define OVERRIDING 566
#define OWNED 567
#define OWNER 568
#define PARALLEL 569
#define PARSER 570
#define PARTIAL 571
#define PARTITION 572
#define PARTITIONED 573
#define PASSING 574
#define PASSWORD 575
#define PERCENT 576
#define PERSISTENT 577
#define PIVOT 578
#define PIVOT_LONGER 579
#define PIVOT_WIDER 580
#define PLACING 581
#define PLANS 582
#define POLICY 583
#define POSITION 584
#define POSITIONAL 585
#define PRAGMA_P 586
#define PRECEDING 587
#define PRECISION 588
#define PREPARE 589
#define PREPARED 590
#define PRESERVE 591
#define PRIMARY 592
#define PRIOR 593
#define PRIVILEGES 594
#define PROCEDURAL 595
#define PROCEDURE 596
#define PROGRAM 597
#define PUBLICATION 598
#define QUALIFY 599
#define QUARTER_P 600
#define QUARTERS_P 601
#define QUOTE 602
#define RANGE 603
#define READ_P 604
#define REAL 605
#define REASSIGN 606
#define RECHECK 607
#define RECURSIVE 608
#define REF 609
#define REFERENCES 610
#define REFERENCING 611
#define REFRESH 612
#define REINDEX 613
#define RELATIVE_P 614
#define RELEASE 615
#define RENAME 616
#define REPEATABLE 617
#define REPLACE 618
#define REPLICA 619
#define RESET 620
#define RESPECT_P 621
#define RESTART 622
#define RESTRICT 623
#define RETURNING 624
#define RETURNS 625
#define REVOKE 626
#define RIGHT 627
#define ROLE 628
#define ROLLBACK 629
#define ROLLUP 630
#define ROW 631
#define ROWS 632
#define RULE 633
#define SAMPLE 634
#define SAVEPOINT 635
#define SCHEMA 636
#define SCHEMAS 637
#define SCOPE 638
#define SCROLL 639
#define SEARCH 640
#define SECOND_P 641
#define SECONDS_P 642
#define SECRET 643
#define SECURITY 644
#define SELECT 645
#define SEMI 646
#define SEQUENCE 647
#define SEQUENCES 648
#define SERIALIZABLE 649
#define SERVER 650
#define SESSION 651
#define SET 652
#define SETOF 653
#define SETS 654
#define SHARE 655
#define SHOW 656
#define SIMILAR 657
#define SIMPLE 658
#define SKIP 659
#define SMALLINT 660
#define SNAPSHOT 661
#define SOME 662
#define SORTED 663
#define SOURCE_P 664
#define SQL_P 665
#define STABLE 666
#define STANDALONE_P 667
#define START 668
#define STATEMENT 669
#define STATISTICS 670
#define STDIN 671
#define STDOUT 672
#define STORAGE 673
#define STORED 674
#define STRICT_P 675
#define STRIP_P 676
#define STRUCT 677
#define SUBSCRIPTION 678
#define SUBSTRING 679
#define SUMMARIZE 680
#define SYMMETRIC 681
#define SYSID 682
#define SYSTEM_P 683
#define TABLE 684
#define TABLES 685
#define TABLESAMPLE 686
#define TABLESPACE 687
#define TARGET_P 688
#define TEMP 689
#define TEMPLATE 690
#define TEMPORARY 691
#define TEXT_P 692
#define THEN 693
#define TIES 694
#define TIME 695
#define TIMESTAMP 696
#define TO 697
#define TRAILING 698
#define TRANSACTION 699
#define TRANSFORM 700
#define TREAT 701
#define TRIGGER 702
#define TRIM 703
#define TRUE_P 704
#define TRUNCATE 705
#define TRUSTED 706
#define TRY_CAST 707
#define TYPE_P 708
#define TYPES_P 709
#define UNBOUNDED 710
#define UNCOMMITTED 711
#define UNENCRYPTED 712
#define UNION 713
#define UNIQUE 714
#define UNKNOWN 715
#define UNLISTEN 716
#define UNLOGGED 717
#define UNPACK 718
#define UNPIVOT 719
#define UNTIL 720
#define UPDATE 721
#define USE_P 722
#define USER 723
#define USING 724
#define VACUUM 725
#define VALID 726
#define VALIDATE 727
#define VALIDATOR 728
#define VALUE_P 729
#define VALUES 730
#define VARCHAR 731
#define VARIABLE_P 732
#define VARIADIC 733
#define VARYING 734
#define VERBOSE 735
#define VERSION_P 736
#define VIEW 737
#define VIEWS 738
#define VIRTUAL 739
#define VOLATILE 740
#define WEEK_P 741
#define WEEKS_P 742
#define WHEN 743
#define WHERE 744
#define WHITESPACE_P 745
#define WINDOW 746
#define WITH 747
#define WITHIN 748
#define WITHOUT 749
#define WORK 750
#define WRAPPER 751
#define WRITE_P 752
#define XML_P 753
#define XMLATTRIBUTES 754
#define XMLCONCAT 755
#define XMLELEMENT 756
#define XMLEXISTS 757
#define XMLFOREST 758
#define XMLNAMESPACES 759
#define XMLPARSE 760
#define XMLPI 761
#define XMLROOT 762
#define XMLSERIALIZE 763
#define XMLTABLE 764
#define YEAR_P 765
#define YEARS_P 766
#define YES_P 767
#define ZONE 768
#define NOT_LA 769
#define NULLS_LA 770
#define WITH_LA 771
#define POSTFIXOP 772
#define UMINUS 773




#if ! defined YYSTYPE && ! defined YYSTYPE_IS_DECLARED
typedef union YYSTYPE
#line 14 "third_party/libpg_query/grammar/grammar.y"
{
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
	PGMergeAction mergeaction;
}
/* Line 1529 of yacc.c.  */
#line 1137 "third_party/libpg_query/grammar/grammar_out.hpp"
	YYSTYPE;
# define yystype YYSTYPE /* obsolescent; will be withdrawn */
# define YYSTYPE_IS_DECLARED 1
# define YYSTYPE_IS_TRIVIAL 1
#endif



#if ! defined YYLTYPE && ! defined YYLTYPE_IS_DECLARED
typedef struct YYLTYPE
{
  int first_line;
  int first_column;
  int last_line;
  int last_column;
} YYLTYPE;
# define yyltype YYLTYPE /* obsolescent; will be withdrawn */
# define YYLTYPE_IS_DECLARED 1
# define YYLTYPE_IS_TRIVIAL 1
#endif


