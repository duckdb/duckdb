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
     LET = 494,
     LEVEL = 495,
     LIKE = 496,
     LIMIT = 497,
     LISTEN = 498,
     LOAD = 499,
     LOCAL = 500,
     LOCATION = 501,
     LOCK_P = 502,
     LOCKED = 503,
     LOGGED = 504,
     MACRO = 505,
     MAP = 506,
     MAPPING = 507,
     MATCH = 508,
     MATCHED = 509,
     MATERIALIZED = 510,
     MAXVALUE = 511,
     MERGE = 512,
     METHOD = 513,
     MICROSECOND_P = 514,
     MICROSECONDS_P = 515,
     MILLENNIA_P = 516,
     MILLENNIUM_P = 517,
     MILLISECOND_P = 518,
     MILLISECONDS_P = 519,
     MINUTE_P = 520,
     MINUTES_P = 521,
     MINVALUE = 522,
     MODE = 523,
     MONTH_P = 524,
     MONTHS_P = 525,
     MOVE = 526,
     NAME_P = 527,
     NAMES = 528,
     NATIONAL = 529,
     NATURAL = 530,
     NCHAR = 531,
     NEW = 532,
     NEXT = 533,
     NO = 534,
     NONE = 535,
     NOT = 536,
     NOTHING = 537,
     NOTIFY = 538,
     NOTNULL = 539,
     NOWAIT = 540,
     NULL_P = 541,
     NULLIF = 542,
     NULLS_P = 543,
     NUMERIC = 544,
     OBJECT_P = 545,
     OF = 546,
     OFF = 547,
     OFFSET = 548,
     OIDS = 549,
     OLD = 550,
     ON = 551,
     ONLY = 552,
     OPERATOR = 553,
     OPTION = 554,
     OPTIONS = 555,
     OR = 556,
     ORDER = 557,
     ORDINALITY = 558,
     OTHERS = 559,
     OUT_P = 560,
     OUTER_P = 561,
     OVER = 562,
     OVERLAPS = 563,
     OVERLAY = 564,
     OVERRIDING = 565,
     OWNED = 566,
     OWNER = 567,
     PARALLEL = 568,
     PARSER = 569,
     PARTIAL = 570,
     PARTITION = 571,
     PARTITIONED = 572,
     PASSING = 573,
     PASSWORD = 574,
     PERCENT = 575,
     PERSISTENT = 576,
     PIVOT = 577,
     PIVOT_LONGER = 578,
     PIVOT_WIDER = 579,
     PLACING = 580,
     PLANS = 581,
     POLICY = 582,
     POSITION = 583,
     POSITIONAL = 584,
     PRAGMA_P = 585,
     PRECEDING = 586,
     PRECISION = 587,
     PREPARE = 588,
     PREPARED = 589,
     PRESERVE = 590,
     PRIMARY = 591,
     PRIOR = 592,
     PRIVILEGES = 593,
     PROCEDURAL = 594,
     PROCEDURE = 595,
     PROGRAM = 596,
     PUBLICATION = 597,
     QUALIFY = 598,
     QUARTER_P = 599,
     QUARTERS_P = 600,
     QUOTE = 601,
     RANGE = 602,
     READ_P = 603,
     REAL = 604,
     REASSIGN = 605,
     RECHECK = 606,
     RECURSIVE = 607,
     REF = 608,
     REFERENCES = 609,
     REFERENCING = 610,
     REFRESH = 611,
     REINDEX = 612,
     RELATIVE_P = 613,
     RELEASE = 614,
     RENAME = 615,
     REPEATABLE = 616,
     REPLACE = 617,
     REPLICA = 618,
     RESET = 619,
     RESPECT_P = 620,
     RESTART = 621,
     RESTRICT = 622,
     RETURNING = 623,
     RETURNS = 624,
     REVOKE = 625,
     RIGHT = 626,
     ROLE = 627,
     ROLLBACK = 628,
     ROLLUP = 629,
     ROW = 630,
     ROWS = 631,
     RULE = 632,
     SAMPLE = 633,
     SAVEPOINT = 634,
     SCHEMA = 635,
     SCHEMAS = 636,
     SCOPE = 637,
     SCROLL = 638,
     SEARCH = 639,
     SECOND_P = 640,
     SECONDS_P = 641,
     SECRET = 642,
     SECURITY = 643,
     SELECT = 644,
     SEMI = 645,
     SEQUENCE = 646,
     SEQUENCES = 647,
     SERIALIZABLE = 648,
     SERVER = 649,
     SESSION = 650,
     SET = 651,
     SETOF = 652,
     SETS = 653,
     SHARE = 654,
     SHOW = 655,
     SIMILAR = 656,
     SIMPLE = 657,
     SKIP = 658,
     SMALLINT = 659,
     SNAPSHOT = 660,
     SOME = 661,
     SORTED = 662,
     SOURCE_P = 663,
     SQL_P = 664,
     STABLE = 665,
     STANDALONE_P = 666,
     START = 667,
     STATEMENT = 668,
     STATISTICS = 669,
     STDIN = 670,
     STDOUT = 671,
     STORAGE = 672,
     STORED = 673,
     STRICT_P = 674,
     STRIP_P = 675,
     STRUCT = 676,
     SUBSCRIPTION = 677,
     SUBSTRING = 678,
     SUMMARIZE = 679,
     SYMMETRIC = 680,
     SYSID = 681,
     SYSTEM_P = 682,
     TABLE = 683,
     TABLES = 684,
     TABLESAMPLE = 685,
     TABLESPACE = 686,
     TARGET_P = 687,
     TEMP = 688,
     TEMPLATE = 689,
     TEMPORARY = 690,
     TEXT_P = 691,
     THEN = 692,
     TIES = 693,
     TIME = 694,
     TIMESTAMP = 695,
     TO = 696,
     TRAILING = 697,
     TRANSACTION = 698,
     TRANSFORM = 699,
     TREAT = 700,
     TRIGGER = 701,
     TRIM = 702,
     TRUE_P = 703,
     TRUNCATE = 704,
     TRUSTED = 705,
     TRY_CAST = 706,
     TYPE_P = 707,
     TYPES_P = 708,
     UNBOUNDED = 709,
     UNCOMMITTED = 710,
     UNENCRYPTED = 711,
     UNION = 712,
     UNIQUE = 713,
     UNKNOWN = 714,
     UNLISTEN = 715,
     UNLOGGED = 716,
     UNPACK = 717,
     UNPIVOT = 718,
     UNTIL = 719,
     UPDATE = 720,
     USE_P = 721,
     USER = 722,
     USING = 723,
     VACUUM = 724,
     VALID = 725,
     VALIDATE = 726,
     VALIDATOR = 727,
     VALUE_P = 728,
     VALUES = 729,
     VARCHAR = 730,
     VARIABLE_P = 731,
     VARIADIC = 732,
     VARYING = 733,
     VERBOSE = 734,
     VERSION_P = 735,
     VIEW = 736,
     VIEWS = 737,
     VIRTUAL = 738,
     VOLATILE = 739,
     WEEK_P = 740,
     WEEKS_P = 741,
     WHEN = 742,
     WHERE = 743,
     WHITESPACE_P = 744,
     WINDOW = 745,
     WITH = 746,
     WITHIN = 747,
     WITHOUT = 748,
     WORK = 749,
     WRAPPER = 750,
     WRITE_P = 751,
     XML_P = 752,
     XMLATTRIBUTES = 753,
     XMLCONCAT = 754,
     XMLELEMENT = 755,
     XMLEXISTS = 756,
     XMLFOREST = 757,
     XMLNAMESPACES = 758,
     XMLPARSE = 759,
     XMLPI = 760,
     XMLROOT = 761,
     XMLSERIALIZE = 762,
     XMLTABLE = 763,
     YEAR_P = 764,
     YEARS_P = 765,
     YES_P = 766,
     ZONE = 767,
     NOT_LA = 768,
     NULLS_LA = 769,
     WITH_LA = 770,
     POSTFIXOP = 771,
     UMINUS = 772
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
#define LET 494
#define LEVEL 495
#define LIKE 496
#define LIMIT 497
#define LISTEN 498
#define LOAD 499
#define LOCAL 500
#define LOCATION 501
#define LOCK_P 502
#define LOCKED 503
#define LOGGED 504
#define MACRO 505
#define MAP 506
#define MAPPING 507
#define MATCH 508
#define MATCHED 509
#define MATERIALIZED 510
#define MAXVALUE 511
#define MERGE 512
#define METHOD 513
#define MICROSECOND_P 514
#define MICROSECONDS_P 515
#define MILLENNIA_P 516
#define MILLENNIUM_P 517
#define MILLISECOND_P 518
#define MILLISECONDS_P 519
#define MINUTE_P 520
#define MINUTES_P 521
#define MINVALUE 522
#define MODE 523
#define MONTH_P 524
#define MONTHS_P 525
#define MOVE 526
#define NAME_P 527
#define NAMES 528
#define NATIONAL 529
#define NATURAL 530
#define NCHAR 531
#define NEW 532
#define NEXT 533
#define NO 534
#define NONE 535
#define NOT 536
#define NOTHING 537
#define NOTIFY 538
#define NOTNULL 539
#define NOWAIT 540
#define NULL_P 541
#define NULLIF 542
#define NULLS_P 543
#define NUMERIC 544
#define OBJECT_P 545
#define OF 546
#define OFF 547
#define OFFSET 548
#define OIDS 549
#define OLD 550
#define ON 551
#define ONLY 552
#define OPERATOR 553
#define OPTION 554
#define OPTIONS 555
#define OR 556
#define ORDER 557
#define ORDINALITY 558
#define OTHERS 559
#define OUT_P 560
#define OUTER_P 561
#define OVER 562
#define OVERLAPS 563
#define OVERLAY 564
#define OVERRIDING 565
#define OWNED 566
#define OWNER 567
#define PARALLEL 568
#define PARSER 569
#define PARTIAL 570
#define PARTITION 571
#define PARTITIONED 572
#define PASSING 573
#define PASSWORD 574
#define PERCENT 575
#define PERSISTENT 576
#define PIVOT 577
#define PIVOT_LONGER 578
#define PIVOT_WIDER 579
#define PLACING 580
#define PLANS 581
#define POLICY 582
#define POSITION 583
#define POSITIONAL 584
#define PRAGMA_P 585
#define PRECEDING 586
#define PRECISION 587
#define PREPARE 588
#define PREPARED 589
#define PRESERVE 590
#define PRIMARY 591
#define PRIOR 592
#define PRIVILEGES 593
#define PROCEDURAL 594
#define PROCEDURE 595
#define PROGRAM 596
#define PUBLICATION 597
#define QUALIFY 598
#define QUARTER_P 599
#define QUARTERS_P 600
#define QUOTE 601
#define RANGE 602
#define READ_P 603
#define REAL 604
#define REASSIGN 605
#define RECHECK 606
#define RECURSIVE 607
#define REF 608
#define REFERENCES 609
#define REFERENCING 610
#define REFRESH 611
#define REINDEX 612
#define RELATIVE_P 613
#define RELEASE 614
#define RENAME 615
#define REPEATABLE 616
#define REPLACE 617
#define REPLICA 618
#define RESET 619
#define RESPECT_P 620
#define RESTART 621
#define RESTRICT 622
#define RETURNING 623
#define RETURNS 624
#define REVOKE 625
#define RIGHT 626
#define ROLE 627
#define ROLLBACK 628
#define ROLLUP 629
#define ROW 630
#define ROWS 631
#define RULE 632
#define SAMPLE 633
#define SAVEPOINT 634
#define SCHEMA 635
#define SCHEMAS 636
#define SCOPE 637
#define SCROLL 638
#define SEARCH 639
#define SECOND_P 640
#define SECONDS_P 641
#define SECRET 642
#define SECURITY 643
#define SELECT 644
#define SEMI 645
#define SEQUENCE 646
#define SEQUENCES 647
#define SERIALIZABLE 648
#define SERVER 649
#define SESSION 650
#define SET 651
#define SETOF 652
#define SETS 653
#define SHARE 654
#define SHOW 655
#define SIMILAR 656
#define SIMPLE 657
#define SKIP 658
#define SMALLINT 659
#define SNAPSHOT 660
#define SOME 661
#define SORTED 662
#define SOURCE_P 663
#define SQL_P 664
#define STABLE 665
#define STANDALONE_P 666
#define START 667
#define STATEMENT 668
#define STATISTICS 669
#define STDIN 670
#define STDOUT 671
#define STORAGE 672
#define STORED 673
#define STRICT_P 674
#define STRIP_P 675
#define STRUCT 676
#define SUBSCRIPTION 677
#define SUBSTRING 678
#define SUMMARIZE 679
#define SYMMETRIC 680
#define SYSID 681
#define SYSTEM_P 682
#define TABLE 683
#define TABLES 684
#define TABLESAMPLE 685
#define TABLESPACE 686
#define TARGET_P 687
#define TEMP 688
#define TEMPLATE 689
#define TEMPORARY 690
#define TEXT_P 691
#define THEN 692
#define TIES 693
#define TIME 694
#define TIMESTAMP 695
#define TO 696
#define TRAILING 697
#define TRANSACTION 698
#define TRANSFORM 699
#define TREAT 700
#define TRIGGER 701
#define TRIM 702
#define TRUE_P 703
#define TRUNCATE 704
#define TRUSTED 705
#define TRY_CAST 706
#define TYPE_P 707
#define TYPES_P 708
#define UNBOUNDED 709
#define UNCOMMITTED 710
#define UNENCRYPTED 711
#define UNION 712
#define UNIQUE 713
#define UNKNOWN 714
#define UNLISTEN 715
#define UNLOGGED 716
#define UNPACK 717
#define UNPIVOT 718
#define UNTIL 719
#define UPDATE 720
#define USE_P 721
#define USER 722
#define USING 723
#define VACUUM 724
#define VALID 725
#define VALIDATE 726
#define VALIDATOR 727
#define VALUE_P 728
#define VALUES 729
#define VARCHAR 730
#define VARIABLE_P 731
#define VARIADIC 732
#define VARYING 733
#define VERBOSE 734
#define VERSION_P 735
#define VIEW 736
#define VIEWS 737
#define VIRTUAL 738
#define VOLATILE 739
#define WEEK_P 740
#define WEEKS_P 741
#define WHEN 742
#define WHERE 743
#define WHITESPACE_P 744
#define WINDOW 745
#define WITH 746
#define WITHIN 747
#define WITHOUT 748
#define WORK 749
#define WRAPPER 750
#define WRITE_P 751
#define XML_P 752
#define XMLATTRIBUTES 753
#define XMLCONCAT 754
#define XMLELEMENT 755
#define XMLEXISTS 756
#define XMLFOREST 757
#define XMLNAMESPACES 758
#define XMLPARSE 759
#define XMLPI 760
#define XMLROOT 761
#define XMLSERIALIZE 762
#define XMLTABLE 763
#define YEAR_P 764
#define YEARS_P 765
#define YES_P 766
#define ZONE 767
#define NOT_LA 768
#define NULLS_LA 769
#define WITH_LA 770
#define POSTFIXOP 771
#define UMINUS 772




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
#line 1135 "third_party/libpg_query/grammar/grammar_out.hpp"
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


