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
     MAXVALUE = 510,
     MERGE = 511,
     METHOD = 512,
     MICROSECOND_P = 513,
     MICROSECONDS_P = 514,
     MILLENNIA_P = 515,
     MILLENNIUM_P = 516,
     MILLISECOND_P = 517,
     MILLISECONDS_P = 518,
     MINUTE_P = 519,
     MINUTES_P = 520,
     MINVALUE = 521,
     MODE = 522,
     MONTH_P = 523,
     MONTHS_P = 524,
     MOVE = 525,
     NAME_P = 526,
     NAMES = 527,
     NATIONAL = 528,
     NATURAL = 529,
     NCHAR = 530,
     NEW = 531,
     NEXT = 532,
     NO = 533,
     NONE = 534,
     NOT = 535,
     NOTHING = 536,
     NOTIFY = 537,
     NOTNULL = 538,
     NOWAIT = 539,
     NULL_P = 540,
     NULLIF = 541,
     NULLS_P = 542,
     NUMERIC = 543,
     OBJECT_P = 544,
     OF = 545,
     OFF = 546,
     OFFSET = 547,
     OIDS = 548,
     OLD = 549,
     ON = 550,
     ONLY = 551,
     OPERATOR = 552,
     OPTION = 553,
     OPTIONS = 554,
     OR = 555,
     ORDER = 556,
     ORDINALITY = 557,
     OTHERS = 558,
     OUT_P = 559,
     OUTER_P = 560,
     OVER = 561,
     OVERLAPS = 562,
     OVERLAY = 563,
     OVERRIDING = 564,
     OWNED = 565,
     OWNER = 566,
     PARALLEL = 567,
     PARSER = 568,
     PARTIAL = 569,
     PARTITION = 570,
     PARTITIONED = 571,
     PASSING = 572,
     PASSWORD = 573,
     PERCENT = 574,
     PERSISTENT = 575,
     PIVOT = 576,
     PIVOT_LONGER = 577,
     PIVOT_WIDER = 578,
     PLACING = 579,
     PLANS = 580,
     POLICY = 581,
     POSITION = 582,
     POSITIONAL = 583,
     PRAGMA_P = 584,
     PRECEDING = 585,
     PRECISION = 586,
     PREPARE = 587,
     PREPARED = 588,
     PRESERVE = 589,
     PRIMARY = 590,
     PRIOR = 591,
     PRIVILEGES = 592,
     PROCEDURAL = 593,
     PROCEDURE = 594,
     PROGRAM = 595,
     PUBLICATION = 596,
     QUALIFY = 597,
     QUARTER_P = 598,
     QUARTERS_P = 599,
     QUOTE = 600,
     RANGE = 601,
     READ_P = 602,
     REAL = 603,
     REASSIGN = 604,
     RECHECK = 605,
     RECURSIVE = 606,
     REF = 607,
     REFERENCES = 608,
     REFERENCING = 609,
     REFRESH = 610,
     REINDEX = 611,
     RELATIVE_P = 612,
     RELEASE = 613,
     RENAME = 614,
     REPEATABLE = 615,
     REPLACE = 616,
     REPLICA = 617,
     RESET = 618,
     RESPECT_P = 619,
     RESTART = 620,
     RESTRICT = 621,
     RETURNING = 622,
     RETURNS = 623,
     REVOKE = 624,
     RIGHT = 625,
     ROLE = 626,
     ROLLBACK = 627,
     ROLLUP = 628,
     ROW = 629,
     ROWS = 630,
     RULE = 631,
     SAMPLE = 632,
     SAVEPOINT = 633,
     SCHEMA = 634,
     SCHEMAS = 635,
     SCOPE = 636,
     SCROLL = 637,
     SEARCH = 638,
     SECOND_P = 639,
     SECONDS_P = 640,
     SECRET = 641,
     SECURITY = 642,
     SELECT = 643,
     SEMI = 644,
     SEQUENCE = 645,
     SEQUENCES = 646,
     SERIALIZABLE = 647,
     SERVER = 648,
     SESSION = 649,
     SET = 650,
     SETOF = 651,
     SETS = 652,
     SHARE = 653,
     SHOW = 654,
     SIMILAR = 655,
     SIMPLE = 656,
     SKIP = 657,
     SMALLINT = 658,
     SNAPSHOT = 659,
     SOME = 660,
     SORTED = 661,
     SOURCE_P = 662,
     SQL_P = 663,
     STABLE = 664,
     STANDALONE_P = 665,
     START = 666,
     STATEMENT = 667,
     STATISTICS = 668,
     STDIN = 669,
     STDOUT = 670,
     STORAGE = 671,
     STORED = 672,
     STRICT_P = 673,
     STRIP_P = 674,
     STRUCT = 675,
     SUBSCRIPTION = 676,
     SUBSTRING = 677,
     SUMMARIZE = 678,
     SWITCH = 679,
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
#define MAXVALUE 510
#define MERGE 511
#define METHOD 512
#define MICROSECOND_P 513
#define MICROSECONDS_P 514
#define MILLENNIA_P 515
#define MILLENNIUM_P 516
#define MILLISECOND_P 517
#define MILLISECONDS_P 518
#define MINUTE_P 519
#define MINUTES_P 520
#define MINVALUE 521
#define MODE 522
#define MONTH_P 523
#define MONTHS_P 524
#define MOVE 525
#define NAME_P 526
#define NAMES 527
#define NATIONAL 528
#define NATURAL 529
#define NCHAR 530
#define NEW 531
#define NEXT 532
#define NO 533
#define NONE 534
#define NOT 535
#define NOTHING 536
#define NOTIFY 537
#define NOTNULL 538
#define NOWAIT 539
#define NULL_P 540
#define NULLIF 541
#define NULLS_P 542
#define NUMERIC 543
#define OBJECT_P 544
#define OF 545
#define OFF 546
#define OFFSET 547
#define OIDS 548
#define OLD 549
#define ON 550
#define ONLY 551
#define OPERATOR 552
#define OPTION 553
#define OPTIONS 554
#define OR 555
#define ORDER 556
#define ORDINALITY 557
#define OTHERS 558
#define OUT_P 559
#define OUTER_P 560
#define OVER 561
#define OVERLAPS 562
#define OVERLAY 563
#define OVERRIDING 564
#define OWNED 565
#define OWNER 566
#define PARALLEL 567
#define PARSER 568
#define PARTIAL 569
#define PARTITION 570
#define PARTITIONED 571
#define PASSING 572
#define PASSWORD 573
#define PERCENT 574
#define PERSISTENT 575
#define PIVOT 576
#define PIVOT_LONGER 577
#define PIVOT_WIDER 578
#define PLACING 579
#define PLANS 580
#define POLICY 581
#define POSITION 582
#define POSITIONAL 583
#define PRAGMA_P 584
#define PRECEDING 585
#define PRECISION 586
#define PREPARE 587
#define PREPARED 588
#define PRESERVE 589
#define PRIMARY 590
#define PRIOR 591
#define PRIVILEGES 592
#define PROCEDURAL 593
#define PROCEDURE 594
#define PROGRAM 595
#define PUBLICATION 596
#define QUALIFY 597
#define QUARTER_P 598
#define QUARTERS_P 599
#define QUOTE 600
#define RANGE 601
#define READ_P 602
#define REAL 603
#define REASSIGN 604
#define RECHECK 605
#define RECURSIVE 606
#define REF 607
#define REFERENCES 608
#define REFERENCING 609
#define REFRESH 610
#define REINDEX 611
#define RELATIVE_P 612
#define RELEASE 613
#define RENAME 614
#define REPEATABLE 615
#define REPLACE 616
#define REPLICA 617
#define RESET 618
#define RESPECT_P 619
#define RESTART 620
#define RESTRICT 621
#define RETURNING 622
#define RETURNS 623
#define REVOKE 624
#define RIGHT 625
#define ROLE 626
#define ROLLBACK 627
#define ROLLUP 628
#define ROW 629
#define ROWS 630
#define RULE 631
#define SAMPLE 632
#define SAVEPOINT 633
#define SCHEMA 634
#define SCHEMAS 635
#define SCOPE 636
#define SCROLL 637
#define SEARCH 638
#define SECOND_P 639
#define SECONDS_P 640
#define SECRET 641
#define SECURITY 642
#define SELECT 643
#define SEMI 644
#define SEQUENCE 645
#define SEQUENCES 646
#define SERIALIZABLE 647
#define SERVER 648
#define SESSION 649
#define SET 650
#define SETOF 651
#define SETS 652
#define SHARE 653
#define SHOW 654
#define SIMILAR 655
#define SIMPLE 656
#define SKIP 657
#define SMALLINT 658
#define SNAPSHOT 659
#define SOME 660
#define SORTED 661
#define SOURCE_P 662
#define SQL_P 663
#define STABLE 664
#define STANDALONE_P 665
#define START 666
#define STATEMENT 667
#define STATISTICS 668
#define STDIN 669
#define STDOUT 670
#define STORAGE 671
#define STORED 672
#define STRICT_P 673
#define STRIP_P 674
#define STRUCT 675
#define SUBSCRIPTION 676
#define SUBSTRING 677
#define SUMMARIZE 678
#define SWITCH 679
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


