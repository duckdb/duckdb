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
     ESCAPE = 401,
     EVENT = 402,
     EXCEPT = 403,
     EXCLUDE = 404,
     EXCLUDING = 405,
     EXCLUSIVE = 406,
     EXECUTE = 407,
     EXISTS = 408,
     EXPLAIN = 409,
     EXPORT_P = 410,
     EXPORT_STATE = 411,
     EXTENSION = 412,
     EXTENSIONS = 413,
     EXTERNAL = 414,
     EXTRACT = 415,
     FALSE_P = 416,
     FAMILY = 417,
     FETCH = 418,
     FILTER = 419,
     FIRST_P = 420,
     FLOAT_P = 421,
     FOLLOWING = 422,
     FOR = 423,
     FORCE = 424,
     FOREIGN = 425,
     FORWARD = 426,
     FREEZE = 427,
     FROM = 428,
     FULL = 429,
     FUNCTION = 430,
     FUNCTIONS = 431,
     GENERATED = 432,
     GLOB = 433,
     GLOBAL = 434,
     GRANT = 435,
     GRANTED = 436,
     GROUP_P = 437,
     GROUPING = 438,
     GROUPING_ID = 439,
     GROUPS = 440,
     HANDLER = 441,
     HAVING = 442,
     HEADER_P = 443,
     HOLD = 444,
     HOUR_P = 445,
     HOURS_P = 446,
     IDENTITY_P = 447,
     IF_P = 448,
     IGNORE_P = 449,
     ILIKE = 450,
     IMMEDIATE = 451,
     IMMUTABLE = 452,
     IMPLICIT_P = 453,
     IMPORT_P = 454,
     IN_P = 455,
     INCLUDE_P = 456,
     INCLUDING = 457,
     INCREMENT = 458,
     INDEX = 459,
     INDEXES = 460,
     INHERIT = 461,
     INHERITS = 462,
     INITIALLY = 463,
     INLINE_P = 464,
     INNER_P = 465,
     INOUT = 466,
     INPUT_P = 467,
     INSENSITIVE = 468,
     INSERT = 469,
     INSTALL = 470,
     INSTEAD = 471,
     INT_P = 472,
     INTEGER = 473,
     INTERSECT = 474,
     INTERVAL = 475,
     INTO = 476,
     INVOKER = 477,
     IS = 478,
     ISNULL = 479,
     ISOLATION = 480,
     JOIN = 481,
     JSON = 482,
     KEY = 483,
     LABEL = 484,
     LAMBDA = 485,
     LANGUAGE = 486,
     LARGE_P = 487,
     LAST_P = 488,
     LATERAL_P = 489,
     LEADING = 490,
     LEAKPROOF = 491,
     LEFT = 492,
     LEVEL = 493,
     LIKE = 494,
     LIMIT = 495,
     LISTEN = 496,
     LOAD = 497,
     LOCAL = 498,
     LOCATION = 499,
     LOCK_P = 500,
     LOCKED = 501,
     LOGGED = 502,
     MACRO = 503,
     MAP = 504,
     MAPPING = 505,
     MATCH = 506,
     MATCHED = 507,
     MATERIALIZED = 508,
     MAXVALUE = 509,
     MERGE = 510,
     METHOD = 511,
     MICROSECOND_P = 512,
     MICROSECONDS_P = 513,
     MILLENNIA_P = 514,
     MILLENNIUM_P = 515,
     MILLISECOND_P = 516,
     MILLISECONDS_P = 517,
     MINUTE_P = 518,
     MINUTES_P = 519,
     MINVALUE = 520,
     MODE = 521,
     MONTH_P = 522,
     MONTHS_P = 523,
     MOVE = 524,
     NAME_P = 525,
     NAMES = 526,
     NATIONAL = 527,
     NATURAL = 528,
     NCHAR = 529,
     NEW = 530,
     NEXT = 531,
     NO = 532,
     NONE = 533,
     NOT = 534,
     NOTHING = 535,
     NOTIFY = 536,
     NOTNULL = 537,
     NOWAIT = 538,
     NULL_P = 539,
     NULLIF = 540,
     NULLS_P = 541,
     NUMERIC = 542,
     OBJECT_P = 543,
     OF = 544,
     OFF = 545,
     OFFSET = 546,
     OIDS = 547,
     OLD = 548,
     ON = 549,
     ONLY = 550,
     OPERATOR = 551,
     OPTION = 552,
     OPTIONS = 553,
     OR = 554,
     ORDER = 555,
     ORDINALITY = 556,
     OTHERS = 557,
     OUT_P = 558,
     OUTER_P = 559,
     OVER = 560,
     OVERLAPS = 561,
     OVERLAY = 562,
     OVERRIDING = 563,
     OWNED = 564,
     OWNER = 565,
     PARALLEL = 566,
     PARSER = 567,
     PARTIAL = 568,
     PARTITION = 569,
     PARTITIONED = 570,
     PASSING = 571,
     PASSWORD = 572,
     PERCENT = 573,
     PERSISTENT = 574,
     PIVOT = 575,
     PIVOT_LONGER = 576,
     PIVOT_WIDER = 577,
     PLACING = 578,
     PLANS = 579,
     POLICY = 580,
     POSITION = 581,
     POSITIONAL = 582,
     PRAGMA_P = 583,
     PRECEDING = 584,
     PRECISION = 585,
     PREPARE = 586,
     PREPARED = 587,
     PRESERVE = 588,
     PRIMARY = 589,
     PRIOR = 590,
     PRIVILEGES = 591,
     PROCEDURAL = 592,
     PROCEDURE = 593,
     PROGRAM = 594,
     PUBLICATION = 595,
     QUALIFY = 596,
     QUARTER_P = 597,
     QUARTERS_P = 598,
     QUOTE = 599,
     RANGE = 600,
     READ_P = 601,
     REAL = 602,
     REASSIGN = 603,
     RECHECK = 604,
     RECURSIVE = 605,
     REF = 606,
     REFERENCES = 607,
     REFERENCING = 608,
     REFRESH = 609,
     REINDEX = 610,
     RELATIVE_P = 611,
     RELEASE = 612,
     RENAME = 613,
     REPEATABLE = 614,
     REPLACE = 615,
     REPLICA = 616,
     RESET = 617,
     RESPECT_P = 618,
     RESTART = 619,
     RESTRICT = 620,
     RETURNING = 621,
     RETURNS = 622,
     REVOKE = 623,
     RIGHT = 624,
     ROLE = 625,
     ROLLBACK = 626,
     ROLLUP = 627,
     ROW = 628,
     ROWS = 629,
     RULE = 630,
     SAMPLE = 631,
     SAVEPOINT = 632,
     SCHEMA = 633,
     SCHEMAS = 634,
     SCOPE = 635,
     SCROLL = 636,
     SEARCH = 637,
     SECOND_P = 638,
     SECONDS_P = 639,
     SECRET = 640,
     SECURITY = 641,
     SELECT = 642,
     SEMI = 643,
     SEQUENCE = 644,
     SEQUENCES = 645,
     SERIALIZABLE = 646,
     SERVER = 647,
     SESSION = 648,
     SET = 649,
     SETOF = 650,
     SETS = 651,
     SHARE = 652,
     SHOW = 653,
     SIMILAR = 654,
     SIMPLE = 655,
     SKIP = 656,
     SMALLINT = 657,
     SNAPSHOT = 658,
     SOME = 659,
     SORTED = 660,
     SQL_P = 661,
     STABLE = 662,
     STANDALONE_P = 663,
     START = 664,
     STATEMENT = 665,
     STATISTICS = 666,
     STDIN = 667,
     STDOUT = 668,
     STORAGE = 669,
     STORED = 670,
     STRICT_P = 671,
     STRIP_P = 672,
     STRUCT = 673,
     SUBSCRIPTION = 674,
     SUBSTRING = 675,
     SUMMARIZE = 676,
     SYMMETRIC = 677,
     SYSID = 678,
     SYSTEM_P = 679,
     TABLE = 680,
     TABLES = 681,
     TABLESAMPLE = 682,
     TABLESPACE = 683,
     TEMP = 684,
     TEMPLATE = 685,
     TEMPORARY = 686,
     TEXT_P = 687,
     THEN = 688,
     TIES = 689,
     TIME = 690,
     TIMESTAMP = 691,
     TO = 692,
     TRAILING = 693,
     TRANSACTION = 694,
     TRANSFORM = 695,
     TREAT = 696,
     TRIGGER = 697,
     TRIM = 698,
     TRUE_P = 699,
     TRUNCATE = 700,
     TRUSTED = 701,
     TRY_CAST = 702,
     TYPE_P = 703,
     TYPES_P = 704,
     UNBOUNDED = 705,
     UNCOMMITTED = 706,
     UNENCRYPTED = 707,
     UNION = 708,
     UNIQUE = 709,
     UNKNOWN = 710,
     UNLISTEN = 711,
     UNLOGGED = 712,
     UNPACK = 713,
     UNPIVOT = 714,
     UNTIL = 715,
     UPDATE = 716,
     USE_P = 717,
     USER = 718,
     USING = 719,
     VACUUM = 720,
     VALID = 721,
     VALIDATE = 722,
     VALIDATOR = 723,
     VALUE_P = 724,
     VALUES = 725,
     VARCHAR = 726,
     VARIABLE_P = 727,
     VARIADIC = 728,
     VARYING = 729,
     VERBOSE = 730,
     VERSION_P = 731,
     VIEW = 732,
     VIEWS = 733,
     VIRTUAL = 734,
     VOLATILE = 735,
     WEEK_P = 736,
     WEEKS_P = 737,
     WHEN = 738,
     WHERE = 739,
     WHITESPACE_P = 740,
     WINDOW = 741,
     WITH = 742,
     WITHIN = 743,
     WITHOUT = 744,
     WORK = 745,
     WRAPPER = 746,
     WRITE_P = 747,
     XML_P = 748,
     XMLATTRIBUTES = 749,
     XMLCONCAT = 750,
     XMLELEMENT = 751,
     XMLEXISTS = 752,
     XMLFOREST = 753,
     XMLNAMESPACES = 754,
     XMLPARSE = 755,
     XMLPI = 756,
     XMLROOT = 757,
     XMLSERIALIZE = 758,
     XMLTABLE = 759,
     YEAR_P = 760,
     YEARS_P = 761,
     YES_P = 762,
     ZONE = 763,
     NOT_LA = 764,
     NULLS_LA = 765,
     WITH_LA = 766,
     POSTFIXOP = 767,
     UMINUS = 768
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
#define ESCAPE 401
#define EVENT 402
#define EXCEPT 403
#define EXCLUDE 404
#define EXCLUDING 405
#define EXCLUSIVE 406
#define EXECUTE 407
#define EXISTS 408
#define EXPLAIN 409
#define EXPORT_P 410
#define EXPORT_STATE 411
#define EXTENSION 412
#define EXTENSIONS 413
#define EXTERNAL 414
#define EXTRACT 415
#define FALSE_P 416
#define FAMILY 417
#define FETCH 418
#define FILTER 419
#define FIRST_P 420
#define FLOAT_P 421
#define FOLLOWING 422
#define FOR 423
#define FORCE 424
#define FOREIGN 425
#define FORWARD 426
#define FREEZE 427
#define FROM 428
#define FULL 429
#define FUNCTION 430
#define FUNCTIONS 431
#define GENERATED 432
#define GLOB 433
#define GLOBAL 434
#define GRANT 435
#define GRANTED 436
#define GROUP_P 437
#define GROUPING 438
#define GROUPING_ID 439
#define GROUPS 440
#define HANDLER 441
#define HAVING 442
#define HEADER_P 443
#define HOLD 444
#define HOUR_P 445
#define HOURS_P 446
#define IDENTITY_P 447
#define IF_P 448
#define IGNORE_P 449
#define ILIKE 450
#define IMMEDIATE 451
#define IMMUTABLE 452
#define IMPLICIT_P 453
#define IMPORT_P 454
#define IN_P 455
#define INCLUDE_P 456
#define INCLUDING 457
#define INCREMENT 458
#define INDEX 459
#define INDEXES 460
#define INHERIT 461
#define INHERITS 462
#define INITIALLY 463
#define INLINE_P 464
#define INNER_P 465
#define INOUT 466
#define INPUT_P 467
#define INSENSITIVE 468
#define INSERT 469
#define INSTALL 470
#define INSTEAD 471
#define INT_P 472
#define INTEGER 473
#define INTERSECT 474
#define INTERVAL 475
#define INTO 476
#define INVOKER 477
#define IS 478
#define ISNULL 479
#define ISOLATION 480
#define JOIN 481
#define JSON 482
#define KEY 483
#define LABEL 484
#define LAMBDA 485
#define LANGUAGE 486
#define LARGE_P 487
#define LAST_P 488
#define LATERAL_P 489
#define LEADING 490
#define LEAKPROOF 491
#define LEFT 492
#define LEVEL 493
#define LIKE 494
#define LIMIT 495
#define LISTEN 496
#define LOAD 497
#define LOCAL 498
#define LOCATION 499
#define LOCK_P 500
#define LOCKED 501
#define LOGGED 502
#define MACRO 503
#define MAP 504
#define MAPPING 505
#define MATCH 506
#define MATCHED 507
#define MATERIALIZED 508
#define MAXVALUE 509
#define MERGE 510
#define METHOD 511
#define MICROSECOND_P 512
#define MICROSECONDS_P 513
#define MILLENNIA_P 514
#define MILLENNIUM_P 515
#define MILLISECOND_P 516
#define MILLISECONDS_P 517
#define MINUTE_P 518
#define MINUTES_P 519
#define MINVALUE 520
#define MODE 521
#define MONTH_P 522
#define MONTHS_P 523
#define MOVE 524
#define NAME_P 525
#define NAMES 526
#define NATIONAL 527
#define NATURAL 528
#define NCHAR 529
#define NEW 530
#define NEXT 531
#define NO 532
#define NONE 533
#define NOT 534
#define NOTHING 535
#define NOTIFY 536
#define NOTNULL 537
#define NOWAIT 538
#define NULL_P 539
#define NULLIF 540
#define NULLS_P 541
#define NUMERIC 542
#define OBJECT_P 543
#define OF 544
#define OFF 545
#define OFFSET 546
#define OIDS 547
#define OLD 548
#define ON 549
#define ONLY 550
#define OPERATOR 551
#define OPTION 552
#define OPTIONS 553
#define OR 554
#define ORDER 555
#define ORDINALITY 556
#define OTHERS 557
#define OUT_P 558
#define OUTER_P 559
#define OVER 560
#define OVERLAPS 561
#define OVERLAY 562
#define OVERRIDING 563
#define OWNED 564
#define OWNER 565
#define PARALLEL 566
#define PARSER 567
#define PARTIAL 568
#define PARTITION 569
#define PARTITIONED 570
#define PASSING 571
#define PASSWORD 572
#define PERCENT 573
#define PERSISTENT 574
#define PIVOT 575
#define PIVOT_LONGER 576
#define PIVOT_WIDER 577
#define PLACING 578
#define PLANS 579
#define POLICY 580
#define POSITION 581
#define POSITIONAL 582
#define PRAGMA_P 583
#define PRECEDING 584
#define PRECISION 585
#define PREPARE 586
#define PREPARED 587
#define PRESERVE 588
#define PRIMARY 589
#define PRIOR 590
#define PRIVILEGES 591
#define PROCEDURAL 592
#define PROCEDURE 593
#define PROGRAM 594
#define PUBLICATION 595
#define QUALIFY 596
#define QUARTER_P 597
#define QUARTERS_P 598
#define QUOTE 599
#define RANGE 600
#define READ_P 601
#define REAL 602
#define REASSIGN 603
#define RECHECK 604
#define RECURSIVE 605
#define REF 606
#define REFERENCES 607
#define REFERENCING 608
#define REFRESH 609
#define REINDEX 610
#define RELATIVE_P 611
#define RELEASE 612
#define RENAME 613
#define REPEATABLE 614
#define REPLACE 615
#define REPLICA 616
#define RESET 617
#define RESPECT_P 618
#define RESTART 619
#define RESTRICT 620
#define RETURNING 621
#define RETURNS 622
#define REVOKE 623
#define RIGHT 624
#define ROLE 625
#define ROLLBACK 626
#define ROLLUP 627
#define ROW 628
#define ROWS 629
#define RULE 630
#define SAMPLE 631
#define SAVEPOINT 632
#define SCHEMA 633
#define SCHEMAS 634
#define SCOPE 635
#define SCROLL 636
#define SEARCH 637
#define SECOND_P 638
#define SECONDS_P 639
#define SECRET 640
#define SECURITY 641
#define SELECT 642
#define SEMI 643
#define SEQUENCE 644
#define SEQUENCES 645
#define SERIALIZABLE 646
#define SERVER 647
#define SESSION 648
#define SET 649
#define SETOF 650
#define SETS 651
#define SHARE 652
#define SHOW 653
#define SIMILAR 654
#define SIMPLE 655
#define SKIP 656
#define SMALLINT 657
#define SNAPSHOT 658
#define SOME 659
#define SORTED 660
#define SQL_P 661
#define STABLE 662
#define STANDALONE_P 663
#define START 664
#define STATEMENT 665
#define STATISTICS 666
#define STDIN 667
#define STDOUT 668
#define STORAGE 669
#define STORED 670
#define STRICT_P 671
#define STRIP_P 672
#define STRUCT 673
#define SUBSCRIPTION 674
#define SUBSTRING 675
#define SUMMARIZE 676
#define SYMMETRIC 677
#define SYSID 678
#define SYSTEM_P 679
#define TABLE 680
#define TABLES 681
#define TABLESAMPLE 682
#define TABLESPACE 683
#define TEMP 684
#define TEMPLATE 685
#define TEMPORARY 686
#define TEXT_P 687
#define THEN 688
#define TIES 689
#define TIME 690
#define TIMESTAMP 691
#define TO 692
#define TRAILING 693
#define TRANSACTION 694
#define TRANSFORM 695
#define TREAT 696
#define TRIGGER 697
#define TRIM 698
#define TRUE_P 699
#define TRUNCATE 700
#define TRUSTED 701
#define TRY_CAST 702
#define TYPE_P 703
#define TYPES_P 704
#define UNBOUNDED 705
#define UNCOMMITTED 706
#define UNENCRYPTED 707
#define UNION 708
#define UNIQUE 709
#define UNKNOWN 710
#define UNLISTEN 711
#define UNLOGGED 712
#define UNPACK 713
#define UNPIVOT 714
#define UNTIL 715
#define UPDATE 716
#define USE_P 717
#define USER 718
#define USING 719
#define VACUUM 720
#define VALID 721
#define VALIDATE 722
#define VALIDATOR 723
#define VALUE_P 724
#define VALUES 725
#define VARCHAR 726
#define VARIABLE_P 727
#define VARIADIC 728
#define VARYING 729
#define VERBOSE 730
#define VERSION_P 731
#define VIEW 732
#define VIEWS 733
#define VIRTUAL 734
#define VOLATILE 735
#define WEEK_P 736
#define WEEKS_P 737
#define WHEN 738
#define WHERE 739
#define WHITESPACE_P 740
#define WINDOW 741
#define WITH 742
#define WITHIN 743
#define WITHOUT 744
#define WORK 745
#define WRAPPER 746
#define WRITE_P 747
#define XML_P 748
#define XMLATTRIBUTES 749
#define XMLCONCAT 750
#define XMLELEMENT 751
#define XMLEXISTS 752
#define XMLFOREST 753
#define XMLNAMESPACES 754
#define XMLPARSE 755
#define XMLPI 756
#define XMLROOT 757
#define XMLSERIALIZE 758
#define XMLTABLE 759
#define YEAR_P 760
#define YEARS_P 761
#define YES_P 762
#define ZONE 763
#define NOT_LA 764
#define NULLS_LA 765
#define WITH_LA 766
#define POSTFIXOP 767
#define UMINUS 768




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
}
/* Line 1529 of yacc.c.  */
#line 1126 "third_party/libpg_query/grammar/grammar_out.hpp"
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


