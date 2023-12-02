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
     LAMBDA_ARROW = 272,
     DOUBLE_ARROW = 273,
     LESS_EQUALS = 274,
     GREATER_EQUALS = 275,
     NOT_EQUALS = 276,
     ABORT_P = 277,
     ABSOLUTE_P = 278,
     ACCESS = 279,
     ACTION = 280,
     ADD_P = 281,
     ADMIN = 282,
     AFTER = 283,
     AGGREGATE = 284,
     ALL = 285,
     ALSO = 286,
     ALTER = 287,
     ALWAYS = 288,
     ANALYSE = 289,
     ANALYZE = 290,
     AND = 291,
     ANTI = 292,
     ANY = 293,
     ARRAY = 294,
     AS = 295,
     ASC_P = 296,
     ASOF = 297,
     ASSERTION = 298,
     ASSIGNMENT = 299,
     ASYMMETRIC = 300,
     AT = 301,
     ATTACH = 302,
     ATTRIBUTE = 303,
     AUTHORIZATION = 304,
     BACKWARD = 305,
     BEFORE = 306,
     BEGIN_P = 307,
     BETWEEN = 308,
     BIGINT = 309,
     BINARY = 310,
     BIT = 311,
     BOOLEAN_P = 312,
     BOTH = 313,
     BY = 314,
     CACHE = 315,
     CALL_P = 316,
     CALLED = 317,
     CASCADE = 318,
     CASCADED = 319,
     CASE = 320,
     CAST = 321,
     CATALOG_P = 322,
     CENTURIES_P = 323,
     CENTURY_P = 324,
     CHAIN = 325,
     CHAR_P = 326,
     CHARACTER = 327,
     CHARACTERISTICS = 328,
     CHECK_P = 329,
     CHECKPOINT = 330,
     CLASS = 331,
     CLOSE = 332,
     CLUSTER = 333,
     COALESCE = 334,
     COLLATE = 335,
     COLLATION = 336,
     COLUMN = 337,
     COLUMNS = 338,
     COMMENT = 339,
     COMMENTS = 340,
     COMMIT = 341,
     COMMITTED = 342,
     COMPRESSION = 343,
     CONCURRENTLY = 344,
     CONFIGURATION = 345,
     CONFLICT = 346,
     CONNECTION = 347,
     CONSTRAINT = 348,
     CONSTRAINTS = 349,
     CONTENT_P = 350,
     CONTINUE_P = 351,
     CONVERSION_P = 352,
     COPY = 353,
     COST = 354,
     CREATE_P = 355,
     CROSS = 356,
     CSV = 357,
     CUBE = 358,
     CURRENT_P = 359,
     CURSOR = 360,
     CYCLE = 361,
     DATA_P = 362,
     DATABASE = 363,
     DAY_P = 364,
     DAYS_P = 365,
     DEALLOCATE = 366,
     DEC = 367,
     DECADE_P = 368,
     DECADES_P = 369,
     DECIMAL_P = 370,
     DECLARE = 371,
     DEFAULT = 372,
     DEFAULTS = 373,
     DEFERRABLE = 374,
     DEFERRED = 375,
     DEFINER = 376,
     DELETE_P = 377,
     DELIMITER = 378,
     DELIMITERS = 379,
     DEPENDS = 380,
     DESC_P = 381,
     DESCRIBE = 382,
     DETACH = 383,
     DICTIONARY = 384,
     DISABLE_P = 385,
     DISCARD = 386,
     DISTINCT = 387,
     DO = 388,
     DOCUMENT_P = 389,
     DOMAIN_P = 390,
     DOUBLE_P = 391,
     DROP = 392,
     EACH = 393,
     ELSE = 394,
     ENABLE_P = 395,
     ENCODING = 396,
     ENCRYPTED = 397,
     END_P = 398,
     ENUM_P = 399,
     ESCAPE = 400,
     EVENT = 401,
     EXCEPT = 402,
     EXCLUDE = 403,
     EXCLUDING = 404,
     EXCLUSIVE = 405,
     EXECUTE = 406,
     EXISTS = 407,
     EXPLAIN = 408,
     EXPORT_P = 409,
     EXPORT_STATE = 410,
     EXTENSION = 411,
     EXTERNAL = 412,
     EXTRACT = 413,
     FALSE_P = 414,
     FAMILY = 415,
     FETCH = 416,
     FILTER = 417,
     FIRST_P = 418,
     FLOAT_P = 419,
     FOLLOWING = 420,
     FOR = 421,
     FORCE = 422,
     FOREIGN = 423,
     FORWARD = 424,
     FREEZE = 425,
     FROM = 426,
     FULL = 427,
     FUNCTION = 428,
     FUNCTIONS = 429,
     GENERATED = 430,
     GLOB = 431,
     GLOBAL = 432,
     GRANT = 433,
     GRANTED = 434,
     GROUP_P = 435,
     GROUPING = 436,
     GROUPING_ID = 437,
     GROUPS = 438,
     HANDLER = 439,
     HAVING = 440,
     HEADER_P = 441,
     HOLD = 442,
     HOUR_P = 443,
     HOURS_P = 444,
     IDENTITY_P = 445,
     IF_P = 446,
     IGNORE_P = 447,
     ILIKE = 448,
     IMMEDIATE = 449,
     IMMUTABLE = 450,
     IMPLICIT_P = 451,
     IMPORT_P = 452,
     IN_P = 453,
     INCLUDE_P = 454,
     INCLUDING = 455,
     INCREMENT = 456,
     INDEX = 457,
     INDEXES = 458,
     INHERIT = 459,
     INHERITS = 460,
     INITIALLY = 461,
     INLINE_P = 462,
     INNER_P = 463,
     INOUT = 464,
     INPUT_P = 465,
     INSENSITIVE = 466,
     INSERT = 467,
     INSTALL = 468,
     INSTEAD = 469,
     INT_P = 470,
     INTEGER = 471,
     INTERSECT = 472,
     INTERVAL = 473,
     INTO = 474,
     INVOKER = 475,
     IS = 476,
     ISNULL = 477,
     ISOLATION = 478,
     JOIN = 479,
     JSON = 480,
     KEY = 481,
     LABEL = 482,
     LANGUAGE = 483,
     LARGE_P = 484,
     LAST_P = 485,
     LATERAL_P = 486,
     LEADING = 487,
     LEAKPROOF = 488,
     LEFT = 489,
     LEVEL = 490,
     LIKE = 491,
     LIMIT = 492,
     LISTEN = 493,
     LOAD = 494,
     LOCAL = 495,
     LOCATION = 496,
     LOCK_P = 497,
     LOCKED = 498,
     LOGGED = 499,
     MACRO = 500,
     MAP = 501,
     MAPPING = 502,
     MATCH = 503,
     MATERIALIZED = 504,
     MAXVALUE = 505,
     METHOD = 506,
     MICROSECOND_P = 507,
     MICROSECONDS_P = 508,
     MILLENNIA_P = 509,
     MILLENNIUM_P = 510,
     MILLISECOND_P = 511,
     MILLISECONDS_P = 512,
     MINUTE_P = 513,
     MINUTES_P = 514,
     MINVALUE = 515,
     MODE = 516,
     MONTH_P = 517,
     MONTHS_P = 518,
     MOVE = 519,
     NAME_P = 520,
     NAMES = 521,
     NATIONAL = 522,
     NATURAL = 523,
     NCHAR = 524,
     NEW = 525,
     NEXT = 526,
     NO = 527,
     NONE = 528,
     NOT = 529,
     NOTHING = 530,
     NOTIFY = 531,
     NOTNULL = 532,
     NOWAIT = 533,
     NULL_P = 534,
     NULLIF = 535,
     NULLS_P = 536,
     NUMERIC = 537,
     OBJECT_P = 538,
     OF = 539,
     OFF = 540,
     OFFSET = 541,
     OIDS = 542,
     OLD = 543,
     ON = 544,
     ONLY = 545,
     OPERATOR = 546,
     OPTION = 547,
     OPTIONS = 548,
     OR = 549,
     ORDER = 550,
     ORDINALITY = 551,
     OTHERS = 552,
     OUT_P = 553,
     OUTER_P = 554,
     OVER = 555,
     OVERLAPS = 556,
     OVERLAY = 557,
     OVERRIDING = 558,
     OWNED = 559,
     OWNER = 560,
     PARALLEL = 561,
     PARSER = 562,
     PARTIAL = 563,
     PARTITION = 564,
     PASSING = 565,
     PASSWORD = 566,
     PERCENT = 567,
     PIVOT = 568,
     PIVOT_LONGER = 569,
     PIVOT_WIDER = 570,
     PLACING = 571,
     PLANS = 572,
     POLICY = 573,
     POSITION = 574,
     POSITIONAL = 575,
     PRAGMA_P = 576,
     PRECEDING = 577,
     PRECISION = 578,
     PREPARE = 579,
     PREPARED = 580,
     PRESERVE = 581,
     PRIMARY = 582,
     PRIOR = 583,
     PRIVILEGES = 584,
     PROCEDURAL = 585,
     PROCEDURE = 586,
     PROGRAM = 587,
     PUBLICATION = 588,
     QUALIFY = 589,
     QUOTE = 590,
     RANGE = 591,
     READ_P = 592,
     REAL = 593,
     REASSIGN = 594,
     RECHECK = 595,
     RECURSIVE = 596,
     REF = 597,
     REFERENCES = 598,
     REFERENCING = 599,
     REFRESH = 600,
     REINDEX = 601,
     RELATIVE_P = 602,
     RELEASE = 603,
     RENAME = 604,
     REPEATABLE = 605,
     REPLACE = 606,
     REPLICA = 607,
     RESET = 608,
     RESPECT_P = 609,
     RESTART = 610,
     RESTRICT = 611,
     RETURNING = 612,
     RETURNS = 613,
     REVOKE = 614,
     RIGHT = 615,
     ROLE = 616,
     ROLLBACK = 617,
     ROLLUP = 618,
     ROW = 619,
     ROWS = 620,
     RULE = 621,
     SAMPLE = 622,
     SAVEPOINT = 623,
     SCHEMA = 624,
     SCHEMAS = 625,
     SCROLL = 626,
     SEARCH = 627,
     SECOND_P = 628,
     SECONDS_P = 629,
     SECURITY = 630,
     SELECT = 631,
     SEMI = 632,
     SEQUENCE = 633,
     SEQUENCES = 634,
     SERIALIZABLE = 635,
     SERVER = 636,
     SESSION = 637,
     SET = 638,
     SETOF = 639,
     SETS = 640,
     SHARE = 641,
     SHOW = 642,
     SIMILAR = 643,
     SIMPLE = 644,
     SKIP = 645,
     SMALLINT = 646,
     SNAPSHOT = 647,
     SOME = 648,
     SQL_P = 649,
     STABLE = 650,
     STANDALONE_P = 651,
     START = 652,
     STATEMENT = 653,
     STATISTICS = 654,
     STDIN = 655,
     STDOUT = 656,
     STORAGE = 657,
     STORED = 658,
     STRICT_P = 659,
     STRIP_P = 660,
     STRUCT = 661,
     SUBSCRIPTION = 662,
     SUBSTRING = 663,
     SUMMARIZE = 664,
     SYMMETRIC = 665,
     SYSID = 666,
     SYSTEM_P = 667,
     TABLE = 668,
     TABLES = 669,
     TABLESAMPLE = 670,
     TABLESPACE = 671,
     TEMP = 672,
     TEMPLATE = 673,
     TEMPORARY = 674,
     TEXT_P = 675,
     THEN = 676,
     TIES = 677,
     TIME = 678,
     TIMESTAMP = 679,
     TO = 680,
     TRAILING = 681,
     TRANSACTION = 682,
     TRANSFORM = 683,
     TREAT = 684,
     TRIGGER = 685,
     TRIM = 686,
     TRUE_P = 687,
     TRUNCATE = 688,
     TRUSTED = 689,
     TRY_CAST = 690,
     TYPE_P = 691,
     TYPES_P = 692,
     UNBOUNDED = 693,
     UNCOMMITTED = 694,
     UNENCRYPTED = 695,
     UNION = 696,
     UNIQUE = 697,
     UNKNOWN = 698,
     UNLISTEN = 699,
     UNLOGGED = 700,
     UNPIVOT = 701,
     UNTIL = 702,
     UPDATE = 703,
     USE_P = 704,
     USER = 705,
     USING = 706,
     VACUUM = 707,
     VALID = 708,
     VALIDATE = 709,
     VALIDATOR = 710,
     VALUE_P = 711,
     VALUES = 712,
     VARCHAR = 713,
     VARIADIC = 714,
     VARYING = 715,
     VERBOSE = 716,
     VERSION_P = 717,
     VIEW = 718,
     VIEWS = 719,
     VIRTUAL = 720,
     VOLATILE = 721,
     WEEK_P = 722,
     WEEKS_P = 723,
     WHEN = 724,
     WHERE = 725,
     WHITESPACE_P = 726,
     WINDOW = 727,
     WITH = 728,
     WITHIN = 729,
     WITHOUT = 730,
     WORK = 731,
     WRAPPER = 732,
     WRITE_P = 733,
     XML_P = 734,
     XMLATTRIBUTES = 735,
     XMLCONCAT = 736,
     XMLELEMENT = 737,
     XMLEXISTS = 738,
     XMLFOREST = 739,
     XMLNAMESPACES = 740,
     XMLPARSE = 741,
     XMLPI = 742,
     XMLROOT = 743,
     XMLSERIALIZE = 744,
     XMLTABLE = 745,
     YEAR_P = 746,
     YEARS_P = 747,
     YES_P = 748,
     ZONE = 749,
     NOT_LA = 750,
     NULLS_LA = 751,
     WITH_LA = 752,
     POSTFIXOP = 753,
     UMINUS = 754
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
#define LAMBDA_ARROW 272
#define DOUBLE_ARROW 273
#define LESS_EQUALS 274
#define GREATER_EQUALS 275
#define NOT_EQUALS 276
#define ABORT_P 277
#define ABSOLUTE_P 278
#define ACCESS 279
#define ACTION 280
#define ADD_P 281
#define ADMIN 282
#define AFTER 283
#define AGGREGATE 284
#define ALL 285
#define ALSO 286
#define ALTER 287
#define ALWAYS 288
#define ANALYSE 289
#define ANALYZE 290
#define AND 291
#define ANTI 292
#define ANY 293
#define ARRAY 294
#define AS 295
#define ASC_P 296
#define ASOF 297
#define ASSERTION 298
#define ASSIGNMENT 299
#define ASYMMETRIC 300
#define AT 301
#define ATTACH 302
#define ATTRIBUTE 303
#define AUTHORIZATION 304
#define BACKWARD 305
#define BEFORE 306
#define BEGIN_P 307
#define BETWEEN 308
#define BIGINT 309
#define BINARY 310
#define BIT 311
#define BOOLEAN_P 312
#define BOTH 313
#define BY 314
#define CACHE 315
#define CALL_P 316
#define CALLED 317
#define CASCADE 318
#define CASCADED 319
#define CASE 320
#define CAST 321
#define CATALOG_P 322
#define CENTURIES_P 323
#define CENTURY_P 324
#define CHAIN 325
#define CHAR_P 326
#define CHARACTER 327
#define CHARACTERISTICS 328
#define CHECK_P 329
#define CHECKPOINT 330
#define CLASS 331
#define CLOSE 332
#define CLUSTER 333
#define COALESCE 334
#define COLLATE 335
#define COLLATION 336
#define COLUMN 337
#define COLUMNS 338
#define COMMENT 339
#define COMMENTS 340
#define COMMIT 341
#define COMMITTED 342
#define COMPRESSION 343
#define CONCURRENTLY 344
#define CONFIGURATION 345
#define CONFLICT 346
#define CONNECTION 347
#define CONSTRAINT 348
#define CONSTRAINTS 349
#define CONTENT_P 350
#define CONTINUE_P 351
#define CONVERSION_P 352
#define COPY 353
#define COST 354
#define CREATE_P 355
#define CROSS 356
#define CSV 357
#define CUBE 358
#define CURRENT_P 359
#define CURSOR 360
#define CYCLE 361
#define DATA_P 362
#define DATABASE 363
#define DAY_P 364
#define DAYS_P 365
#define DEALLOCATE 366
#define DEC 367
#define DECADE_P 368
#define DECADES_P 369
#define DECIMAL_P 370
#define DECLARE 371
#define DEFAULT 372
#define DEFAULTS 373
#define DEFERRABLE 374
#define DEFERRED 375
#define DEFINER 376
#define DELETE_P 377
#define DELIMITER 378
#define DELIMITERS 379
#define DEPENDS 380
#define DESC_P 381
#define DESCRIBE 382
#define DETACH 383
#define DICTIONARY 384
#define DISABLE_P 385
#define DISCARD 386
#define DISTINCT 387
#define DO 388
#define DOCUMENT_P 389
#define DOMAIN_P 390
#define DOUBLE_P 391
#define DROP 392
#define EACH 393
#define ELSE 394
#define ENABLE_P 395
#define ENCODING 396
#define ENCRYPTED 397
#define END_P 398
#define ENUM_P 399
#define ESCAPE 400
#define EVENT 401
#define EXCEPT 402
#define EXCLUDE 403
#define EXCLUDING 404
#define EXCLUSIVE 405
#define EXECUTE 406
#define EXISTS 407
#define EXPLAIN 408
#define EXPORT_P 409
#define EXPORT_STATE 410
#define EXTENSION 411
#define EXTERNAL 412
#define EXTRACT 413
#define FALSE_P 414
#define FAMILY 415
#define FETCH 416
#define FILTER 417
#define FIRST_P 418
#define FLOAT_P 419
#define FOLLOWING 420
#define FOR 421
#define FORCE 422
#define FOREIGN 423
#define FORWARD 424
#define FREEZE 425
#define FROM 426
#define FULL 427
#define FUNCTION 428
#define FUNCTIONS 429
#define GENERATED 430
#define GLOB 431
#define GLOBAL 432
#define GRANT 433
#define GRANTED 434
#define GROUP_P 435
#define GROUPING 436
#define GROUPING_ID 437
#define GROUPS 438
#define HANDLER 439
#define HAVING 440
#define HEADER_P 441
#define HOLD 442
#define HOUR_P 443
#define HOURS_P 444
#define IDENTITY_P 445
#define IF_P 446
#define IGNORE_P 447
#define ILIKE 448
#define IMMEDIATE 449
#define IMMUTABLE 450
#define IMPLICIT_P 451
#define IMPORT_P 452
#define IN_P 453
#define INCLUDE_P 454
#define INCLUDING 455
#define INCREMENT 456
#define INDEX 457
#define INDEXES 458
#define INHERIT 459
#define INHERITS 460
#define INITIALLY 461
#define INLINE_P 462
#define INNER_P 463
#define INOUT 464
#define INPUT_P 465
#define INSENSITIVE 466
#define INSERT 467
#define INSTALL 468
#define INSTEAD 469
#define INT_P 470
#define INTEGER 471
#define INTERSECT 472
#define INTERVAL 473
#define INTO 474
#define INVOKER 475
#define IS 476
#define ISNULL 477
#define ISOLATION 478
#define JOIN 479
#define JSON 480
#define KEY 481
#define LABEL 482
#define LANGUAGE 483
#define LARGE_P 484
#define LAST_P 485
#define LATERAL_P 486
#define LEADING 487
#define LEAKPROOF 488
#define LEFT 489
#define LEVEL 490
#define LIKE 491
#define LIMIT 492
#define LISTEN 493
#define LOAD 494
#define LOCAL 495
#define LOCATION 496
#define LOCK_P 497
#define LOCKED 498
#define LOGGED 499
#define MACRO 500
#define MAP 501
#define MAPPING 502
#define MATCH 503
#define MATERIALIZED 504
#define MAXVALUE 505
#define METHOD 506
#define MICROSECOND_P 507
#define MICROSECONDS_P 508
#define MILLENNIA_P 509
#define MILLENNIUM_P 510
#define MILLISECOND_P 511
#define MILLISECONDS_P 512
#define MINUTE_P 513
#define MINUTES_P 514
#define MINVALUE 515
#define MODE 516
#define MONTH_P 517
#define MONTHS_P 518
#define MOVE 519
#define NAME_P 520
#define NAMES 521
#define NATIONAL 522
#define NATURAL 523
#define NCHAR 524
#define NEW 525
#define NEXT 526
#define NO 527
#define NONE 528
#define NOT 529
#define NOTHING 530
#define NOTIFY 531
#define NOTNULL 532
#define NOWAIT 533
#define NULL_P 534
#define NULLIF 535
#define NULLS_P 536
#define NUMERIC 537
#define OBJECT_P 538
#define OF 539
#define OFF 540
#define OFFSET 541
#define OIDS 542
#define OLD 543
#define ON 544
#define ONLY 545
#define OPERATOR 546
#define OPTION 547
#define OPTIONS 548
#define OR 549
#define ORDER 550
#define ORDINALITY 551
#define OTHERS 552
#define OUT_P 553
#define OUTER_P 554
#define OVER 555
#define OVERLAPS 556
#define OVERLAY 557
#define OVERRIDING 558
#define OWNED 559
#define OWNER 560
#define PARALLEL 561
#define PARSER 562
#define PARTIAL 563
#define PARTITION 564
#define PASSING 565
#define PASSWORD 566
#define PERCENT 567
#define PIVOT 568
#define PIVOT_LONGER 569
#define PIVOT_WIDER 570
#define PLACING 571
#define PLANS 572
#define POLICY 573
#define POSITION 574
#define POSITIONAL 575
#define PRAGMA_P 576
#define PRECEDING 577
#define PRECISION 578
#define PREPARE 579
#define PREPARED 580
#define PRESERVE 581
#define PRIMARY 582
#define PRIOR 583
#define PRIVILEGES 584
#define PROCEDURAL 585
#define PROCEDURE 586
#define PROGRAM 587
#define PUBLICATION 588
#define QUALIFY 589
#define QUOTE 590
#define RANGE 591
#define READ_P 592
#define REAL 593
#define REASSIGN 594
#define RECHECK 595
#define RECURSIVE 596
#define REF 597
#define REFERENCES 598
#define REFERENCING 599
#define REFRESH 600
#define REINDEX 601
#define RELATIVE_P 602
#define RELEASE 603
#define RENAME 604
#define REPEATABLE 605
#define REPLACE 606
#define REPLICA 607
#define RESET 608
#define RESPECT_P 609
#define RESTART 610
#define RESTRICT 611
#define RETURNING 612
#define RETURNS 613
#define REVOKE 614
#define RIGHT 615
#define ROLE 616
#define ROLLBACK 617
#define ROLLUP 618
#define ROW 619
#define ROWS 620
#define RULE 621
#define SAMPLE 622
#define SAVEPOINT 623
#define SCHEMA 624
#define SCHEMAS 625
#define SCROLL 626
#define SEARCH 627
#define SECOND_P 628
#define SECONDS_P 629
#define SECURITY 630
#define SELECT 631
#define SEMI 632
#define SEQUENCE 633
#define SEQUENCES 634
#define SERIALIZABLE 635
#define SERVER 636
#define SESSION 637
#define SET 638
#define SETOF 639
#define SETS 640
#define SHARE 641
#define SHOW 642
#define SIMILAR 643
#define SIMPLE 644
#define SKIP 645
#define SMALLINT 646
#define SNAPSHOT 647
#define SOME 648
#define SQL_P 649
#define STABLE 650
#define STANDALONE_P 651
#define START 652
#define STATEMENT 653
#define STATISTICS 654
#define STDIN 655
#define STDOUT 656
#define STORAGE 657
#define STORED 658
#define STRICT_P 659
#define STRIP_P 660
#define STRUCT 661
#define SUBSCRIPTION 662
#define SUBSTRING 663
#define SUMMARIZE 664
#define SYMMETRIC 665
#define SYSID 666
#define SYSTEM_P 667
#define TABLE 668
#define TABLES 669
#define TABLESAMPLE 670
#define TABLESPACE 671
#define TEMP 672
#define TEMPLATE 673
#define TEMPORARY 674
#define TEXT_P 675
#define THEN 676
#define TIES 677
#define TIME 678
#define TIMESTAMP 679
#define TO 680
#define TRAILING 681
#define TRANSACTION 682
#define TRANSFORM 683
#define TREAT 684
#define TRIGGER 685
#define TRIM 686
#define TRUE_P 687
#define TRUNCATE 688
#define TRUSTED 689
#define TRY_CAST 690
#define TYPE_P 691
#define TYPES_P 692
#define UNBOUNDED 693
#define UNCOMMITTED 694
#define UNENCRYPTED 695
#define UNION 696
#define UNIQUE 697
#define UNKNOWN 698
#define UNLISTEN 699
#define UNLOGGED 700
#define UNPIVOT 701
#define UNTIL 702
#define UPDATE 703
#define USE_P 704
#define USER 705
#define USING 706
#define VACUUM 707
#define VALID 708
#define VALIDATE 709
#define VALIDATOR 710
#define VALUE_P 711
#define VALUES 712
#define VARCHAR 713
#define VARIADIC 714
#define VARYING 715
#define VERBOSE 716
#define VERSION_P 717
#define VIEW 718
#define VIEWS 719
#define VIRTUAL 720
#define VOLATILE 721
#define WEEK_P 722
#define WEEKS_P 723
#define WHEN 724
#define WHERE 725
#define WHITESPACE_P 726
#define WINDOW 727
#define WITH 728
#define WITHIN 729
#define WITHOUT 730
#define WORK 731
#define WRAPPER 732
#define WRITE_P 733
#define XML_P 734
#define XMLATTRIBUTES 735
#define XMLCONCAT 736
#define XMLELEMENT 737
#define XMLEXISTS 738
#define XMLFOREST 739
#define XMLNAMESPACES 740
#define XMLPARSE 741
#define XMLPI 742
#define XMLROOT 743
#define XMLSERIALIZE 744
#define XMLTABLE 745
#define YEAR_P 746
#define YEARS_P 747
#define YES_P 748
#define ZONE 749
#define NOT_LA 750
#define NULLS_LA 751
#define WITH_LA 752
#define POSTFIXOP 753
#define UMINUS 754




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
	PGConstrType           constr;
	PGLockClauseStrength lockstrength;
	PGLockWaitPolicy lockwaitpolicy;
	PGSubLinkType subquerytype;
	PGViewCheckOption viewcheckoption;
	PGInsertColumnOrder bynameorposition;
}
/* Line 1529 of yacc.c.  */
#line 1095 "third_party/libpg_query/grammar/grammar_out.hpp"
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


