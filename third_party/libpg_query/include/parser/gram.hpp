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
     CHAIN = 323,
     CHAR_P = 324,
     CHARACTER = 325,
     CHARACTERISTICS = 326,
     CHECK_P = 327,
     CHECKPOINT = 328,
     CLASS = 329,
     CLOSE = 330,
     CLUSTER = 331,
     COALESCE = 332,
     COLLATE = 333,
     COLLATION = 334,
     COLUMN = 335,
     COLUMNS = 336,
     COMMENT = 337,
     COMMENTS = 338,
     COMMIT = 339,
     COMMITTED = 340,
     COMPRESSION = 341,
     CONCURRENTLY = 342,
     CONFIGURATION = 343,
     CONFLICT = 344,
     CONNECTION = 345,
     CONSTRAINT = 346,
     CONSTRAINTS = 347,
     CONTENT_P = 348,
     CONTINUE_P = 349,
     CONVERSION_P = 350,
     COPY = 351,
     COST = 352,
     CREATE_P = 353,
     CROSS = 354,
     CSV = 355,
     CUBE = 356,
     CURRENT_P = 357,
     CURSOR = 358,
     CYCLE = 359,
     DATA_P = 360,
     DATABASE = 361,
     DAY_P = 362,
     DAYS_P = 363,
     DEALLOCATE = 364,
     DEC = 365,
     DECIMAL_P = 366,
     DECLARE = 367,
     DEFAULT = 368,
     DEFAULTS = 369,
     DEFERRABLE = 370,
     DEFERRED = 371,
     DEFINER = 372,
     DELETE_P = 373,
     DELIMITER = 374,
     DELIMITERS = 375,
     DEPENDS = 376,
     DESC_P = 377,
     DESCRIBE = 378,
     DETACH = 379,
     DICTIONARY = 380,
     DISABLE_P = 381,
     DISCARD = 382,
     DISTINCT = 383,
     DO = 384,
     DOCUMENT_P = 385,
     DOMAIN_P = 386,
     DOUBLE_P = 387,
     DROP = 388,
     EACH = 389,
     ELSE = 390,
     ENABLE_P = 391,
     ENCODING = 392,
     ENCRYPTED = 393,
     END_P = 394,
     ENUM_P = 395,
     ESCAPE = 396,
     EVENT = 397,
     EXCEPT = 398,
     EXCLUDE = 399,
     EXCLUDING = 400,
     EXCLUSIVE = 401,
     EXECUTE = 402,
     EXISTS = 403,
     EXPLAIN = 404,
     EXPORT_P = 405,
     EXPORT_STATE = 406,
     EXTENSION = 407,
     EXTERNAL = 408,
     EXTRACT = 409,
     FALSE_P = 410,
     FAMILY = 411,
     FETCH = 412,
     FIELD = 413,
     FILTER = 414,
     FIRST_P = 415,
     FLOAT_P = 416,
     FOLLOWING = 417,
     FOR = 418,
     FORCE = 419,
     FOREIGN = 420,
     FORWARD = 421,
     FREEZE = 422,
     FROM = 423,
     FULL = 424,
     FUNCTION = 425,
     FUNCTIONS = 426,
     GENERATED = 427,
     GLOB = 428,
     GLOBAL = 429,
     GRANT = 430,
     GRANTED = 431,
     GROUP_P = 432,
     GROUPING = 433,
     GROUPING_ID = 434,
     HANDLER = 435,
     HAVING = 436,
     HEADER_P = 437,
     HOLD = 438,
     HOUR_P = 439,
     HOURS_P = 440,
     IDENTITY_P = 441,
     IDS = 442,
     IF_P = 443,
     IGNORE_P = 444,
     ILIKE = 445,
     IMMEDIATE = 446,
     IMMUTABLE = 447,
     IMPLICIT_P = 448,
     IMPORT_P = 449,
     IN_P = 450,
     INCLUDE_P = 451,
     INCLUDING = 452,
     INCREMENT = 453,
     INDEX = 454,
     INDEXES = 455,
     INHERIT = 456,
     INHERITS = 457,
     INITIALLY = 458,
     INLINE_P = 459,
     INNER_P = 460,
     INOUT = 461,
     INPUT_P = 462,
     INSENSITIVE = 463,
     INSERT = 464,
     INSTALL = 465,
     INSTEAD = 466,
     INT_P = 467,
     INTEGER = 468,
     INTERSECT = 469,
     INTERVAL = 470,
     INTO = 471,
     INVOKER = 472,
     IS = 473,
     ISNULL = 474,
     ISOLATION = 475,
     JOIN = 476,
     JSON = 477,
     KEY = 478,
     LABEL = 479,
     LANGUAGE = 480,
     LARGE_P = 481,
     LAST_P = 482,
     LATERAL_P = 483,
     LEADING = 484,
     LEAKPROOF = 485,
     LEFT = 486,
     LEVEL = 487,
     LIKE = 488,
     LIMIT = 489,
     LISTEN = 490,
     LOAD = 491,
     LOCAL = 492,
     LOCATION = 493,
     LOCK_P = 494,
     LOCKED = 495,
     LOGGED = 496,
     MACRO = 497,
     MAP = 498,
     MAPPING = 499,
     MATCH = 500,
     MATERIALIZED = 501,
     MAXVALUE = 502,
     METHOD = 503,
     MICROSECOND_P = 504,
     MICROSECONDS_P = 505,
     MILLISECOND_P = 506,
     MILLISECONDS_P = 507,
     MINUTE_P = 508,
     MINUTES_P = 509,
     MINVALUE = 510,
     MODE = 511,
     MONTH_P = 512,
     MONTHS_P = 513,
     MOVE = 514,
     NAME_P = 515,
     NAMES = 516,
     NATIONAL = 517,
     NATURAL = 518,
     NCHAR = 519,
     NEW = 520,
     NEXT = 521,
     NO = 522,
     NONE = 523,
     NOT = 524,
     NOTHING = 525,
     NOTIFY = 526,
     NOTNULL = 527,
     NOWAIT = 528,
     NULL_P = 529,
     NULLIF = 530,
     NULLS_P = 531,
     NUMERIC = 532,
     OBJECT_P = 533,
     OF = 534,
     OFF = 535,
     OFFSET = 536,
     OIDS = 537,
     OLD = 538,
     ON = 539,
     ONLY = 540,
     OPERATOR = 541,
     OPTION = 542,
     OPTIONS = 543,
     OR = 544,
     ORDER = 545,
     ORDINALITY = 546,
     OUT_P = 547,
     OUTER_P = 548,
     OVER = 549,
     OVERLAPS = 550,
     OVERLAY = 551,
     OVERRIDING = 552,
     OWNED = 553,
     OWNER = 554,
     PARALLEL = 555,
     PARSER = 556,
     PARTIAL = 557,
     PARTITION = 558,
     PASSING = 559,
     PASSWORD = 560,
     PERCENT = 561,
     PIVOT = 562,
     PIVOT_LONGER = 563,
     PIVOT_WIDER = 564,
     PLACING = 565,
     PLANS = 566,
     POLICY = 567,
     POSITION = 568,
     POSITIONAL = 569,
     PRAGMA_P = 570,
     PRECEDING = 571,
     PRECISION = 572,
     PREPARE = 573,
     PREPARED = 574,
     PRESERVE = 575,
     PRIMARY = 576,
     PRIOR = 577,
     PRIVILEGES = 578,
     PROCEDURAL = 579,
     PROCEDURE = 580,
     PROGRAM = 581,
     PUBLICATION = 582,
     QUALIFY = 583,
     QUOTE = 584,
     RANGE = 585,
     READ_P = 586,
     REAL = 587,
     REASSIGN = 588,
     RECHECK = 589,
     RECURSIVE = 590,
     REF = 591,
     REFERENCES = 592,
     REFERENCING = 593,
     REFRESH = 594,
     REINDEX = 595,
     RELATIVE_P = 596,
     RELEASE = 597,
     RENAME = 598,
     REPEATABLE = 599,
     REPLACE = 600,
     REPLICA = 601,
     RESET = 602,
     RESPECT_P = 603,
     RESTART = 604,
     RESTRICT = 605,
     RETURNING = 606,
     RETURNS = 607,
     REVOKE = 608,
     RIGHT = 609,
     ROLE = 610,
     ROLLBACK = 611,
     ROLLUP = 612,
     ROW = 613,
     ROWS = 614,
     RULE = 615,
     SAMPLE = 616,
     SAVEPOINT = 617,
     SCHEMA = 618,
     SCHEMAS = 619,
     SCROLL = 620,
     SEARCH = 621,
     SECOND_P = 622,
     SECONDS_P = 623,
     SECURITY = 624,
     SELECT = 625,
     SEMI = 626,
     SEQUENCE = 627,
     SEQUENCES = 628,
     SERIALIZABLE = 629,
     SERVER = 630,
     SESSION = 631,
     SET = 632,
     SETOF = 633,
     SETS = 634,
     SHARE = 635,
     SHOW = 636,
     SIMILAR = 637,
     SIMPLE = 638,
     SKIP = 639,
     SMALLINT = 640,
     SNAPSHOT = 641,
     SOME = 642,
     SQL_P = 643,
     STABLE = 644,
     STANDALONE_P = 645,
     START = 646,
     STATEMENT = 647,
     STATISTICS = 648,
     STDIN = 649,
     STDOUT = 650,
     STORAGE = 651,
     STORED = 652,
     STRICT_P = 653,
     STRIP_P = 654,
     STRUCT = 655,
     SUBSCRIPTION = 656,
     SUBSTRING = 657,
     SUMMARIZE = 658,
     SYMMETRIC = 659,
     SYSID = 660,
     SYSTEM_P = 661,
     TABLE = 662,
     TABLES = 663,
     TABLESAMPLE = 664,
     TABLESPACE = 665,
     TEMP = 666,
     TEMPLATE = 667,
     TEMPORARY = 668,
     TEXT_P = 669,
     THEN = 670,
     TIME = 671,
     TIMESTAMP = 672,
     TO = 673,
     TRAILING = 674,
     TRANSACTION = 675,
     TRANSFORM = 676,
     TREAT = 677,
     TRIGGER = 678,
     TRIM = 679,
     TRUE_P = 680,
     TRUNCATE = 681,
     TRUSTED = 682,
     TRY_CAST = 683,
     TYPE_P = 684,
     TYPES_P = 685,
     UNBOUNDED = 686,
     UNCOMMITTED = 687,
     UNENCRYPTED = 688,
     UNION = 689,
     UNIQUE = 690,
     UNKNOWN = 691,
     UNLISTEN = 692,
     UNLOGGED = 693,
     UNPIVOT = 694,
     UNTIL = 695,
     UPDATE = 696,
     USE_P = 697,
     USER = 698,
     USING = 699,
     VACUUM = 700,
     VALID = 701,
     VALIDATE = 702,
     VALIDATOR = 703,
     VALUE_P = 704,
     VALUES = 705,
     VARCHAR = 706,
     VARIADIC = 707,
     VARYING = 708,
     VERBOSE = 709,
     VERSION_P = 710,
     VIEW = 711,
     VIEWS = 712,
     VIRTUAL = 713,
     VOLATILE = 714,
     WHEN = 715,
     WHERE = 716,
     WHITESPACE_P = 717,
     WINDOW = 718,
     WITH = 719,
     WITHIN = 720,
     WITHOUT = 721,
     WORK = 722,
     WRAPPER = 723,
     WRITE_P = 724,
     XML_P = 725,
     XMLATTRIBUTES = 726,
     XMLCONCAT = 727,
     XMLELEMENT = 728,
     XMLEXISTS = 729,
     XMLFOREST = 730,
     XMLNAMESPACES = 731,
     XMLPARSE = 732,
     XMLPI = 733,
     XMLROOT = 734,
     XMLSERIALIZE = 735,
     XMLTABLE = 736,
     YEAR_P = 737,
     YEARS_P = 738,
     YES_P = 739,
     ZONE = 740,
     NOT_LA = 741,
     NULLS_LA = 742,
     WITH_LA = 743,
     POSTFIXOP = 744,
     UMINUS = 745
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
#define CHAIN 323
#define CHAR_P 324
#define CHARACTER 325
#define CHARACTERISTICS 326
#define CHECK_P 327
#define CHECKPOINT 328
#define CLASS 329
#define CLOSE 330
#define CLUSTER 331
#define COALESCE 332
#define COLLATE 333
#define COLLATION 334
#define COLUMN 335
#define COLUMNS 336
#define COMMENT 337
#define COMMENTS 338
#define COMMIT 339
#define COMMITTED 340
#define COMPRESSION 341
#define CONCURRENTLY 342
#define CONFIGURATION 343
#define CONFLICT 344
#define CONNECTION 345
#define CONSTRAINT 346
#define CONSTRAINTS 347
#define CONTENT_P 348
#define CONTINUE_P 349
#define CONVERSION_P 350
#define COPY 351
#define COST 352
#define CREATE_P 353
#define CROSS 354
#define CSV 355
#define CUBE 356
#define CURRENT_P 357
#define CURSOR 358
#define CYCLE 359
#define DATA_P 360
#define DATABASE 361
#define DAY_P 362
#define DAYS_P 363
#define DEALLOCATE 364
#define DEC 365
#define DECIMAL_P 366
#define DECLARE 367
#define DEFAULT 368
#define DEFAULTS 369
#define DEFERRABLE 370
#define DEFERRED 371
#define DEFINER 372
#define DELETE_P 373
#define DELIMITER 374
#define DELIMITERS 375
#define DEPENDS 376
#define DESC_P 377
#define DESCRIBE 378
#define DETACH 379
#define DICTIONARY 380
#define DISABLE_P 381
#define DISCARD 382
#define DISTINCT 383
#define DO 384
#define DOCUMENT_P 385
#define DOMAIN_P 386
#define DOUBLE_P 387
#define DROP 388
#define EACH 389
#define ELSE 390
#define ENABLE_P 391
#define ENCODING 392
#define ENCRYPTED 393
#define END_P 394
#define ENUM_P 395
#define ESCAPE 396
#define EVENT 397
#define EXCEPT 398
#define EXCLUDE 399
#define EXCLUDING 400
#define EXCLUSIVE 401
#define EXECUTE 402
#define EXISTS 403
#define EXPLAIN 404
#define EXPORT_P 405
#define EXPORT_STATE 406
#define EXTENSION 407
#define EXTERNAL 408
#define EXTRACT 409
#define FALSE_P 410
#define FAMILY 411
#define FETCH 412
#define FIELD 413
#define FILTER 414
#define FIRST_P 415
#define FLOAT_P 416
#define FOLLOWING 417
#define FOR 418
#define FORCE 419
#define FOREIGN 420
#define FORWARD 421
#define FREEZE 422
#define FROM 423
#define FULL 424
#define FUNCTION 425
#define FUNCTIONS 426
#define GENERATED 427
#define GLOB 428
#define GLOBAL 429
#define GRANT 430
#define GRANTED 431
#define GROUP_P 432
#define GROUPING 433
#define GROUPING_ID 434
#define HANDLER 435
#define HAVING 436
#define HEADER_P 437
#define HOLD 438
#define HOUR_P 439
#define HOURS_P 440
#define IDENTITY_P 441
#define IDS 442
#define IF_P 443
#define IGNORE_P 444
#define ILIKE 445
#define IMMEDIATE 446
#define IMMUTABLE 447
#define IMPLICIT_P 448
#define IMPORT_P 449
#define IN_P 450
#define INCLUDE_P 451
#define INCLUDING 452
#define INCREMENT 453
#define INDEX 454
#define INDEXES 455
#define INHERIT 456
#define INHERITS 457
#define INITIALLY 458
#define INLINE_P 459
#define INNER_P 460
#define INOUT 461
#define INPUT_P 462
#define INSENSITIVE 463
#define INSERT 464
#define INSTALL 465
#define INSTEAD 466
#define INT_P 467
#define INTEGER 468
#define INTERSECT 469
#define INTERVAL 470
#define INTO 471
#define INVOKER 472
#define IS 473
#define ISNULL 474
#define ISOLATION 475
#define JOIN 476
#define JSON 477
#define KEY 478
#define LABEL 479
#define LANGUAGE 480
#define LARGE_P 481
#define LAST_P 482
#define LATERAL_P 483
#define LEADING 484
#define LEAKPROOF 485
#define LEFT 486
#define LEVEL 487
#define LIKE 488
#define LIMIT 489
#define LISTEN 490
#define LOAD 491
#define LOCAL 492
#define LOCATION 493
#define LOCK_P 494
#define LOCKED 495
#define LOGGED 496
#define MACRO 497
#define MAP 498
#define MAPPING 499
#define MATCH 500
#define MATERIALIZED 501
#define MAXVALUE 502
#define METHOD 503
#define MICROSECOND_P 504
#define MICROSECONDS_P 505
#define MILLISECOND_P 506
#define MILLISECONDS_P 507
#define MINUTE_P 508
#define MINUTES_P 509
#define MINVALUE 510
#define MODE 511
#define MONTH_P 512
#define MONTHS_P 513
#define MOVE 514
#define NAME_P 515
#define NAMES 516
#define NATIONAL 517
#define NATURAL 518
#define NCHAR 519
#define NEW 520
#define NEXT 521
#define NO 522
#define NONE 523
#define NOT 524
#define NOTHING 525
#define NOTIFY 526
#define NOTNULL 527
#define NOWAIT 528
#define NULL_P 529
#define NULLIF 530
#define NULLS_P 531
#define NUMERIC 532
#define OBJECT_P 533
#define OF 534
#define OFF 535
#define OFFSET 536
#define OIDS 537
#define OLD 538
#define ON 539
#define ONLY 540
#define OPERATOR 541
#define OPTION 542
#define OPTIONS 543
#define OR 544
#define ORDER 545
#define ORDINALITY 546
#define OUT_P 547
#define OUTER_P 548
#define OVER 549
#define OVERLAPS 550
#define OVERLAY 551
#define OVERRIDING 552
#define OWNED 553
#define OWNER 554
#define PARALLEL 555
#define PARSER 556
#define PARTIAL 557
#define PARTITION 558
#define PASSING 559
#define PASSWORD 560
#define PERCENT 561
#define PIVOT 562
#define PIVOT_LONGER 563
#define PIVOT_WIDER 564
#define PLACING 565
#define PLANS 566
#define POLICY 567
#define POSITION 568
#define POSITIONAL 569
#define PRAGMA_P 570
#define PRECEDING 571
#define PRECISION 572
#define PREPARE 573
#define PREPARED 574
#define PRESERVE 575
#define PRIMARY 576
#define PRIOR 577
#define PRIVILEGES 578
#define PROCEDURAL 579
#define PROCEDURE 580
#define PROGRAM 581
#define PUBLICATION 582
#define QUALIFY 583
#define QUOTE 584
#define RANGE 585
#define READ_P 586
#define REAL 587
#define REASSIGN 588
#define RECHECK 589
#define RECURSIVE 590
#define REF 591
#define REFERENCES 592
#define REFERENCING 593
#define REFRESH 594
#define REINDEX 595
#define RELATIVE_P 596
#define RELEASE 597
#define RENAME 598
#define REPEATABLE 599
#define REPLACE 600
#define REPLICA 601
#define RESET 602
#define RESPECT_P 603
#define RESTART 604
#define RESTRICT 605
#define RETURNING 606
#define RETURNS 607
#define REVOKE 608
#define RIGHT 609
#define ROLE 610
#define ROLLBACK 611
#define ROLLUP 612
#define ROW 613
#define ROWS 614
#define RULE 615
#define SAMPLE 616
#define SAVEPOINT 617
#define SCHEMA 618
#define SCHEMAS 619
#define SCROLL 620
#define SEARCH 621
#define SECOND_P 622
#define SECONDS_P 623
#define SECURITY 624
#define SELECT 625
#define SEMI 626
#define SEQUENCE 627
#define SEQUENCES 628
#define SERIALIZABLE 629
#define SERVER 630
#define SESSION 631
#define SET 632
#define SETOF 633
#define SETS 634
#define SHARE 635
#define SHOW 636
#define SIMILAR 637
#define SIMPLE 638
#define SKIP 639
#define SMALLINT 640
#define SNAPSHOT 641
#define SOME 642
#define SQL_P 643
#define STABLE 644
#define STANDALONE_P 645
#define START 646
#define STATEMENT 647
#define STATISTICS 648
#define STDIN 649
#define STDOUT 650
#define STORAGE 651
#define STORED 652
#define STRICT_P 653
#define STRIP_P 654
#define STRUCT 655
#define SUBSCRIPTION 656
#define SUBSTRING 657
#define SUMMARIZE 658
#define SYMMETRIC 659
#define SYSID 660
#define SYSTEM_P 661
#define TABLE 662
#define TABLES 663
#define TABLESAMPLE 664
#define TABLESPACE 665
#define TEMP 666
#define TEMPLATE 667
#define TEMPORARY 668
#define TEXT_P 669
#define THEN 670
#define TIME 671
#define TIMESTAMP 672
#define TO 673
#define TRAILING 674
#define TRANSACTION 675
#define TRANSFORM 676
#define TREAT 677
#define TRIGGER 678
#define TRIM 679
#define TRUE_P 680
#define TRUNCATE 681
#define TRUSTED 682
#define TRY_CAST 683
#define TYPE_P 684
#define TYPES_P 685
#define UNBOUNDED 686
#define UNCOMMITTED 687
#define UNENCRYPTED 688
#define UNION 689
#define UNIQUE 690
#define UNKNOWN 691
#define UNLISTEN 692
#define UNLOGGED 693
#define UNPIVOT 694
#define UNTIL 695
#define UPDATE 696
#define USE_P 697
#define USER 698
#define USING 699
#define VACUUM 700
#define VALID 701
#define VALIDATE 702
#define VALIDATOR 703
#define VALUE_P 704
#define VALUES 705
#define VARCHAR 706
#define VARIADIC 707
#define VARYING 708
#define VERBOSE 709
#define VERSION_P 710
#define VIEW 711
#define VIEWS 712
#define VIRTUAL 713
#define VOLATILE 714
#define WHEN 715
#define WHERE 716
#define WHITESPACE_P 717
#define WINDOW 718
#define WITH 719
#define WITHIN 720
#define WITHOUT 721
#define WORK 722
#define WRAPPER 723
#define WRITE_P 724
#define XML_P 725
#define XMLATTRIBUTES 726
#define XMLCONCAT 727
#define XMLELEMENT 728
#define XMLEXISTS 729
#define XMLFOREST 730
#define XMLNAMESPACES 731
#define XMLPARSE 732
#define XMLPI 733
#define XMLROOT 734
#define XMLSERIALIZE 735
#define XMLTABLE 736
#define YEAR_P 737
#define YEARS_P 738
#define YES_P 739
#define ZONE 740
#define NOT_LA 741
#define NULLS_LA 742
#define WITH_LA 743
#define POSTFIXOP 744
#define UMINUS 745




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
#line 1076 "third_party/libpg_query/grammar/grammar_out.hpp"
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


