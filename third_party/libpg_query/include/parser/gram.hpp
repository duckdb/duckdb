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
     POWER_OF = 270,
     LAMBDA_ARROW = 271,
     DOUBLE_ARROW = 272,
     LESS_EQUALS = 273,
     GREATER_EQUALS = 274,
     NOT_EQUALS = 275,
     ABORT_P = 276,
     ABSOLUTE_P = 277,
     ACCESS = 278,
     ACTION = 279,
     ADD_P = 280,
     ADMIN = 281,
     AFTER = 282,
     AGGREGATE = 283,
     ALL = 284,
     ALSO = 285,
     ALTER = 286,
     ALWAYS = 287,
     ANALYSE = 288,
     ANALYZE = 289,
     AND = 290,
     ANTI = 291,
     ANY = 292,
     ARRAY = 293,
     AS = 294,
     ASC_P = 295,
     ASOF = 296,
     ASSERTION = 297,
     ASSIGNMENT = 298,
     ASYMMETRIC = 299,
     AT = 300,
     ATTACH = 301,
     ATTRIBUTE = 302,
     AUTHORIZATION = 303,
     BACKWARD = 304,
     BEFORE = 305,
     BEGIN_P = 306,
     BETWEEN = 307,
     BIGINT = 308,
     BINARY = 309,
     BIT = 310,
     BOOLEAN_P = 311,
     BOTH = 312,
     BY = 313,
     CACHE = 314,
     CALL_P = 315,
     CALLED = 316,
     CASCADE = 317,
     CASCADED = 318,
     CASE = 319,
     CAST = 320,
     CATALOG_P = 321,
     CHAIN = 322,
     CHAR_P = 323,
     CHARACTER = 324,
     CHARACTERISTICS = 325,
     CHECK_P = 326,
     CHECKPOINT = 327,
     CLASS = 328,
     CLOSE = 329,
     CLUSTER = 330,
     COALESCE = 331,
     COLLATE = 332,
     COLLATION = 333,
     COLUMN = 334,
     COLUMNS = 335,
     COMMENT = 336,
     COMMENTS = 337,
     COMMIT = 338,
     COMMITTED = 339,
     COMPRESSION = 340,
     CONCURRENTLY = 341,
     CONFIGURATION = 342,
     CONFLICT = 343,
     CONNECTION = 344,
     CONSTRAINT = 345,
     CONSTRAINTS = 346,
     CONTENT_P = 347,
     CONTINUE_P = 348,
     CONVERSION_P = 349,
     COPY = 350,
     COST = 351,
     CREATE_P = 352,
     CROSS = 353,
     CSV = 354,
     CUBE = 355,
     CURRENT_P = 356,
     CURSOR = 357,
     CYCLE = 358,
     DATA_P = 359,
     DATABASE = 360,
     DAY_P = 361,
     DAYS_P = 362,
     DEALLOCATE = 363,
     DEC = 364,
     DECIMAL_P = 365,
     DECLARE = 366,
     DEFAULT = 367,
     DEFAULTS = 368,
     DEFERRABLE = 369,
     DEFERRED = 370,
     DEFINER = 371,
     DELETE_P = 372,
     DELIMITER = 373,
     DELIMITERS = 374,
     DEPENDS = 375,
     DESC_P = 376,
     DESCRIBE = 377,
     DETACH = 378,
     DICTIONARY = 379,
     DISABLE_P = 380,
     DISCARD = 381,
     DISTINCT = 382,
     DO = 383,
     DOCUMENT_P = 384,
     DOMAIN_P = 385,
     DOUBLE_P = 386,
     DROP = 387,
     EACH = 388,
     ELSE = 389,
     ENABLE_P = 390,
     ENCODING = 391,
     ENCRYPTED = 392,
     END_P = 393,
     ENUM_P = 394,
     ESCAPE = 395,
     EVENT = 396,
     EXCEPT = 397,
     EXCLUDE = 398,
     EXCLUDING = 399,
     EXCLUSIVE = 400,
     EXECUTE = 401,
     EXISTS = 402,
     EXPLAIN = 403,
     EXPORT_P = 404,
     EXPORT_STATE = 405,
     EXTENSION = 406,
     EXTERNAL = 407,
     EXTRACT = 408,
     FALSE_P = 409,
     FAMILY = 410,
     FETCH = 411,
     FILTER = 412,
     FIRST_P = 413,
     FLOAT_P = 414,
     FOLLOWING = 415,
     FOR = 416,
     FORCE = 417,
     FOREIGN = 418,
     FORWARD = 419,
     FREEZE = 420,
     FROM = 421,
     FULL = 422,
     FUNCTION = 423,
     FUNCTIONS = 424,
     GENERATED = 425,
     GLOB = 426,
     GLOBAL = 427,
     GRANT = 428,
     GRANTED = 429,
     GROUP_P = 430,
     GROUPING = 431,
     GROUPING_ID = 432,
     HANDLER = 433,
     HAVING = 434,
     HEADER_P = 435,
     HOLD = 436,
     HOUR_P = 437,
     HOURS_P = 438,
     IDENTITY_P = 439,
     IF_P = 440,
     IGNORE_P = 441,
     ILIKE = 442,
     IMMEDIATE = 443,
     IMMUTABLE = 444,
     IMPLICIT_P = 445,
     IMPORT_P = 446,
     IN_P = 447,
     INCLUDE_P = 448,
     INCLUDING = 449,
     INCREMENT = 450,
     INDEX = 451,
     INDEXES = 452,
     INHERIT = 453,
     INHERITS = 454,
     INITIALLY = 455,
     INLINE_P = 456,
     INNER_P = 457,
     INOUT = 458,
     INPUT_P = 459,
     INSENSITIVE = 460,
     INSERT = 461,
     INSTALL = 462,
     INSTEAD = 463,
     INT_P = 464,
     INTEGER = 465,
     INTERSECT = 466,
     INTERVAL = 467,
     INTO = 468,
     INVOKER = 469,
     IS = 470,
     ISNULL = 471,
     ISOLATION = 472,
     JOIN = 473,
     JSON = 474,
     KEY = 475,
     LABEL = 476,
     LANGUAGE = 477,
     LARGE_P = 478,
     LAST_P = 479,
     LATERAL_P = 480,
     LEADING = 481,
     LEAKPROOF = 482,
     LEFT = 483,
     LEVEL = 484,
     LIKE = 485,
     LIMIT = 486,
     LISTEN = 487,
     LOAD = 488,
     LOCAL = 489,
     LOCATION = 490,
     LOCK_P = 491,
     LOCKED = 492,
     LOGGED = 493,
     MACRO = 494,
     MAP = 495,
     MAPPING = 496,
     MATCH = 497,
     MATERIALIZED = 498,
     MAXVALUE = 499,
     METHOD = 500,
     MICROSECOND_P = 501,
     MICROSECONDS_P = 502,
     MILLISECOND_P = 503,
     MILLISECONDS_P = 504,
     MINUTE_P = 505,
     MINUTES_P = 506,
     MINVALUE = 507,
     MODE = 508,
     MONTH_P = 509,
     MONTHS_P = 510,
     MOVE = 511,
     NAME_P = 512,
     NAMES = 513,
     NATIONAL = 514,
     NATURAL = 515,
     NCHAR = 516,
     NEW = 517,
     NEXT = 518,
     NO = 519,
     NONE = 520,
     NOT = 521,
     NOTHING = 522,
     NOTIFY = 523,
     NOTNULL = 524,
     NOWAIT = 525,
     NULL_P = 526,
     NULLIF = 527,
     NULLS_P = 528,
     NUMERIC = 529,
     OBJECT_P = 530,
     OF = 531,
     OFF = 532,
     OFFSET = 533,
     OIDS = 534,
     OLD = 535,
     ON = 536,
     ONLY = 537,
     OPERATOR = 538,
     OPTION = 539,
     OPTIONS = 540,
     OR = 541,
     ORDER = 542,
     ORDINALITY = 543,
     OUT_P = 544,
     OUTER_P = 545,
     OVER = 546,
     OVERLAPS = 547,
     OVERLAY = 548,
     OVERRIDING = 549,
     OWNED = 550,
     OWNER = 551,
     PARALLEL = 552,
     PARSER = 553,
     PARTIAL = 554,
     PARTITION = 555,
     PASSING = 556,
     PASSWORD = 557,
     PERCENT = 558,
     PIVOT = 559,
     PIVOT_LONGER = 560,
     PIVOT_WIDER = 561,
     PLACING = 562,
     PLANS = 563,
     POLICY = 564,
     POSITION = 565,
     POSITIONAL = 566,
     PRAGMA_P = 567,
     PRECEDING = 568,
     PRECISION = 569,
     PREPARE = 570,
     PREPARED = 571,
     PRESERVE = 572,
     PRIMARY = 573,
     PRIOR = 574,
     PRIVILEGES = 575,
     PROCEDURAL = 576,
     PROCEDURE = 577,
     PROGRAM = 578,
     PUBLICATION = 579,
     QUALIFY = 580,
     QUOTE = 581,
     RANGE = 582,
     READ_P = 583,
     REAL = 584,
     REASSIGN = 585,
     RECHECK = 586,
     RECURSIVE = 587,
     REF = 588,
     REFERENCES = 589,
     REFERENCING = 590,
     REFRESH = 591,
     REINDEX = 592,
     RELATIVE_P = 593,
     RELEASE = 594,
     RENAME = 595,
     REPEATABLE = 596,
     REPLACE = 597,
     REPLICA = 598,
     RESET = 599,
     RESPECT_P = 600,
     RESTART = 601,
     RESTRICT = 602,
     RETURNING = 603,
     RETURNS = 604,
     REVOKE = 605,
     RIGHT = 606,
     ROLE = 607,
     ROLLBACK = 608,
     ROLLUP = 609,
     ROW = 610,
     ROWS = 611,
     RULE = 612,
     SAMPLE = 613,
     SAVEPOINT = 614,
     SCHEMA = 615,
     SCHEMAS = 616,
     SCROLL = 617,
     SEARCH = 618,
     SECOND_P = 619,
     SECONDS_P = 620,
     SECURITY = 621,
     SELECT = 622,
     SEMI = 623,
     SEQUENCE = 624,
     SEQUENCES = 625,
     SERIALIZABLE = 626,
     SERVER = 627,
     SESSION = 628,
     SET = 629,
     SETOF = 630,
     SETS = 631,
     SHARE = 632,
     SHOW = 633,
     SIMILAR = 634,
     SIMPLE = 635,
     SKIP = 636,
     SMALLINT = 637,
     SNAPSHOT = 638,
     SOME = 639,
     SQL_P = 640,
     STABLE = 641,
     STANDALONE_P = 642,
     START = 643,
     STATEMENT = 644,
     STATISTICS = 645,
     STDIN = 646,
     STDOUT = 647,
     STORAGE = 648,
     STORED = 649,
     STRICT_P = 650,
     STRIP_P = 651,
     STRUCT = 652,
     SUBSCRIPTION = 653,
     SUBSTRING = 654,
     SUMMARIZE = 655,
     SYMMETRIC = 656,
     SYSID = 657,
     SYSTEM_P = 658,
     TABLE = 659,
     TABLES = 660,
     TABLESAMPLE = 661,
     TABLESPACE = 662,
     TEMP = 663,
     TEMPLATE = 664,
     TEMPORARY = 665,
     TEXT_P = 666,
     THEN = 667,
     TIME = 668,
     TIMESTAMP = 669,
     TO = 670,
     TRAILING = 671,
     TRANSACTION = 672,
     TRANSFORM = 673,
     TREAT = 674,
     TRIGGER = 675,
     TRIM = 676,
     TRUE_P = 677,
     TRUNCATE = 678,
     TRUSTED = 679,
     TRY_CAST = 680,
     TYPE_P = 681,
     TYPES_P = 682,
     UNBOUNDED = 683,
     UNCOMMITTED = 684,
     UNENCRYPTED = 685,
     UNION = 686,
     UNIQUE = 687,
     UNKNOWN = 688,
     UNLISTEN = 689,
     UNLOGGED = 690,
     UNPIVOT = 691,
     UNTIL = 692,
     UPDATE = 693,
     USE_P = 694,
     USER = 695,
     USING = 696,
     VACUUM = 697,
     VALID = 698,
     VALIDATE = 699,
     VALIDATOR = 700,
     VALUE_P = 701,
     VALUES = 702,
     VARCHAR = 703,
     VARIADIC = 704,
     VARYING = 705,
     VERBOSE = 706,
     VERSION_P = 707,
     VIEW = 708,
     VIEWS = 709,
     VIRTUAL = 710,
     VOLATILE = 711,
     WHEN = 712,
     WHERE = 713,
     WHITESPACE_P = 714,
     WINDOW = 715,
     WITH = 716,
     WITHIN = 717,
     WITHOUT = 718,
     WORK = 719,
     WRAPPER = 720,
     WRITE_P = 721,
     XML_P = 722,
     XMLATTRIBUTES = 723,
     XMLCONCAT = 724,
     XMLELEMENT = 725,
     XMLEXISTS = 726,
     XMLFOREST = 727,
     XMLNAMESPACES = 728,
     XMLPARSE = 729,
     XMLPI = 730,
     XMLROOT = 731,
     XMLSERIALIZE = 732,
     XMLTABLE = 733,
     YEAR_P = 734,
     YEARS_P = 735,
     YES_P = 736,
     ZONE = 737,
     NOT_LA = 738,
     NULLS_LA = 739,
     WITH_LA = 740,
     POSTFIXOP = 741,
     UMINUS = 742
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
#define POWER_OF 270
#define LAMBDA_ARROW 271
#define DOUBLE_ARROW 272
#define LESS_EQUALS 273
#define GREATER_EQUALS 274
#define NOT_EQUALS 275
#define ABORT_P 276
#define ABSOLUTE_P 277
#define ACCESS 278
#define ACTION 279
#define ADD_P 280
#define ADMIN 281
#define AFTER 282
#define AGGREGATE 283
#define ALL 284
#define ALSO 285
#define ALTER 286
#define ALWAYS 287
#define ANALYSE 288
#define ANALYZE 289
#define AND 290
#define ANTI 291
#define ANY 292
#define ARRAY 293
#define AS 294
#define ASC_P 295
#define ASOF 296
#define ASSERTION 297
#define ASSIGNMENT 298
#define ASYMMETRIC 299
#define AT 300
#define ATTACH 301
#define ATTRIBUTE 302
#define AUTHORIZATION 303
#define BACKWARD 304
#define BEFORE 305
#define BEGIN_P 306
#define BETWEEN 307
#define BIGINT 308
#define BINARY 309
#define BIT 310
#define BOOLEAN_P 311
#define BOTH 312
#define BY 313
#define CACHE 314
#define CALL_P 315
#define CALLED 316
#define CASCADE 317
#define CASCADED 318
#define CASE 319
#define CAST 320
#define CATALOG_P 321
#define CHAIN 322
#define CHAR_P 323
#define CHARACTER 324
#define CHARACTERISTICS 325
#define CHECK_P 326
#define CHECKPOINT 327
#define CLASS 328
#define CLOSE 329
#define CLUSTER 330
#define COALESCE 331
#define COLLATE 332
#define COLLATION 333
#define COLUMN 334
#define COLUMNS 335
#define COMMENT 336
#define COMMENTS 337
#define COMMIT 338
#define COMMITTED 339
#define COMPRESSION 340
#define CONCURRENTLY 341
#define CONFIGURATION 342
#define CONFLICT 343
#define CONNECTION 344
#define CONSTRAINT 345
#define CONSTRAINTS 346
#define CONTENT_P 347
#define CONTINUE_P 348
#define CONVERSION_P 349
#define COPY 350
#define COST 351
#define CREATE_P 352
#define CROSS 353
#define CSV 354
#define CUBE 355
#define CURRENT_P 356
#define CURSOR 357
#define CYCLE 358
#define DATA_P 359
#define DATABASE 360
#define DAY_P 361
#define DAYS_P 362
#define DEALLOCATE 363
#define DEC 364
#define DECIMAL_P 365
#define DECLARE 366
#define DEFAULT 367
#define DEFAULTS 368
#define DEFERRABLE 369
#define DEFERRED 370
#define DEFINER 371
#define DELETE_P 372
#define DELIMITER 373
#define DELIMITERS 374
#define DEPENDS 375
#define DESC_P 376
#define DESCRIBE 377
#define DETACH 378
#define DICTIONARY 379
#define DISABLE_P 380
#define DISCARD 381
#define DISTINCT 382
#define DO 383
#define DOCUMENT_P 384
#define DOMAIN_P 385
#define DOUBLE_P 386
#define DROP 387
#define EACH 388
#define ELSE 389
#define ENABLE_P 390
#define ENCODING 391
#define ENCRYPTED 392
#define END_P 393
#define ENUM_P 394
#define ESCAPE 395
#define EVENT 396
#define EXCEPT 397
#define EXCLUDE 398
#define EXCLUDING 399
#define EXCLUSIVE 400
#define EXECUTE 401
#define EXISTS 402
#define EXPLAIN 403
#define EXPORT_P 404
#define EXPORT_STATE 405
#define EXTENSION 406
#define EXTERNAL 407
#define EXTRACT 408
#define FALSE_P 409
#define FAMILY 410
#define FETCH 411
#define FILTER 412
#define FIRST_P 413
#define FLOAT_P 414
#define FOLLOWING 415
#define FOR 416
#define FORCE 417
#define FOREIGN 418
#define FORWARD 419
#define FREEZE 420
#define FROM 421
#define FULL 422
#define FUNCTION 423
#define FUNCTIONS 424
#define GENERATED 425
#define GLOB 426
#define GLOBAL 427
#define GRANT 428
#define GRANTED 429
#define GROUP_P 430
#define GROUPING 431
#define GROUPING_ID 432
#define HANDLER 433
#define HAVING 434
#define HEADER_P 435
#define HOLD 436
#define HOUR_P 437
#define HOURS_P 438
#define IDENTITY_P 439
#define IF_P 440
#define IGNORE_P 441
#define ILIKE 442
#define IMMEDIATE 443
#define IMMUTABLE 444
#define IMPLICIT_P 445
#define IMPORT_P 446
#define IN_P 447
#define INCLUDE_P 448
#define INCLUDING 449
#define INCREMENT 450
#define INDEX 451
#define INDEXES 452
#define INHERIT 453
#define INHERITS 454
#define INITIALLY 455
#define INLINE_P 456
#define INNER_P 457
#define INOUT 458
#define INPUT_P 459
#define INSENSITIVE 460
#define INSERT 461
#define INSTALL 462
#define INSTEAD 463
#define INT_P 464
#define INTEGER 465
#define INTERSECT 466
#define INTERVAL 467
#define INTO 468
#define INVOKER 469
#define IS 470
#define ISNULL 471
#define ISOLATION 472
#define JOIN 473
#define JSON 474
#define KEY 475
#define LABEL 476
#define LANGUAGE 477
#define LARGE_P 478
#define LAST_P 479
#define LATERAL_P 480
#define LEADING 481
#define LEAKPROOF 482
#define LEFT 483
#define LEVEL 484
#define LIKE 485
#define LIMIT 486
#define LISTEN 487
#define LOAD 488
#define LOCAL 489
#define LOCATION 490
#define LOCK_P 491
#define LOCKED 492
#define LOGGED 493
#define MACRO 494
#define MAP 495
#define MAPPING 496
#define MATCH 497
#define MATERIALIZED 498
#define MAXVALUE 499
#define METHOD 500
#define MICROSECOND_P 501
#define MICROSECONDS_P 502
#define MILLISECOND_P 503
#define MILLISECONDS_P 504
#define MINUTE_P 505
#define MINUTES_P 506
#define MINVALUE 507
#define MODE 508
#define MONTH_P 509
#define MONTHS_P 510
#define MOVE 511
#define NAME_P 512
#define NAMES 513
#define NATIONAL 514
#define NATURAL 515
#define NCHAR 516
#define NEW 517
#define NEXT 518
#define NO 519
#define NONE 520
#define NOT 521
#define NOTHING 522
#define NOTIFY 523
#define NOTNULL 524
#define NOWAIT 525
#define NULL_P 526
#define NULLIF 527
#define NULLS_P 528
#define NUMERIC 529
#define OBJECT_P 530
#define OF 531
#define OFF 532
#define OFFSET 533
#define OIDS 534
#define OLD 535
#define ON 536
#define ONLY 537
#define OPERATOR 538
#define OPTION 539
#define OPTIONS 540
#define OR 541
#define ORDER 542
#define ORDINALITY 543
#define OUT_P 544
#define OUTER_P 545
#define OVER 546
#define OVERLAPS 547
#define OVERLAY 548
#define OVERRIDING 549
#define OWNED 550
#define OWNER 551
#define PARALLEL 552
#define PARSER 553
#define PARTIAL 554
#define PARTITION 555
#define PASSING 556
#define PASSWORD 557
#define PERCENT 558
#define PIVOT 559
#define PIVOT_LONGER 560
#define PIVOT_WIDER 561
#define PLACING 562
#define PLANS 563
#define POLICY 564
#define POSITION 565
#define POSITIONAL 566
#define PRAGMA_P 567
#define PRECEDING 568
#define PRECISION 569
#define PREPARE 570
#define PREPARED 571
#define PRESERVE 572
#define PRIMARY 573
#define PRIOR 574
#define PRIVILEGES 575
#define PROCEDURAL 576
#define PROCEDURE 577
#define PROGRAM 578
#define PUBLICATION 579
#define QUALIFY 580
#define QUOTE 581
#define RANGE 582
#define READ_P 583
#define REAL 584
#define REASSIGN 585
#define RECHECK 586
#define RECURSIVE 587
#define REF 588
#define REFERENCES 589
#define REFERENCING 590
#define REFRESH 591
#define REINDEX 592
#define RELATIVE_P 593
#define RELEASE 594
#define RENAME 595
#define REPEATABLE 596
#define REPLACE 597
#define REPLICA 598
#define RESET 599
#define RESPECT_P 600
#define RESTART 601
#define RESTRICT 602
#define RETURNING 603
#define RETURNS 604
#define REVOKE 605
#define RIGHT 606
#define ROLE 607
#define ROLLBACK 608
#define ROLLUP 609
#define ROW 610
#define ROWS 611
#define RULE 612
#define SAMPLE 613
#define SAVEPOINT 614
#define SCHEMA 615
#define SCHEMAS 616
#define SCROLL 617
#define SEARCH 618
#define SECOND_P 619
#define SECONDS_P 620
#define SECURITY 621
#define SELECT 622
#define SEMI 623
#define SEQUENCE 624
#define SEQUENCES 625
#define SERIALIZABLE 626
#define SERVER 627
#define SESSION 628
#define SET 629
#define SETOF 630
#define SETS 631
#define SHARE 632
#define SHOW 633
#define SIMILAR 634
#define SIMPLE 635
#define SKIP 636
#define SMALLINT 637
#define SNAPSHOT 638
#define SOME 639
#define SQL_P 640
#define STABLE 641
#define STANDALONE_P 642
#define START 643
#define STATEMENT 644
#define STATISTICS 645
#define STDIN 646
#define STDOUT 647
#define STORAGE 648
#define STORED 649
#define STRICT_P 650
#define STRIP_P 651
#define STRUCT 652
#define SUBSCRIPTION 653
#define SUBSTRING 654
#define SUMMARIZE 655
#define SYMMETRIC 656
#define SYSID 657
#define SYSTEM_P 658
#define TABLE 659
#define TABLES 660
#define TABLESAMPLE 661
#define TABLESPACE 662
#define TEMP 663
#define TEMPLATE 664
#define TEMPORARY 665
#define TEXT_P 666
#define THEN 667
#define TIME 668
#define TIMESTAMP 669
#define TO 670
#define TRAILING 671
#define TRANSACTION 672
#define TRANSFORM 673
#define TREAT 674
#define TRIGGER 675
#define TRIM 676
#define TRUE_P 677
#define TRUNCATE 678
#define TRUSTED 679
#define TRY_CAST 680
#define TYPE_P 681
#define TYPES_P 682
#define UNBOUNDED 683
#define UNCOMMITTED 684
#define UNENCRYPTED 685
#define UNION 686
#define UNIQUE 687
#define UNKNOWN 688
#define UNLISTEN 689
#define UNLOGGED 690
#define UNPIVOT 691
#define UNTIL 692
#define UPDATE 693
#define USE_P 694
#define USER 695
#define USING 696
#define VACUUM 697
#define VALID 698
#define VALIDATE 699
#define VALIDATOR 700
#define VALUE_P 701
#define VALUES 702
#define VARCHAR 703
#define VARIADIC 704
#define VARYING 705
#define VERBOSE 706
#define VERSION_P 707
#define VIEW 708
#define VIEWS 709
#define VIRTUAL 710
#define VOLATILE 711
#define WHEN 712
#define WHERE 713
#define WHITESPACE_P 714
#define WINDOW 715
#define WITH 716
#define WITHIN 717
#define WITHOUT 718
#define WORK 719
#define WRAPPER 720
#define WRITE_P 721
#define XML_P 722
#define XMLATTRIBUTES 723
#define XMLCONCAT 724
#define XMLELEMENT 725
#define XMLEXISTS 726
#define XMLFOREST 727
#define XMLNAMESPACES 728
#define XMLPARSE 729
#define XMLPI 730
#define XMLROOT 731
#define XMLSERIALIZE 732
#define XMLTABLE 733
#define YEAR_P 734
#define YEARS_P 735
#define YES_P 736
#define ZONE 737
#define NOT_LA 738
#define NULLS_LA 739
#define WITH_LA 740
#define POSTFIXOP 741
#define UMINUS 742




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
}
/* Line 1529 of yacc.c.  */
#line 1069 "third_party/libpg_query/grammar/grammar_out.hpp"
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


