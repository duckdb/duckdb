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
     FILTER = 413,
     FIRST_P = 414,
     FLOAT_P = 415,
     FOLLOWING = 416,
     FOR = 417,
     FORCE = 418,
     FOREIGN = 419,
     FORWARD = 420,
     FREEZE = 421,
     FROM = 422,
     FULL = 423,
     FUNCTION = 424,
     FUNCTIONS = 425,
     GENERATED = 426,
     GLOB = 427,
     GLOBAL = 428,
     GRANT = 429,
     GRANTED = 430,
     GROUP_P = 431,
     GROUPING = 432,
     GROUPING_ID = 433,
     HANDLER = 434,
     HAVING = 435,
     HEADER_P = 436,
     HOLD = 437,
     HOUR_P = 438,
     HOURS_P = 439,
     IDENTITY_P = 440,
     IF_P = 441,
     IGNORE_P = 442,
     ILIKE = 443,
     IMMEDIATE = 444,
     IMMUTABLE = 445,
     IMPLICIT_P = 446,
     IMPORT_P = 447,
     IN_P = 448,
     INCLUDE_P = 449,
     INCLUDING = 450,
     INCREMENT = 451,
     INDEX = 452,
     INDEXES = 453,
     INHERIT = 454,
     INHERITS = 455,
     INITIALLY = 456,
     INLINE_P = 457,
     INNER_P = 458,
     INOUT = 459,
     INPUT_P = 460,
     INSENSITIVE = 461,
     INSERT = 462,
     INSTALL = 463,
     INSTEAD = 464,
     INT_P = 465,
     INTEGER = 466,
     INTERSECT = 467,
     INTERVAL = 468,
     INTO = 469,
     INVOKER = 470,
     IS = 471,
     ISNULL = 472,
     ISOLATION = 473,
     JOIN = 474,
     JSON = 475,
     KEY = 476,
     LABEL = 477,
     LANGUAGE = 478,
     LARGE_P = 479,
     LAST_P = 480,
     LATERAL_P = 481,
     LEADING = 482,
     LEAKPROOF = 483,
     LEFT = 484,
     LEVEL = 485,
     LIKE = 486,
     LIMIT = 487,
     LISTEN = 488,
     LOAD = 489,
     LOCAL = 490,
     LOCATION = 491,
     LOCK_P = 492,
     LOCKED = 493,
     LOGGED = 494,
     MACRO = 495,
     MAP = 496,
     MAPPING = 497,
     MATCH = 498,
     MATERIALIZED = 499,
     MAXVALUE = 500,
     METHOD = 501,
     MICROSECOND_P = 502,
     MICROSECONDS_P = 503,
     MILLISECOND_P = 504,
     MILLISECONDS_P = 505,
     MINUTE_P = 506,
     MINUTES_P = 507,
     MINVALUE = 508,
     MODE = 509,
     MONTH_P = 510,
     MONTHS_P = 511,
     MOVE = 512,
     NAME_P = 513,
     NAMES = 514,
     NATIONAL = 515,
     NATURAL = 516,
     NCHAR = 517,
     NEW = 518,
     NEXT = 519,
     NO = 520,
     NONE = 521,
     NOT = 522,
     NOTHING = 523,
     NOTIFY = 524,
     NOTNULL = 525,
     NOWAIT = 526,
     NULL_P = 527,
     NULLIF = 528,
     NULLS_P = 529,
     NUMERIC = 530,
     OBJECT_P = 531,
     OF = 532,
     OFF = 533,
     OFFSET = 534,
     OIDS = 535,
     OLD = 536,
     ON = 537,
     ONLY = 538,
     OPERATOR = 539,
     OPTION = 540,
     OPTIONS = 541,
     OR = 542,
     ORDER = 543,
     ORDINALITY = 544,
     OUT_P = 545,
     OUTER_P = 546,
     OVER = 547,
     OVERLAPS = 548,
     OVERLAY = 549,
     OVERRIDING = 550,
     OWNED = 551,
     OWNER = 552,
     PARALLEL = 553,
     PARSER = 554,
     PARTIAL = 555,
     PARTITION = 556,
     PASSING = 557,
     PASSWORD = 558,
     PERCENT = 559,
     PIVOT = 560,
     PIVOT_LONGER = 561,
     PIVOT_WIDER = 562,
     PLACING = 563,
     PLANS = 564,
     POLICY = 565,
     POSITION = 566,
     POSITIONAL = 567,
     PRAGMA_P = 568,
     PRECEDING = 569,
     PRECISION = 570,
     PREPARE = 571,
     PREPARED = 572,
     PRESERVE = 573,
     PRIMARY = 574,
     PRIOR = 575,
     PRIVILEGES = 576,
     PROCEDURAL = 577,
     PROCEDURE = 578,
     PROGRAM = 579,
     PUBLICATION = 580,
     QUALIFY = 581,
     QUOTE = 582,
     RANGE = 583,
     READ_P = 584,
     REAL = 585,
     REASSIGN = 586,
     RECHECK = 587,
     RECURSIVE = 588,
     REF = 589,
     REFERENCES = 590,
     REFERENCING = 591,
     REFRESH = 592,
     REINDEX = 593,
     RELATIVE_P = 594,
     RELEASE = 595,
     RENAME = 596,
     REPEATABLE = 597,
     REPLACE = 598,
     REPLICA = 599,
     RESET = 600,
     RESPECT_P = 601,
     RESTART = 602,
     RESTRICT = 603,
     RETURNING = 604,
     RETURNS = 605,
     REVOKE = 606,
     RIGHT = 607,
     ROLE = 608,
     ROLLBACK = 609,
     ROLLUP = 610,
     ROW = 611,
     ROWS = 612,
     RULE = 613,
     SAMPLE = 614,
     SAVEPOINT = 615,
     SCHEMA = 616,
     SCHEMAS = 617,
     SCROLL = 618,
     SEARCH = 619,
     SECOND_P = 620,
     SECONDS_P = 621,
     SECURITY = 622,
     SELECT = 623,
     SEMI = 624,
     SEQUENCE = 625,
     SEQUENCES = 626,
     SERIALIZABLE = 627,
     SERVER = 628,
     SESSION = 629,
     SET = 630,
     SETOF = 631,
     SETS = 632,
     SHARE = 633,
     SHOW = 634,
     SIMILAR = 635,
     SIMPLE = 636,
     SKIP = 637,
     SMALLINT = 638,
     SNAPSHOT = 639,
     SOME = 640,
     SQL_P = 641,
     STABLE = 642,
     STANDALONE_P = 643,
     START = 644,
     STATEMENT = 645,
     STATISTICS = 646,
     STDIN = 647,
     STDOUT = 648,
     STORAGE = 649,
     STORED = 650,
     STRICT_P = 651,
     STRIP_P = 652,
     STRUCT = 653,
     SUBSCRIPTION = 654,
     SUBSTRING = 655,
     SUMMARIZE = 656,
     SYMMETRIC = 657,
     SYSID = 658,
     SYSTEM_P = 659,
     TABLE = 660,
     TABLES = 661,
     TABLESAMPLE = 662,
     TABLESPACE = 663,
     TEMP = 664,
     TEMPLATE = 665,
     TEMPORARY = 666,
     TEXT_P = 667,
     THEN = 668,
     TIME = 669,
     TIMESTAMP = 670,
     TO = 671,
     TRAILING = 672,
     TRANSACTION = 673,
     TRANSFORM = 674,
     TREAT = 675,
     TRIGGER = 676,
     TRIM = 677,
     TRUE_P = 678,
     TRUNCATE = 679,
     TRUSTED = 680,
     TRY_CAST = 681,
     TYPE_P = 682,
     TYPES_P = 683,
     UNBOUNDED = 684,
     UNCOMMITTED = 685,
     UNENCRYPTED = 686,
     UNION = 687,
     UNIQUE = 688,
     UNKNOWN = 689,
     UNLISTEN = 690,
     UNLOGGED = 691,
     UNPIVOT = 692,
     UNTIL = 693,
     UPDATE = 694,
     USE_P = 695,
     USER = 696,
     USING = 697,
     VACUUM = 698,
     VALID = 699,
     VALIDATE = 700,
     VALIDATOR = 701,
     VALUE_P = 702,
     VALUES = 703,
     VARCHAR = 704,
     VARIADIC = 705,
     VARYING = 706,
     VERBOSE = 707,
     VERSION_P = 708,
     VIEW = 709,
     VIEWS = 710,
     VIRTUAL = 711,
     VOLATILE = 712,
     WHEN = 713,
     WHERE = 714,
     WHITESPACE_P = 715,
     WINDOW = 716,
     WITH = 717,
     WITHIN = 718,
     WITHOUT = 719,
     WORK = 720,
     WRAPPER = 721,
     WRITE_P = 722,
     XML_P = 723,
     XMLATTRIBUTES = 724,
     XMLCONCAT = 725,
     XMLELEMENT = 726,
     XMLEXISTS = 727,
     XMLFOREST = 728,
     XMLNAMESPACES = 729,
     XMLPARSE = 730,
     XMLPI = 731,
     XMLROOT = 732,
     XMLSERIALIZE = 733,
     XMLTABLE = 734,
     YEAR_P = 735,
     YEARS_P = 736,
     YES_P = 737,
     ZONE = 738,
     NOT_LA = 739,
     NULLS_LA = 740,
     WITH_LA = 741,
     POSTFIXOP = 742,
     UMINUS = 743
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
#define FILTER 413
#define FIRST_P 414
#define FLOAT_P 415
#define FOLLOWING 416
#define FOR 417
#define FORCE 418
#define FOREIGN 419
#define FORWARD 420
#define FREEZE 421
#define FROM 422
#define FULL 423
#define FUNCTION 424
#define FUNCTIONS 425
#define GENERATED 426
#define GLOB 427
#define GLOBAL 428
#define GRANT 429
#define GRANTED 430
#define GROUP_P 431
#define GROUPING 432
#define GROUPING_ID 433
#define HANDLER 434
#define HAVING 435
#define HEADER_P 436
#define HOLD 437
#define HOUR_P 438
#define HOURS_P 439
#define IDENTITY_P 440
#define IF_P 441
#define IGNORE_P 442
#define ILIKE 443
#define IMMEDIATE 444
#define IMMUTABLE 445
#define IMPLICIT_P 446
#define IMPORT_P 447
#define IN_P 448
#define INCLUDE_P 449
#define INCLUDING 450
#define INCREMENT 451
#define INDEX 452
#define INDEXES 453
#define INHERIT 454
#define INHERITS 455
#define INITIALLY 456
#define INLINE_P 457
#define INNER_P 458
#define INOUT 459
#define INPUT_P 460
#define INSENSITIVE 461
#define INSERT 462
#define INSTALL 463
#define INSTEAD 464
#define INT_P 465
#define INTEGER 466
#define INTERSECT 467
#define INTERVAL 468
#define INTO 469
#define INVOKER 470
#define IS 471
#define ISNULL 472
#define ISOLATION 473
#define JOIN 474
#define JSON 475
#define KEY 476
#define LABEL 477
#define LANGUAGE 478
#define LARGE_P 479
#define LAST_P 480
#define LATERAL_P 481
#define LEADING 482
#define LEAKPROOF 483
#define LEFT 484
#define LEVEL 485
#define LIKE 486
#define LIMIT 487
#define LISTEN 488
#define LOAD 489
#define LOCAL 490
#define LOCATION 491
#define LOCK_P 492
#define LOCKED 493
#define LOGGED 494
#define MACRO 495
#define MAP 496
#define MAPPING 497
#define MATCH 498
#define MATERIALIZED 499
#define MAXVALUE 500
#define METHOD 501
#define MICROSECOND_P 502
#define MICROSECONDS_P 503
#define MILLISECOND_P 504
#define MILLISECONDS_P 505
#define MINUTE_P 506
#define MINUTES_P 507
#define MINVALUE 508
#define MODE 509
#define MONTH_P 510
#define MONTHS_P 511
#define MOVE 512
#define NAME_P 513
#define NAMES 514
#define NATIONAL 515
#define NATURAL 516
#define NCHAR 517
#define NEW 518
#define NEXT 519
#define NO 520
#define NONE 521
#define NOT 522
#define NOTHING 523
#define NOTIFY 524
#define NOTNULL 525
#define NOWAIT 526
#define NULL_P 527
#define NULLIF 528
#define NULLS_P 529
#define NUMERIC 530
#define OBJECT_P 531
#define OF 532
#define OFF 533
#define OFFSET 534
#define OIDS 535
#define OLD 536
#define ON 537
#define ONLY 538
#define OPERATOR 539
#define OPTION 540
#define OPTIONS 541
#define OR 542
#define ORDER 543
#define ORDINALITY 544
#define OUT_P 545
#define OUTER_P 546
#define OVER 547
#define OVERLAPS 548
#define OVERLAY 549
#define OVERRIDING 550
#define OWNED 551
#define OWNER 552
#define PARALLEL 553
#define PARSER 554
#define PARTIAL 555
#define PARTITION 556
#define PASSING 557
#define PASSWORD 558
#define PERCENT 559
#define PIVOT 560
#define PIVOT_LONGER 561
#define PIVOT_WIDER 562
#define PLACING 563
#define PLANS 564
#define POLICY 565
#define POSITION 566
#define POSITIONAL 567
#define PRAGMA_P 568
#define PRECEDING 569
#define PRECISION 570
#define PREPARE 571
#define PREPARED 572
#define PRESERVE 573
#define PRIMARY 574
#define PRIOR 575
#define PRIVILEGES 576
#define PROCEDURAL 577
#define PROCEDURE 578
#define PROGRAM 579
#define PUBLICATION 580
#define QUALIFY 581
#define QUOTE 582
#define RANGE 583
#define READ_P 584
#define REAL 585
#define REASSIGN 586
#define RECHECK 587
#define RECURSIVE 588
#define REF 589
#define REFERENCES 590
#define REFERENCING 591
#define REFRESH 592
#define REINDEX 593
#define RELATIVE_P 594
#define RELEASE 595
#define RENAME 596
#define REPEATABLE 597
#define REPLACE 598
#define REPLICA 599
#define RESET 600
#define RESPECT_P 601
#define RESTART 602
#define RESTRICT 603
#define RETURNING 604
#define RETURNS 605
#define REVOKE 606
#define RIGHT 607
#define ROLE 608
#define ROLLBACK 609
#define ROLLUP 610
#define ROW 611
#define ROWS 612
#define RULE 613
#define SAMPLE 614
#define SAVEPOINT 615
#define SCHEMA 616
#define SCHEMAS 617
#define SCROLL 618
#define SEARCH 619
#define SECOND_P 620
#define SECONDS_P 621
#define SECURITY 622
#define SELECT 623
#define SEMI 624
#define SEQUENCE 625
#define SEQUENCES 626
#define SERIALIZABLE 627
#define SERVER 628
#define SESSION 629
#define SET 630
#define SETOF 631
#define SETS 632
#define SHARE 633
#define SHOW 634
#define SIMILAR 635
#define SIMPLE 636
#define SKIP 637
#define SMALLINT 638
#define SNAPSHOT 639
#define SOME 640
#define SQL_P 641
#define STABLE 642
#define STANDALONE_P 643
#define START 644
#define STATEMENT 645
#define STATISTICS 646
#define STDIN 647
#define STDOUT 648
#define STORAGE 649
#define STORED 650
#define STRICT_P 651
#define STRIP_P 652
#define STRUCT 653
#define SUBSCRIPTION 654
#define SUBSTRING 655
#define SUMMARIZE 656
#define SYMMETRIC 657
#define SYSID 658
#define SYSTEM_P 659
#define TABLE 660
#define TABLES 661
#define TABLESAMPLE 662
#define TABLESPACE 663
#define TEMP 664
#define TEMPLATE 665
#define TEMPORARY 666
#define TEXT_P 667
#define THEN 668
#define TIME 669
#define TIMESTAMP 670
#define TO 671
#define TRAILING 672
#define TRANSACTION 673
#define TRANSFORM 674
#define TREAT 675
#define TRIGGER 676
#define TRIM 677
#define TRUE_P 678
#define TRUNCATE 679
#define TRUSTED 680
#define TRY_CAST 681
#define TYPE_P 682
#define TYPES_P 683
#define UNBOUNDED 684
#define UNCOMMITTED 685
#define UNENCRYPTED 686
#define UNION 687
#define UNIQUE 688
#define UNKNOWN 689
#define UNLISTEN 690
#define UNLOGGED 691
#define UNPIVOT 692
#define UNTIL 693
#define UPDATE 694
#define USE_P 695
#define USER 696
#define USING 697
#define VACUUM 698
#define VALID 699
#define VALIDATE 700
#define VALIDATOR 701
#define VALUE_P 702
#define VALUES 703
#define VARCHAR 704
#define VARIADIC 705
#define VARYING 706
#define VERBOSE 707
#define VERSION_P 708
#define VIEW 709
#define VIEWS 710
#define VIRTUAL 711
#define VOLATILE 712
#define WHEN 713
#define WHERE 714
#define WHITESPACE_P 715
#define WINDOW 716
#define WITH 717
#define WITHIN 718
#define WITHOUT 719
#define WORK 720
#define WRAPPER 721
#define WRITE_P 722
#define XML_P 723
#define XMLATTRIBUTES 724
#define XMLCONCAT 725
#define XMLELEMENT 726
#define XMLEXISTS 727
#define XMLFOREST 728
#define XMLNAMESPACES 729
#define XMLPARSE 730
#define XMLPI 731
#define XMLROOT 732
#define XMLSERIALIZE 733
#define XMLTABLE 734
#define YEAR_P 735
#define YEARS_P 736
#define YES_P 737
#define ZONE 738
#define NOT_LA 739
#define NULLS_LA 740
#define WITH_LA 741
#define POSTFIXOP 742
#define UMINUS 743




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
#line 1073 "third_party/libpg_query/grammar/grammar_out.hpp"
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


