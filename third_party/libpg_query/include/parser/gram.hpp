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
     LAMBDA_ARROW = 270,
     LESS_EQUALS = 271,
     GREATER_EQUALS = 272,
     NOT_EQUALS = 273,
     ABORT_P = 274,
     ABSOLUTE_P = 275,
     ACCESS = 276,
     ACTION = 277,
     ADD_P = 278,
     ADMIN = 279,
     AFTER = 280,
     AGGREGATE = 281,
     ALL = 282,
     ALSO = 283,
     ALTER = 284,
     ALWAYS = 285,
     ANALYSE = 286,
     ANALYZE = 287,
     AND = 288,
     ANY = 289,
     ARRAY = 290,
     AS = 291,
     ASC_P = 292,
     ASSERTION = 293,
     ASSIGNMENT = 294,
     ASYMMETRIC = 295,
     AT = 296,
     ATTACH = 297,
     ATTRIBUTE = 298,
     AUTHORIZATION = 299,
     BACKWARD = 300,
     BEFORE = 301,
     BEGIN_P = 302,
     BETWEEN = 303,
     BIGINT = 304,
     BINARY = 305,
     BIT = 306,
     BOOLEAN_P = 307,
     BOTH = 308,
     BY = 309,
     CACHE = 310,
     CALL_P = 311,
     CALLED = 312,
     CASCADE = 313,
     CASCADED = 314,
     CASE = 315,
     CAST = 316,
     CATALOG_P = 317,
     CHAIN = 318,
     CHAR_P = 319,
     CHARACTER = 320,
     CHARACTERISTICS = 321,
     CHECK_P = 322,
     CHECKPOINT = 323,
     CLASS = 324,
     CLOSE = 325,
     CLUSTER = 326,
     COALESCE = 327,
     COLLATE = 328,
     COLLATION = 329,
     COLUMN = 330,
     COLUMNS = 331,
     COMMENT = 332,
     COMMENTS = 333,
     COMMIT = 334,
     COMMITTED = 335,
     COMPRESSION = 336,
     CONCURRENTLY = 337,
     CONFIGURATION = 338,
     CONFLICT = 339,
     CONNECTION = 340,
     CONSTRAINT = 341,
     CONSTRAINTS = 342,
     CONTENT_P = 343,
     CONTINUE_P = 344,
     CONVERSION_P = 345,
     COPY = 346,
     COST = 347,
     CREATE_P = 348,
     CROSS = 349,
     CSV = 350,
     CUBE = 351,
     CURRENT_P = 352,
     CURRENT_CATALOG = 353,
     CURRENT_DATE = 354,
     CURRENT_ROLE = 355,
     CURRENT_SCHEMA = 356,
     CURRENT_TIME = 357,
     CURRENT_TIMESTAMP = 358,
     CURRENT_USER = 359,
     CURSOR = 360,
     CYCLE = 361,
     DATA_P = 362,
     DATABASE = 363,
     DAY_P = 364,
     DAYS_P = 365,
     DEALLOCATE = 366,
     DEC = 367,
     DECIMAL_P = 368,
     DECLARE = 369,
     DEFAULT = 370,
     DEFAULTS = 371,
     DEFERRABLE = 372,
     DEFERRED = 373,
     DEFINER = 374,
     DELETE_P = 375,
     DELIMITER = 376,
     DELIMITERS = 377,
     DEPENDS = 378,
     DESC_P = 379,
     DESCRIBE = 380,
     DETACH = 381,
     DICTIONARY = 382,
     DISABLE_P = 383,
     DISCARD = 384,
     DISTINCT = 385,
     DO = 386,
     DOCUMENT_P = 387,
     DOMAIN_P = 388,
     DOUBLE_P = 389,
     DROP = 390,
     EACH = 391,
     ELSE = 392,
     ENABLE_P = 393,
     ENCODING = 394,
     ENCRYPTED = 395,
     END_P = 396,
     ENUM_P = 397,
     ESCAPE = 398,
     EVENT = 399,
     EXCEPT = 400,
     EXCLUDE = 401,
     EXCLUDING = 402,
     EXCLUSIVE = 403,
     EXECUTE = 404,
     EXISTS = 405,
     EXPLAIN = 406,
     EXPORT_P = 407,
     EXTENSION = 408,
     EXTERNAL = 409,
     EXTRACT = 410,
     FALSE_P = 411,
     FAMILY = 412,
     FETCH = 413,
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
     IF_P = 442,
     IGNORE_P = 443,
     ILIKE = 444,
     IMMEDIATE = 445,
     IMMUTABLE = 446,
     IMPLICIT_P = 447,
     IMPORT_P = 448,
     IN_P = 449,
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
     KEY = 474,
     LABEL = 475,
     LANGUAGE = 476,
     LARGE_P = 477,
     LAST_P = 478,
     LATERAL_P = 479,
     LEADING = 480,
     LEAKPROOF = 481,
     LEFT = 482,
     LEVEL = 483,
     LIKE = 484,
     LIMIT = 485,
     LISTEN = 486,
     LOAD = 487,
     LOCAL = 488,
     LOCALTIME = 489,
     LOCALTIMESTAMP = 490,
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
     PLACING = 560,
     PLANS = 561,
     POLICY = 562,
     POSITION = 563,
     PRAGMA_P = 564,
     PRECEDING = 565,
     PRECISION = 566,
     PREPARE = 567,
     PREPARED = 568,
     PRESERVE = 569,
     PRIMARY = 570,
     PRIOR = 571,
     PRIVILEGES = 572,
     PROCEDURAL = 573,
     PROCEDURE = 574,
     PROGRAM = 575,
     PUBLICATION = 576,
     QUOTE = 577,
     RANGE = 578,
     READ_P = 579,
     REAL = 580,
     REASSIGN = 581,
     RECHECK = 582,
     RECURSIVE = 583,
     REF = 584,
     REFERENCES = 585,
     REFERENCING = 586,
     REFRESH = 587,
     REINDEX = 588,
     RELATIVE_P = 589,
     RELEASE = 590,
     RENAME = 591,
     REPEATABLE = 592,
     REPLACE = 593,
     REPLICA = 594,
     RESET = 595,
     RESPECT_P = 596,
     RESTART = 597,
     RESTRICT = 598,
     RETURNING = 599,
     RETURNS = 600,
     REVOKE = 601,
     RIGHT = 602,
     ROLE = 603,
     ROLLBACK = 604,
     ROLLUP = 605,
     ROW = 606,
     ROWS = 607,
     RULE = 608,
     SAMPLE = 609,
     SAVEPOINT = 610,
     SCHEMA = 611,
     SCHEMAS = 612,
     SCROLL = 613,
     SEARCH = 614,
     SECOND_P = 615,
     SECONDS_P = 616,
     SECURITY = 617,
     SELECT = 618,
     SEQUENCE = 619,
     SEQUENCES = 620,
     SERIALIZABLE = 621,
     SERVER = 622,
     SESSION = 623,
     SESSION_USER = 624,
     SET = 625,
     SETOF = 626,
     SETS = 627,
     SHARE = 628,
     SHOW = 629,
     SIMILAR = 630,
     SIMPLE = 631,
     SKIP = 632,
     SMALLINT = 633,
     SNAPSHOT = 634,
     SOME = 635,
     SQL_P = 636,
     STABLE = 637,
     STANDALONE_P = 638,
     START = 639,
     STATEMENT = 640,
     STATISTICS = 641,
     STDIN = 642,
     STDOUT = 643,
     STORAGE = 644,
     STRICT_P = 645,
     STRIP_P = 646,
     STRUCT = 647,
     SUBSCRIPTION = 648,
     SUBSTRING = 649,
     SUMMARIZE = 650,
     SYMMETRIC = 651,
     SYSID = 652,
     SYSTEM_P = 653,
     TABLE = 654,
     TABLES = 655,
     TABLESAMPLE = 656,
     TABLESPACE = 657,
     TEMP = 658,
     TEMPLATE = 659,
     TEMPORARY = 660,
     TEXT_P = 661,
     THEN = 662,
     TIME = 663,
     TIMESTAMP = 664,
     TO = 665,
     TRAILING = 666,
     TRANSACTION = 667,
     TRANSFORM = 668,
     TREAT = 669,
     TRIGGER = 670,
     TRIM = 671,
     TRUE_P = 672,
     TRUNCATE = 673,
     TRUSTED = 674,
     TRY_CAST = 675,
     TYPE_P = 676,
     TYPES_P = 677,
     UNBOUNDED = 678,
     UNCOMMITTED = 679,
     UNENCRYPTED = 680,
     UNION = 681,
     UNIQUE = 682,
     UNKNOWN = 683,
     UNLISTEN = 684,
     UNLOGGED = 685,
     UNTIL = 686,
     UPDATE = 687,
     USER = 688,
     USING = 689,
     VACUUM = 690,
     VALID = 691,
     VALIDATE = 692,
     VALIDATOR = 693,
     VALUE_P = 694,
     VALUES = 695,
     VARCHAR = 696,
     VARIADIC = 697,
     VARYING = 698,
     VERBOSE = 699,
     VERSION_P = 700,
     VIEW = 701,
     VIEWS = 702,
     VOLATILE = 703,
     WHEN = 704,
     WHERE = 705,
     WHITESPACE_P = 706,
     WINDOW = 707,
     WITH = 708,
     WITHIN = 709,
     WITHOUT = 710,
     WORK = 711,
     WRAPPER = 712,
     WRITE_P = 713,
     XML_P = 714,
     XMLATTRIBUTES = 715,
     XMLCONCAT = 716,
     XMLELEMENT = 717,
     XMLEXISTS = 718,
     XMLFOREST = 719,
     XMLNAMESPACES = 720,
     XMLPARSE = 721,
     XMLPI = 722,
     XMLROOT = 723,
     XMLSERIALIZE = 724,
     XMLTABLE = 725,
     YEAR_P = 726,
     YEARS_P = 727,
     YES_P = 728,
     ZONE = 729,
     NOT_LA = 730,
     NULLS_LA = 731,
     WITH_LA = 732,
     POSTFIXOP = 733,
     UMINUS = 734
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
#define LAMBDA_ARROW 270
#define LESS_EQUALS 271
#define GREATER_EQUALS 272
#define NOT_EQUALS 273
#define ABORT_P 274
#define ABSOLUTE_P 275
#define ACCESS 276
#define ACTION 277
#define ADD_P 278
#define ADMIN 279
#define AFTER 280
#define AGGREGATE 281
#define ALL 282
#define ALSO 283
#define ALTER 284
#define ALWAYS 285
#define ANALYSE 286
#define ANALYZE 287
#define AND 288
#define ANY 289
#define ARRAY 290
#define AS 291
#define ASC_P 292
#define ASSERTION 293
#define ASSIGNMENT 294
#define ASYMMETRIC 295
#define AT 296
#define ATTACH 297
#define ATTRIBUTE 298
#define AUTHORIZATION 299
#define BACKWARD 300
#define BEFORE 301
#define BEGIN_P 302
#define BETWEEN 303
#define BIGINT 304
#define BINARY 305
#define BIT 306
#define BOOLEAN_P 307
#define BOTH 308
#define BY 309
#define CACHE 310
#define CALL_P 311
#define CALLED 312
#define CASCADE 313
#define CASCADED 314
#define CASE 315
#define CAST 316
#define CATALOG_P 317
#define CHAIN 318
#define CHAR_P 319
#define CHARACTER 320
#define CHARACTERISTICS 321
#define CHECK_P 322
#define CHECKPOINT 323
#define CLASS 324
#define CLOSE 325
#define CLUSTER 326
#define COALESCE 327
#define COLLATE 328
#define COLLATION 329
#define COLUMN 330
#define COLUMNS 331
#define COMMENT 332
#define COMMENTS 333
#define COMMIT 334
#define COMMITTED 335
#define COMPRESSION 336
#define CONCURRENTLY 337
#define CONFIGURATION 338
#define CONFLICT 339
#define CONNECTION 340
#define CONSTRAINT 341
#define CONSTRAINTS 342
#define CONTENT_P 343
#define CONTINUE_P 344
#define CONVERSION_P 345
#define COPY 346
#define COST 347
#define CREATE_P 348
#define CROSS 349
#define CSV 350
#define CUBE 351
#define CURRENT_P 352
#define CURRENT_CATALOG 353
#define CURRENT_DATE 354
#define CURRENT_ROLE 355
#define CURRENT_SCHEMA 356
#define CURRENT_TIME 357
#define CURRENT_TIMESTAMP 358
#define CURRENT_USER 359
#define CURSOR 360
#define CYCLE 361
#define DATA_P 362
#define DATABASE 363
#define DAY_P 364
#define DAYS_P 365
#define DEALLOCATE 366
#define DEC 367
#define DECIMAL_P 368
#define DECLARE 369
#define DEFAULT 370
#define DEFAULTS 371
#define DEFERRABLE 372
#define DEFERRED 373
#define DEFINER 374
#define DELETE_P 375
#define DELIMITER 376
#define DELIMITERS 377
#define DEPENDS 378
#define DESC_P 379
#define DESCRIBE 380
#define DETACH 381
#define DICTIONARY 382
#define DISABLE_P 383
#define DISCARD 384
#define DISTINCT 385
#define DO 386
#define DOCUMENT_P 387
#define DOMAIN_P 388
#define DOUBLE_P 389
#define DROP 390
#define EACH 391
#define ELSE 392
#define ENABLE_P 393
#define ENCODING 394
#define ENCRYPTED 395
#define END_P 396
#define ENUM_P 397
#define ESCAPE 398
#define EVENT 399
#define EXCEPT 400
#define EXCLUDE 401
#define EXCLUDING 402
#define EXCLUSIVE 403
#define EXECUTE 404
#define EXISTS 405
#define EXPLAIN 406
#define EXPORT_P 407
#define EXTENSION 408
#define EXTERNAL 409
#define EXTRACT 410
#define FALSE_P 411
#define FAMILY 412
#define FETCH 413
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
#define IF_P 442
#define IGNORE_P 443
#define ILIKE 444
#define IMMEDIATE 445
#define IMMUTABLE 446
#define IMPLICIT_P 447
#define IMPORT_P 448
#define IN_P 449
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
#define KEY 474
#define LABEL 475
#define LANGUAGE 476
#define LARGE_P 477
#define LAST_P 478
#define LATERAL_P 479
#define LEADING 480
#define LEAKPROOF 481
#define LEFT 482
#define LEVEL 483
#define LIKE 484
#define LIMIT 485
#define LISTEN 486
#define LOAD 487
#define LOCAL 488
#define LOCALTIME 489
#define LOCALTIMESTAMP 490
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
#define PLACING 560
#define PLANS 561
#define POLICY 562
#define POSITION 563
#define PRAGMA_P 564
#define PRECEDING 565
#define PRECISION 566
#define PREPARE 567
#define PREPARED 568
#define PRESERVE 569
#define PRIMARY 570
#define PRIOR 571
#define PRIVILEGES 572
#define PROCEDURAL 573
#define PROCEDURE 574
#define PROGRAM 575
#define PUBLICATION 576
#define QUOTE 577
#define RANGE 578
#define READ_P 579
#define REAL 580
#define REASSIGN 581
#define RECHECK 582
#define RECURSIVE 583
#define REF 584
#define REFERENCES 585
#define REFERENCING 586
#define REFRESH 587
#define REINDEX 588
#define RELATIVE_P 589
#define RELEASE 590
#define RENAME 591
#define REPEATABLE 592
#define REPLACE 593
#define REPLICA 594
#define RESET 595
#define RESPECT_P 596
#define RESTART 597
#define RESTRICT 598
#define RETURNING 599
#define RETURNS 600
#define REVOKE 601
#define RIGHT 602
#define ROLE 603
#define ROLLBACK 604
#define ROLLUP 605
#define ROW 606
#define ROWS 607
#define RULE 608
#define SAMPLE 609
#define SAVEPOINT 610
#define SCHEMA 611
#define SCHEMAS 612
#define SCROLL 613
#define SEARCH 614
#define SECOND_P 615
#define SECONDS_P 616
#define SECURITY 617
#define SELECT 618
#define SEQUENCE 619
#define SEQUENCES 620
#define SERIALIZABLE 621
#define SERVER 622
#define SESSION 623
#define SESSION_USER 624
#define SET 625
#define SETOF 626
#define SETS 627
#define SHARE 628
#define SHOW 629
#define SIMILAR 630
#define SIMPLE 631
#define SKIP 632
#define SMALLINT 633
#define SNAPSHOT 634
#define SOME 635
#define SQL_P 636
#define STABLE 637
#define STANDALONE_P 638
#define START 639
#define STATEMENT 640
#define STATISTICS 641
#define STDIN 642
#define STDOUT 643
#define STORAGE 644
#define STRICT_P 645
#define STRIP_P 646
#define STRUCT 647
#define SUBSCRIPTION 648
#define SUBSTRING 649
#define SUMMARIZE 650
#define SYMMETRIC 651
#define SYSID 652
#define SYSTEM_P 653
#define TABLE 654
#define TABLES 655
#define TABLESAMPLE 656
#define TABLESPACE 657
#define TEMP 658
#define TEMPLATE 659
#define TEMPORARY 660
#define TEXT_P 661
#define THEN 662
#define TIME 663
#define TIMESTAMP 664
#define TO 665
#define TRAILING 666
#define TRANSACTION 667
#define TRANSFORM 668
#define TREAT 669
#define TRIGGER 670
#define TRIM 671
#define TRUE_P 672
#define TRUNCATE 673
#define TRUSTED 674
#define TRY_CAST 675
#define TYPE_P 676
#define TYPES_P 677
#define UNBOUNDED 678
#define UNCOMMITTED 679
#define UNENCRYPTED 680
#define UNION 681
#define UNIQUE 682
#define UNKNOWN 683
#define UNLISTEN 684
#define UNLOGGED 685
#define UNTIL 686
#define UPDATE 687
#define USER 688
#define USING 689
#define VACUUM 690
#define VALID 691
#define VALIDATE 692
#define VALIDATOR 693
#define VALUE_P 694
#define VALUES 695
#define VARCHAR 696
#define VARIADIC 697
#define VARYING 698
#define VERBOSE 699
#define VERSION_P 700
#define VIEW 701
#define VIEWS 702
#define VOLATILE 703
#define WHEN 704
#define WHERE 705
#define WHITESPACE_P 706
#define WINDOW 707
#define WITH 708
#define WITHIN 709
#define WITHOUT 710
#define WORK 711
#define WRAPPER 712
#define WRITE_P 713
#define XML_P 714
#define XMLATTRIBUTES 715
#define XMLCONCAT 716
#define XMLELEMENT 717
#define XMLEXISTS 718
#define XMLFOREST 719
#define XMLNAMESPACES 720
#define XMLPARSE 721
#define XMLPI 722
#define XMLROOT 723
#define XMLSERIALIZE 724
#define XMLTABLE 725
#define YEAR_P 726
#define YEARS_P 727
#define YES_P 728
#define ZONE 729
#define NOT_LA 730
#define NULLS_LA 731
#define WITH_LA 732
#define POSTFIXOP 733
#define UMINUS 734




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
}
/* Line 1489 of yacc.c.  */
#line 1051 "third_party/libpg_query/grammar/grammar_out.hpp"
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


