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
     LOCALTIME = 490,
     LOCALTIMESTAMP = 491,
     LOCATION = 492,
     LOCK_P = 493,
     LOCKED = 494,
     LOGGED = 495,
     MACRO = 496,
     MAP = 497,
     MAPPING = 498,
     MATCH = 499,
     MATERIALIZED = 500,
     MAXVALUE = 501,
     METHOD = 502,
     MICROSECOND_P = 503,
     MICROSECONDS_P = 504,
     MILLISECOND_P = 505,
     MILLISECONDS_P = 506,
     MINUTE_P = 507,
     MINUTES_P = 508,
     MINVALUE = 509,
     MODE = 510,
     MONTH_P = 511,
     MONTHS_P = 512,
     MOVE = 513,
     NAME_P = 514,
     NAMES = 515,
     NATIONAL = 516,
     NATURAL = 517,
     NCHAR = 518,
     NEW = 519,
     NEXT = 520,
     NO = 521,
     NONE = 522,
     NOT = 523,
     NOTHING = 524,
     NOTIFY = 525,
     NOTNULL = 526,
     NOWAIT = 527,
     NULL_P = 528,
     NULLIF = 529,
     NULLS_P = 530,
     NUMERIC = 531,
     OBJECT_P = 532,
     OF = 533,
     OFF = 534,
     OFFSET = 535,
     OIDS = 536,
     OLD = 537,
     ON = 538,
     ONLY = 539,
     OPERATOR = 540,
     OPTION = 541,
     OPTIONS = 542,
     OR = 543,
     ORDER = 544,
     ORDINALITY = 545,
     OUT_P = 546,
     OUTER_P = 547,
     OVER = 548,
     OVERLAPS = 549,
     OVERLAY = 550,
     OVERRIDING = 551,
     OWNED = 552,
     OWNER = 553,
     PARALLEL = 554,
     PARSER = 555,
     PARTIAL = 556,
     PARTITION = 557,
     PASSING = 558,
     PASSWORD = 559,
     PERCENT = 560,
     PLACING = 561,
     PLANS = 562,
     POLICY = 563,
     POSITION = 564,
     PRAGMA_P = 565,
     PRECEDING = 566,
     PRECISION = 567,
     PREPARE = 568,
     PREPARED = 569,
     PRESERVE = 570,
     PRIMARY = 571,
     PRIOR = 572,
     PRIVILEGES = 573,
     PROCEDURAL = 574,
     PROCEDURE = 575,
     PROGRAM = 576,
     PUBLICATION = 577,
     QUOTE = 578,
     RANGE = 579,
     READ_P = 580,
     REAL = 581,
     REASSIGN = 582,
     RECHECK = 583,
     RECURSIVE = 584,
     REF = 585,
     REFERENCES = 586,
     REFERENCING = 587,
     REFRESH = 588,
     REINDEX = 589,
     RELATIVE_P = 590,
     RELEASE = 591,
     RENAME = 592,
     REPEATABLE = 593,
     REPLACE = 594,
     REPLICA = 595,
     RESET = 596,
     RESPECT_P = 597,
     RESTART = 598,
     RESTRICT = 599,
     RETURNING = 600,
     RETURNS = 601,
     REVOKE = 602,
     RIGHT = 603,
     ROLE = 604,
     ROLLBACK = 605,
     ROLLUP = 606,
     ROW = 607,
     ROWS = 608,
     RULE = 609,
     SAMPLE = 610,
     SAVEPOINT = 611,
     SCHEMA = 612,
     SCHEMAS = 613,
     SCROLL = 614,
     SEARCH = 615,
     SECOND_P = 616,
     SECONDS_P = 617,
     SECURITY = 618,
     SELECT = 619,
     SEQUENCE = 620,
     SEQUENCES = 621,
     SERIALIZABLE = 622,
     SERVER = 623,
     SESSION = 624,
     SESSION_USER = 625,
     SET = 626,
     SETOF = 627,
     SETS = 628,
     SHARE = 629,
     SHOW = 630,
     SIMILAR = 631,
     SIMPLE = 632,
     SKIP = 633,
     SMALLINT = 634,
     SNAPSHOT = 635,
     SOME = 636,
     SQL_P = 637,
     STABLE = 638,
     STANDALONE_P = 639,
     START = 640,
     STATEMENT = 641,
     STATISTICS = 642,
     STDIN = 643,
     STDOUT = 644,
     STORAGE = 645,
     STRICT_P = 646,
     STRIP_P = 647,
     STRUCT = 648,
     SUBSCRIPTION = 649,
     SUBSTRING = 650,
     SUMMARIZE = 651,
     SYMMETRIC = 652,
     SYSID = 653,
     SYSTEM_P = 654,
     TABLE = 655,
     TABLES = 656,
     TABLESAMPLE = 657,
     TABLESPACE = 658,
     TEMP = 659,
     TEMPLATE = 660,
     TEMPORARY = 661,
     TEXT_P = 662,
     THEN = 663,
     TIME = 664,
     TIMESTAMP = 665,
     TO = 666,
     TRAILING = 667,
     TRANSACTION = 668,
     TRANSFORM = 669,
     TREAT = 670,
     TRIGGER = 671,
     TRIM = 672,
     TRUE_P = 673,
     TRUNCATE = 674,
     TRUSTED = 675,
     TRY_CAST = 676,
     TYPE_P = 677,
     TYPES_P = 678,
     UNBOUNDED = 679,
     UNCOMMITTED = 680,
     UNENCRYPTED = 681,
     UNION = 682,
     UNIQUE = 683,
     UNKNOWN = 684,
     UNLISTEN = 685,
     UNLOGGED = 686,
     UNTIL = 687,
     UPDATE = 688,
     USER = 689,
     USING = 690,
     VACUUM = 691,
     VALID = 692,
     VALIDATE = 693,
     VALIDATOR = 694,
     VALUE_P = 695,
     VALUES = 696,
     VARCHAR = 697,
     VARIADIC = 698,
     VARYING = 699,
     VERBOSE = 700,
     VERSION_P = 701,
     VIEW = 702,
     VIEWS = 703,
     VOLATILE = 704,
     WHEN = 705,
     WHERE = 706,
     WHITESPACE_P = 707,
     WINDOW = 708,
     WITH = 709,
     WITHIN = 710,
     WITHOUT = 711,
     WORK = 712,
     WRAPPER = 713,
     WRITE_P = 714,
     XML_P = 715,
     XMLATTRIBUTES = 716,
     XMLCONCAT = 717,
     XMLELEMENT = 718,
     XMLEXISTS = 719,
     XMLFOREST = 720,
     XMLNAMESPACES = 721,
     XMLPARSE = 722,
     XMLPI = 723,
     XMLROOT = 724,
     XMLSERIALIZE = 725,
     XMLTABLE = 726,
     YEAR_P = 727,
     YEARS_P = 728,
     YES_P = 729,
     ZONE = 730,
     NOT_LA = 731,
     NULLS_LA = 732,
     WITH_LA = 733,
     POSTFIXOP = 734,
     UMINUS = 735
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
#define LOCALTIME 490
#define LOCALTIMESTAMP 491
#define LOCATION 492
#define LOCK_P 493
#define LOCKED 494
#define LOGGED 495
#define MACRO 496
#define MAP 497
#define MAPPING 498
#define MATCH 499
#define MATERIALIZED 500
#define MAXVALUE 501
#define METHOD 502
#define MICROSECOND_P 503
#define MICROSECONDS_P 504
#define MILLISECOND_P 505
#define MILLISECONDS_P 506
#define MINUTE_P 507
#define MINUTES_P 508
#define MINVALUE 509
#define MODE 510
#define MONTH_P 511
#define MONTHS_P 512
#define MOVE 513
#define NAME_P 514
#define NAMES 515
#define NATIONAL 516
#define NATURAL 517
#define NCHAR 518
#define NEW 519
#define NEXT 520
#define NO 521
#define NONE 522
#define NOT 523
#define NOTHING 524
#define NOTIFY 525
#define NOTNULL 526
#define NOWAIT 527
#define NULL_P 528
#define NULLIF 529
#define NULLS_P 530
#define NUMERIC 531
#define OBJECT_P 532
#define OF 533
#define OFF 534
#define OFFSET 535
#define OIDS 536
#define OLD 537
#define ON 538
#define ONLY 539
#define OPERATOR 540
#define OPTION 541
#define OPTIONS 542
#define OR 543
#define ORDER 544
#define ORDINALITY 545
#define OUT_P 546
#define OUTER_P 547
#define OVER 548
#define OVERLAPS 549
#define OVERLAY 550
#define OVERRIDING 551
#define OWNED 552
#define OWNER 553
#define PARALLEL 554
#define PARSER 555
#define PARTIAL 556
#define PARTITION 557
#define PASSING 558
#define PASSWORD 559
#define PERCENT 560
#define PLACING 561
#define PLANS 562
#define POLICY 563
#define POSITION 564
#define PRAGMA_P 565
#define PRECEDING 566
#define PRECISION 567
#define PREPARE 568
#define PREPARED 569
#define PRESERVE 570
#define PRIMARY 571
#define PRIOR 572
#define PRIVILEGES 573
#define PROCEDURAL 574
#define PROCEDURE 575
#define PROGRAM 576
#define PUBLICATION 577
#define QUOTE 578
#define RANGE 579
#define READ_P 580
#define REAL 581
#define REASSIGN 582
#define RECHECK 583
#define RECURSIVE 584
#define REF 585
#define REFERENCES 586
#define REFERENCING 587
#define REFRESH 588
#define REINDEX 589
#define RELATIVE_P 590
#define RELEASE 591
#define RENAME 592
#define REPEATABLE 593
#define REPLACE 594
#define REPLICA 595
#define RESET 596
#define RESPECT_P 597
#define RESTART 598
#define RESTRICT 599
#define RETURNING 600
#define RETURNS 601
#define REVOKE 602
#define RIGHT 603
#define ROLE 604
#define ROLLBACK 605
#define ROLLUP 606
#define ROW 607
#define ROWS 608
#define RULE 609
#define SAMPLE 610
#define SAVEPOINT 611
#define SCHEMA 612
#define SCHEMAS 613
#define SCROLL 614
#define SEARCH 615
#define SECOND_P 616
#define SECONDS_P 617
#define SECURITY 618
#define SELECT 619
#define SEQUENCE 620
#define SEQUENCES 621
#define SERIALIZABLE 622
#define SERVER 623
#define SESSION 624
#define SESSION_USER 625
#define SET 626
#define SETOF 627
#define SETS 628
#define SHARE 629
#define SHOW 630
#define SIMILAR 631
#define SIMPLE 632
#define SKIP 633
#define SMALLINT 634
#define SNAPSHOT 635
#define SOME 636
#define SQL_P 637
#define STABLE 638
#define STANDALONE_P 639
#define START 640
#define STATEMENT 641
#define STATISTICS 642
#define STDIN 643
#define STDOUT 644
#define STORAGE 645
#define STRICT_P 646
#define STRIP_P 647
#define STRUCT 648
#define SUBSCRIPTION 649
#define SUBSTRING 650
#define SUMMARIZE 651
#define SYMMETRIC 652
#define SYSID 653
#define SYSTEM_P 654
#define TABLE 655
#define TABLES 656
#define TABLESAMPLE 657
#define TABLESPACE 658
#define TEMP 659
#define TEMPLATE 660
#define TEMPORARY 661
#define TEXT_P 662
#define THEN 663
#define TIME 664
#define TIMESTAMP 665
#define TO 666
#define TRAILING 667
#define TRANSACTION 668
#define TRANSFORM 669
#define TREAT 670
#define TRIGGER 671
#define TRIM 672
#define TRUE_P 673
#define TRUNCATE 674
#define TRUSTED 675
#define TRY_CAST 676
#define TYPE_P 677
#define TYPES_P 678
#define UNBOUNDED 679
#define UNCOMMITTED 680
#define UNENCRYPTED 681
#define UNION 682
#define UNIQUE 683
#define UNKNOWN 684
#define UNLISTEN 685
#define UNLOGGED 686
#define UNTIL 687
#define UPDATE 688
#define USER 689
#define USING 690
#define VACUUM 691
#define VALID 692
#define VALIDATE 693
#define VALIDATOR 694
#define VALUE_P 695
#define VALUES 696
#define VARCHAR 697
#define VARIADIC 698
#define VARYING 699
#define VERBOSE 700
#define VERSION_P 701
#define VIEW 702
#define VIEWS 703
#define VOLATILE 704
#define WHEN 705
#define WHERE 706
#define WHITESPACE_P 707
#define WINDOW 708
#define WITH 709
#define WITHIN 710
#define WITHOUT 711
#define WORK 712
#define WRAPPER 713
#define WRITE_P 714
#define XML_P 715
#define XMLATTRIBUTES 716
#define XMLCONCAT 717
#define XMLELEMENT 718
#define XMLEXISTS 719
#define XMLFOREST 720
#define XMLNAMESPACES 721
#define XMLPARSE 722
#define XMLPI 723
#define XMLROOT 724
#define XMLSERIALIZE 725
#define XMLTABLE 726
#define YEAR_P 727
#define YEARS_P 728
#define YES_P 729
#define ZONE 730
#define NOT_LA 731
#define NULLS_LA 732
#define WITH_LA 733
#define POSTFIXOP 734
#define UMINUS 735




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
#line 1053 "third_party/libpg_query/grammar/grammar_out.hpp"
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


