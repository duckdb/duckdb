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
     CONCURRENTLY = 336,
     CONFIGURATION = 337,
     CONFLICT = 338,
     CONNECTION = 339,
     CONSTRAINT = 340,
     CONSTRAINTS = 341,
     CONTENT_P = 342,
     CONTINUE_P = 343,
     CONVERSION_P = 344,
     COPY = 345,
     COST = 346,
     CREATE_P = 347,
     CROSS = 348,
     CSV = 349,
     CUBE = 350,
     CURRENT_P = 351,
     CURRENT_CATALOG = 352,
     CURRENT_DATE = 353,
     CURRENT_ROLE = 354,
     CURRENT_SCHEMA = 355,
     CURRENT_TIME = 356,
     CURRENT_TIMESTAMP = 357,
     CURRENT_USER = 358,
     CURSOR = 359,
     CYCLE = 360,
     DATA_P = 361,
     DATABASE = 362,
     DAY_P = 363,
     DAYS_P = 364,
     DEALLOCATE = 365,
     DEC = 366,
     DECIMAL_P = 367,
     DECLARE = 368,
     DEFAULT = 369,
     DEFAULTS = 370,
     DEFERRABLE = 371,
     DEFERRED = 372,
     DEFINER = 373,
     DELETE_P = 374,
     DELIMITER = 375,
     DELIMITERS = 376,
     DEPENDS = 377,
     DESC_P = 378,
     DESCRIBE = 379,
     DETACH = 380,
     DICTIONARY = 381,
     DISABLE_P = 382,
     DISCARD = 383,
     DISTINCT = 384,
     DO = 385,
     DOCUMENT_P = 386,
     DOMAIN_P = 387,
     DOUBLE_P = 388,
     DROP = 389,
     EACH = 390,
     ELSE = 391,
     ENABLE_P = 392,
     ENCODING = 393,
     ENCRYPTED = 394,
     END_P = 395,
     ENUM_P = 396,
     ESCAPE = 397,
     EVENT = 398,
     EXCEPT = 399,
     EXCLUDE = 400,
     EXCLUDING = 401,
     EXCLUSIVE = 402,
     EXECUTE = 403,
     EXISTS = 404,
     EXPLAIN = 405,
     EXPORT_P = 406,
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
     ILIKE = 442,
     IMMEDIATE = 443,
     IMMUTABLE = 444,
     IMPLICIT_P = 445,
     IMPORT_P = 446,
     IN_P = 447,
     INCLUDING = 448,
     INCREMENT = 449,
     INDEX = 450,
     INDEXES = 451,
     INHERIT = 452,
     INHERITS = 453,
     INITIALLY = 454,
     INLINE_P = 455,
     INNER_P = 456,
     INOUT = 457,
     INPUT_P = 458,
     INSENSITIVE = 459,
     INSERT = 460,
     INSTEAD = 461,
     INT_P = 462,
     INTEGER = 463,
     INTERSECT = 464,
     INTERVAL = 465,
     INTO = 466,
     INVOKER = 467,
     IS = 468,
     ISNULL = 469,
     ISOLATION = 470,
     JOIN = 471,
     KEY = 472,
     LABEL = 473,
     LANGUAGE = 474,
     LARGE_P = 475,
     LAST_P = 476,
     LATERAL_P = 477,
     LEADING = 478,
     LEAKPROOF = 479,
     LEFT = 480,
     LEVEL = 481,
     LIKE = 482,
     LIMIT = 483,
     LISTEN = 484,
     LOAD = 485,
     LOCAL = 486,
     LOCALTIME = 487,
     LOCALTIMESTAMP = 488,
     LOCATION = 489,
     LOCK_P = 490,
     LOCKED = 491,
     LOGGED = 492,
     MACRO = 493,
     MAP = 494,
     MAPPING = 495,
     MATCH = 496,
     MATERIALIZED = 497,
     MAXVALUE = 498,
     METHOD = 499,
     MICROSECOND_P = 500,
     MICROSECONDS_P = 501,
     MILLISECOND_P = 502,
     MILLISECONDS_P = 503,
     MINUTE_P = 504,
     MINUTES_P = 505,
     MINVALUE = 506,
     MODE = 507,
     MONTH_P = 508,
     MONTHS_P = 509,
     MOVE = 510,
     NAME_P = 511,
     NAMES = 512,
     NATIONAL = 513,
     NATURAL = 514,
     NCHAR = 515,
     NEW = 516,
     NEXT = 517,
     NO = 518,
     NONE = 519,
     NOT = 520,
     NOTHING = 521,
     NOTIFY = 522,
     NOTNULL = 523,
     NOWAIT = 524,
     NULL_P = 525,
     NULLIF = 526,
     NULLS_P = 527,
     NUMERIC = 528,
     OBJECT_P = 529,
     OF = 530,
     OFF = 531,
     OFFSET = 532,
     OIDS = 533,
     OLD = 534,
     ON = 535,
     ONLY = 536,
     OPERATOR = 537,
     OPTION = 538,
     OPTIONS = 539,
     OR = 540,
     ORDER = 541,
     ORDINALITY = 542,
     OUT_P = 543,
     OUTER_P = 544,
     OVER = 545,
     OVERLAPS = 546,
     OVERLAY = 547,
     OVERRIDING = 548,
     OWNED = 549,
     OWNER = 550,
     PARALLEL = 551,
     PARSER = 552,
     PARTIAL = 553,
     PARTITION = 554,
     PASSING = 555,
     PASSWORD = 556,
     PERCENT = 557,
     PLACING = 558,
     PLANS = 559,
     POLICY = 560,
     POSITION = 561,
     PRAGMA_P = 562,
     PRECEDING = 563,
     PRECISION = 564,
     PREPARE = 565,
     PREPARED = 566,
     PRESERVE = 567,
     PRIMARY = 568,
     PRIOR = 569,
     PRIVILEGES = 570,
     PROCEDURAL = 571,
     PROCEDURE = 572,
     PROGRAM = 573,
     PUBLICATION = 574,
     QUOTE = 575,
     RANGE = 576,
     READ_P = 577,
     REAL = 578,
     REASSIGN = 579,
     RECHECK = 580,
     RECURSIVE = 581,
     REF = 582,
     REFERENCES = 583,
     REFERENCING = 584,
     REFRESH = 585,
     REINDEX = 586,
     RELATIVE_P = 587,
     RELEASE = 588,
     RENAME = 589,
     REPEATABLE = 590,
     REPLACE = 591,
     REPLICA = 592,
     RESET = 593,
     RESTART = 594,
     RESTRICT = 595,
     RETURNING = 596,
     RETURNS = 597,
     REVOKE = 598,
     RIGHT = 599,
     ROLE = 600,
     ROLLBACK = 601,
     ROLLUP = 602,
     ROW = 603,
     ROWS = 604,
     RULE = 605,
     SAMPLE = 606,
     SAVEPOINT = 607,
     SCHEMA = 608,
     SCHEMAS = 609,
     SCROLL = 610,
     SEARCH = 611,
     SECOND_P = 612,
     SECONDS_P = 613,
     SECURITY = 614,
     SELECT = 615,
     SEQUENCE = 616,
     SEQUENCES = 617,
     SERIALIZABLE = 618,
     SERVER = 619,
     SESSION = 620,
     SESSION_USER = 621,
     SET = 622,
     SETOF = 623,
     SETS = 624,
     SHARE = 625,
     SHOW = 626,
     SIMILAR = 627,
     SIMPLE = 628,
     SKIP = 629,
     SMALLINT = 630,
     SNAPSHOT = 631,
     SOME = 632,
     SQL_P = 633,
     STABLE = 634,
     STANDALONE_P = 635,
     START = 636,
     STATEMENT = 637,
     STATISTICS = 638,
     STDIN = 639,
     STDOUT = 640,
     STORAGE = 641,
     STRICT_P = 642,
     STRIP_P = 643,
     STRUCT = 644,
     SUBSCRIPTION = 645,
     SUBSTRING = 646,
     SUMMARIZE = 647,
     SYMMETRIC = 648,
     SYSID = 649,
     SYSTEM_P = 650,
     TABLE = 651,
     TABLES = 652,
     TABLESAMPLE = 653,
     TABLESPACE = 654,
     TEMP = 655,
     TEMPLATE = 656,
     TEMPORARY = 657,
     TEXT_P = 658,
     THEN = 659,
     TIME = 660,
     TIMESTAMP = 661,
     TO = 662,
     TRAILING = 663,
     TRANSACTION = 664,
     TRANSFORM = 665,
     TREAT = 666,
     TRIGGER = 667,
     TRIM = 668,
     TRUE_P = 669,
     TRUNCATE = 670,
     TRUSTED = 671,
     TRY_CAST = 672,
     TYPE_P = 673,
     TYPES_P = 674,
     UNBOUNDED = 675,
     UNCOMMITTED = 676,
     UNENCRYPTED = 677,
     UNION = 678,
     UNIQUE = 679,
     UNKNOWN = 680,
     UNLISTEN = 681,
     UNLOGGED = 682,
     UNTIL = 683,
     UPDATE = 684,
     USER = 685,
     USING = 686,
     VACUUM = 687,
     VALID = 688,
     VALIDATE = 689,
     VALIDATOR = 690,
     VALUE_P = 691,
     VALUES = 692,
     VARCHAR = 693,
     VARIADIC = 694,
     VARYING = 695,
     VERBOSE = 696,
     VERSION_P = 697,
     VIEW = 698,
     VIEWS = 699,
     VOLATILE = 700,
     WHEN = 701,
     WHERE = 702,
     WHITESPACE_P = 703,
     WINDOW = 704,
     WITH = 705,
     WITHIN = 706,
     WITHOUT = 707,
     WORK = 708,
     WRAPPER = 709,
     WRITE_P = 710,
     XML_P = 711,
     XMLATTRIBUTES = 712,
     XMLCONCAT = 713,
     XMLELEMENT = 714,
     XMLEXISTS = 715,
     XMLFOREST = 716,
     XMLNAMESPACES = 717,
     XMLPARSE = 718,
     XMLPI = 719,
     XMLROOT = 720,
     XMLSERIALIZE = 721,
     XMLTABLE = 722,
     YEAR_P = 723,
     YEARS_P = 724,
     YES_P = 725,
     ZONE = 726,
     NOT_LA = 727,
     NULLS_LA = 728,
     WITH_LA = 729,
     POSTFIXOP = 730,
     UMINUS = 731
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
#define CONCURRENTLY 336
#define CONFIGURATION 337
#define CONFLICT 338
#define CONNECTION 339
#define CONSTRAINT 340
#define CONSTRAINTS 341
#define CONTENT_P 342
#define CONTINUE_P 343
#define CONVERSION_P 344
#define COPY 345
#define COST 346
#define CREATE_P 347
#define CROSS 348
#define CSV 349
#define CUBE 350
#define CURRENT_P 351
#define CURRENT_CATALOG 352
#define CURRENT_DATE 353
#define CURRENT_ROLE 354
#define CURRENT_SCHEMA 355
#define CURRENT_TIME 356
#define CURRENT_TIMESTAMP 357
#define CURRENT_USER 358
#define CURSOR 359
#define CYCLE 360
#define DATA_P 361
#define DATABASE 362
#define DAY_P 363
#define DAYS_P 364
#define DEALLOCATE 365
#define DEC 366
#define DECIMAL_P 367
#define DECLARE 368
#define DEFAULT 369
#define DEFAULTS 370
#define DEFERRABLE 371
#define DEFERRED 372
#define DEFINER 373
#define DELETE_P 374
#define DELIMITER 375
#define DELIMITERS 376
#define DEPENDS 377
#define DESC_P 378
#define DESCRIBE 379
#define DETACH 380
#define DICTIONARY 381
#define DISABLE_P 382
#define DISCARD 383
#define DISTINCT 384
#define DO 385
#define DOCUMENT_P 386
#define DOMAIN_P 387
#define DOUBLE_P 388
#define DROP 389
#define EACH 390
#define ELSE 391
#define ENABLE_P 392
#define ENCODING 393
#define ENCRYPTED 394
#define END_P 395
#define ENUM_P 396
#define ESCAPE 397
#define EVENT 398
#define EXCEPT 399
#define EXCLUDE 400
#define EXCLUDING 401
#define EXCLUSIVE 402
#define EXECUTE 403
#define EXISTS 404
#define EXPLAIN 405
#define EXPORT_P 406
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
#define ILIKE 442
#define IMMEDIATE 443
#define IMMUTABLE 444
#define IMPLICIT_P 445
#define IMPORT_P 446
#define IN_P 447
#define INCLUDING 448
#define INCREMENT 449
#define INDEX 450
#define INDEXES 451
#define INHERIT 452
#define INHERITS 453
#define INITIALLY 454
#define INLINE_P 455
#define INNER_P 456
#define INOUT 457
#define INPUT_P 458
#define INSENSITIVE 459
#define INSERT 460
#define INSTEAD 461
#define INT_P 462
#define INTEGER 463
#define INTERSECT 464
#define INTERVAL 465
#define INTO 466
#define INVOKER 467
#define IS 468
#define ISNULL 469
#define ISOLATION 470
#define JOIN 471
#define KEY 472
#define LABEL 473
#define LANGUAGE 474
#define LARGE_P 475
#define LAST_P 476
#define LATERAL_P 477
#define LEADING 478
#define LEAKPROOF 479
#define LEFT 480
#define LEVEL 481
#define LIKE 482
#define LIMIT 483
#define LISTEN 484
#define LOAD 485
#define LOCAL 486
#define LOCALTIME 487
#define LOCALTIMESTAMP 488
#define LOCATION 489
#define LOCK_P 490
#define LOCKED 491
#define LOGGED 492
#define MACRO 493
#define MAP 494
#define MAPPING 495
#define MATCH 496
#define MATERIALIZED 497
#define MAXVALUE 498
#define METHOD 499
#define MICROSECOND_P 500
#define MICROSECONDS_P 501
#define MILLISECOND_P 502
#define MILLISECONDS_P 503
#define MINUTE_P 504
#define MINUTES_P 505
#define MINVALUE 506
#define MODE 507
#define MONTH_P 508
#define MONTHS_P 509
#define MOVE 510
#define NAME_P 511
#define NAMES 512
#define NATIONAL 513
#define NATURAL 514
#define NCHAR 515
#define NEW 516
#define NEXT 517
#define NO 518
#define NONE 519
#define NOT 520
#define NOTHING 521
#define NOTIFY 522
#define NOTNULL 523
#define NOWAIT 524
#define NULL_P 525
#define NULLIF 526
#define NULLS_P 527
#define NUMERIC 528
#define OBJECT_P 529
#define OF 530
#define OFF 531
#define OFFSET 532
#define OIDS 533
#define OLD 534
#define ON 535
#define ONLY 536
#define OPERATOR 537
#define OPTION 538
#define OPTIONS 539
#define OR 540
#define ORDER 541
#define ORDINALITY 542
#define OUT_P 543
#define OUTER_P 544
#define OVER 545
#define OVERLAPS 546
#define OVERLAY 547
#define OVERRIDING 548
#define OWNED 549
#define OWNER 550
#define PARALLEL 551
#define PARSER 552
#define PARTIAL 553
#define PARTITION 554
#define PASSING 555
#define PASSWORD 556
#define PERCENT 557
#define PLACING 558
#define PLANS 559
#define POLICY 560
#define POSITION 561
#define PRAGMA_P 562
#define PRECEDING 563
#define PRECISION 564
#define PREPARE 565
#define PREPARED 566
#define PRESERVE 567
#define PRIMARY 568
#define PRIOR 569
#define PRIVILEGES 570
#define PROCEDURAL 571
#define PROCEDURE 572
#define PROGRAM 573
#define PUBLICATION 574
#define QUOTE 575
#define RANGE 576
#define READ_P 577
#define REAL 578
#define REASSIGN 579
#define RECHECK 580
#define RECURSIVE 581
#define REF 582
#define REFERENCES 583
#define REFERENCING 584
#define REFRESH 585
#define REINDEX 586
#define RELATIVE_P 587
#define RELEASE 588
#define RENAME 589
#define REPEATABLE 590
#define REPLACE 591
#define REPLICA 592
#define RESET 593
#define RESTART 594
#define RESTRICT 595
#define RETURNING 596
#define RETURNS 597
#define REVOKE 598
#define RIGHT 599
#define ROLE 600
#define ROLLBACK 601
#define ROLLUP 602
#define ROW 603
#define ROWS 604
#define RULE 605
#define SAMPLE 606
#define SAVEPOINT 607
#define SCHEMA 608
#define SCHEMAS 609
#define SCROLL 610
#define SEARCH 611
#define SECOND_P 612
#define SECONDS_P 613
#define SECURITY 614
#define SELECT 615
#define SEQUENCE 616
#define SEQUENCES 617
#define SERIALIZABLE 618
#define SERVER 619
#define SESSION 620
#define SESSION_USER 621
#define SET 622
#define SETOF 623
#define SETS 624
#define SHARE 625
#define SHOW 626
#define SIMILAR 627
#define SIMPLE 628
#define SKIP 629
#define SMALLINT 630
#define SNAPSHOT 631
#define SOME 632
#define SQL_P 633
#define STABLE 634
#define STANDALONE_P 635
#define START 636
#define STATEMENT 637
#define STATISTICS 638
#define STDIN 639
#define STDOUT 640
#define STORAGE 641
#define STRICT_P 642
#define STRIP_P 643
#define STRUCT 644
#define SUBSCRIPTION 645
#define SUBSTRING 646
#define SUMMARIZE 647
#define SYMMETRIC 648
#define SYSID 649
#define SYSTEM_P 650
#define TABLE 651
#define TABLES 652
#define TABLESAMPLE 653
#define TABLESPACE 654
#define TEMP 655
#define TEMPLATE 656
#define TEMPORARY 657
#define TEXT_P 658
#define THEN 659
#define TIME 660
#define TIMESTAMP 661
#define TO 662
#define TRAILING 663
#define TRANSACTION 664
#define TRANSFORM 665
#define TREAT 666
#define TRIGGER 667
#define TRIM 668
#define TRUE_P 669
#define TRUNCATE 670
#define TRUSTED 671
#define TRY_CAST 672
#define TYPE_P 673
#define TYPES_P 674
#define UNBOUNDED 675
#define UNCOMMITTED 676
#define UNENCRYPTED 677
#define UNION 678
#define UNIQUE 679
#define UNKNOWN 680
#define UNLISTEN 681
#define UNLOGGED 682
#define UNTIL 683
#define UPDATE 684
#define USER 685
#define USING 686
#define VACUUM 687
#define VALID 688
#define VALIDATE 689
#define VALIDATOR 690
#define VALUE_P 691
#define VALUES 692
#define VARCHAR 693
#define VARIADIC 694
#define VARYING 695
#define VERBOSE 696
#define VERSION_P 697
#define VIEW 698
#define VIEWS 699
#define VOLATILE 700
#define WHEN 701
#define WHERE 702
#define WHITESPACE_P 703
#define WINDOW 704
#define WITH 705
#define WITHIN 706
#define WITHOUT 707
#define WORK 708
#define WRAPPER 709
#define WRITE_P 710
#define XML_P 711
#define XMLATTRIBUTES 712
#define XMLCONCAT 713
#define XMLELEMENT 714
#define XMLEXISTS 715
#define XMLFOREST 716
#define XMLNAMESPACES 717
#define XMLPARSE 718
#define XMLPI 719
#define XMLROOT 720
#define XMLSERIALIZE 721
#define XMLTABLE 722
#define YEAR_P 723
#define YEARS_P 724
#define YES_P 725
#define ZONE 726
#define NOT_LA 727
#define NULLS_LA 728
#define WITH_LA 729
#define POSTFIXOP 730
#define UMINUS 731




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
/* Line 1529 of yacc.c.  */
#line 1045 "third_party/libpg_query/grammar/grammar_out.hpp"
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


