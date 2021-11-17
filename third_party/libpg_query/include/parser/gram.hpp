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
     ILIKE = 443,
     IMMEDIATE = 444,
     IMMUTABLE = 445,
     IMPLICIT_P = 446,
     IMPORT_P = 447,
     IN_P = 448,
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
     INSTEAD = 462,
     INT_P = 463,
     INTEGER = 464,
     INTERSECT = 465,
     INTERVAL = 466,
     INTO = 467,
     INVOKER = 468,
     IS = 469,
     ISNULL = 470,
     ISOLATION = 471,
     JOIN = 472,
     KEY = 473,
     LABEL = 474,
     LANGUAGE = 475,
     LARGE_P = 476,
     LAST_P = 477,
     LATERAL_P = 478,
     LEADING = 479,
     LEAKPROOF = 480,
     LEFT = 481,
     LEVEL = 482,
     LIKE = 483,
     LIMIT = 484,
     LISTEN = 485,
     LOAD = 486,
     LOCAL = 487,
     LOCALTIME = 488,
     LOCALTIMESTAMP = 489,
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
     PLACING = 559,
     PLANS = 560,
     POLICY = 561,
     POSITION = 562,
     PRAGMA_P = 563,
     PRECEDING = 564,
     PRECISION = 565,
     PREPARE = 566,
     PREPARED = 567,
     PRESERVE = 568,
     PRIMARY = 569,
     PRIOR = 570,
     PRIVILEGES = 571,
     PROCEDURAL = 572,
     PROCEDURE = 573,
     PROGRAM = 574,
     PUBLICATION = 575,
     QUOTE = 576,
     RANGE = 577,
     READ_P = 578,
     REAL = 579,
     REASSIGN = 580,
     RECHECK = 581,
     RECURSIVE = 582,
     REF = 583,
     REFERENCES = 584,
     REFERENCING = 585,
     REFRESH = 586,
     REINDEX = 587,
     RELATIVE_P = 588,
     RELEASE = 589,
     RENAME = 590,
     REPEATABLE = 591,
     REPLACE = 592,
     REPLICA = 593,
     RESET = 594,
     RESTART = 595,
     RESTRICT = 596,
     RETURNING = 597,
     RETURNS = 598,
     REVOKE = 599,
     RIGHT = 600,
     ROLE = 601,
     ROLLBACK = 602,
     ROLLUP = 603,
     ROW = 604,
     ROWS = 605,
     RULE = 606,
     SAMPLE = 607,
     SAVEPOINT = 608,
     SCHEMA = 609,
     SCHEMAS = 610,
     SCROLL = 611,
     SEARCH = 612,
     SECOND_P = 613,
     SECONDS_P = 614,
     SECURITY = 615,
     SELECT = 616,
     SEQUENCE = 617,
     SEQUENCES = 618,
     SERIALIZABLE = 619,
     SERVER = 620,
     SESSION = 621,
     SESSION_USER = 622,
     SET = 623,
     SETOF = 624,
     SETS = 625,
     SHARE = 626,
     SHOW = 627,
     SIMILAR = 628,
     SIMPLE = 629,
     SKIP = 630,
     SMALLINT = 631,
     SNAPSHOT = 632,
     SOME = 633,
     SQL_P = 634,
     STABLE = 635,
     STANDALONE_P = 636,
     START = 637,
     STATEMENT = 638,
     STATISTICS = 639,
     STDIN = 640,
     STDOUT = 641,
     STORAGE = 642,
     STRICT_P = 643,
     STRIP_P = 644,
     STRUCT = 645,
     SUBSCRIPTION = 646,
     SUBSTRING = 647,
     SUMMARIZE = 648,
     SYMMETRIC = 649,
     SYSID = 650,
     SYSTEM_P = 651,
     TABLE = 652,
     TABLES = 653,
     TABLESAMPLE = 654,
     TABLESPACE = 655,
     TEMP = 656,
     TEMPLATE = 657,
     TEMPORARY = 658,
     TEXT_P = 659,
     THEN = 660,
     TIME = 661,
     TIMESTAMP = 662,
     TO = 663,
     TRAILING = 664,
     TRANSACTION = 665,
     TRANSFORM = 666,
     TREAT = 667,
     TRIGGER = 668,
     TRIM = 669,
     TRUE_P = 670,
     TRUNCATE = 671,
     TRUSTED = 672,
     TRY_CAST = 673,
     TYPE_P = 674,
     TYPES_P = 675,
     UNBOUNDED = 676,
     UNCOMMITTED = 677,
     UNENCRYPTED = 678,
     UNION = 679,
     UNIQUE = 680,
     UNKNOWN = 681,
     UNLISTEN = 682,
     UNLOGGED = 683,
     UNTIL = 684,
     UPDATE = 685,
     USER = 686,
     USING = 687,
     VACUUM = 688,
     VALID = 689,
     VALIDATE = 690,
     VALIDATOR = 691,
     VALUE_P = 692,
     VALUES = 693,
     VARCHAR = 694,
     VARIADIC = 695,
     VARYING = 696,
     VERBOSE = 697,
     VERSION_P = 698,
     VIEW = 699,
     VIEWS = 700,
     VOLATILE = 701,
     WHEN = 702,
     WHERE = 703,
     WHITESPACE_P = 704,
     WINDOW = 705,
     WITH = 706,
     WITHIN = 707,
     WITHOUT = 708,
     WORK = 709,
     WRAPPER = 710,
     WRITE_P = 711,
     XML_P = 712,
     XMLATTRIBUTES = 713,
     XMLCONCAT = 714,
     XMLELEMENT = 715,
     XMLEXISTS = 716,
     XMLFOREST = 717,
     XMLNAMESPACES = 718,
     XMLPARSE = 719,
     XMLPI = 720,
     XMLROOT = 721,
     XMLSERIALIZE = 722,
     XMLTABLE = 723,
     YEAR_P = 724,
     YEARS_P = 725,
     YES_P = 726,
     ZONE = 727,
     NOT_LA = 728,
     NULLS_LA = 729,
     WITH_LA = 730,
     POSTFIXOP = 731,
     UMINUS = 732
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
#define ILIKE 443
#define IMMEDIATE 444
#define IMMUTABLE 445
#define IMPLICIT_P 446
#define IMPORT_P 447
#define IN_P 448
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
#define INSTEAD 462
#define INT_P 463
#define INTEGER 464
#define INTERSECT 465
#define INTERVAL 466
#define INTO 467
#define INVOKER 468
#define IS 469
#define ISNULL 470
#define ISOLATION 471
#define JOIN 472
#define KEY 473
#define LABEL 474
#define LANGUAGE 475
#define LARGE_P 476
#define LAST_P 477
#define LATERAL_P 478
#define LEADING 479
#define LEAKPROOF 480
#define LEFT 481
#define LEVEL 482
#define LIKE 483
#define LIMIT 484
#define LISTEN 485
#define LOAD 486
#define LOCAL 487
#define LOCALTIME 488
#define LOCALTIMESTAMP 489
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
#define PLACING 559
#define PLANS 560
#define POLICY 561
#define POSITION 562
#define PRAGMA_P 563
#define PRECEDING 564
#define PRECISION 565
#define PREPARE 566
#define PREPARED 567
#define PRESERVE 568
#define PRIMARY 569
#define PRIOR 570
#define PRIVILEGES 571
#define PROCEDURAL 572
#define PROCEDURE 573
#define PROGRAM 574
#define PUBLICATION 575
#define QUOTE 576
#define RANGE 577
#define READ_P 578
#define REAL 579
#define REASSIGN 580
#define RECHECK 581
#define RECURSIVE 582
#define REF 583
#define REFERENCES 584
#define REFERENCING 585
#define REFRESH 586
#define REINDEX 587
#define RELATIVE_P 588
#define RELEASE 589
#define RENAME 590
#define REPEATABLE 591
#define REPLACE 592
#define REPLICA 593
#define RESET 594
#define RESTART 595
#define RESTRICT 596
#define RETURNING 597
#define RETURNS 598
#define REVOKE 599
#define RIGHT 600
#define ROLE 601
#define ROLLBACK 602
#define ROLLUP 603
#define ROW 604
#define ROWS 605
#define RULE 606
#define SAMPLE 607
#define SAVEPOINT 608
#define SCHEMA 609
#define SCHEMAS 610
#define SCROLL 611
#define SEARCH 612
#define SECOND_P 613
#define SECONDS_P 614
#define SECURITY 615
#define SELECT 616
#define SEQUENCE 617
#define SEQUENCES 618
#define SERIALIZABLE 619
#define SERVER 620
#define SESSION 621
#define SESSION_USER 622
#define SET 623
#define SETOF 624
#define SETS 625
#define SHARE 626
#define SHOW 627
#define SIMILAR 628
#define SIMPLE 629
#define SKIP 630
#define SMALLINT 631
#define SNAPSHOT 632
#define SOME 633
#define SQL_P 634
#define STABLE 635
#define STANDALONE_P 636
#define START 637
#define STATEMENT 638
#define STATISTICS 639
#define STDIN 640
#define STDOUT 641
#define STORAGE 642
#define STRICT_P 643
#define STRIP_P 644
#define STRUCT 645
#define SUBSCRIPTION 646
#define SUBSTRING 647
#define SUMMARIZE 648
#define SYMMETRIC 649
#define SYSID 650
#define SYSTEM_P 651
#define TABLE 652
#define TABLES 653
#define TABLESAMPLE 654
#define TABLESPACE 655
#define TEMP 656
#define TEMPLATE 657
#define TEMPORARY 658
#define TEXT_P 659
#define THEN 660
#define TIME 661
#define TIMESTAMP 662
#define TO 663
#define TRAILING 664
#define TRANSACTION 665
#define TRANSFORM 666
#define TREAT 667
#define TRIGGER 668
#define TRIM 669
#define TRUE_P 670
#define TRUNCATE 671
#define TRUSTED 672
#define TRY_CAST 673
#define TYPE_P 674
#define TYPES_P 675
#define UNBOUNDED 676
#define UNCOMMITTED 677
#define UNENCRYPTED 678
#define UNION 679
#define UNIQUE 680
#define UNKNOWN 681
#define UNLISTEN 682
#define UNLOGGED 683
#define UNTIL 684
#define UPDATE 685
#define USER 686
#define USING 687
#define VACUUM 688
#define VALID 689
#define VALIDATE 690
#define VALIDATOR 691
#define VALUE_P 692
#define VALUES 693
#define VARCHAR 694
#define VARIADIC 695
#define VARYING 696
#define VERBOSE 697
#define VERSION_P 698
#define VIEW 699
#define VIEWS 700
#define VOLATILE 701
#define WHEN 702
#define WHERE 703
#define WHITESPACE_P 704
#define WINDOW 705
#define WITH 706
#define WITHIN 707
#define WITHOUT 708
#define WORK 709
#define WRAPPER 710
#define WRITE_P 711
#define XML_P 712
#define XMLATTRIBUTES 713
#define XMLCONCAT 714
#define XMLELEMENT 715
#define XMLEXISTS 716
#define XMLFOREST 717
#define XMLNAMESPACES 718
#define XMLPARSE 719
#define XMLPI 720
#define XMLROOT 721
#define XMLSERIALIZE 722
#define XMLTABLE 723
#define YEAR_P 724
#define YEARS_P 725
#define YES_P 726
#define ZONE 727
#define NOT_LA 728
#define NULLS_LA 729
#define WITH_LA 730
#define POSTFIXOP 731
#define UMINUS 732




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
#line 1047 "third_party/libpg_query/grammar/grammar_out.hpp"
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


