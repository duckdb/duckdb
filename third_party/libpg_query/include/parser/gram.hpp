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
     HANDLER = 433,
     HAVING = 434,
     HEADER_P = 435,
     HOLD = 436,
     HOUR_P = 437,
     HOURS_P = 438,
     IDENTITY_P = 439,
     IF_P = 440,
     ILIKE = 441,
     IMMEDIATE = 442,
     IMMUTABLE = 443,
     IMPLICIT_P = 444,
     IMPORT_P = 445,
     IN_P = 446,
     INCLUDING = 447,
     INCREMENT = 448,
     INDEX = 449,
     INDEXES = 450,
     INHERIT = 451,
     INHERITS = 452,
     INITIALLY = 453,
     INLINE_P = 454,
     INNER_P = 455,
     INOUT = 456,
     INPUT_P = 457,
     INSENSITIVE = 458,
     INSERT = 459,
     INSTEAD = 460,
     INT_P = 461,
     INTEGER = 462,
     INTERSECT = 463,
     INTERVAL = 464,
     INTO = 465,
     INVOKER = 466,
     IS = 467,
     ISNULL = 468,
     ISOLATION = 469,
     JOIN = 470,
     KEY = 471,
     LABEL = 472,
     LANGUAGE = 473,
     LARGE_P = 474,
     LAST_P = 475,
     LATERAL_P = 476,
     LEADING = 477,
     LEAKPROOF = 478,
     LEFT = 479,
     LEVEL = 480,
     LIKE = 481,
     LIMIT = 482,
     LISTEN = 483,
     LOAD = 484,
     LOCAL = 485,
     LOCALTIME = 486,
     LOCALTIMESTAMP = 487,
     LOCATION = 488,
     LOCK_P = 489,
     LOCKED = 490,
     LOGGED = 491,
     MACRO = 492,
     MAP = 493,
     MAPPING = 494,
     MATCH = 495,
     MATERIALIZED = 496,
     MAXVALUE = 497,
     METHOD = 498,
     MICROSECOND_P = 499,
     MICROSECONDS_P = 500,
     MILLISECOND_P = 501,
     MILLISECONDS_P = 502,
     MINUTE_P = 503,
     MINUTES_P = 504,
     MINVALUE = 505,
     MODE = 506,
     MONTH_P = 507,
     MONTHS_P = 508,
     MOVE = 509,
     NAME_P = 510,
     NAMES = 511,
     NATIONAL = 512,
     NATURAL = 513,
     NCHAR = 514,
     NEW = 515,
     NEXT = 516,
     NO = 517,
     NONE = 518,
     NOT = 519,
     NOTHING = 520,
     NOTIFY = 521,
     NOTNULL = 522,
     NOWAIT = 523,
     NULL_P = 524,
     NULLIF = 525,
     NULLS_P = 526,
     NUMERIC = 527,
     OBJECT_P = 528,
     OF = 529,
     OFF = 530,
     OFFSET = 531,
     OIDS = 532,
     OLD = 533,
     ON = 534,
     ONLY = 535,
     OPERATOR = 536,
     OPTION = 537,
     OPTIONS = 538,
     OR = 539,
     ORDER = 540,
     ORDINALITY = 541,
     OUT_P = 542,
     OUTER_P = 543,
     OVER = 544,
     OVERLAPS = 545,
     OVERLAY = 546,
     OVERRIDING = 547,
     OWNED = 548,
     OWNER = 549,
     PARALLEL = 550,
     PARSER = 551,
     PARTIAL = 552,
     PARTITION = 553,
     PASSING = 554,
     PASSWORD = 555,
     PERCENT = 556,
     PLACING = 557,
     PLANS = 558,
     POLICY = 559,
     POSITION = 560,
     PRAGMA_P = 561,
     PRECEDING = 562,
     PRECISION = 563,
     PREPARE = 564,
     PREPARED = 565,
     PRESERVE = 566,
     PRIMARY = 567,
     PRIOR = 568,
     PRIVILEGES = 569,
     PROCEDURAL = 570,
     PROCEDURE = 571,
     PROGRAM = 572,
     PUBLICATION = 573,
     QUOTE = 574,
     RANGE = 575,
     READ_P = 576,
     REAL = 577,
     REASSIGN = 578,
     RECHECK = 579,
     RECURSIVE = 580,
     REF = 581,
     REFERENCES = 582,
     REFERENCING = 583,
     REFRESH = 584,
     REINDEX = 585,
     RELATIVE_P = 586,
     RELEASE = 587,
     RENAME = 588,
     REPEATABLE = 589,
     REPLACE = 590,
     REPLICA = 591,
     RESET = 592,
     RESTART = 593,
     RESTRICT = 594,
     RETURNING = 595,
     RETURNS = 596,
     REVOKE = 597,
     RIGHT = 598,
     ROLE = 599,
     ROLLBACK = 600,
     ROLLUP = 601,
     ROW = 602,
     ROWS = 603,
     RULE = 604,
     SAMPLE = 605,
     SAVEPOINT = 606,
     SCHEMA = 607,
     SCHEMAS = 608,
     SCROLL = 609,
     SEARCH = 610,
     SECOND_P = 611,
     SECONDS_P = 612,
     SECURITY = 613,
     SELECT = 614,
     SEQUENCE = 615,
     SEQUENCES = 616,
     SERIALIZABLE = 617,
     SERVER = 618,
     SESSION = 619,
     SESSION_USER = 620,
     SET = 621,
     SETOF = 622,
     SETS = 623,
     SHARE = 624,
     SHOW = 625,
     SIMILAR = 626,
     SIMPLE = 627,
     SKIP = 628,
     SMALLINT = 629,
     SNAPSHOT = 630,
     SOME = 631,
     SQL_P = 632,
     STABLE = 633,
     STANDALONE_P = 634,
     START = 635,
     STATEMENT = 636,
     STATISTICS = 637,
     STDIN = 638,
     STDOUT = 639,
     STORAGE = 640,
     STRICT_P = 641,
     STRIP_P = 642,
     STRUCT = 643,
     SUBSCRIPTION = 644,
     SUBSTRING = 645,
     SUMMARIZE = 646,
     SYMMETRIC = 647,
     SYSID = 648,
     SYSTEM_P = 649,
     TABLE = 650,
     TABLES = 651,
     TABLESAMPLE = 652,
     TABLESPACE = 653,
     TEMP = 654,
     TEMPLATE = 655,
     TEMPORARY = 656,
     TEXT_P = 657,
     THEN = 658,
     TIME = 659,
     TIMESTAMP = 660,
     TO = 661,
     TRAILING = 662,
     TRANSACTION = 663,
     TRANSFORM = 664,
     TREAT = 665,
     TRIGGER = 666,
     TRIM = 667,
     TRUE_P = 668,
     TRUNCATE = 669,
     TRUSTED = 670,
     TRY_CAST = 671,
     TYPE_P = 672,
     TYPES_P = 673,
     UNBOUNDED = 674,
     UNCOMMITTED = 675,
     UNENCRYPTED = 676,
     UNION = 677,
     UNIQUE = 678,
     UNKNOWN = 679,
     UNLISTEN = 680,
     UNLOGGED = 681,
     UNTIL = 682,
     UPDATE = 683,
     USER = 684,
     USING = 685,
     VACUUM = 686,
     VALID = 687,
     VALIDATE = 688,
     VALIDATOR = 689,
     VALUE_P = 690,
     VALUES = 691,
     VARCHAR = 692,
     VARIADIC = 693,
     VARYING = 694,
     VERBOSE = 695,
     VERSION_P = 696,
     VIEW = 697,
     VIEWS = 698,
     VOLATILE = 699,
     WHEN = 700,
     WHERE = 701,
     WHITESPACE_P = 702,
     WINDOW = 703,
     WITH = 704,
     WITHIN = 705,
     WITHOUT = 706,
     WORK = 707,
     WRAPPER = 708,
     WRITE_P = 709,
     XML_P = 710,
     XMLATTRIBUTES = 711,
     XMLCONCAT = 712,
     XMLELEMENT = 713,
     XMLEXISTS = 714,
     XMLFOREST = 715,
     XMLNAMESPACES = 716,
     XMLPARSE = 717,
     XMLPI = 718,
     XMLROOT = 719,
     XMLSERIALIZE = 720,
     XMLTABLE = 721,
     YEAR_P = 722,
     YEARS_P = 723,
     YES_P = 724,
     ZONE = 725,
     NOT_LA = 726,
     NULLS_LA = 727,
     WITH_LA = 728,
     POSTFIXOP = 729,
     UMINUS = 730
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
#define HANDLER 433
#define HAVING 434
#define HEADER_P 435
#define HOLD 436
#define HOUR_P 437
#define HOURS_P 438
#define IDENTITY_P 439
#define IF_P 440
#define ILIKE 441
#define IMMEDIATE 442
#define IMMUTABLE 443
#define IMPLICIT_P 444
#define IMPORT_P 445
#define IN_P 446
#define INCLUDING 447
#define INCREMENT 448
#define INDEX 449
#define INDEXES 450
#define INHERIT 451
#define INHERITS 452
#define INITIALLY 453
#define INLINE_P 454
#define INNER_P 455
#define INOUT 456
#define INPUT_P 457
#define INSENSITIVE 458
#define INSERT 459
#define INSTEAD 460
#define INT_P 461
#define INTEGER 462
#define INTERSECT 463
#define INTERVAL 464
#define INTO 465
#define INVOKER 466
#define IS 467
#define ISNULL 468
#define ISOLATION 469
#define JOIN 470
#define KEY 471
#define LABEL 472
#define LANGUAGE 473
#define LARGE_P 474
#define LAST_P 475
#define LATERAL_P 476
#define LEADING 477
#define LEAKPROOF 478
#define LEFT 479
#define LEVEL 480
#define LIKE 481
#define LIMIT 482
#define LISTEN 483
#define LOAD 484
#define LOCAL 485
#define LOCALTIME 486
#define LOCALTIMESTAMP 487
#define LOCATION 488
#define LOCK_P 489
#define LOCKED 490
#define LOGGED 491
#define MACRO 492
#define MAP 493
#define MAPPING 494
#define MATCH 495
#define MATERIALIZED 496
#define MAXVALUE 497
#define METHOD 498
#define MICROSECOND_P 499
#define MICROSECONDS_P 500
#define MILLISECOND_P 501
#define MILLISECONDS_P 502
#define MINUTE_P 503
#define MINUTES_P 504
#define MINVALUE 505
#define MODE 506
#define MONTH_P 507
#define MONTHS_P 508
#define MOVE 509
#define NAME_P 510
#define NAMES 511
#define NATIONAL 512
#define NATURAL 513
#define NCHAR 514
#define NEW 515
#define NEXT 516
#define NO 517
#define NONE 518
#define NOT 519
#define NOTHING 520
#define NOTIFY 521
#define NOTNULL 522
#define NOWAIT 523
#define NULL_P 524
#define NULLIF 525
#define NULLS_P 526
#define NUMERIC 527
#define OBJECT_P 528
#define OF 529
#define OFF 530
#define OFFSET 531
#define OIDS 532
#define OLD 533
#define ON 534
#define ONLY 535
#define OPERATOR 536
#define OPTION 537
#define OPTIONS 538
#define OR 539
#define ORDER 540
#define ORDINALITY 541
#define OUT_P 542
#define OUTER_P 543
#define OVER 544
#define OVERLAPS 545
#define OVERLAY 546
#define OVERRIDING 547
#define OWNED 548
#define OWNER 549
#define PARALLEL 550
#define PARSER 551
#define PARTIAL 552
#define PARTITION 553
#define PASSING 554
#define PASSWORD 555
#define PERCENT 556
#define PLACING 557
#define PLANS 558
#define POLICY 559
#define POSITION 560
#define PRAGMA_P 561
#define PRECEDING 562
#define PRECISION 563
#define PREPARE 564
#define PREPARED 565
#define PRESERVE 566
#define PRIMARY 567
#define PRIOR 568
#define PRIVILEGES 569
#define PROCEDURAL 570
#define PROCEDURE 571
#define PROGRAM 572
#define PUBLICATION 573
#define QUOTE 574
#define RANGE 575
#define READ_P 576
#define REAL 577
#define REASSIGN 578
#define RECHECK 579
#define RECURSIVE 580
#define REF 581
#define REFERENCES 582
#define REFERENCING 583
#define REFRESH 584
#define REINDEX 585
#define RELATIVE_P 586
#define RELEASE 587
#define RENAME 588
#define REPEATABLE 589
#define REPLACE 590
#define REPLICA 591
#define RESET 592
#define RESTART 593
#define RESTRICT 594
#define RETURNING 595
#define RETURNS 596
#define REVOKE 597
#define RIGHT 598
#define ROLE 599
#define ROLLBACK 600
#define ROLLUP 601
#define ROW 602
#define ROWS 603
#define RULE 604
#define SAMPLE 605
#define SAVEPOINT 606
#define SCHEMA 607
#define SCHEMAS 608
#define SCROLL 609
#define SEARCH 610
#define SECOND_P 611
#define SECONDS_P 612
#define SECURITY 613
#define SELECT 614
#define SEQUENCE 615
#define SEQUENCES 616
#define SERIALIZABLE 617
#define SERVER 618
#define SESSION 619
#define SESSION_USER 620
#define SET 621
#define SETOF 622
#define SETS 623
#define SHARE 624
#define SHOW 625
#define SIMILAR 626
#define SIMPLE 627
#define SKIP 628
#define SMALLINT 629
#define SNAPSHOT 630
#define SOME 631
#define SQL_P 632
#define STABLE 633
#define STANDALONE_P 634
#define START 635
#define STATEMENT 636
#define STATISTICS 637
#define STDIN 638
#define STDOUT 639
#define STORAGE 640
#define STRICT_P 641
#define STRIP_P 642
#define STRUCT 643
#define SUBSCRIPTION 644
#define SUBSTRING 645
#define SUMMARIZE 646
#define SYMMETRIC 647
#define SYSID 648
#define SYSTEM_P 649
#define TABLE 650
#define TABLES 651
#define TABLESAMPLE 652
#define TABLESPACE 653
#define TEMP 654
#define TEMPLATE 655
#define TEMPORARY 656
#define TEXT_P 657
#define THEN 658
#define TIME 659
#define TIMESTAMP 660
#define TO 661
#define TRAILING 662
#define TRANSACTION 663
#define TRANSFORM 664
#define TREAT 665
#define TRIGGER 666
#define TRIM 667
#define TRUE_P 668
#define TRUNCATE 669
#define TRUSTED 670
#define TRY_CAST 671
#define TYPE_P 672
#define TYPES_P 673
#define UNBOUNDED 674
#define UNCOMMITTED 675
#define UNENCRYPTED 676
#define UNION 677
#define UNIQUE 678
#define UNKNOWN 679
#define UNLISTEN 680
#define UNLOGGED 681
#define UNTIL 682
#define UPDATE 683
#define USER 684
#define USING 685
#define VACUUM 686
#define VALID 687
#define VALIDATE 688
#define VALIDATOR 689
#define VALUE_P 690
#define VALUES 691
#define VARCHAR 692
#define VARIADIC 693
#define VARYING 694
#define VERBOSE 695
#define VERSION_P 696
#define VIEW 697
#define VIEWS 698
#define VOLATILE 699
#define WHEN 700
#define WHERE 701
#define WHITESPACE_P 702
#define WINDOW 703
#define WITH 704
#define WITHIN 705
#define WITHOUT 706
#define WORK 707
#define WRAPPER 708
#define WRITE_P 709
#define XML_P 710
#define XMLATTRIBUTES 711
#define XMLCONCAT 712
#define XMLELEMENT 713
#define XMLEXISTS 714
#define XMLFOREST 715
#define XMLNAMESPACES 716
#define XMLPARSE 717
#define XMLPI 718
#define XMLROOT 719
#define XMLSERIALIZE 720
#define XMLTABLE 721
#define YEAR_P 722
#define YEARS_P 723
#define YES_P 724
#define ZONE 725
#define NOT_LA 726
#define NULLS_LA 727
#define WITH_LA 728
#define POSTFIXOP 729
#define UMINUS 730




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
#line 1043 "third_party/libpg_query/grammar/grammar_out.hpp"
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


