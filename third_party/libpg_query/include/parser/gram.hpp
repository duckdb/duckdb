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
     MAPPING = 493,
     MATCH = 494,
     MATERIALIZED = 495,
     MAXVALUE = 496,
     METHOD = 497,
     MICROSECOND_P = 498,
     MICROSECONDS_P = 499,
     MILLISECOND_P = 500,
     MILLISECONDS_P = 501,
     MINUTE_P = 502,
     MINUTES_P = 503,
     MINVALUE = 504,
     MODE = 505,
     MONTH_P = 506,
     MONTHS_P = 507,
     MOVE = 508,
     NAME_P = 509,
     NAMES = 510,
     NATIONAL = 511,
     NATURAL = 512,
     NCHAR = 513,
     NEW = 514,
     NEXT = 515,
     NO = 516,
     NONE = 517,
     NOT = 518,
     NOTHING = 519,
     NOTIFY = 520,
     NOTNULL = 521,
     NOWAIT = 522,
     NULL_P = 523,
     NULLIF = 524,
     NULLS_P = 525,
     NUMERIC = 526,
     OBJECT_P = 527,
     OF = 528,
     OFF = 529,
     OFFSET = 530,
     OIDS = 531,
     OLD = 532,
     ON = 533,
     ONLY = 534,
     OPERATOR = 535,
     OPTION = 536,
     OPTIONS = 537,
     OR = 538,
     ORDER = 539,
     ORDINALITY = 540,
     OUT_P = 541,
     OUTER_P = 542,
     OVER = 543,
     OVERLAPS = 544,
     OVERLAY = 545,
     OVERRIDING = 546,
     OWNED = 547,
     OWNER = 548,
     PARALLEL = 549,
     PARSER = 550,
     PARTIAL = 551,
     PARTITION = 552,
     PASSING = 553,
     PASSWORD = 554,
     PERCENT = 555,
     PLACING = 556,
     PLANS = 557,
     POLICY = 558,
     POSITION = 559,
     PRAGMA_P = 560,
     PRECEDING = 561,
     PRECISION = 562,
     PREPARE = 563,
     PREPARED = 564,
     PRESERVE = 565,
     PRIMARY = 566,
     PRIOR = 567,
     PRIVILEGES = 568,
     PROCEDURAL = 569,
     PROCEDURE = 570,
     PROGRAM = 571,
     PUBLICATION = 572,
     QUOTE = 573,
     RANGE = 574,
     READ_P = 575,
     REAL = 576,
     REASSIGN = 577,
     RECHECK = 578,
     RECURSIVE = 579,
     REF = 580,
     REFERENCES = 581,
     REFERENCING = 582,
     REFRESH = 583,
     REINDEX = 584,
     RELATIVE_P = 585,
     RELEASE = 586,
     RENAME = 587,
     REPEATABLE = 588,
     REPLACE = 589,
     REPLICA = 590,
     RESET = 591,
     RESTART = 592,
     RESTRICT = 593,
     RETURNING = 594,
     RETURNS = 595,
     REVOKE = 596,
     RIGHT = 597,
     ROLE = 598,
     ROLLBACK = 599,
     ROLLUP = 600,
     ROW = 601,
     ROWS = 602,
     RULE = 603,
     SAMPLE = 604,
     SAVEPOINT = 605,
     SCHEMA = 606,
     SCHEMAS = 607,
     SCROLL = 608,
     SEARCH = 609,
     SECOND_P = 610,
     SECONDS_P = 611,
     SECURITY = 612,
     SELECT = 613,
     SEQUENCE = 614,
     SEQUENCES = 615,
     SERIALIZABLE = 616,
     SERVER = 617,
     SESSION = 618,
     SESSION_USER = 619,
     SET = 620,
     SETOF = 621,
     SETS = 622,
     SHARE = 623,
     SHOW = 624,
     SIMILAR = 625,
     SIMPLE = 626,
     SKIP = 627,
     SMALLINT = 628,
     SNAPSHOT = 629,
     SOME = 630,
     SQL_P = 631,
     STABLE = 632,
     STANDALONE_P = 633,
     START = 634,
     STATEMENT = 635,
     STATISTICS = 636,
     STDIN = 637,
     STDOUT = 638,
     STORAGE = 639,
     STRICT_P = 640,
     STRIP_P = 641,
     SUBSCRIPTION = 642,
     SUBSTRING = 643,
     SYMMETRIC = 644,
     SYSID = 645,
     SYSTEM_P = 646,
     TABLE = 647,
     TABLES = 648,
     TABLESAMPLE = 649,
     TABLESPACE = 650,
     TEMP = 651,
     TEMPLATE = 652,
     TEMPORARY = 653,
     TEXT_P = 654,
     THEN = 655,
     TIME = 656,
     TIMESTAMP = 657,
     TO = 658,
     TRAILING = 659,
     TRANSACTION = 660,
     TRANSFORM = 661,
     TREAT = 662,
     TRIGGER = 663,
     TRIM = 664,
     TRUE_P = 665,
     TRUNCATE = 666,
     TRUSTED = 667,
     TYPE_P = 668,
     TYPES_P = 669,
     UNBOUNDED = 670,
     UNCOMMITTED = 671,
     UNENCRYPTED = 672,
     UNION = 673,
     UNIQUE = 674,
     UNKNOWN = 675,
     UNLISTEN = 676,
     UNLOGGED = 677,
     UNTIL = 678,
     UPDATE = 679,
     USER = 680,
     USING = 681,
     VACUUM = 682,
     VALID = 683,
     VALIDATE = 684,
     VALIDATOR = 685,
     VALUE_P = 686,
     VALUES = 687,
     VARCHAR = 688,
     VARIADIC = 689,
     VARYING = 690,
     VERBOSE = 691,
     VERSION_P = 692,
     VIEW = 693,
     VIEWS = 694,
     VOLATILE = 695,
     WHEN = 696,
     WHERE = 697,
     WHITESPACE_P = 698,
     WINDOW = 699,
     WITH = 700,
     WITHIN = 701,
     WITHOUT = 702,
     WORK = 703,
     WRAPPER = 704,
     WRITE_P = 705,
     XML_P = 706,
     XMLATTRIBUTES = 707,
     XMLCONCAT = 708,
     XMLELEMENT = 709,
     XMLEXISTS = 710,
     XMLFOREST = 711,
     XMLNAMESPACES = 712,
     XMLPARSE = 713,
     XMLPI = 714,
     XMLROOT = 715,
     XMLSERIALIZE = 716,
     XMLTABLE = 717,
     YEAR_P = 718,
     YEARS_P = 719,
     YES_P = 720,
     ZONE = 721,
     NOT_LA = 722,
     NULLS_LA = 723,
     WITH_LA = 724,
     POSTFIXOP = 725,
     UMINUS = 726
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
#define MAPPING 493
#define MATCH 494
#define MATERIALIZED 495
#define MAXVALUE 496
#define METHOD 497
#define MICROSECOND_P 498
#define MICROSECONDS_P 499
#define MILLISECOND_P 500
#define MILLISECONDS_P 501
#define MINUTE_P 502
#define MINUTES_P 503
#define MINVALUE 504
#define MODE 505
#define MONTH_P 506
#define MONTHS_P 507
#define MOVE 508
#define NAME_P 509
#define NAMES 510
#define NATIONAL 511
#define NATURAL 512
#define NCHAR 513
#define NEW 514
#define NEXT 515
#define NO 516
#define NONE 517
#define NOT 518
#define NOTHING 519
#define NOTIFY 520
#define NOTNULL 521
#define NOWAIT 522
#define NULL_P 523
#define NULLIF 524
#define NULLS_P 525
#define NUMERIC 526
#define OBJECT_P 527
#define OF 528
#define OFF 529
#define OFFSET 530
#define OIDS 531
#define OLD 532
#define ON 533
#define ONLY 534
#define OPERATOR 535
#define OPTION 536
#define OPTIONS 537
#define OR 538
#define ORDER 539
#define ORDINALITY 540
#define OUT_P 541
#define OUTER_P 542
#define OVER 543
#define OVERLAPS 544
#define OVERLAY 545
#define OVERRIDING 546
#define OWNED 547
#define OWNER 548
#define PARALLEL 549
#define PARSER 550
#define PARTIAL 551
#define PARTITION 552
#define PASSING 553
#define PASSWORD 554
#define PERCENT 555
#define PLACING 556
#define PLANS 557
#define POLICY 558
#define POSITION 559
#define PRAGMA_P 560
#define PRECEDING 561
#define PRECISION 562
#define PREPARE 563
#define PREPARED 564
#define PRESERVE 565
#define PRIMARY 566
#define PRIOR 567
#define PRIVILEGES 568
#define PROCEDURAL 569
#define PROCEDURE 570
#define PROGRAM 571
#define PUBLICATION 572
#define QUOTE 573
#define RANGE 574
#define READ_P 575
#define REAL 576
#define REASSIGN 577
#define RECHECK 578
#define RECURSIVE 579
#define REF 580
#define REFERENCES 581
#define REFERENCING 582
#define REFRESH 583
#define REINDEX 584
#define RELATIVE_P 585
#define RELEASE 586
#define RENAME 587
#define REPEATABLE 588
#define REPLACE 589
#define REPLICA 590
#define RESET 591
#define RESTART 592
#define RESTRICT 593
#define RETURNING 594
#define RETURNS 595
#define REVOKE 596
#define RIGHT 597
#define ROLE 598
#define ROLLBACK 599
#define ROLLUP 600
#define ROW 601
#define ROWS 602
#define RULE 603
#define SAMPLE 604
#define SAVEPOINT 605
#define SCHEMA 606
#define SCHEMAS 607
#define SCROLL 608
#define SEARCH 609
#define SECOND_P 610
#define SECONDS_P 611
#define SECURITY 612
#define SELECT 613
#define SEQUENCE 614
#define SEQUENCES 615
#define SERIALIZABLE 616
#define SERVER 617
#define SESSION 618
#define SESSION_USER 619
#define SET 620
#define SETOF 621
#define SETS 622
#define SHARE 623
#define SHOW 624
#define SIMILAR 625
#define SIMPLE 626
#define SKIP 627
#define SMALLINT 628
#define SNAPSHOT 629
#define SOME 630
#define SQL_P 631
#define STABLE 632
#define STANDALONE_P 633
#define START 634
#define STATEMENT 635
#define STATISTICS 636
#define STDIN 637
#define STDOUT 638
#define STORAGE 639
#define STRICT_P 640
#define STRIP_P 641
#define SUBSCRIPTION 642
#define SUBSTRING 643
#define SYMMETRIC 644
#define SYSID 645
#define SYSTEM_P 646
#define TABLE 647
#define TABLES 648
#define TABLESAMPLE 649
#define TABLESPACE 650
#define TEMP 651
#define TEMPLATE 652
#define TEMPORARY 653
#define TEXT_P 654
#define THEN 655
#define TIME 656
#define TIMESTAMP 657
#define TO 658
#define TRAILING 659
#define TRANSACTION 660
#define TRANSFORM 661
#define TREAT 662
#define TRIGGER 663
#define TRIM 664
#define TRUE_P 665
#define TRUNCATE 666
#define TRUSTED 667
#define TYPE_P 668
#define TYPES_P 669
#define UNBOUNDED 670
#define UNCOMMITTED 671
#define UNENCRYPTED 672
#define UNION 673
#define UNIQUE 674
#define UNKNOWN 675
#define UNLISTEN 676
#define UNLOGGED 677
#define UNTIL 678
#define UPDATE 679
#define USER 680
#define USING 681
#define VACUUM 682
#define VALID 683
#define VALIDATE 684
#define VALIDATOR 685
#define VALUE_P 686
#define VALUES 687
#define VARCHAR 688
#define VARIADIC 689
#define VARYING 690
#define VERBOSE 691
#define VERSION_P 692
#define VIEW 693
#define VIEWS 694
#define VOLATILE 695
#define WHEN 696
#define WHERE 697
#define WHITESPACE_P 698
#define WINDOW 699
#define WITH 700
#define WITHIN 701
#define WITHOUT 702
#define WORK 703
#define WRAPPER 704
#define WRITE_P 705
#define XML_P 706
#define XMLATTRIBUTES 707
#define XMLCONCAT 708
#define XMLELEMENT 709
#define XMLEXISTS 710
#define XMLFOREST 711
#define XMLNAMESPACES 712
#define XMLPARSE 713
#define XMLPI 714
#define XMLROOT 715
#define XMLSERIALIZE 716
#define XMLTABLE 717
#define YEAR_P 718
#define YEARS_P 719
#define YES_P 720
#define ZONE 721
#define NOT_LA 722
#define NULLS_LA 723
#define WITH_LA 724
#define POSTFIXOP 725
#define UMINUS 726




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
#line 1034 "third_party/libpg_query/grammar/grammar_out.hpp"
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


