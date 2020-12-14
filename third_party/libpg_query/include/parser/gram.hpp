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
     LESS_EQUALS = 270,
     GREATER_EQUALS = 271,
     NOT_EQUALS = 272,
     ABORT_P = 273,
     ABSOLUTE_P = 274,
     ACCESS = 275,
     ACTION = 276,
     ADD_P = 277,
     ADMIN = 278,
     AFTER = 279,
     AGGREGATE = 280,
     ALL = 281,
     ALSO = 282,
     ALTER = 283,
     ALWAYS = 284,
     ANALYSE = 285,
     ANALYZE = 286,
     AND = 287,
     ANY = 288,
     ARRAY = 289,
     AS = 290,
     ASC_P = 291,
     ASSERTION = 292,
     ASSIGNMENT = 293,
     ASYMMETRIC = 294,
     AT = 295,
     ATTACH = 296,
     ATTRIBUTE = 297,
     AUTHORIZATION = 298,
     BACKWARD = 299,
     BEFORE = 300,
     BEGIN_P = 301,
     BETWEEN = 302,
     BIGINT = 303,
     BINARY = 304,
     BIT = 305,
     BOOLEAN_P = 306,
     BOTH = 307,
     BY = 308,
     CACHE = 309,
     CALL_P = 310,
     CALLED = 311,
     CASCADE = 312,
     CASCADED = 313,
     CASE = 314,
     CAST = 315,
     CATALOG_P = 316,
     CHAIN = 317,
     CHAR_P = 318,
     CHARACTER = 319,
     CHARACTERISTICS = 320,
     CHECK_P = 321,
     CHECKPOINT = 322,
     CLASS = 323,
     CLOSE = 324,
     CLUSTER = 325,
     COALESCE = 326,
     COLLATE = 327,
     COLLATION = 328,
     COLUMN = 329,
     COLUMNS = 330,
     COMMENT = 331,
     COMMENTS = 332,
     COMMIT = 333,
     COMMITTED = 334,
     CONCURRENTLY = 335,
     CONFIGURATION = 336,
     CONFLICT = 337,
     CONNECTION = 338,
     CONSTRAINT = 339,
     CONSTRAINTS = 340,
     CONTENT_P = 341,
     CONTINUE_P = 342,
     CONVERSION_P = 343,
     COPY = 344,
     COST = 345,
     CREATE_P = 346,
     CROSS = 347,
     CSV = 348,
     CUBE = 349,
     CURRENT_P = 350,
     CURRENT_CATALOG = 351,
     CURRENT_DATE = 352,
     CURRENT_ROLE = 353,
     CURRENT_SCHEMA = 354,
     CURRENT_TIME = 355,
     CURRENT_TIMESTAMP = 356,
     CURRENT_USER = 357,
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
     HANDLER = 432,
     HAVING = 433,
     HEADER_P = 434,
     HOLD = 435,
     HOUR_P = 436,
     HOURS_P = 437,
     IDENTITY_P = 438,
     IF_P = 439,
     ILIKE = 440,
     IMMEDIATE = 441,
     IMMUTABLE = 442,
     IMPLICIT_P = 443,
     IMPORT_P = 444,
     IN_P = 445,
     INCLUDING = 446,
     INCREMENT = 447,
     INDEX = 448,
     INDEXES = 449,
     INHERIT = 450,
     INHERITS = 451,
     INITIALLY = 452,
     INLINE_P = 453,
     INNER_P = 454,
     INOUT = 455,
     INPUT_P = 456,
     INSENSITIVE = 457,
     INSERT = 458,
     INSTEAD = 459,
     INT_P = 460,
     INTEGER = 461,
     INTERSECT = 462,
     INTERVAL = 463,
     INTO = 464,
     INVOKER = 465,
     IS = 466,
     ISNULL = 467,
     ISOLATION = 468,
     JOIN = 469,
     KEY = 470,
     LABEL = 471,
     LANGUAGE = 472,
     LARGE_P = 473,
     LAST_P = 474,
     LATERAL_P = 475,
     LEADING = 476,
     LEAKPROOF = 477,
     LEFT = 478,
     LEVEL = 479,
     LIKE = 480,
     LIMIT = 481,
     LISTEN = 482,
     LOAD = 483,
     LOCAL = 484,
     LOCALTIME = 485,
     LOCALTIMESTAMP = 486,
     LOCATION = 487,
     LOCK_P = 488,
     LOCKED = 489,
     LOGGED = 490,
     MACRO = 491,
     MAPPING = 492,
     MATCH = 493,
     MATERIALIZED = 494,
     MAXVALUE = 495,
     METHOD = 496,
     MICROSECOND_P = 497,
     MICROSECONDS_P = 498,
     MILLISECOND_P = 499,
     MILLISECONDS_P = 500,
     MINUTE_P = 501,
     MINUTES_P = 502,
     MINVALUE = 503,
     MODE = 504,
     MONTH_P = 505,
     MONTHS_P = 506,
     MOVE = 507,
     NAME_P = 508,
     NAMES = 509,
     NATIONAL = 510,
     NATURAL = 511,
     NCHAR = 512,
     NEW = 513,
     NEXT = 514,
     NO = 515,
     NONE = 516,
     NOT = 517,
     NOTHING = 518,
     NOTIFY = 519,
     NOTNULL = 520,
     NOWAIT = 521,
     NULL_P = 522,
     NULLIF = 523,
     NULLS_P = 524,
     NUMERIC = 525,
     OBJECT_P = 526,
     OF = 527,
     OFF = 528,
     OFFSET = 529,
     OIDS = 530,
     OLD = 531,
     ON = 532,
     ONLY = 533,
     OPERATOR = 534,
     OPTION = 535,
     OPTIONS = 536,
     OR = 537,
     ORDER = 538,
     ORDINALITY = 539,
     OUT_P = 540,
     OUTER_P = 541,
     OVER = 542,
     OVERLAPS = 543,
     OVERLAY = 544,
     OVERRIDING = 545,
     OWNED = 546,
     OWNER = 547,
     PARALLEL = 548,
     PARSER = 549,
     PARTIAL = 550,
     PARTITION = 551,
     PASSING = 552,
     PASSWORD = 553,
     PERCENT = 554,
     PLACING = 555,
     PLANS = 556,
     POLICY = 557,
     POSITION = 558,
     PRAGMA_P = 559,
     PRECEDING = 560,
     PRECISION = 561,
     PREPARE = 562,
     PREPARED = 563,
     PRESERVE = 564,
     PRIMARY = 565,
     PRIOR = 566,
     PRIVILEGES = 567,
     PROCEDURAL = 568,
     PROCEDURE = 569,
     PROGRAM = 570,
     PUBLICATION = 571,
     QUOTE = 572,
     RANGE = 573,
     READ_P = 574,
     REAL = 575,
     REASSIGN = 576,
     RECHECK = 577,
     RECURSIVE = 578,
     REF = 579,
     REFERENCES = 580,
     REFERENCING = 581,
     REFRESH = 582,
     REINDEX = 583,
     RELATIVE_P = 584,
     RELEASE = 585,
     RENAME = 586,
     REPEATABLE = 587,
     REPLACE = 588,
     REPLICA = 589,
     RESET = 590,
     RESTART = 591,
     RESTRICT = 592,
     RETURNING = 593,
     RETURNS = 594,
     REVOKE = 595,
     RIGHT = 596,
     ROLE = 597,
     ROLLBACK = 598,
     ROLLUP = 599,
     ROW = 600,
     ROWS = 601,
     RULE = 602,
     SAMPLE = 603,
     SAVEPOINT = 604,
     SCHEMA = 605,
     SCHEMAS = 606,
     SCROLL = 607,
     SEARCH = 608,
     SECOND_P = 609,
     SECONDS_P = 610,
     SECURITY = 611,
     SELECT = 612,
     SEQUENCE = 613,
     SEQUENCES = 614,
     SERIALIZABLE = 615,
     SERVER = 616,
     SESSION = 617,
     SESSION_USER = 618,
     SET = 619,
     SETOF = 620,
     SETS = 621,
     SHARE = 622,
     SHOW = 623,
     SIMILAR = 624,
     SIMPLE = 625,
     SKIP = 626,
     SMALLINT = 627,
     SNAPSHOT = 628,
     SOME = 629,
     SQL_P = 630,
     STABLE = 631,
     STANDALONE_P = 632,
     START = 633,
     STATEMENT = 634,
     STATISTICS = 635,
     STDIN = 636,
     STDOUT = 637,
     STORAGE = 638,
     STRICT_P = 639,
     STRIP_P = 640,
     SUBSCRIPTION = 641,
     SUBSTRING = 642,
     SYMMETRIC = 643,
     SYSID = 644,
     SYSTEM_P = 645,
     TABLE = 646,
     TABLES = 647,
     TABLESAMPLE = 648,
     TABLESPACE = 649,
     TEMP = 650,
     TEMPLATE = 651,
     TEMPORARY = 652,
     TEXT_P = 653,
     THEN = 654,
     TIME = 655,
     TIMESTAMP = 656,
     TO = 657,
     TRAILING = 658,
     TRANSACTION = 659,
     TRANSFORM = 660,
     TREAT = 661,
     TRIGGER = 662,
     TRIM = 663,
     TRUE_P = 664,
     TRUNCATE = 665,
     TRUSTED = 666,
     TYPE_P = 667,
     TYPES_P = 668,
     UNBOUNDED = 669,
     UNCOMMITTED = 670,
     UNENCRYPTED = 671,
     UNION = 672,
     UNIQUE = 673,
     UNKNOWN = 674,
     UNLISTEN = 675,
     UNLOGGED = 676,
     UNTIL = 677,
     UPDATE = 678,
     USER = 679,
     USING = 680,
     VACUUM = 681,
     VALID = 682,
     VALIDATE = 683,
     VALIDATOR = 684,
     VALUE_P = 685,
     VALUES = 686,
     VARCHAR = 687,
     VARIADIC = 688,
     VARYING = 689,
     VERBOSE = 690,
     VERSION_P = 691,
     VIEW = 692,
     VIEWS = 693,
     VOLATILE = 694,
     WHEN = 695,
     WHERE = 696,
     WHITESPACE_P = 697,
     WINDOW = 698,
     WITH = 699,
     WITHIN = 700,
     WITHOUT = 701,
     WORK = 702,
     WRAPPER = 703,
     WRITE_P = 704,
     XML_P = 705,
     XMLATTRIBUTES = 706,
     XMLCONCAT = 707,
     XMLELEMENT = 708,
     XMLEXISTS = 709,
     XMLFOREST = 710,
     XMLNAMESPACES = 711,
     XMLPARSE = 712,
     XMLPI = 713,
     XMLROOT = 714,
     XMLSERIALIZE = 715,
     XMLTABLE = 716,
     YEAR_P = 717,
     YEARS_P = 718,
     YES_P = 719,
     ZONE = 720,
     NOT_LA = 721,
     NULLS_LA = 722,
     WITH_LA = 723,
     POSTFIXOP = 724,
     UMINUS = 725
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
#define LESS_EQUALS 270
#define GREATER_EQUALS 271
#define NOT_EQUALS 272
#define ABORT_P 273
#define ABSOLUTE_P 274
#define ACCESS 275
#define ACTION 276
#define ADD_P 277
#define ADMIN 278
#define AFTER 279
#define AGGREGATE 280
#define ALL 281
#define ALSO 282
#define ALTER 283
#define ALWAYS 284
#define ANALYSE 285
#define ANALYZE 286
#define AND 287
#define ANY 288
#define ARRAY 289
#define AS 290
#define ASC_P 291
#define ASSERTION 292
#define ASSIGNMENT 293
#define ASYMMETRIC 294
#define AT 295
#define ATTACH 296
#define ATTRIBUTE 297
#define AUTHORIZATION 298
#define BACKWARD 299
#define BEFORE 300
#define BEGIN_P 301
#define BETWEEN 302
#define BIGINT 303
#define BINARY 304
#define BIT 305
#define BOOLEAN_P 306
#define BOTH 307
#define BY 308
#define CACHE 309
#define CALL_P 310
#define CALLED 311
#define CASCADE 312
#define CASCADED 313
#define CASE 314
#define CAST 315
#define CATALOG_P 316
#define CHAIN 317
#define CHAR_P 318
#define CHARACTER 319
#define CHARACTERISTICS 320
#define CHECK_P 321
#define CHECKPOINT 322
#define CLASS 323
#define CLOSE 324
#define CLUSTER 325
#define COALESCE 326
#define COLLATE 327
#define COLLATION 328
#define COLUMN 329
#define COLUMNS 330
#define COMMENT 331
#define COMMENTS 332
#define COMMIT 333
#define COMMITTED 334
#define CONCURRENTLY 335
#define CONFIGURATION 336
#define CONFLICT 337
#define CONNECTION 338
#define CONSTRAINT 339
#define CONSTRAINTS 340
#define CONTENT_P 341
#define CONTINUE_P 342
#define CONVERSION_P 343
#define COPY 344
#define COST 345
#define CREATE_P 346
#define CROSS 347
#define CSV 348
#define CUBE 349
#define CURRENT_P 350
#define CURRENT_CATALOG 351
#define CURRENT_DATE 352
#define CURRENT_ROLE 353
#define CURRENT_SCHEMA 354
#define CURRENT_TIME 355
#define CURRENT_TIMESTAMP 356
#define CURRENT_USER 357
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
#define HANDLER 432
#define HAVING 433
#define HEADER_P 434
#define HOLD 435
#define HOUR_P 436
#define HOURS_P 437
#define IDENTITY_P 438
#define IF_P 439
#define ILIKE 440
#define IMMEDIATE 441
#define IMMUTABLE 442
#define IMPLICIT_P 443
#define IMPORT_P 444
#define IN_P 445
#define INCLUDING 446
#define INCREMENT 447
#define INDEX 448
#define INDEXES 449
#define INHERIT 450
#define INHERITS 451
#define INITIALLY 452
#define INLINE_P 453
#define INNER_P 454
#define INOUT 455
#define INPUT_P 456
#define INSENSITIVE 457
#define INSERT 458
#define INSTEAD 459
#define INT_P 460
#define INTEGER 461
#define INTERSECT 462
#define INTERVAL 463
#define INTO 464
#define INVOKER 465
#define IS 466
#define ISNULL 467
#define ISOLATION 468
#define JOIN 469
#define KEY 470
#define LABEL 471
#define LANGUAGE 472
#define LARGE_P 473
#define LAST_P 474
#define LATERAL_P 475
#define LEADING 476
#define LEAKPROOF 477
#define LEFT 478
#define LEVEL 479
#define LIKE 480
#define LIMIT 481
#define LISTEN 482
#define LOAD 483
#define LOCAL 484
#define LOCALTIME 485
#define LOCALTIMESTAMP 486
#define LOCATION 487
#define LOCK_P 488
#define LOCKED 489
#define LOGGED 490
#define MACRO 491
#define MAPPING 492
#define MATCH 493
#define MATERIALIZED 494
#define MAXVALUE 495
#define METHOD 496
#define MICROSECOND_P 497
#define MICROSECONDS_P 498
#define MILLISECOND_P 499
#define MILLISECONDS_P 500
#define MINUTE_P 501
#define MINUTES_P 502
#define MINVALUE 503
#define MODE 504
#define MONTH_P 505
#define MONTHS_P 506
#define MOVE 507
#define NAME_P 508
#define NAMES 509
#define NATIONAL 510
#define NATURAL 511
#define NCHAR 512
#define NEW 513
#define NEXT 514
#define NO 515
#define NONE 516
#define NOT 517
#define NOTHING 518
#define NOTIFY 519
#define NOTNULL 520
#define NOWAIT 521
#define NULL_P 522
#define NULLIF 523
#define NULLS_P 524
#define NUMERIC 525
#define OBJECT_P 526
#define OF 527
#define OFF 528
#define OFFSET 529
#define OIDS 530
#define OLD 531
#define ON 532
#define ONLY 533
#define OPERATOR 534
#define OPTION 535
#define OPTIONS 536
#define OR 537
#define ORDER 538
#define ORDINALITY 539
#define OUT_P 540
#define OUTER_P 541
#define OVER 542
#define OVERLAPS 543
#define OVERLAY 544
#define OVERRIDING 545
#define OWNED 546
#define OWNER 547
#define PARALLEL 548
#define PARSER 549
#define PARTIAL 550
#define PARTITION 551
#define PASSING 552
#define PASSWORD 553
#define PERCENT 554
#define PLACING 555
#define PLANS 556
#define POLICY 557
#define POSITION 558
#define PRAGMA_P 559
#define PRECEDING 560
#define PRECISION 561
#define PREPARE 562
#define PREPARED 563
#define PRESERVE 564
#define PRIMARY 565
#define PRIOR 566
#define PRIVILEGES 567
#define PROCEDURAL 568
#define PROCEDURE 569
#define PROGRAM 570
#define PUBLICATION 571
#define QUOTE 572
#define RANGE 573
#define READ_P 574
#define REAL 575
#define REASSIGN 576
#define RECHECK 577
#define RECURSIVE 578
#define REF 579
#define REFERENCES 580
#define REFERENCING 581
#define REFRESH 582
#define REINDEX 583
#define RELATIVE_P 584
#define RELEASE 585
#define RENAME 586
#define REPEATABLE 587
#define REPLACE 588
#define REPLICA 589
#define RESET 590
#define RESTART 591
#define RESTRICT 592
#define RETURNING 593
#define RETURNS 594
#define REVOKE 595
#define RIGHT 596
#define ROLE 597
#define ROLLBACK 598
#define ROLLUP 599
#define ROW 600
#define ROWS 601
#define RULE 602
#define SAMPLE 603
#define SAVEPOINT 604
#define SCHEMA 605
#define SCHEMAS 606
#define SCROLL 607
#define SEARCH 608
#define SECOND_P 609
#define SECONDS_P 610
#define SECURITY 611
#define SELECT 612
#define SEQUENCE 613
#define SEQUENCES 614
#define SERIALIZABLE 615
#define SERVER 616
#define SESSION 617
#define SESSION_USER 618
#define SET 619
#define SETOF 620
#define SETS 621
#define SHARE 622
#define SHOW 623
#define SIMILAR 624
#define SIMPLE 625
#define SKIP 626
#define SMALLINT 627
#define SNAPSHOT 628
#define SOME 629
#define SQL_P 630
#define STABLE 631
#define STANDALONE_P 632
#define START 633
#define STATEMENT 634
#define STATISTICS 635
#define STDIN 636
#define STDOUT 637
#define STORAGE 638
#define STRICT_P 639
#define STRIP_P 640
#define SUBSCRIPTION 641
#define SUBSTRING 642
#define SYMMETRIC 643
#define SYSID 644
#define SYSTEM_P 645
#define TABLE 646
#define TABLES 647
#define TABLESAMPLE 648
#define TABLESPACE 649
#define TEMP 650
#define TEMPLATE 651
#define TEMPORARY 652
#define TEXT_P 653
#define THEN 654
#define TIME 655
#define TIMESTAMP 656
#define TO 657
#define TRAILING 658
#define TRANSACTION 659
#define TRANSFORM 660
#define TREAT 661
#define TRIGGER 662
#define TRIM 663
#define TRUE_P 664
#define TRUNCATE 665
#define TRUSTED 666
#define TYPE_P 667
#define TYPES_P 668
#define UNBOUNDED 669
#define UNCOMMITTED 670
#define UNENCRYPTED 671
#define UNION 672
#define UNIQUE 673
#define UNKNOWN 674
#define UNLISTEN 675
#define UNLOGGED 676
#define UNTIL 677
#define UPDATE 678
#define USER 679
#define USING 680
#define VACUUM 681
#define VALID 682
#define VALIDATE 683
#define VALIDATOR 684
#define VALUE_P 685
#define VALUES 686
#define VARCHAR 687
#define VARIADIC 688
#define VARYING 689
#define VERBOSE 690
#define VERSION_P 691
#define VIEW 692
#define VIEWS 693
#define VOLATILE 694
#define WHEN 695
#define WHERE 696
#define WHITESPACE_P 697
#define WINDOW 698
#define WITH 699
#define WITHIN 700
#define WITHOUT 701
#define WORK 702
#define WRAPPER 703
#define WRITE_P 704
#define XML_P 705
#define XMLATTRIBUTES 706
#define XMLCONCAT 707
#define XMLELEMENT 708
#define XMLEXISTS 709
#define XMLFOREST 710
#define XMLNAMESPACES 711
#define XMLPARSE 712
#define XMLPI 713
#define XMLROOT 714
#define XMLSERIALIZE 715
#define XMLTABLE 716
#define YEAR_P 717
#define YEARS_P 718
#define YES_P 719
#define ZONE 720
#define NOT_LA 721
#define NULLS_LA 722
#define WITH_LA 723
#define POSTFIXOP 724
#define UMINUS 725




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
#line 1032 "third_party/libpg_query/grammar/grammar_out.hpp"
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


