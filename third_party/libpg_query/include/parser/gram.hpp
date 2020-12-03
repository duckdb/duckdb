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
     MINUTE_P = 497,
     MINUTES_P = 498,
     MINVALUE = 499,
     MODE = 500,
     MONTH_P = 501,
     MONTHS_P = 502,
     MOVE = 503,
     NAME_P = 504,
     NAMES = 505,
     NATIONAL = 506,
     NATURAL = 507,
     NCHAR = 508,
     NEW = 509,
     NEXT = 510,
     NO = 511,
     NONE = 512,
     NOT = 513,
     NOTHING = 514,
     NOTIFY = 515,
     NOTNULL = 516,
     NOWAIT = 517,
     NULL_P = 518,
     NULLIF = 519,
     NULLS_P = 520,
     NUMERIC = 521,
     OBJECT_P = 522,
     OF = 523,
     OFF = 524,
     OFFSET = 525,
     OIDS = 526,
     OLD = 527,
     ON = 528,
     ONLY = 529,
     OPERATOR = 530,
     OPTION = 531,
     OPTIONS = 532,
     OR = 533,
     ORDER = 534,
     ORDINALITY = 535,
     OUT_P = 536,
     OUTER_P = 537,
     OVER = 538,
     OVERLAPS = 539,
     OVERLAY = 540,
     OVERRIDING = 541,
     OWNED = 542,
     OWNER = 543,
     PARALLEL = 544,
     PARSER = 545,
     PARTIAL = 546,
     PARTITION = 547,
     PASSING = 548,
     PASSWORD = 549,
     PLACING = 550,
     PLANS = 551,
     POLICY = 552,
     POSITION = 553,
     PRAGMA_P = 554,
     PRECEDING = 555,
     PRECISION = 556,
     PREPARE = 557,
     PREPARED = 558,
     PRESERVE = 559,
     PRIMARY = 560,
     PRIOR = 561,
     PRIVILEGES = 562,
     PROCEDURAL = 563,
     PROCEDURE = 564,
     PROGRAM = 565,
     PUBLICATION = 566,
     QUOTE = 567,
     RANGE = 568,
     READ_P = 569,
     REAL = 570,
     REASSIGN = 571,
     RECHECK = 572,
     RECURSIVE = 573,
     REF = 574,
     REFERENCES = 575,
     REFERENCING = 576,
     REFRESH = 577,
     REINDEX = 578,
     RELATIVE_P = 579,
     RELEASE = 580,
     RENAME = 581,
     REPEATABLE = 582,
     REPLACE = 583,
     REPLICA = 584,
     RESET = 585,
     RESTART = 586,
     RESTRICT = 587,
     RETURNING = 588,
     RETURNS = 589,
     REVOKE = 590,
     RIGHT = 591,
     ROLE = 592,
     ROLLBACK = 593,
     ROLLUP = 594,
     ROW = 595,
     ROWS = 596,
     RULE = 597,
     SAVEPOINT = 598,
     SCHEMA = 599,
     SCHEMAS = 600,
     SCROLL = 601,
     SEARCH = 602,
     SECOND_P = 603,
     SECONDS_P = 604,
     SECURITY = 605,
     SELECT = 606,
     SEQUENCE = 607,
     SEQUENCES = 608,
     SERIALIZABLE = 609,
     SERVER = 610,
     SESSION = 611,
     SESSION_USER = 612,
     SET = 613,
     SETOF = 614,
     SETS = 615,
     SHARE = 616,
     SHOW = 617,
     SIMILAR = 618,
     SIMPLE = 619,
     SKIP = 620,
     SMALLINT = 621,
     SNAPSHOT = 622,
     SOME = 623,
     SQL_P = 624,
     STABLE = 625,
     STANDALONE_P = 626,
     START = 627,
     STATEMENT = 628,
     STATISTICS = 629,
     STDIN = 630,
     STDOUT = 631,
     STORAGE = 632,
     STRICT_P = 633,
     STRIP_P = 634,
     SUBSCRIPTION = 635,
     SUBSTRING = 636,
     SYMMETRIC = 637,
     SYSID = 638,
     SYSTEM_P = 639,
     TABLE = 640,
     TABLES = 641,
     TABLESAMPLE = 642,
     TABLESPACE = 643,
     TEMP = 644,
     TEMPLATE = 645,
     TEMPORARY = 646,
     TEXT_P = 647,
     THEN = 648,
     TIME = 649,
     TIMESTAMP = 650,
     TO = 651,
     TRAILING = 652,
     TRANSACTION = 653,
     TRANSFORM = 654,
     TREAT = 655,
     TRIGGER = 656,
     TRIM = 657,
     TRUE_P = 658,
     TRUNCATE = 659,
     TRUSTED = 660,
     TYPE_P = 661,
     TYPES_P = 662,
     UNBOUNDED = 663,
     UNCOMMITTED = 664,
     UNENCRYPTED = 665,
     UNION = 666,
     UNIQUE = 667,
     UNKNOWN = 668,
     UNLISTEN = 669,
     UNLOGGED = 670,
     UNTIL = 671,
     UPDATE = 672,
     USER = 673,
     USING = 674,
     VACUUM = 675,
     VALID = 676,
     VALIDATE = 677,
     VALIDATOR = 678,
     VALUE_P = 679,
     VALUES = 680,
     VARCHAR = 681,
     VARIADIC = 682,
     VARYING = 683,
     VERBOSE = 684,
     VERSION_P = 685,
     VIEW = 686,
     VIEWS = 687,
     VOLATILE = 688,
     WHEN = 689,
     WHERE = 690,
     WHITESPACE_P = 691,
     WINDOW = 692,
     WITH = 693,
     WITHIN = 694,
     WITHOUT = 695,
     WORK = 696,
     WRAPPER = 697,
     WRITE_P = 698,
     XML_P = 699,
     XMLATTRIBUTES = 700,
     XMLCONCAT = 701,
     XMLELEMENT = 702,
     XMLEXISTS = 703,
     XMLFOREST = 704,
     XMLNAMESPACES = 705,
     XMLPARSE = 706,
     XMLPI = 707,
     XMLROOT = 708,
     XMLSERIALIZE = 709,
     XMLTABLE = 710,
     YEAR_P = 711,
     YEARS_P = 712,
     YES_P = 713,
     ZONE = 714,
     NOT_LA = 715,
     NULLS_LA = 716,
     WITH_LA = 717,
     POSTFIXOP = 718,
     UMINUS = 719
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
#define MINUTE_P 497
#define MINUTES_P 498
#define MINVALUE 499
#define MODE 500
#define MONTH_P 501
#define MONTHS_P 502
#define MOVE 503
#define NAME_P 504
#define NAMES 505
#define NATIONAL 506
#define NATURAL 507
#define NCHAR 508
#define NEW 509
#define NEXT 510
#define NO 511
#define NONE 512
#define NOT 513
#define NOTHING 514
#define NOTIFY 515
#define NOTNULL 516
#define NOWAIT 517
#define NULL_P 518
#define NULLIF 519
#define NULLS_P 520
#define NUMERIC 521
#define OBJECT_P 522
#define OF 523
#define OFF 524
#define OFFSET 525
#define OIDS 526
#define OLD 527
#define ON 528
#define ONLY 529
#define OPERATOR 530
#define OPTION 531
#define OPTIONS 532
#define OR 533
#define ORDER 534
#define ORDINALITY 535
#define OUT_P 536
#define OUTER_P 537
#define OVER 538
#define OVERLAPS 539
#define OVERLAY 540
#define OVERRIDING 541
#define OWNED 542
#define OWNER 543
#define PARALLEL 544
#define PARSER 545
#define PARTIAL 546
#define PARTITION 547
#define PASSING 548
#define PASSWORD 549
#define PLACING 550
#define PLANS 551
#define POLICY 552
#define POSITION 553
#define PRAGMA_P 554
#define PRECEDING 555
#define PRECISION 556
#define PREPARE 557
#define PREPARED 558
#define PRESERVE 559
#define PRIMARY 560
#define PRIOR 561
#define PRIVILEGES 562
#define PROCEDURAL 563
#define PROCEDURE 564
#define PROGRAM 565
#define PUBLICATION 566
#define QUOTE 567
#define RANGE 568
#define READ_P 569
#define REAL 570
#define REASSIGN 571
#define RECHECK 572
#define RECURSIVE 573
#define REF 574
#define REFERENCES 575
#define REFERENCING 576
#define REFRESH 577
#define REINDEX 578
#define RELATIVE_P 579
#define RELEASE 580
#define RENAME 581
#define REPEATABLE 582
#define REPLACE 583
#define REPLICA 584
#define RESET 585
#define RESTART 586
#define RESTRICT 587
#define RETURNING 588
#define RETURNS 589
#define REVOKE 590
#define RIGHT 591
#define ROLE 592
#define ROLLBACK 593
#define ROLLUP 594
#define ROW 595
#define ROWS 596
#define RULE 597
#define SAVEPOINT 598
#define SCHEMA 599
#define SCHEMAS 600
#define SCROLL 601
#define SEARCH 602
#define SECOND_P 603
#define SECONDS_P 604
#define SECURITY 605
#define SELECT 606
#define SEQUENCE 607
#define SEQUENCES 608
#define SERIALIZABLE 609
#define SERVER 610
#define SESSION 611
#define SESSION_USER 612
#define SET 613
#define SETOF 614
#define SETS 615
#define SHARE 616
#define SHOW 617
#define SIMILAR 618
#define SIMPLE 619
#define SKIP 620
#define SMALLINT 621
#define SNAPSHOT 622
#define SOME 623
#define SQL_P 624
#define STABLE 625
#define STANDALONE_P 626
#define START 627
#define STATEMENT 628
#define STATISTICS 629
#define STDIN 630
#define STDOUT 631
#define STORAGE 632
#define STRICT_P 633
#define STRIP_P 634
#define SUBSCRIPTION 635
#define SUBSTRING 636
#define SYMMETRIC 637
#define SYSID 638
#define SYSTEM_P 639
#define TABLE 640
#define TABLES 641
#define TABLESAMPLE 642
#define TABLESPACE 643
#define TEMP 644
#define TEMPLATE 645
#define TEMPORARY 646
#define TEXT_P 647
#define THEN 648
#define TIME 649
#define TIMESTAMP 650
#define TO 651
#define TRAILING 652
#define TRANSACTION 653
#define TRANSFORM 654
#define TREAT 655
#define TRIGGER 656
#define TRIM 657
#define TRUE_P 658
#define TRUNCATE 659
#define TRUSTED 660
#define TYPE_P 661
#define TYPES_P 662
#define UNBOUNDED 663
#define UNCOMMITTED 664
#define UNENCRYPTED 665
#define UNION 666
#define UNIQUE 667
#define UNKNOWN 668
#define UNLISTEN 669
#define UNLOGGED 670
#define UNTIL 671
#define UPDATE 672
#define USER 673
#define USING 674
#define VACUUM 675
#define VALID 676
#define VALIDATE 677
#define VALIDATOR 678
#define VALUE_P 679
#define VALUES 680
#define VARCHAR 681
#define VARIADIC 682
#define VARYING 683
#define VERBOSE 684
#define VERSION_P 685
#define VIEW 686
#define VIEWS 687
#define VOLATILE 688
#define WHEN 689
#define WHERE 690
#define WHITESPACE_P 691
#define WINDOW 692
#define WITH 693
#define WITHIN 694
#define WITHOUT 695
#define WORK 696
#define WRAPPER 697
#define WRITE_P 698
#define XML_P 699
#define XMLATTRIBUTES 700
#define XMLCONCAT 701
#define XMLELEMENT 702
#define XMLEXISTS 703
#define XMLFOREST 704
#define XMLNAMESPACES 705
#define XMLPARSE 706
#define XMLPI 707
#define XMLROOT 708
#define XMLSERIALIZE 709
#define XMLTABLE 710
#define YEAR_P 711
#define YEARS_P 712
#define YES_P 713
#define ZONE 714
#define NOT_LA 715
#define NULLS_LA 716
#define WITH_LA 717
#define POSTFIXOP 718
#define UMINUS 719




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
#line 1020 "third_party/libpg_query/grammar/grammar_out.hpp"
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


