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
     EXTENSION = 405,
     EXTERNAL = 406,
     EXTRACT = 407,
     FALSE_P = 408,
     FAMILY = 409,
     FETCH = 410,
     FILTER = 411,
     FIRST_P = 412,
     FLOAT_P = 413,
     FOLLOWING = 414,
     FOR = 415,
     FORCE = 416,
     FOREIGN = 417,
     FORWARD = 418,
     FREEZE = 419,
     FROM = 420,
     FULL = 421,
     FUNCTION = 422,
     FUNCTIONS = 423,
     GENERATED = 424,
     GLOB = 425,
     GLOBAL = 426,
     GRANT = 427,
     GRANTED = 428,
     GROUP_P = 429,
     GROUPING = 430,
     HANDLER = 431,
     HAVING = 432,
     HEADER_P = 433,
     HOLD = 434,
     HOUR_P = 435,
     IDENTITY_P = 436,
     IF_P = 437,
     ILIKE = 438,
     IMMEDIATE = 439,
     IMMUTABLE = 440,
     IMPLICIT_P = 441,
     IMPORT_P = 442,
     IN_P = 443,
     INCLUDING = 444,
     INCREMENT = 445,
     INDEX = 446,
     INDEXES = 447,
     INHERIT = 448,
     INHERITS = 449,
     INITIALLY = 450,
     INLINE_P = 451,
     INNER_P = 452,
     INOUT = 453,
     INPUT_P = 454,
     INSENSITIVE = 455,
     INSERT = 456,
     INSTEAD = 457,
     INT_P = 458,
     INTEGER = 459,
     INTERSECT = 460,
     INTERVAL = 461,
     INTO = 462,
     INVOKER = 463,
     IS = 464,
     ISNULL = 465,
     ISOLATION = 466,
     JOIN = 467,
     KEY = 468,
     LABEL = 469,
     LANGUAGE = 470,
     LARGE_P = 471,
     LAST_P = 472,
     LATERAL_P = 473,
     LEADING = 474,
     LEAKPROOF = 475,
     LEFT = 476,
     LEVEL = 477,
     LIKE = 478,
     LIMIT = 479,
     LISTEN = 480,
     LOAD = 481,
     LOCAL = 482,
     LOCALTIME = 483,
     LOCALTIMESTAMP = 484,
     LOCATION = 485,
     LOCK_P = 486,
     LOCKED = 487,
     LOGGED = 488,
     MAPPING = 489,
     MATCH = 490,
     MATERIALIZED = 491,
     MAXVALUE = 492,
     METHOD = 493,
     MINUTE_P = 494,
     MINVALUE = 495,
     MODE = 496,
     MONTH_P = 497,
     MOVE = 498,
     NAME_P = 499,
     NAMES = 500,
     NATIONAL = 501,
     NATURAL = 502,
     NCHAR = 503,
     NEW = 504,
     NEXT = 505,
     NO = 506,
     NONE = 507,
     NOT = 508,
     NOTHING = 509,
     NOTIFY = 510,
     NOTNULL = 511,
     NOWAIT = 512,
     NULL_P = 513,
     NULLIF = 514,
     NULLS_P = 515,
     NUMERIC = 516,
     OBJECT_P = 517,
     OF = 518,
     OFF = 519,
     OFFSET = 520,
     OIDS = 521,
     OLD = 522,
     ON = 523,
     ONLY = 524,
     OPERATOR = 525,
     OPTION = 526,
     OPTIONS = 527,
     OR = 528,
     ORDER = 529,
     ORDINALITY = 530,
     OUT_P = 531,
     OUTER_P = 532,
     OVER = 533,
     OVERLAPS = 534,
     OVERLAY = 535,
     OVERRIDING = 536,
     OWNED = 537,
     OWNER = 538,
     PARALLEL = 539,
     PARSER = 540,
     PARTIAL = 541,
     PARTITION = 542,
     PASSING = 543,
     PASSWORD = 544,
     PLACING = 545,
     PLANS = 546,
     POLICY = 547,
     POSITION = 548,
     PRAGMA_P = 549,
     PRECEDING = 550,
     PRECISION = 551,
     PREPARE = 552,
     PREPARED = 553,
     PRESERVE = 554,
     PRIMARY = 555,
     PRIOR = 556,
     PRIVILEGES = 557,
     PROCEDURAL = 558,
     PROCEDURE = 559,
     PROGRAM = 560,
     PUBLICATION = 561,
     QUOTE = 562,
     RANGE = 563,
     READ_P = 564,
     REAL = 565,
     REASSIGN = 566,
     RECHECK = 567,
     RECURSIVE = 568,
     REF = 569,
     REFERENCES = 570,
     REFERENCING = 571,
     REFRESH = 572,
     REINDEX = 573,
     RELATIVE_P = 574,
     RELEASE = 575,
     RENAME = 576,
     REPEATABLE = 577,
     REPLACE = 578,
     REPLICA = 579,
     RESET = 580,
     RESTART = 581,
     RESTRICT = 582,
     RETURNING = 583,
     RETURNS = 584,
     REVOKE = 585,
     RIGHT = 586,
     ROLE = 587,
     ROLLBACK = 588,
     ROLLUP = 589,
     ROW = 590,
     ROWS = 591,
     RULE = 592,
     SAVEPOINT = 593,
     SCHEMA = 594,
     SCHEMAS = 595,
     SCROLL = 596,
     SEARCH = 597,
     SECOND_P = 598,
     SECURITY = 599,
     SELECT = 600,
     SEQUENCE = 601,
     SEQUENCES = 602,
     SERIALIZABLE = 603,
     SERVER = 604,
     SESSION = 605,
     SESSION_USER = 606,
     SET = 607,
     SETOF = 608,
     SETS = 609,
     SHARE = 610,
     SHOW = 611,
     SIMILAR = 612,
     SIMPLE = 613,
     SKIP = 614,
     SMALLINT = 615,
     SNAPSHOT = 616,
     SOME = 617,
     SQL_P = 618,
     STABLE = 619,
     STANDALONE_P = 620,
     START = 621,
     STATEMENT = 622,
     STATISTICS = 623,
     STDIN = 624,
     STDOUT = 625,
     STORAGE = 626,
     STRICT_P = 627,
     STRIP_P = 628,
     SUBSCRIPTION = 629,
     SUBSTRING = 630,
     SYMMETRIC = 631,
     SYSID = 632,
     SYSTEM_P = 633,
     TABLE = 634,
     TABLES = 635,
     TABLESAMPLE = 636,
     TABLESPACE = 637,
     TEMP = 638,
     TEMPLATE = 639,
     TEMPORARY = 640,
     TEXT_P = 641,
     THEN = 642,
     TIME = 643,
     TIMESTAMP = 644,
     TO = 645,
     TRAILING = 646,
     TRANSACTION = 647,
     TRANSFORM = 648,
     TREAT = 649,
     TRIGGER = 650,
     TRIM = 651,
     TRUE_P = 652,
     TRUNCATE = 653,
     TRUSTED = 654,
     TYPE_P = 655,
     TYPES_P = 656,
     UNBOUNDED = 657,
     UNCOMMITTED = 658,
     UNENCRYPTED = 659,
     UNION = 660,
     UNIQUE = 661,
     UNKNOWN = 662,
     UNLISTEN = 663,
     UNLOGGED = 664,
     UNTIL = 665,
     UPDATE = 666,
     USER = 667,
     USING = 668,
     VACUUM = 669,
     VALID = 670,
     VALIDATE = 671,
     VALIDATOR = 672,
     VALUE_P = 673,
     VALUES = 674,
     VARCHAR = 675,
     VARIADIC = 676,
     VARYING = 677,
     VERBOSE = 678,
     VERSION_P = 679,
     VIEW = 680,
     VIEWS = 681,
     VOLATILE = 682,
     WHEN = 683,
     WHERE = 684,
     WHITESPACE_P = 685,
     WINDOW = 686,
     WITH = 687,
     WITHIN = 688,
     WITHOUT = 689,
     WORK = 690,
     WRAPPER = 691,
     WRITE_P = 692,
     XML_P = 693,
     XMLATTRIBUTES = 694,
     XMLCONCAT = 695,
     XMLELEMENT = 696,
     XMLEXISTS = 697,
     XMLFOREST = 698,
     XMLNAMESPACES = 699,
     XMLPARSE = 700,
     XMLPI = 701,
     XMLROOT = 702,
     XMLSERIALIZE = 703,
     XMLTABLE = 704,
     YEAR_P = 705,
     YES_P = 706,
     ZONE = 707,
     NOT_LA = 708,
     NULLS_LA = 709,
     WITH_LA = 710,
     POSTFIXOP = 711,
     UMINUS = 712
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
#define EXTENSION 405
#define EXTERNAL 406
#define EXTRACT 407
#define FALSE_P 408
#define FAMILY 409
#define FETCH 410
#define FILTER 411
#define FIRST_P 412
#define FLOAT_P 413
#define FOLLOWING 414
#define FOR 415
#define FORCE 416
#define FOREIGN 417
#define FORWARD 418
#define FREEZE 419
#define FROM 420
#define FULL 421
#define FUNCTION 422
#define FUNCTIONS 423
#define GENERATED 424
#define GLOB 425
#define GLOBAL 426
#define GRANT 427
#define GRANTED 428
#define GROUP_P 429
#define GROUPING 430
#define HANDLER 431
#define HAVING 432
#define HEADER_P 433
#define HOLD 434
#define HOUR_P 435
#define IDENTITY_P 436
#define IF_P 437
#define ILIKE 438
#define IMMEDIATE 439
#define IMMUTABLE 440
#define IMPLICIT_P 441
#define IMPORT_P 442
#define IN_P 443
#define INCLUDING 444
#define INCREMENT 445
#define INDEX 446
#define INDEXES 447
#define INHERIT 448
#define INHERITS 449
#define INITIALLY 450
#define INLINE_P 451
#define INNER_P 452
#define INOUT 453
#define INPUT_P 454
#define INSENSITIVE 455
#define INSERT 456
#define INSTEAD 457
#define INT_P 458
#define INTEGER 459
#define INTERSECT 460
#define INTERVAL 461
#define INTO 462
#define INVOKER 463
#define IS 464
#define ISNULL 465
#define ISOLATION 466
#define JOIN 467
#define KEY 468
#define LABEL 469
#define LANGUAGE 470
#define LARGE_P 471
#define LAST_P 472
#define LATERAL_P 473
#define LEADING 474
#define LEAKPROOF 475
#define LEFT 476
#define LEVEL 477
#define LIKE 478
#define LIMIT 479
#define LISTEN 480
#define LOAD 481
#define LOCAL 482
#define LOCALTIME 483
#define LOCALTIMESTAMP 484
#define LOCATION 485
#define LOCK_P 486
#define LOCKED 487
#define LOGGED 488
#define MAPPING 489
#define MATCH 490
#define MATERIALIZED 491
#define MAXVALUE 492
#define METHOD 493
#define MINUTE_P 494
#define MINVALUE 495
#define MODE 496
#define MONTH_P 497
#define MOVE 498
#define NAME_P 499
#define NAMES 500
#define NATIONAL 501
#define NATURAL 502
#define NCHAR 503
#define NEW 504
#define NEXT 505
#define NO 506
#define NONE 507
#define NOT 508
#define NOTHING 509
#define NOTIFY 510
#define NOTNULL 511
#define NOWAIT 512
#define NULL_P 513
#define NULLIF 514
#define NULLS_P 515
#define NUMERIC 516
#define OBJECT_P 517
#define OF 518
#define OFF 519
#define OFFSET 520
#define OIDS 521
#define OLD 522
#define ON 523
#define ONLY 524
#define OPERATOR 525
#define OPTION 526
#define OPTIONS 527
#define OR 528
#define ORDER 529
#define ORDINALITY 530
#define OUT_P 531
#define OUTER_P 532
#define OVER 533
#define OVERLAPS 534
#define OVERLAY 535
#define OVERRIDING 536
#define OWNED 537
#define OWNER 538
#define PARALLEL 539
#define PARSER 540
#define PARTIAL 541
#define PARTITION 542
#define PASSING 543
#define PASSWORD 544
#define PLACING 545
#define PLANS 546
#define POLICY 547
#define POSITION 548
#define PRAGMA_P 549
#define PRECEDING 550
#define PRECISION 551
#define PREPARE 552
#define PREPARED 553
#define PRESERVE 554
#define PRIMARY 555
#define PRIOR 556
#define PRIVILEGES 557
#define PROCEDURAL 558
#define PROCEDURE 559
#define PROGRAM 560
#define PUBLICATION 561
#define QUOTE 562
#define RANGE 563
#define READ_P 564
#define REAL 565
#define REASSIGN 566
#define RECHECK 567
#define RECURSIVE 568
#define REF 569
#define REFERENCES 570
#define REFERENCING 571
#define REFRESH 572
#define REINDEX 573
#define RELATIVE_P 574
#define RELEASE 575
#define RENAME 576
#define REPEATABLE 577
#define REPLACE 578
#define REPLICA 579
#define RESET 580
#define RESTART 581
#define RESTRICT 582
#define RETURNING 583
#define RETURNS 584
#define REVOKE 585
#define RIGHT 586
#define ROLE 587
#define ROLLBACK 588
#define ROLLUP 589
#define ROW 590
#define ROWS 591
#define RULE 592
#define SAVEPOINT 593
#define SCHEMA 594
#define SCHEMAS 595
#define SCROLL 596
#define SEARCH 597
#define SECOND_P 598
#define SECURITY 599
#define SELECT 600
#define SEQUENCE 601
#define SEQUENCES 602
#define SERIALIZABLE 603
#define SERVER 604
#define SESSION 605
#define SESSION_USER 606
#define SET 607
#define SETOF 608
#define SETS 609
#define SHARE 610
#define SHOW 611
#define SIMILAR 612
#define SIMPLE 613
#define SKIP 614
#define SMALLINT 615
#define SNAPSHOT 616
#define SOME 617
#define SQL_P 618
#define STABLE 619
#define STANDALONE_P 620
#define START 621
#define STATEMENT 622
#define STATISTICS 623
#define STDIN 624
#define STDOUT 625
#define STORAGE 626
#define STRICT_P 627
#define STRIP_P 628
#define SUBSCRIPTION 629
#define SUBSTRING 630
#define SYMMETRIC 631
#define SYSID 632
#define SYSTEM_P 633
#define TABLE 634
#define TABLES 635
#define TABLESAMPLE 636
#define TABLESPACE 637
#define TEMP 638
#define TEMPLATE 639
#define TEMPORARY 640
#define TEXT_P 641
#define THEN 642
#define TIME 643
#define TIMESTAMP 644
#define TO 645
#define TRAILING 646
#define TRANSACTION 647
#define TRANSFORM 648
#define TREAT 649
#define TRIGGER 650
#define TRIM 651
#define TRUE_P 652
#define TRUNCATE 653
#define TRUSTED 654
#define TYPE_P 655
#define TYPES_P 656
#define UNBOUNDED 657
#define UNCOMMITTED 658
#define UNENCRYPTED 659
#define UNION 660
#define UNIQUE 661
#define UNKNOWN 662
#define UNLISTEN 663
#define UNLOGGED 664
#define UNTIL 665
#define UPDATE 666
#define USER 667
#define USING 668
#define VACUUM 669
#define VALID 670
#define VALIDATE 671
#define VALIDATOR 672
#define VALUE_P 673
#define VALUES 674
#define VARCHAR 675
#define VARIADIC 676
#define VARYING 677
#define VERBOSE 678
#define VERSION_P 679
#define VIEW 680
#define VIEWS 681
#define VOLATILE 682
#define WHEN 683
#define WHERE 684
#define WHITESPACE_P 685
#define WINDOW 686
#define WITH 687
#define WITHIN 688
#define WITHOUT 689
#define WORK 690
#define WRAPPER 691
#define WRITE_P 692
#define XML_P 693
#define XMLATTRIBUTES 694
#define XMLCONCAT 695
#define XMLELEMENT 696
#define XMLEXISTS 697
#define XMLFOREST 698
#define XMLNAMESPACES 699
#define XMLPARSE 700
#define XMLPI 701
#define XMLROOT 702
#define XMLSERIALIZE 703
#define XMLTABLE 704
#define YEAR_P 705
#define YES_P 706
#define ZONE 707
#define NOT_LA 708
#define NULLS_LA 709
#define WITH_LA 710
#define POSTFIXOP 711
#define UMINUS 712




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
#line 1006 "third_party/libpg_query/grammar/grammar_out.hpp"
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


