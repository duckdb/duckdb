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
     CALLED = 310,
     CASCADE = 311,
     CASCADED = 312,
     CASE = 313,
     CAST = 314,
     CATALOG_P = 315,
     CHAIN = 316,
     CHAR_P = 317,
     CHARACTER = 318,
     CHARACTERISTICS = 319,
     CHECK_P = 320,
     CHECKPOINT = 321,
     CLASS = 322,
     CLOSE = 323,
     CLUSTER = 324,
     COALESCE = 325,
     COLLATE = 326,
     COLLATION = 327,
     COLUMN = 328,
     COLUMNS = 329,
     COMMENT = 330,
     COMMENTS = 331,
     COMMIT = 332,
     COMMITTED = 333,
     CONCURRENTLY = 334,
     CONFIGURATION = 335,
     CONFLICT = 336,
     CONNECTION = 337,
     CONSTRAINT = 338,
     CONSTRAINTS = 339,
     CONTENT_P = 340,
     CONTINUE_P = 341,
     CONVERSION_P = 342,
     COPY = 343,
     COST = 344,
     CREATE_P = 345,
     CROSS = 346,
     CSV = 347,
     CUBE = 348,
     CURRENT_P = 349,
     CURRENT_CATALOG = 350,
     CURRENT_DATE = 351,
     CURRENT_ROLE = 352,
     CURRENT_SCHEMA = 353,
     CURRENT_TIME = 354,
     CURRENT_TIMESTAMP = 355,
     CURRENT_USER = 356,
     CURSOR = 357,
     CYCLE = 358,
     DATA_P = 359,
     DATABASE = 360,
     DAY_P = 361,
     DEALLOCATE = 362,
     DEC = 363,
     DECIMAL_P = 364,
     DECLARE = 365,
     DEFAULT = 366,
     DEFAULTS = 367,
     DEFERRABLE = 368,
     DEFERRED = 369,
     DEFINER = 370,
     DELETE_P = 371,
     DELIMITER = 372,
     DELIMITERS = 373,
     DEPENDS = 374,
     DESC_P = 375,
     DESCRIBE = 376,
     DETACH = 377,
     DICTIONARY = 378,
     DISABLE_P = 379,
     DISCARD = 380,
     DISTINCT = 381,
     DO = 382,
     DOCUMENT_P = 383,
     DOMAIN_P = 384,
     DOUBLE_P = 385,
     DROP = 386,
     EACH = 387,
     ELSE = 388,
     ENABLE_P = 389,
     ENCODING = 390,
     ENCRYPTED = 391,
     END_P = 392,
     ENUM_P = 393,
     ESCAPE = 394,
     EVENT = 395,
     EXCEPT = 396,
     EXCLUDE = 397,
     EXCLUDING = 398,
     EXCLUSIVE = 399,
     EXECUTE = 400,
     EXISTS = 401,
     EXPLAIN = 402,
     EXPORT_P = 403,
     EXTENSION = 404,
     EXTERNAL = 405,
     EXTRACT = 406,
     FALSE_P = 407,
     FAMILY = 408,
     FETCH = 409,
     FILTER = 410,
     FIRST_P = 411,
     FLOAT_P = 412,
     FOLLOWING = 413,
     FOR = 414,
     FORCE = 415,
     FOREIGN = 416,
     FORWARD = 417,
     FREEZE = 418,
     FROM = 419,
     FULL = 420,
     FUNCTION = 421,
     FUNCTIONS = 422,
     GENERATED = 423,
     GLOBAL = 424,
     GRANT = 425,
     GRANTED = 426,
     GROUP_P = 427,
     GROUPING = 428,
     HANDLER = 429,
     HAVING = 430,
     HEADER_P = 431,
     HOLD = 432,
     HOUR_P = 433,
     IDENTITY_P = 434,
     IF_P = 435,
     ILIKE = 436,
     IMMEDIATE = 437,
     IMMUTABLE = 438,
     IMPLICIT_P = 439,
     IMPORT_P = 440,
     IN_P = 441,
     INCLUDING = 442,
     INCREMENT = 443,
     INDEX = 444,
     INDEXES = 445,
     INHERIT = 446,
     INHERITS = 447,
     INITIALLY = 448,
     INLINE_P = 449,
     INNER_P = 450,
     INOUT = 451,
     INPUT_P = 452,
     INSENSITIVE = 453,
     INSERT = 454,
     INSTEAD = 455,
     INT_P = 456,
     INTEGER = 457,
     INTERSECT = 458,
     INTERVAL = 459,
     INTO = 460,
     INVOKER = 461,
     IS = 462,
     ISNULL = 463,
     ISOLATION = 464,
     JOIN = 465,
     KEY = 466,
     LABEL = 467,
     LANGUAGE = 468,
     LARGE_P = 469,
     LAST_P = 470,
     LATERAL_P = 471,
     LEADING = 472,
     LEAKPROOF = 473,
     LEFT = 474,
     LEVEL = 475,
     LIKE = 476,
     LIMIT = 477,
     LISTEN = 478,
     LOAD = 479,
     LOCAL = 480,
     LOCALTIME = 481,
     LOCALTIMESTAMP = 482,
     LOCATION = 483,
     LOCK_P = 484,
     LOCKED = 485,
     LOGGED = 486,
     MAPPING = 487,
     MATCH = 488,
     MATERIALIZED = 489,
     MAXVALUE = 490,
     METHOD = 491,
     MINUTE_P = 492,
     MINVALUE = 493,
     MODE = 494,
     MONTH_P = 495,
     MOVE = 496,
     NAME_P = 497,
     NAMES = 498,
     NATIONAL = 499,
     NATURAL = 500,
     NCHAR = 501,
     NEW = 502,
     NEXT = 503,
     NO = 504,
     NONE = 505,
     NOT = 506,
     NOTHING = 507,
     NOTIFY = 508,
     NOTNULL = 509,
     NOWAIT = 510,
     NULL_P = 511,
     NULLIF = 512,
     NULLS_P = 513,
     NUMERIC = 514,
     OBJECT_P = 515,
     OF = 516,
     OFF = 517,
     OFFSET = 518,
     OIDS = 519,
     OLD = 520,
     ON = 521,
     ONLY = 522,
     OPERATOR = 523,
     OPTION = 524,
     OPTIONS = 525,
     OR = 526,
     ORDER = 527,
     ORDINALITY = 528,
     OUT_P = 529,
     OUTER_P = 530,
     OVER = 531,
     OVERLAPS = 532,
     OVERLAY = 533,
     OVERRIDING = 534,
     OWNED = 535,
     OWNER = 536,
     PARALLEL = 537,
     PARSER = 538,
     PARTIAL = 539,
     PARTITION = 540,
     PASSING = 541,
     PASSWORD = 542,
     PLACING = 543,
     PLANS = 544,
     POLICY = 545,
     POSITION = 546,
     PRAGMA_P = 547,
     PRECEDING = 548,
     PRECISION = 549,
     PREPARE = 550,
     PREPARED = 551,
     PRESERVE = 552,
     PRIMARY = 553,
     PRIOR = 554,
     PRIVILEGES = 555,
     PROCEDURAL = 556,
     PROCEDURE = 557,
     PROGRAM = 558,
     PUBLICATION = 559,
     QUOTE = 560,
     RANGE = 561,
     READ_P = 562,
     REAL = 563,
     REASSIGN = 564,
     RECHECK = 565,
     RECURSIVE = 566,
     REF = 567,
     REFERENCES = 568,
     REFERENCING = 569,
     REFRESH = 570,
     REINDEX = 571,
     RELATIVE_P = 572,
     RELEASE = 573,
     RENAME = 574,
     REPEATABLE = 575,
     REPLACE = 576,
     REPLICA = 577,
     RESET = 578,
     RESTART = 579,
     RESTRICT = 580,
     RETURNING = 581,
     RETURNS = 582,
     REVOKE = 583,
     RIGHT = 584,
     ROLE = 585,
     ROLLBACK = 586,
     ROLLUP = 587,
     ROW = 588,
     ROWS = 589,
     RULE = 590,
     SAVEPOINT = 591,
     SCHEMA = 592,
     SCHEMAS = 593,
     SCROLL = 594,
     SEARCH = 595,
     SECOND_P = 596,
     SECURITY = 597,
     SELECT = 598,
     SEQUENCE = 599,
     SEQUENCES = 600,
     SERIALIZABLE = 601,
     SERVER = 602,
     SESSION = 603,
     SESSION_USER = 604,
     SET = 605,
     SETOF = 606,
     SETS = 607,
     SHARE = 608,
     SHOW = 609,
     SIMILAR = 610,
     SIMPLE = 611,
     SKIP = 612,
     SMALLINT = 613,
     SNAPSHOT = 614,
     SOME = 615,
     SQL_P = 616,
     STABLE = 617,
     STANDALONE_P = 618,
     START = 619,
     STATEMENT = 620,
     STATISTICS = 621,
     STDIN = 622,
     STDOUT = 623,
     STORAGE = 624,
     STRICT_P = 625,
     STRIP_P = 626,
     SUBSCRIPTION = 627,
     SUBSTRING = 628,
     SYMMETRIC = 629,
     SYSID = 630,
     SYSTEM_P = 631,
     TABLE = 632,
     TABLES = 633,
     TABLESAMPLE = 634,
     TABLESPACE = 635,
     TEMP = 636,
     TEMPLATE = 637,
     TEMPORARY = 638,
     TEXT_P = 639,
     THEN = 640,
     TIME = 641,
     TIMESTAMP = 642,
     TO = 643,
     TRAILING = 644,
     TRANSACTION = 645,
     TRANSFORM = 646,
     TREAT = 647,
     TRIGGER = 648,
     TRIM = 649,
     TRUE_P = 650,
     TRUNCATE = 651,
     TRUSTED = 652,
     TYPE_P = 653,
     TYPES_P = 654,
     UNBOUNDED = 655,
     UNCOMMITTED = 656,
     UNENCRYPTED = 657,
     UNION = 658,
     UNIQUE = 659,
     UNKNOWN = 660,
     UNLISTEN = 661,
     UNLOGGED = 662,
     UNTIL = 663,
     UPDATE = 664,
     USER = 665,
     USING = 666,
     VACUUM = 667,
     VALID = 668,
     VALIDATE = 669,
     VALIDATOR = 670,
     VALUE_P = 671,
     VALUES = 672,
     VARCHAR = 673,
     VARIADIC = 674,
     VARYING = 675,
     VERBOSE = 676,
     VERSION_P = 677,
     VIEW = 678,
     VIEWS = 679,
     VOLATILE = 680,
     WHEN = 681,
     WHERE = 682,
     WHITESPACE_P = 683,
     WINDOW = 684,
     WITH = 685,
     WITHIN = 686,
     WITHOUT = 687,
     WORK = 688,
     WRAPPER = 689,
     WRITE_P = 690,
     XML_P = 691,
     XMLATTRIBUTES = 692,
     XMLCONCAT = 693,
     XMLELEMENT = 694,
     XMLEXISTS = 695,
     XMLFOREST = 696,
     XMLNAMESPACES = 697,
     XMLPARSE = 698,
     XMLPI = 699,
     XMLROOT = 700,
     XMLSERIALIZE = 701,
     XMLTABLE = 702,
     YEAR_P = 703,
     YES_P = 704,
     ZONE = 705,
     NOT_LA = 706,
     NULLS_LA = 707,
     WITH_LA = 708,
     POSTFIXOP = 709,
     UMINUS = 710
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
#define CALLED 310
#define CASCADE 311
#define CASCADED 312
#define CASE 313
#define CAST 314
#define CATALOG_P 315
#define CHAIN 316
#define CHAR_P 317
#define CHARACTER 318
#define CHARACTERISTICS 319
#define CHECK_P 320
#define CHECKPOINT 321
#define CLASS 322
#define CLOSE 323
#define CLUSTER 324
#define COALESCE 325
#define COLLATE 326
#define COLLATION 327
#define COLUMN 328
#define COLUMNS 329
#define COMMENT 330
#define COMMENTS 331
#define COMMIT 332
#define COMMITTED 333
#define CONCURRENTLY 334
#define CONFIGURATION 335
#define CONFLICT 336
#define CONNECTION 337
#define CONSTRAINT 338
#define CONSTRAINTS 339
#define CONTENT_P 340
#define CONTINUE_P 341
#define CONVERSION_P 342
#define COPY 343
#define COST 344
#define CREATE_P 345
#define CROSS 346
#define CSV 347
#define CUBE 348
#define CURRENT_P 349
#define CURRENT_CATALOG 350
#define CURRENT_DATE 351
#define CURRENT_ROLE 352
#define CURRENT_SCHEMA 353
#define CURRENT_TIME 354
#define CURRENT_TIMESTAMP 355
#define CURRENT_USER 356
#define CURSOR 357
#define CYCLE 358
#define DATA_P 359
#define DATABASE 360
#define DAY_P 361
#define DEALLOCATE 362
#define DEC 363
#define DECIMAL_P 364
#define DECLARE 365
#define DEFAULT 366
#define DEFAULTS 367
#define DEFERRABLE 368
#define DEFERRED 369
#define DEFINER 370
#define DELETE_P 371
#define DELIMITER 372
#define DELIMITERS 373
#define DEPENDS 374
#define DESC_P 375
#define DESCRIBE 376
#define DETACH 377
#define DICTIONARY 378
#define DISABLE_P 379
#define DISCARD 380
#define DISTINCT 381
#define DO 382
#define DOCUMENT_P 383
#define DOMAIN_P 384
#define DOUBLE_P 385
#define DROP 386
#define EACH 387
#define ELSE 388
#define ENABLE_P 389
#define ENCODING 390
#define ENCRYPTED 391
#define END_P 392
#define ENUM_P 393
#define ESCAPE 394
#define EVENT 395
#define EXCEPT 396
#define EXCLUDE 397
#define EXCLUDING 398
#define EXCLUSIVE 399
#define EXECUTE 400
#define EXISTS 401
#define EXPLAIN 402
#define EXPORT_P 403
#define EXTENSION 404
#define EXTERNAL 405
#define EXTRACT 406
#define FALSE_P 407
#define FAMILY 408
#define FETCH 409
#define FILTER 410
#define FIRST_P 411
#define FLOAT_P 412
#define FOLLOWING 413
#define FOR 414
#define FORCE 415
#define FOREIGN 416
#define FORWARD 417
#define FREEZE 418
#define FROM 419
#define FULL 420
#define FUNCTION 421
#define FUNCTIONS 422
#define GENERATED 423
#define GLOBAL 424
#define GRANT 425
#define GRANTED 426
#define GROUP_P 427
#define GROUPING 428
#define HANDLER 429
#define HAVING 430
#define HEADER_P 431
#define HOLD 432
#define HOUR_P 433
#define IDENTITY_P 434
#define IF_P 435
#define ILIKE 436
#define IMMEDIATE 437
#define IMMUTABLE 438
#define IMPLICIT_P 439
#define IMPORT_P 440
#define IN_P 441
#define INCLUDING 442
#define INCREMENT 443
#define INDEX 444
#define INDEXES 445
#define INHERIT 446
#define INHERITS 447
#define INITIALLY 448
#define INLINE_P 449
#define INNER_P 450
#define INOUT 451
#define INPUT_P 452
#define INSENSITIVE 453
#define INSERT 454
#define INSTEAD 455
#define INT_P 456
#define INTEGER 457
#define INTERSECT 458
#define INTERVAL 459
#define INTO 460
#define INVOKER 461
#define IS 462
#define ISNULL 463
#define ISOLATION 464
#define JOIN 465
#define KEY 466
#define LABEL 467
#define LANGUAGE 468
#define LARGE_P 469
#define LAST_P 470
#define LATERAL_P 471
#define LEADING 472
#define LEAKPROOF 473
#define LEFT 474
#define LEVEL 475
#define LIKE 476
#define LIMIT 477
#define LISTEN 478
#define LOAD 479
#define LOCAL 480
#define LOCALTIME 481
#define LOCALTIMESTAMP 482
#define LOCATION 483
#define LOCK_P 484
#define LOCKED 485
#define LOGGED 486
#define MAPPING 487
#define MATCH 488
#define MATERIALIZED 489
#define MAXVALUE 490
#define METHOD 491
#define MINUTE_P 492
#define MINVALUE 493
#define MODE 494
#define MONTH_P 495
#define MOVE 496
#define NAME_P 497
#define NAMES 498
#define NATIONAL 499
#define NATURAL 500
#define NCHAR 501
#define NEW 502
#define NEXT 503
#define NO 504
#define NONE 505
#define NOT 506
#define NOTHING 507
#define NOTIFY 508
#define NOTNULL 509
#define NOWAIT 510
#define NULL_P 511
#define NULLIF 512
#define NULLS_P 513
#define NUMERIC 514
#define OBJECT_P 515
#define OF 516
#define OFF 517
#define OFFSET 518
#define OIDS 519
#define OLD 520
#define ON 521
#define ONLY 522
#define OPERATOR 523
#define OPTION 524
#define OPTIONS 525
#define OR 526
#define ORDER 527
#define ORDINALITY 528
#define OUT_P 529
#define OUTER_P 530
#define OVER 531
#define OVERLAPS 532
#define OVERLAY 533
#define OVERRIDING 534
#define OWNED 535
#define OWNER 536
#define PARALLEL 537
#define PARSER 538
#define PARTIAL 539
#define PARTITION 540
#define PASSING 541
#define PASSWORD 542
#define PLACING 543
#define PLANS 544
#define POLICY 545
#define POSITION 546
#define PRAGMA_P 547
#define PRECEDING 548
#define PRECISION 549
#define PREPARE 550
#define PREPARED 551
#define PRESERVE 552
#define PRIMARY 553
#define PRIOR 554
#define PRIVILEGES 555
#define PROCEDURAL 556
#define PROCEDURE 557
#define PROGRAM 558
#define PUBLICATION 559
#define QUOTE 560
#define RANGE 561
#define READ_P 562
#define REAL 563
#define REASSIGN 564
#define RECHECK 565
#define RECURSIVE 566
#define REF 567
#define REFERENCES 568
#define REFERENCING 569
#define REFRESH 570
#define REINDEX 571
#define RELATIVE_P 572
#define RELEASE 573
#define RENAME 574
#define REPEATABLE 575
#define REPLACE 576
#define REPLICA 577
#define RESET 578
#define RESTART 579
#define RESTRICT 580
#define RETURNING 581
#define RETURNS 582
#define REVOKE 583
#define RIGHT 584
#define ROLE 585
#define ROLLBACK 586
#define ROLLUP 587
#define ROW 588
#define ROWS 589
#define RULE 590
#define SAVEPOINT 591
#define SCHEMA 592
#define SCHEMAS 593
#define SCROLL 594
#define SEARCH 595
#define SECOND_P 596
#define SECURITY 597
#define SELECT 598
#define SEQUENCE 599
#define SEQUENCES 600
#define SERIALIZABLE 601
#define SERVER 602
#define SESSION 603
#define SESSION_USER 604
#define SET 605
#define SETOF 606
#define SETS 607
#define SHARE 608
#define SHOW 609
#define SIMILAR 610
#define SIMPLE 611
#define SKIP 612
#define SMALLINT 613
#define SNAPSHOT 614
#define SOME 615
#define SQL_P 616
#define STABLE 617
#define STANDALONE_P 618
#define START 619
#define STATEMENT 620
#define STATISTICS 621
#define STDIN 622
#define STDOUT 623
#define STORAGE 624
#define STRICT_P 625
#define STRIP_P 626
#define SUBSCRIPTION 627
#define SUBSTRING 628
#define SYMMETRIC 629
#define SYSID 630
#define SYSTEM_P 631
#define TABLE 632
#define TABLES 633
#define TABLESAMPLE 634
#define TABLESPACE 635
#define TEMP 636
#define TEMPLATE 637
#define TEMPORARY 638
#define TEXT_P 639
#define THEN 640
#define TIME 641
#define TIMESTAMP 642
#define TO 643
#define TRAILING 644
#define TRANSACTION 645
#define TRANSFORM 646
#define TREAT 647
#define TRIGGER 648
#define TRIM 649
#define TRUE_P 650
#define TRUNCATE 651
#define TRUSTED 652
#define TYPE_P 653
#define TYPES_P 654
#define UNBOUNDED 655
#define UNCOMMITTED 656
#define UNENCRYPTED 657
#define UNION 658
#define UNIQUE 659
#define UNKNOWN 660
#define UNLISTEN 661
#define UNLOGGED 662
#define UNTIL 663
#define UPDATE 664
#define USER 665
#define USING 666
#define VACUUM 667
#define VALID 668
#define VALIDATE 669
#define VALIDATOR 670
#define VALUE_P 671
#define VALUES 672
#define VARCHAR 673
#define VARIADIC 674
#define VARYING 675
#define VERBOSE 676
#define VERSION_P 677
#define VIEW 678
#define VIEWS 679
#define VOLATILE 680
#define WHEN 681
#define WHERE 682
#define WHITESPACE_P 683
#define WINDOW 684
#define WITH 685
#define WITHIN 686
#define WITHOUT 687
#define WORK 688
#define WRAPPER 689
#define WRITE_P 690
#define XML_P 691
#define XMLATTRIBUTES 692
#define XMLCONCAT 693
#define XMLELEMENT 694
#define XMLEXISTS 695
#define XMLFOREST 696
#define XMLNAMESPACES 697
#define XMLPARSE 698
#define XMLPI 699
#define XMLROOT 700
#define XMLSERIALIZE 701
#define XMLTABLE 702
#define YEAR_P 703
#define YES_P 704
#define ZONE 705
#define NOT_LA 706
#define NULLS_LA 707
#define WITH_LA 708
#define POSTFIXOP 709
#define UMINUS 710




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
#line 1002 "third_party/libpg_query/grammar/grammar_out.hpp"
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


