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
     EXTENSION = 403,
     EXTERNAL = 404,
     EXTRACT = 405,
     FALSE_P = 406,
     FAMILY = 407,
     FETCH = 408,
     FILTER = 409,
     FIRST_P = 410,
     FLOAT_P = 411,
     FOLLOWING = 412,
     FOR = 413,
     FORCE = 414,
     FOREIGN = 415,
     FORWARD = 416,
     FREEZE = 417,
     FROM = 418,
     FULL = 419,
     FUNCTION = 420,
     FUNCTIONS = 421,
     GENERATED = 422,
     GLOBAL = 423,
     GRANT = 424,
     GRANTED = 425,
     GROUP_P = 426,
     GROUPING = 427,
     HANDLER = 428,
     HAVING = 429,
     HEADER_P = 430,
     HOLD = 431,
     HOUR_P = 432,
     IDENTITY_P = 433,
     IF_P = 434,
     ILIKE = 435,
     IMMEDIATE = 436,
     IMMUTABLE = 437,
     IMPLICIT_P = 438,
     IMPORT_P = 439,
     IN_P = 440,
     INCLUDING = 441,
     INCREMENT = 442,
     INDEX = 443,
     INDEXES = 444,
     INHERIT = 445,
     INHERITS = 446,
     INITIALLY = 447,
     INLINE_P = 448,
     INNER_P = 449,
     INOUT = 450,
     INPUT_P = 451,
     INSENSITIVE = 452,
     INSERT = 453,
     INSTEAD = 454,
     INT_P = 455,
     INTEGER = 456,
     INTERSECT = 457,
     INTERVAL = 458,
     INTO = 459,
     INVOKER = 460,
     IS = 461,
     ISNULL = 462,
     ISOLATION = 463,
     JOIN = 464,
     KEY = 465,
     LABEL = 466,
     LANGUAGE = 467,
     LARGE_P = 468,
     LAST_P = 469,
     LATERAL_P = 470,
     LEADING = 471,
     LEAKPROOF = 472,
     LEFT = 473,
     LEVEL = 474,
     LIKE = 475,
     LIMIT = 476,
     LISTEN = 477,
     LOAD = 478,
     LOCAL = 479,
     LOCALTIME = 480,
     LOCALTIMESTAMP = 481,
     LOCATION = 482,
     LOCK_P = 483,
     LOCKED = 484,
     LOGGED = 485,
     MAPPING = 486,
     MATCH = 487,
     MATERIALIZED = 488,
     MAXVALUE = 489,
     METHOD = 490,
     MINUTE_P = 491,
     MINVALUE = 492,
     MODE = 493,
     MONTH_P = 494,
     MOVE = 495,
     NAME_P = 496,
     NAMES = 497,
     NATIONAL = 498,
     NATURAL = 499,
     NCHAR = 500,
     NEW = 501,
     NEXT = 502,
     NO = 503,
     NONE = 504,
     NOT = 505,
     NOTHING = 506,
     NOTIFY = 507,
     NOTNULL = 508,
     NOWAIT = 509,
     NULL_P = 510,
     NULLIF = 511,
     NULLS_P = 512,
     NUMERIC = 513,
     OBJECT_P = 514,
     OF = 515,
     OFF = 516,
     OFFSET = 517,
     OIDS = 518,
     OLD = 519,
     ON = 520,
     ONLY = 521,
     OPERATOR = 522,
     OPTION = 523,
     OPTIONS = 524,
     OR = 525,
     ORDER = 526,
     ORDINALITY = 527,
     OUT_P = 528,
     OUTER_P = 529,
     OVER = 530,
     OVERLAPS = 531,
     OVERLAY = 532,
     OVERRIDING = 533,
     OWNED = 534,
     OWNER = 535,
     PARALLEL = 536,
     PARSER = 537,
     PARTIAL = 538,
     PARTITION = 539,
     PASSING = 540,
     PASSWORD = 541,
     PLACING = 542,
     PLANS = 543,
     POLICY = 544,
     POSITION = 545,
     PRAGMA_P = 546,
     PRECEDING = 547,
     PRECISION = 548,
     PREPARE = 549,
     PREPARED = 550,
     PRESERVE = 551,
     PRIMARY = 552,
     PRIOR = 553,
     PRIVILEGES = 554,
     PROCEDURAL = 555,
     PROCEDURE = 556,
     PROGRAM = 557,
     PUBLICATION = 558,
     QUOTE = 559,
     RANGE = 560,
     READ_P = 561,
     REAL = 562,
     REASSIGN = 563,
     RECHECK = 564,
     RECURSIVE = 565,
     REF = 566,
     REFERENCES = 567,
     REFERENCING = 568,
     REFRESH = 569,
     REINDEX = 570,
     RELATIVE_P = 571,
     RELEASE = 572,
     RENAME = 573,
     REPEATABLE = 574,
     REPLACE = 575,
     REPLICA = 576,
     RESET = 577,
     RESTART = 578,
     RESTRICT = 579,
     RETURNING = 580,
     RETURNS = 581,
     REVOKE = 582,
     RIGHT = 583,
     ROLE = 584,
     ROLLBACK = 585,
     ROLLUP = 586,
     ROW = 587,
     ROWS = 588,
     RULE = 589,
     SAVEPOINT = 590,
     SCHEMA = 591,
     SCHEMAS = 592,
     SCROLL = 593,
     SEARCH = 594,
     SECOND_P = 595,
     SECURITY = 596,
     SELECT = 597,
     SEQUENCE = 598,
     SEQUENCES = 599,
     SERIALIZABLE = 600,
     SERVER = 601,
     SESSION = 602,
     SESSION_USER = 603,
     SET = 604,
     SETOF = 605,
     SETS = 606,
     SHARE = 607,
     SHOW = 608,
     SIMILAR = 609,
     SIMPLE = 610,
     SKIP = 611,
     SMALLINT = 612,
     SNAPSHOT = 613,
     SOME = 614,
     SQL_P = 615,
     STABLE = 616,
     STANDALONE_P = 617,
     START = 618,
     STATEMENT = 619,
     STATISTICS = 620,
     STDIN = 621,
     STDOUT = 622,
     STORAGE = 623,
     STRICT_P = 624,
     STRIP_P = 625,
     SUBSCRIPTION = 626,
     SUBSTRING = 627,
     SYMMETRIC = 628,
     SYSID = 629,
     SYSTEM_P = 630,
     TABLE = 631,
     TABLES = 632,
     TABLESAMPLE = 633,
     TABLESPACE = 634,
     TEMP = 635,
     TEMPLATE = 636,
     TEMPORARY = 637,
     TEXT_P = 638,
     THEN = 639,
     TIME = 640,
     TIMESTAMP = 641,
     TO = 642,
     TRAILING = 643,
     TRANSACTION = 644,
     TRANSFORM = 645,
     TREAT = 646,
     TRIGGER = 647,
     TRIM = 648,
     TRUE_P = 649,
     TRUNCATE = 650,
     TRUSTED = 651,
     TYPE_P = 652,
     TYPES_P = 653,
     UNBOUNDED = 654,
     UNCOMMITTED = 655,
     UNENCRYPTED = 656,
     UNION = 657,
     UNIQUE = 658,
     UNKNOWN = 659,
     UNLISTEN = 660,
     UNLOGGED = 661,
     UNTIL = 662,
     UPDATE = 663,
     USER = 664,
     USING = 665,
     VACUUM = 666,
     VALID = 667,
     VALIDATE = 668,
     VALIDATOR = 669,
     VALUE_P = 670,
     VALUES = 671,
     VARCHAR = 672,
     VARIADIC = 673,
     VARYING = 674,
     VERBOSE = 675,
     VERSION_P = 676,
     VIEW = 677,
     VIEWS = 678,
     VOLATILE = 679,
     WHEN = 680,
     WHERE = 681,
     WHITESPACE_P = 682,
     WINDOW = 683,
     WITH = 684,
     WITHIN = 685,
     WITHOUT = 686,
     WORK = 687,
     WRAPPER = 688,
     WRITE_P = 689,
     XML_P = 690,
     XMLATTRIBUTES = 691,
     XMLCONCAT = 692,
     XMLELEMENT = 693,
     XMLEXISTS = 694,
     XMLFOREST = 695,
     XMLNAMESPACES = 696,
     XMLPARSE = 697,
     XMLPI = 698,
     XMLROOT = 699,
     XMLSERIALIZE = 700,
     XMLTABLE = 701,
     YEAR_P = 702,
     YES_P = 703,
     ZONE = 704,
     NOT_LA = 705,
     NULLS_LA = 706,
     WITH_LA = 707,
     POSTFIXOP = 708,
     UMINUS = 709
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
#define EXTENSION 403
#define EXTERNAL 404
#define EXTRACT 405
#define FALSE_P 406
#define FAMILY 407
#define FETCH 408
#define FILTER 409
#define FIRST_P 410
#define FLOAT_P 411
#define FOLLOWING 412
#define FOR 413
#define FORCE 414
#define FOREIGN 415
#define FORWARD 416
#define FREEZE 417
#define FROM 418
#define FULL 419
#define FUNCTION 420
#define FUNCTIONS 421
#define GENERATED 422
#define GLOBAL 423
#define GRANT 424
#define GRANTED 425
#define GROUP_P 426
#define GROUPING 427
#define HANDLER 428
#define HAVING 429
#define HEADER_P 430
#define HOLD 431
#define HOUR_P 432
#define IDENTITY_P 433
#define IF_P 434
#define ILIKE 435
#define IMMEDIATE 436
#define IMMUTABLE 437
#define IMPLICIT_P 438
#define IMPORT_P 439
#define IN_P 440
#define INCLUDING 441
#define INCREMENT 442
#define INDEX 443
#define INDEXES 444
#define INHERIT 445
#define INHERITS 446
#define INITIALLY 447
#define INLINE_P 448
#define INNER_P 449
#define INOUT 450
#define INPUT_P 451
#define INSENSITIVE 452
#define INSERT 453
#define INSTEAD 454
#define INT_P 455
#define INTEGER 456
#define INTERSECT 457
#define INTERVAL 458
#define INTO 459
#define INVOKER 460
#define IS 461
#define ISNULL 462
#define ISOLATION 463
#define JOIN 464
#define KEY 465
#define LABEL 466
#define LANGUAGE 467
#define LARGE_P 468
#define LAST_P 469
#define LATERAL_P 470
#define LEADING 471
#define LEAKPROOF 472
#define LEFT 473
#define LEVEL 474
#define LIKE 475
#define LIMIT 476
#define LISTEN 477
#define LOAD 478
#define LOCAL 479
#define LOCALTIME 480
#define LOCALTIMESTAMP 481
#define LOCATION 482
#define LOCK_P 483
#define LOCKED 484
#define LOGGED 485
#define MAPPING 486
#define MATCH 487
#define MATERIALIZED 488
#define MAXVALUE 489
#define METHOD 490
#define MINUTE_P 491
#define MINVALUE 492
#define MODE 493
#define MONTH_P 494
#define MOVE 495
#define NAME_P 496
#define NAMES 497
#define NATIONAL 498
#define NATURAL 499
#define NCHAR 500
#define NEW 501
#define NEXT 502
#define NO 503
#define NONE 504
#define NOT 505
#define NOTHING 506
#define NOTIFY 507
#define NOTNULL 508
#define NOWAIT 509
#define NULL_P 510
#define NULLIF 511
#define NULLS_P 512
#define NUMERIC 513
#define OBJECT_P 514
#define OF 515
#define OFF 516
#define OFFSET 517
#define OIDS 518
#define OLD 519
#define ON 520
#define ONLY 521
#define OPERATOR 522
#define OPTION 523
#define OPTIONS 524
#define OR 525
#define ORDER 526
#define ORDINALITY 527
#define OUT_P 528
#define OUTER_P 529
#define OVER 530
#define OVERLAPS 531
#define OVERLAY 532
#define OVERRIDING 533
#define OWNED 534
#define OWNER 535
#define PARALLEL 536
#define PARSER 537
#define PARTIAL 538
#define PARTITION 539
#define PASSING 540
#define PASSWORD 541
#define PLACING 542
#define PLANS 543
#define POLICY 544
#define POSITION 545
#define PRAGMA_P 546
#define PRECEDING 547
#define PRECISION 548
#define PREPARE 549
#define PREPARED 550
#define PRESERVE 551
#define PRIMARY 552
#define PRIOR 553
#define PRIVILEGES 554
#define PROCEDURAL 555
#define PROCEDURE 556
#define PROGRAM 557
#define PUBLICATION 558
#define QUOTE 559
#define RANGE 560
#define READ_P 561
#define REAL 562
#define REASSIGN 563
#define RECHECK 564
#define RECURSIVE 565
#define REF 566
#define REFERENCES 567
#define REFERENCING 568
#define REFRESH 569
#define REINDEX 570
#define RELATIVE_P 571
#define RELEASE 572
#define RENAME 573
#define REPEATABLE 574
#define REPLACE 575
#define REPLICA 576
#define RESET 577
#define RESTART 578
#define RESTRICT 579
#define RETURNING 580
#define RETURNS 581
#define REVOKE 582
#define RIGHT 583
#define ROLE 584
#define ROLLBACK 585
#define ROLLUP 586
#define ROW 587
#define ROWS 588
#define RULE 589
#define SAVEPOINT 590
#define SCHEMA 591
#define SCHEMAS 592
#define SCROLL 593
#define SEARCH 594
#define SECOND_P 595
#define SECURITY 596
#define SELECT 597
#define SEQUENCE 598
#define SEQUENCES 599
#define SERIALIZABLE 600
#define SERVER 601
#define SESSION 602
#define SESSION_USER 603
#define SET 604
#define SETOF 605
#define SETS 606
#define SHARE 607
#define SHOW 608
#define SIMILAR 609
#define SIMPLE 610
#define SKIP 611
#define SMALLINT 612
#define SNAPSHOT 613
#define SOME 614
#define SQL_P 615
#define STABLE 616
#define STANDALONE_P 617
#define START 618
#define STATEMENT 619
#define STATISTICS 620
#define STDIN 621
#define STDOUT 622
#define STORAGE 623
#define STRICT_P 624
#define STRIP_P 625
#define SUBSCRIPTION 626
#define SUBSTRING 627
#define SYMMETRIC 628
#define SYSID 629
#define SYSTEM_P 630
#define TABLE 631
#define TABLES 632
#define TABLESAMPLE 633
#define TABLESPACE 634
#define TEMP 635
#define TEMPLATE 636
#define TEMPORARY 637
#define TEXT_P 638
#define THEN 639
#define TIME 640
#define TIMESTAMP 641
#define TO 642
#define TRAILING 643
#define TRANSACTION 644
#define TRANSFORM 645
#define TREAT 646
#define TRIGGER 647
#define TRIM 648
#define TRUE_P 649
#define TRUNCATE 650
#define TRUSTED 651
#define TYPE_P 652
#define TYPES_P 653
#define UNBOUNDED 654
#define UNCOMMITTED 655
#define UNENCRYPTED 656
#define UNION 657
#define UNIQUE 658
#define UNKNOWN 659
#define UNLISTEN 660
#define UNLOGGED 661
#define UNTIL 662
#define UPDATE 663
#define USER 664
#define USING 665
#define VACUUM 666
#define VALID 667
#define VALIDATE 668
#define VALIDATOR 669
#define VALUE_P 670
#define VALUES 671
#define VARCHAR 672
#define VARIADIC 673
#define VARYING 674
#define VERBOSE 675
#define VERSION_P 676
#define VIEW 677
#define VIEWS 678
#define VOLATILE 679
#define WHEN 680
#define WHERE 681
#define WHITESPACE_P 682
#define WINDOW 683
#define WITH 684
#define WITHIN 685
#define WITHOUT 686
#define WORK 687
#define WRAPPER 688
#define WRITE_P 689
#define XML_P 690
#define XMLATTRIBUTES 691
#define XMLCONCAT 692
#define XMLELEMENT 693
#define XMLEXISTS 694
#define XMLFOREST 695
#define XMLNAMESPACES 696
#define XMLPARSE 697
#define XMLPI 698
#define XMLROOT 699
#define XMLSERIALIZE 700
#define XMLTABLE 701
#define YEAR_P 702
#define YES_P 703
#define ZONE 704
#define NOT_LA 705
#define NULLS_LA 706
#define WITH_LA 707
#define POSTFIXOP 708
#define UMINUS 709




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
#line 1000 "third_party/libpg_query/grammar/grammar_out.hpp"
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


