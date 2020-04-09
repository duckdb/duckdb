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
     GREATEST = 426,
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
     LEAST = 474,
     LEFT = 475,
     LEVEL = 476,
     LIKE = 477,
     LIMIT = 478,
     LISTEN = 479,
     LOAD = 480,
     LOCAL = 481,
     LOCALTIME = 482,
     LOCALTIMESTAMP = 483,
     LOCATION = 484,
     LOCK_P = 485,
     LOCKED = 486,
     LOGGED = 487,
     MAPPING = 488,
     MATCH = 489,
     MATERIALIZED = 490,
     MAXVALUE = 491,
     METHOD = 492,
     MINUTE_P = 493,
     MINVALUE = 494,
     MODE = 495,
     MONTH_P = 496,
     MOVE = 497,
     NAME_P = 498,
     NAMES = 499,
     NATIONAL = 500,
     NATURAL = 501,
     NCHAR = 502,
     NEW = 503,
     NEXT = 504,
     NO = 505,
     NONE = 506,
     NOT = 507,
     NOTHING = 508,
     NOTIFY = 509,
     NOTNULL = 510,
     NOWAIT = 511,
     NULL_P = 512,
     NULLIF = 513,
     NULLS_P = 514,
     NUMERIC = 515,
     OBJECT_P = 516,
     OF = 517,
     OFF = 518,
     OFFSET = 519,
     OIDS = 520,
     OLD = 521,
     ON = 522,
     ONLY = 523,
     OPERATOR = 524,
     OPTION = 525,
     OPTIONS = 526,
     OR = 527,
     ORDER = 528,
     ORDINALITY = 529,
     OUT_P = 530,
     OUTER_P = 531,
     OVER = 532,
     OVERLAPS = 533,
     OVERLAY = 534,
     OVERRIDING = 535,
     OWNED = 536,
     OWNER = 537,
     PARALLEL = 538,
     PARSER = 539,
     PARTIAL = 540,
     PARTITION = 541,
     PASSING = 542,
     PASSWORD = 543,
     PLACING = 544,
     PLANS = 545,
     POLICY = 546,
     POSITION = 547,
     PRAGMA_P = 548,
     PRECEDING = 549,
     PRECISION = 550,
     PREPARE = 551,
     PREPARED = 552,
     PRESERVE = 553,
     PRIMARY = 554,
     PRIOR = 555,
     PRIVILEGES = 556,
     PROCEDURAL = 557,
     PROCEDURE = 558,
     PROGRAM = 559,
     PUBLICATION = 560,
     QUOTE = 561,
     RANGE = 562,
     READ_P = 563,
     REAL = 564,
     REASSIGN = 565,
     RECHECK = 566,
     RECURSIVE = 567,
     REF = 568,
     REFERENCES = 569,
     REFERENCING = 570,
     REFRESH = 571,
     REINDEX = 572,
     RELATIVE_P = 573,
     RELEASE = 574,
     RENAME = 575,
     REPEATABLE = 576,
     REPLACE = 577,
     REPLICA = 578,
     RESET = 579,
     RESTART = 580,
     RESTRICT = 581,
     RETURNING = 582,
     RETURNS = 583,
     REVOKE = 584,
     RIGHT = 585,
     ROLE = 586,
     ROLLBACK = 587,
     ROLLUP = 588,
     ROW = 589,
     ROWS = 590,
     RULE = 591,
     SAVEPOINT = 592,
     SCHEMA = 593,
     SCHEMAS = 594,
     SCROLL = 595,
     SEARCH = 596,
     SECOND_P = 597,
     SECURITY = 598,
     SELECT = 599,
     SEQUENCE = 600,
     SEQUENCES = 601,
     SERIALIZABLE = 602,
     SERVER = 603,
     SESSION = 604,
     SESSION_USER = 605,
     SET = 606,
     SETOF = 607,
     SETS = 608,
     SHARE = 609,
     SHOW = 610,
     SIMILAR = 611,
     SIMPLE = 612,
     SKIP = 613,
     SMALLINT = 614,
     SNAPSHOT = 615,
     SOME = 616,
     SQL_P = 617,
     STABLE = 618,
     STANDALONE_P = 619,
     START = 620,
     STATEMENT = 621,
     STATISTICS = 622,
     STDIN = 623,
     STDOUT = 624,
     STORAGE = 625,
     STRICT_P = 626,
     STRIP_P = 627,
     SUBSCRIPTION = 628,
     SUBSTRING = 629,
     SYMMETRIC = 630,
     SYSID = 631,
     SYSTEM_P = 632,
     TABLE = 633,
     TABLES = 634,
     TABLESAMPLE = 635,
     TABLESPACE = 636,
     TEMP = 637,
     TEMPLATE = 638,
     TEMPORARY = 639,
     TEXT_P = 640,
     THEN = 641,
     TIME = 642,
     TIMESTAMP = 643,
     TO = 644,
     TRAILING = 645,
     TRANSACTION = 646,
     TRANSFORM = 647,
     TREAT = 648,
     TRIGGER = 649,
     TRIM = 650,
     TRUE_P = 651,
     TRUNCATE = 652,
     TRUSTED = 653,
     TYPE_P = 654,
     TYPES_P = 655,
     UNBOUNDED = 656,
     UNCOMMITTED = 657,
     UNENCRYPTED = 658,
     UNION = 659,
     UNIQUE = 660,
     UNKNOWN = 661,
     UNLISTEN = 662,
     UNLOGGED = 663,
     UNTIL = 664,
     UPDATE = 665,
     USER = 666,
     USING = 667,
     VACUUM = 668,
     VALID = 669,
     VALIDATE = 670,
     VALIDATOR = 671,
     VALUE_P = 672,
     VALUES = 673,
     VARCHAR = 674,
     VARIADIC = 675,
     VARYING = 676,
     VERBOSE = 677,
     VERSION_P = 678,
     VIEW = 679,
     VIEWS = 680,
     VOLATILE = 681,
     WHEN = 682,
     WHERE = 683,
     WHITESPACE_P = 684,
     WINDOW = 685,
     WITH = 686,
     WITHIN = 687,
     WITHOUT = 688,
     WORK = 689,
     WRAPPER = 690,
     WRITE_P = 691,
     XML_P = 692,
     XMLATTRIBUTES = 693,
     XMLCONCAT = 694,
     XMLELEMENT = 695,
     XMLEXISTS = 696,
     XMLFOREST = 697,
     XMLNAMESPACES = 698,
     XMLPARSE = 699,
     XMLPI = 700,
     XMLROOT = 701,
     XMLSERIALIZE = 702,
     XMLTABLE = 703,
     YEAR_P = 704,
     YES_P = 705,
     ZONE = 706,
     NOT_LA = 707,
     NULLS_LA = 708,
     WITH_LA = 709,
     POSTFIXOP = 710,
     UMINUS = 711
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
#define GREATEST 426
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
#define LEAST 474
#define LEFT 475
#define LEVEL 476
#define LIKE 477
#define LIMIT 478
#define LISTEN 479
#define LOAD 480
#define LOCAL 481
#define LOCALTIME 482
#define LOCALTIMESTAMP 483
#define LOCATION 484
#define LOCK_P 485
#define LOCKED 486
#define LOGGED 487
#define MAPPING 488
#define MATCH 489
#define MATERIALIZED 490
#define MAXVALUE 491
#define METHOD 492
#define MINUTE_P 493
#define MINVALUE 494
#define MODE 495
#define MONTH_P 496
#define MOVE 497
#define NAME_P 498
#define NAMES 499
#define NATIONAL 500
#define NATURAL 501
#define NCHAR 502
#define NEW 503
#define NEXT 504
#define NO 505
#define NONE 506
#define NOT 507
#define NOTHING 508
#define NOTIFY 509
#define NOTNULL 510
#define NOWAIT 511
#define NULL_P 512
#define NULLIF 513
#define NULLS_P 514
#define NUMERIC 515
#define OBJECT_P 516
#define OF 517
#define OFF 518
#define OFFSET 519
#define OIDS 520
#define OLD 521
#define ON 522
#define ONLY 523
#define OPERATOR 524
#define OPTION 525
#define OPTIONS 526
#define OR 527
#define ORDER 528
#define ORDINALITY 529
#define OUT_P 530
#define OUTER_P 531
#define OVER 532
#define OVERLAPS 533
#define OVERLAY 534
#define OVERRIDING 535
#define OWNED 536
#define OWNER 537
#define PARALLEL 538
#define PARSER 539
#define PARTIAL 540
#define PARTITION 541
#define PASSING 542
#define PASSWORD 543
#define PLACING 544
#define PLANS 545
#define POLICY 546
#define POSITION 547
#define PRAGMA_P 548
#define PRECEDING 549
#define PRECISION 550
#define PREPARE 551
#define PREPARED 552
#define PRESERVE 553
#define PRIMARY 554
#define PRIOR 555
#define PRIVILEGES 556
#define PROCEDURAL 557
#define PROCEDURE 558
#define PROGRAM 559
#define PUBLICATION 560
#define QUOTE 561
#define RANGE 562
#define READ_P 563
#define REAL 564
#define REASSIGN 565
#define RECHECK 566
#define RECURSIVE 567
#define REF 568
#define REFERENCES 569
#define REFERENCING 570
#define REFRESH 571
#define REINDEX 572
#define RELATIVE_P 573
#define RELEASE 574
#define RENAME 575
#define REPEATABLE 576
#define REPLACE 577
#define REPLICA 578
#define RESET 579
#define RESTART 580
#define RESTRICT 581
#define RETURNING 582
#define RETURNS 583
#define REVOKE 584
#define RIGHT 585
#define ROLE 586
#define ROLLBACK 587
#define ROLLUP 588
#define ROW 589
#define ROWS 590
#define RULE 591
#define SAVEPOINT 592
#define SCHEMA 593
#define SCHEMAS 594
#define SCROLL 595
#define SEARCH 596
#define SECOND_P 597
#define SECURITY 598
#define SELECT 599
#define SEQUENCE 600
#define SEQUENCES 601
#define SERIALIZABLE 602
#define SERVER 603
#define SESSION 604
#define SESSION_USER 605
#define SET 606
#define SETOF 607
#define SETS 608
#define SHARE 609
#define SHOW 610
#define SIMILAR 611
#define SIMPLE 612
#define SKIP 613
#define SMALLINT 614
#define SNAPSHOT 615
#define SOME 616
#define SQL_P 617
#define STABLE 618
#define STANDALONE_P 619
#define START 620
#define STATEMENT 621
#define STATISTICS 622
#define STDIN 623
#define STDOUT 624
#define STORAGE 625
#define STRICT_P 626
#define STRIP_P 627
#define SUBSCRIPTION 628
#define SUBSTRING 629
#define SYMMETRIC 630
#define SYSID 631
#define SYSTEM_P 632
#define TABLE 633
#define TABLES 634
#define TABLESAMPLE 635
#define TABLESPACE 636
#define TEMP 637
#define TEMPLATE 638
#define TEMPORARY 639
#define TEXT_P 640
#define THEN 641
#define TIME 642
#define TIMESTAMP 643
#define TO 644
#define TRAILING 645
#define TRANSACTION 646
#define TRANSFORM 647
#define TREAT 648
#define TRIGGER 649
#define TRIM 650
#define TRUE_P 651
#define TRUNCATE 652
#define TRUSTED 653
#define TYPE_P 654
#define TYPES_P 655
#define UNBOUNDED 656
#define UNCOMMITTED 657
#define UNENCRYPTED 658
#define UNION 659
#define UNIQUE 660
#define UNKNOWN 661
#define UNLISTEN 662
#define UNLOGGED 663
#define UNTIL 664
#define UPDATE 665
#define USER 666
#define USING 667
#define VACUUM 668
#define VALID 669
#define VALIDATE 670
#define VALIDATOR 671
#define VALUE_P 672
#define VALUES 673
#define VARCHAR 674
#define VARIADIC 675
#define VARYING 676
#define VERBOSE 677
#define VERSION_P 678
#define VIEW 679
#define VIEWS 680
#define VOLATILE 681
#define WHEN 682
#define WHERE 683
#define WHITESPACE_P 684
#define WINDOW 685
#define WITH 686
#define WITHIN 687
#define WITHOUT 688
#define WORK 689
#define WRAPPER 690
#define WRITE_P 691
#define XML_P 692
#define XMLATTRIBUTES 693
#define XMLCONCAT 694
#define XMLELEMENT 695
#define XMLEXISTS 696
#define XMLFOREST 697
#define XMLNAMESPACES 698
#define XMLPARSE 699
#define XMLPI 700
#define XMLROOT 701
#define XMLSERIALIZE 702
#define XMLTABLE 703
#define YEAR_P 704
#define YES_P 705
#define ZONE 706
#define NOT_LA 707
#define NULLS_LA 708
#define WITH_LA 709
#define POSTFIXOP 710
#define UMINUS 711




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
#line 1004 "third_party/libpg_query/grammar/grammar_out.hpp"
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


