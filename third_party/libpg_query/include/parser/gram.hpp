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
     POWER_OF = 270,
     LAMBDA_ARROW = 271,
     DOUBLE_ARROW = 272,
     LESS_EQUALS = 273,
     GREATER_EQUALS = 274,
     NOT_EQUALS = 275,
     ABORT_P = 276,
     ABSOLUTE_P = 277,
     ACCESS = 278,
     ACTION = 279,
     ADD_P = 280,
     ADMIN = 281,
     AFTER = 282,
     AGGREGATE = 283,
     ALL = 284,
     ALSO = 285,
     ALTER = 286,
     ALWAYS = 287,
     ANALYSE = 288,
     ANALYZE = 289,
     AND = 290,
     ANY = 291,
     ARRAY = 292,
     AS = 293,
     ASC_P = 294,
     ASSERTION = 295,
     ASSIGNMENT = 296,
     ASYMMETRIC = 297,
     AT = 298,
     ATTACH = 299,
     ATTRIBUTE = 300,
     AUTHORIZATION = 301,
     BACKWARD = 302,
     BEFORE = 303,
     BEGIN_P = 304,
     BETWEEN = 305,
     BIGINT = 306,
     BINARY = 307,
     BIT = 308,
     BOOLEAN_P = 309,
     BOTH = 310,
     BY = 311,
     CACHE = 312,
     CALL_P = 313,
     CALLED = 314,
     CASCADE = 315,
     CASCADED = 316,
     CASE = 317,
     CAST = 318,
     CATALOG_P = 319,
     CHAIN = 320,
     CHAR_P = 321,
     CHARACTER = 322,
     CHARACTERISTICS = 323,
     CHECK_P = 324,
     CHECKPOINT = 325,
     CLASS = 326,
     CLOSE = 327,
     CLUSTER = 328,
     COALESCE = 329,
     COLLATE = 330,
     COLLATION = 331,
     COLUMN = 332,
     COLUMNS = 333,
     COMMENT = 334,
     COMMENTS = 335,
     COMMIT = 336,
     COMMITTED = 337,
     COMPRESSION = 338,
     CONCURRENTLY = 339,
     CONFIGURATION = 340,
     CONFLICT = 341,
     CONNECTION = 342,
     CONSTRAINT = 343,
     CONSTRAINTS = 344,
     CONTENT_P = 345,
     CONTINUE_P = 346,
     CONVERSION_P = 347,
     COPY = 348,
     COST = 349,
     CREATE_P = 350,
     CROSS = 351,
     CSV = 352,
     CUBE = 353,
     CURRENT_P = 354,
     CURRENT_CATALOG = 355,
     CURRENT_DATE = 356,
     CURRENT_ROLE = 357,
     CURRENT_SCHEMA = 358,
     CURRENT_TIME = 359,
     CURRENT_TIMESTAMP = 360,
     CURRENT_USER = 361,
     CURSOR = 362,
     CYCLE = 363,
     DATA_P = 364,
     DATABASE = 365,
     DAY_P = 366,
     DAYS_P = 367,
     DEALLOCATE = 368,
     DEC = 369,
     DECIMAL_P = 370,
     DECLARE = 371,
     DEFAULT = 372,
     DEFAULTS = 373,
     DEFERRABLE = 374,
     DEFERRED = 375,
     DEFINER = 376,
     DELETE_P = 377,
     DELIMITER = 378,
     DELIMITERS = 379,
     DEPENDS = 380,
     DESC_P = 381,
     DESCRIBE = 382,
     DETACH = 383,
     DICTIONARY = 384,
     DISABLE_P = 385,
     DISCARD = 386,
     DISTINCT = 387,
     DO = 388,
     DOCUMENT_P = 389,
     DOMAIN_P = 390,
     DOUBLE_P = 391,
     DROP = 392,
     EACH = 393,
     ELSE = 394,
     ENABLE_P = 395,
     ENCODING = 396,
     ENCRYPTED = 397,
     END_P = 398,
     ENUM_P = 399,
     ESCAPE = 400,
     EVENT = 401,
     EXCEPT = 402,
     EXCLUDE = 403,
     EXCLUDING = 404,
     EXCLUSIVE = 405,
     EXECUTE = 406,
     EXISTS = 407,
     EXPLAIN = 408,
     EXPORT_P = 409,
     EXPORT_STATE = 410,
     EXTENSION = 411,
     EXTERNAL = 412,
     EXTRACT = 413,
     FALSE_P = 414,
     FAMILY = 415,
     FETCH = 416,
     FILTER = 417,
     FIRST_P = 418,
     FLOAT_P = 419,
     FOLLOWING = 420,
     FOR = 421,
     FORCE = 422,
     FOREIGN = 423,
     FORWARD = 424,
     FREEZE = 425,
     FROM = 426,
     FULL = 427,
     FUNCTION = 428,
     FUNCTIONS = 429,
     GENERATED = 430,
     GLOB = 431,
     GLOBAL = 432,
     GRANT = 433,
     GRANTED = 434,
     GROUP_P = 435,
     GROUPING = 436,
     GROUPING_ID = 437,
     HANDLER = 438,
     HAVING = 439,
     HEADER_P = 440,
     HOLD = 441,
     HOUR_P = 442,
     HOURS_P = 443,
     IDENTITY_P = 444,
     IF_P = 445,
     IGNORE_P = 446,
     ILIKE = 447,
     IMMEDIATE = 448,
     IMMUTABLE = 449,
     IMPLICIT_P = 450,
     IMPORT_P = 451,
     IN_P = 452,
     INCLUDING = 453,
     INCREMENT = 454,
     INDEX = 455,
     INDEXES = 456,
     INHERIT = 457,
     INHERITS = 458,
     INITIALLY = 459,
     INLINE_P = 460,
     INNER_P = 461,
     INOUT = 462,
     INPUT_P = 463,
     INSENSITIVE = 464,
     INSERT = 465,
     INSTALL = 466,
     INSTEAD = 467,
     INT_P = 468,
     INTEGER = 469,
     INTERSECT = 470,
     INTERVAL = 471,
     INTO = 472,
     INVOKER = 473,
     IS = 474,
     ISNULL = 475,
     ISOLATION = 476,
     JOIN = 477,
     JSON = 478,
     KEY = 479,
     LABEL = 480,
     LANGUAGE = 481,
     LARGE_P = 482,
     LAST_P = 483,
     LATERAL_P = 484,
     LEADING = 485,
     LEAKPROOF = 486,
     LEFT = 487,
     LEVEL = 488,
     LIKE = 489,
     LIMIT = 490,
     LISTEN = 491,
     LOAD = 492,
     LOCAL = 493,
     LOCALTIME = 494,
     LOCALTIMESTAMP = 495,
     LOCATION = 496,
     LOCK_P = 497,
     LOCKED = 498,
     LOGGED = 499,
     MACRO = 500,
     MAP = 501,
     MAPPING = 502,
     MATCH = 503,
     MATERIALIZED = 504,
     MAXVALUE = 505,
     METHOD = 506,
     MICROSECOND_P = 507,
     MICROSECONDS_P = 508,
     MILLISECOND_P = 509,
     MILLISECONDS_P = 510,
     MINUTE_P = 511,
     MINUTES_P = 512,
     MINVALUE = 513,
     MODE = 514,
     MONTH_P = 515,
     MONTHS_P = 516,
     MOVE = 517,
     NAME_P = 518,
     NAMES = 519,
     NATIONAL = 520,
     NATURAL = 521,
     NCHAR = 522,
     NEW = 523,
     NEXT = 524,
     NO = 525,
     NONE = 526,
     NOT = 527,
     NOTHING = 528,
     NOTIFY = 529,
     NOTNULL = 530,
     NOWAIT = 531,
     NULL_P = 532,
     NULLIF = 533,
     NULLS_P = 534,
     NUMERIC = 535,
     OBJECT_P = 536,
     OF = 537,
     OFF = 538,
     OFFSET = 539,
     OIDS = 540,
     OLD = 541,
     ON = 542,
     ONLY = 543,
     OPERATOR = 544,
     OPTION = 545,
     OPTIONS = 546,
     OR = 547,
     ORDER = 548,
     ORDINALITY = 549,
     OUT_P = 550,
     OUTER_P = 551,
     OVER = 552,
     OVERLAPS = 553,
     OVERLAY = 554,
     OVERRIDING = 555,
     OWNED = 556,
     OWNER = 557,
     PARALLEL = 558,
     PARSER = 559,
     PARTIAL = 560,
     PARTITION = 561,
     PASSING = 562,
     PASSWORD = 563,
     PERCENT = 564,
     PIVOT = 565,
     PLACING = 566,
     PLANS = 567,
     POLICY = 568,
     POSITION = 569,
     POSITIONAL = 570,
     PRAGMA_P = 571,
     PRECEDING = 572,
     PRECISION = 573,
     PREPARE = 574,
     PREPARED = 575,
     PRESERVE = 576,
     PRIMARY = 577,
     PRIOR = 578,
     PRIVILEGES = 579,
     PROCEDURAL = 580,
     PROCEDURE = 581,
     PROGRAM = 582,
     PUBLICATION = 583,
     QUALIFY = 584,
     QUOTE = 585,
     RANGE = 586,
     READ_P = 587,
     REAL = 588,
     REASSIGN = 589,
     RECHECK = 590,
     RECURSIVE = 591,
     REF = 592,
     REFERENCES = 593,
     REFERENCING = 594,
     REFRESH = 595,
     REINDEX = 596,
     RELATIVE_P = 597,
     RELEASE = 598,
     RENAME = 599,
     REPEATABLE = 600,
     REPLACE = 601,
     REPLICA = 602,
     RESET = 603,
     RESPECT_P = 604,
     RESTART = 605,
     RESTRICT = 606,
     RETURNING = 607,
     RETURNS = 608,
     REVOKE = 609,
     RIGHT = 610,
     ROLE = 611,
     ROLLBACK = 612,
     ROLLUP = 613,
     ROW = 614,
     ROWS = 615,
     RULE = 616,
     SAMPLE = 617,
     SAVEPOINT = 618,
     SCHEMA = 619,
     SCHEMAS = 620,
     SCROLL = 621,
     SEARCH = 622,
     SECOND_P = 623,
     SECONDS_P = 624,
     SECURITY = 625,
     SELECT = 626,
     SEQUENCE = 627,
     SEQUENCES = 628,
     SERIALIZABLE = 629,
     SERVER = 630,
     SESSION = 631,
     SESSION_USER = 632,
     SET = 633,
     SETOF = 634,
     SETS = 635,
     SHARE = 636,
     SHOW = 637,
     SIMILAR = 638,
     SIMPLE = 639,
     SKIP = 640,
     SMALLINT = 641,
     SNAPSHOT = 642,
     SOME = 643,
     SQL_P = 644,
     STABLE = 645,
     STANDALONE_P = 646,
     START = 647,
     STATEMENT = 648,
     STATISTICS = 649,
     STDIN = 650,
     STDOUT = 651,
     STORAGE = 652,
     STORED = 653,
     STRICT_P = 654,
     STRIP_P = 655,
     STRUCT = 656,
     SUBSCRIPTION = 657,
     SUBSTRING = 658,
     SUMMARIZE = 659,
     SYMMETRIC = 660,
     SYSID = 661,
     SYSTEM_P = 662,
     TABLE = 663,
     TABLES = 664,
     TABLESAMPLE = 665,
     TABLESPACE = 666,
     TEMP = 667,
     TEMPLATE = 668,
     TEMPORARY = 669,
     TEXT_P = 670,
     THEN = 671,
     TIME = 672,
     TIMESTAMP = 673,
     TO = 674,
     TRAILING = 675,
     TRANSACTION = 676,
     TRANSFORM = 677,
     TREAT = 678,
     TRIGGER = 679,
     TRIM = 680,
     TRUE_P = 681,
     TRUNCATE = 682,
     TRUSTED = 683,
     TRY_CAST = 684,
     TYPE_P = 685,
     TYPES_P = 686,
     UNBOUNDED = 687,
     UNCOMMITTED = 688,
     UNENCRYPTED = 689,
     UNION = 690,
     UNIQUE = 691,
     UNKNOWN = 692,
     UNLISTEN = 693,
     UNLOGGED = 694,
     UNTIL = 695,
     UPDATE = 696,
     USE_P = 697,
     USER = 698,
     USING = 699,
     VACUUM = 700,
     VALID = 701,
     VALIDATE = 702,
     VALIDATOR = 703,
     VALUE_P = 704,
     VALUES = 705,
     VARCHAR = 706,
     VARIADIC = 707,
     VARYING = 708,
     VERBOSE = 709,
     VERSION_P = 710,
     VIEW = 711,
     VIEWS = 712,
     VIRTUAL = 713,
     VOLATILE = 714,
     WHEN = 715,
     WHERE = 716,
     WHITESPACE_P = 717,
     WINDOW = 718,
     WITH = 719,
     WITHIN = 720,
     WITHOUT = 721,
     WORK = 722,
     WRAPPER = 723,
     WRITE_P = 724,
     XML_P = 725,
     XMLATTRIBUTES = 726,
     XMLCONCAT = 727,
     XMLELEMENT = 728,
     XMLEXISTS = 729,
     XMLFOREST = 730,
     XMLNAMESPACES = 731,
     XMLPARSE = 732,
     XMLPI = 733,
     XMLROOT = 734,
     XMLSERIALIZE = 735,
     XMLTABLE = 736,
     YEAR_P = 737,
     YEARS_P = 738,
     YES_P = 739,
     ZONE = 740,
     NOT_LA = 741,
     NULLS_LA = 742,
     WITH_LA = 743,
     POSTFIXOP = 744,
     UMINUS = 745
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
#define POWER_OF 270
#define LAMBDA_ARROW 271
#define DOUBLE_ARROW 272
#define LESS_EQUALS 273
#define GREATER_EQUALS 274
#define NOT_EQUALS 275
#define ABORT_P 276
#define ABSOLUTE_P 277
#define ACCESS 278
#define ACTION 279
#define ADD_P 280
#define ADMIN 281
#define AFTER 282
#define AGGREGATE 283
#define ALL 284
#define ALSO 285
#define ALTER 286
#define ALWAYS 287
#define ANALYSE 288
#define ANALYZE 289
#define AND 290
#define ANY 291
#define ARRAY 292
#define AS 293
#define ASC_P 294
#define ASSERTION 295
#define ASSIGNMENT 296
#define ASYMMETRIC 297
#define AT 298
#define ATTACH 299
#define ATTRIBUTE 300
#define AUTHORIZATION 301
#define BACKWARD 302
#define BEFORE 303
#define BEGIN_P 304
#define BETWEEN 305
#define BIGINT 306
#define BINARY 307
#define BIT 308
#define BOOLEAN_P 309
#define BOTH 310
#define BY 311
#define CACHE 312
#define CALL_P 313
#define CALLED 314
#define CASCADE 315
#define CASCADED 316
#define CASE 317
#define CAST 318
#define CATALOG_P 319
#define CHAIN 320
#define CHAR_P 321
#define CHARACTER 322
#define CHARACTERISTICS 323
#define CHECK_P 324
#define CHECKPOINT 325
#define CLASS 326
#define CLOSE 327
#define CLUSTER 328
#define COALESCE 329
#define COLLATE 330
#define COLLATION 331
#define COLUMN 332
#define COLUMNS 333
#define COMMENT 334
#define COMMENTS 335
#define COMMIT 336
#define COMMITTED 337
#define COMPRESSION 338
#define CONCURRENTLY 339
#define CONFIGURATION 340
#define CONFLICT 341
#define CONNECTION 342
#define CONSTRAINT 343
#define CONSTRAINTS 344
#define CONTENT_P 345
#define CONTINUE_P 346
#define CONVERSION_P 347
#define COPY 348
#define COST 349
#define CREATE_P 350
#define CROSS 351
#define CSV 352
#define CUBE 353
#define CURRENT_P 354
#define CURRENT_CATALOG 355
#define CURRENT_DATE 356
#define CURRENT_ROLE 357
#define CURRENT_SCHEMA 358
#define CURRENT_TIME 359
#define CURRENT_TIMESTAMP 360
#define CURRENT_USER 361
#define CURSOR 362
#define CYCLE 363
#define DATA_P 364
#define DATABASE 365
#define DAY_P 366
#define DAYS_P 367
#define DEALLOCATE 368
#define DEC 369
#define DECIMAL_P 370
#define DECLARE 371
#define DEFAULT 372
#define DEFAULTS 373
#define DEFERRABLE 374
#define DEFERRED 375
#define DEFINER 376
#define DELETE_P 377
#define DELIMITER 378
#define DELIMITERS 379
#define DEPENDS 380
#define DESC_P 381
#define DESCRIBE 382
#define DETACH 383
#define DICTIONARY 384
#define DISABLE_P 385
#define DISCARD 386
#define DISTINCT 387
#define DO 388
#define DOCUMENT_P 389
#define DOMAIN_P 390
#define DOUBLE_P 391
#define DROP 392
#define EACH 393
#define ELSE 394
#define ENABLE_P 395
#define ENCODING 396
#define ENCRYPTED 397
#define END_P 398
#define ENUM_P 399
#define ESCAPE 400
#define EVENT 401
#define EXCEPT 402
#define EXCLUDE 403
#define EXCLUDING 404
#define EXCLUSIVE 405
#define EXECUTE 406
#define EXISTS 407
#define EXPLAIN 408
#define EXPORT_P 409
#define EXPORT_STATE 410
#define EXTENSION 411
#define EXTERNAL 412
#define EXTRACT 413
#define FALSE_P 414
#define FAMILY 415
#define FETCH 416
#define FILTER 417
#define FIRST_P 418
#define FLOAT_P 419
#define FOLLOWING 420
#define FOR 421
#define FORCE 422
#define FOREIGN 423
#define FORWARD 424
#define FREEZE 425
#define FROM 426
#define FULL 427
#define FUNCTION 428
#define FUNCTIONS 429
#define GENERATED 430
#define GLOB 431
#define GLOBAL 432
#define GRANT 433
#define GRANTED 434
#define GROUP_P 435
#define GROUPING 436
#define GROUPING_ID 437
#define HANDLER 438
#define HAVING 439
#define HEADER_P 440
#define HOLD 441
#define HOUR_P 442
#define HOURS_P 443
#define IDENTITY_P 444
#define IF_P 445
#define IGNORE_P 446
#define ILIKE 447
#define IMMEDIATE 448
#define IMMUTABLE 449
#define IMPLICIT_P 450
#define IMPORT_P 451
#define IN_P 452
#define INCLUDING 453
#define INCREMENT 454
#define INDEX 455
#define INDEXES 456
#define INHERIT 457
#define INHERITS 458
#define INITIALLY 459
#define INLINE_P 460
#define INNER_P 461
#define INOUT 462
#define INPUT_P 463
#define INSENSITIVE 464
#define INSERT 465
#define INSTALL 466
#define INSTEAD 467
#define INT_P 468
#define INTEGER 469
#define INTERSECT 470
#define INTERVAL 471
#define INTO 472
#define INVOKER 473
#define IS 474
#define ISNULL 475
#define ISOLATION 476
#define JOIN 477
#define JSON 478
#define KEY 479
#define LABEL 480
#define LANGUAGE 481
#define LARGE_P 482
#define LAST_P 483
#define LATERAL_P 484
#define LEADING 485
#define LEAKPROOF 486
#define LEFT 487
#define LEVEL 488
#define LIKE 489
#define LIMIT 490
#define LISTEN 491
#define LOAD 492
#define LOCAL 493
#define LOCALTIME 494
#define LOCALTIMESTAMP 495
#define LOCATION 496
#define LOCK_P 497
#define LOCKED 498
#define LOGGED 499
#define MACRO 500
#define MAP 501
#define MAPPING 502
#define MATCH 503
#define MATERIALIZED 504
#define MAXVALUE 505
#define METHOD 506
#define MICROSECOND_P 507
#define MICROSECONDS_P 508
#define MILLISECOND_P 509
#define MILLISECONDS_P 510
#define MINUTE_P 511
#define MINUTES_P 512
#define MINVALUE 513
#define MODE 514
#define MONTH_P 515
#define MONTHS_P 516
#define MOVE 517
#define NAME_P 518
#define NAMES 519
#define NATIONAL 520
#define NATURAL 521
#define NCHAR 522
#define NEW 523
#define NEXT 524
#define NO 525
#define NONE 526
#define NOT 527
#define NOTHING 528
#define NOTIFY 529
#define NOTNULL 530
#define NOWAIT 531
#define NULL_P 532
#define NULLIF 533
#define NULLS_P 534
#define NUMERIC 535
#define OBJECT_P 536
#define OF 537
#define OFF 538
#define OFFSET 539
#define OIDS 540
#define OLD 541
#define ON 542
#define ONLY 543
#define OPERATOR 544
#define OPTION 545
#define OPTIONS 546
#define OR 547
#define ORDER 548
#define ORDINALITY 549
#define OUT_P 550
#define OUTER_P 551
#define OVER 552
#define OVERLAPS 553
#define OVERLAY 554
#define OVERRIDING 555
#define OWNED 556
#define OWNER 557
#define PARALLEL 558
#define PARSER 559
#define PARTIAL 560
#define PARTITION 561
#define PASSING 562
#define PASSWORD 563
#define PERCENT 564
#define PIVOT 565
#define PLACING 566
#define PLANS 567
#define POLICY 568
#define POSITION 569
#define POSITIONAL 570
#define PRAGMA_P 571
#define PRECEDING 572
#define PRECISION 573
#define PREPARE 574
#define PREPARED 575
#define PRESERVE 576
#define PRIMARY 577
#define PRIOR 578
#define PRIVILEGES 579
#define PROCEDURAL 580
#define PROCEDURE 581
#define PROGRAM 582
#define PUBLICATION 583
#define QUALIFY 584
#define QUOTE 585
#define RANGE 586
#define READ_P 587
#define REAL 588
#define REASSIGN 589
#define RECHECK 590
#define RECURSIVE 591
#define REF 592
#define REFERENCES 593
#define REFERENCING 594
#define REFRESH 595
#define REINDEX 596
#define RELATIVE_P 597
#define RELEASE 598
#define RENAME 599
#define REPEATABLE 600
#define REPLACE 601
#define REPLICA 602
#define RESET 603
#define RESPECT_P 604
#define RESTART 605
#define RESTRICT 606
#define RETURNING 607
#define RETURNS 608
#define REVOKE 609
#define RIGHT 610
#define ROLE 611
#define ROLLBACK 612
#define ROLLUP 613
#define ROW 614
#define ROWS 615
#define RULE 616
#define SAMPLE 617
#define SAVEPOINT 618
#define SCHEMA 619
#define SCHEMAS 620
#define SCROLL 621
#define SEARCH 622
#define SECOND_P 623
#define SECONDS_P 624
#define SECURITY 625
#define SELECT 626
#define SEQUENCE 627
#define SEQUENCES 628
#define SERIALIZABLE 629
#define SERVER 630
#define SESSION 631
#define SESSION_USER 632
#define SET 633
#define SETOF 634
#define SETS 635
#define SHARE 636
#define SHOW 637
#define SIMILAR 638
#define SIMPLE 639
#define SKIP 640
#define SMALLINT 641
#define SNAPSHOT 642
#define SOME 643
#define SQL_P 644
#define STABLE 645
#define STANDALONE_P 646
#define START 647
#define STATEMENT 648
#define STATISTICS 649
#define STDIN 650
#define STDOUT 651
#define STORAGE 652
#define STORED 653
#define STRICT_P 654
#define STRIP_P 655
#define STRUCT 656
#define SUBSCRIPTION 657
#define SUBSTRING 658
#define SUMMARIZE 659
#define SYMMETRIC 660
#define SYSID 661
#define SYSTEM_P 662
#define TABLE 663
#define TABLES 664
#define TABLESAMPLE 665
#define TABLESPACE 666
#define TEMP 667
#define TEMPLATE 668
#define TEMPORARY 669
#define TEXT_P 670
#define THEN 671
#define TIME 672
#define TIMESTAMP 673
#define TO 674
#define TRAILING 675
#define TRANSACTION 676
#define TRANSFORM 677
#define TREAT 678
#define TRIGGER 679
#define TRIM 680
#define TRUE_P 681
#define TRUNCATE 682
#define TRUSTED 683
#define TRY_CAST 684
#define TYPE_P 685
#define TYPES_P 686
#define UNBOUNDED 687
#define UNCOMMITTED 688
#define UNENCRYPTED 689
#define UNION 690
#define UNIQUE 691
#define UNKNOWN 692
#define UNLISTEN 693
#define UNLOGGED 694
#define UNTIL 695
#define UPDATE 696
#define USE_P 697
#define USER 698
#define USING 699
#define VACUUM 700
#define VALID 701
#define VALIDATE 702
#define VALIDATOR 703
#define VALUE_P 704
#define VALUES 705
#define VARCHAR 706
#define VARIADIC 707
#define VARYING 708
#define VERBOSE 709
#define VERSION_P 710
#define VIEW 711
#define VIEWS 712
#define VIRTUAL 713
#define VOLATILE 714
#define WHEN 715
#define WHERE 716
#define WHITESPACE_P 717
#define WINDOW 718
#define WITH 719
#define WITHIN 720
#define WITHOUT 721
#define WORK 722
#define WRAPPER 723
#define WRITE_P 724
#define XML_P 725
#define XMLATTRIBUTES 726
#define XMLCONCAT 727
#define XMLELEMENT 728
#define XMLEXISTS 729
#define XMLFOREST 730
#define XMLNAMESPACES 731
#define XMLPARSE 732
#define XMLPI 733
#define XMLROOT 734
#define XMLSERIALIZE 735
#define XMLTABLE 736
#define YEAR_P 737
#define YEARS_P 738
#define YES_P 739
#define ZONE 740
#define NOT_LA 741
#define NULLS_LA 742
#define WITH_LA 743
#define POSTFIXOP 744
#define UMINUS 745




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
	PGOnConflictActionAlias onconflictshorthand;
	PGAIndices			*aind;
	PGResTarget			*target;
	PGInsertStmt			*istmt;
	PGVariableSetStmt		*vsetstmt;
	PGOverridingKind       override;
	PGSortByDir            sortorder;
	PGSortByNulls          nullorder;
	PGConstrType           constr;
	PGLockClauseStrength lockstrength;
	PGLockWaitPolicy lockwaitpolicy;
	PGSubLinkType subquerytype;
	PGViewCheckOption viewcheckoption;
}
/* Line 1529 of yacc.c.  */
#line 1075 "third_party/libpg_query/grammar/grammar_out.hpp"
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


