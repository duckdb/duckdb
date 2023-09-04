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
     INTEGER_DIVISION = 270,
     POWER_OF = 271,
     LAMBDA_ARROW = 272,
     DOUBLE_ARROW = 273,
     LESS_EQUALS = 274,
     GREATER_EQUALS = 275,
     NOT_EQUALS = 276,
     ABORT_P = 277,
     ABSOLUTE_P = 278,
     ACCESS = 279,
     ACTION = 280,
     ADD_P = 281,
     ADMIN = 282,
     AFTER = 283,
     AGGREGATE = 284,
     ALL = 285,
     ALSO = 286,
     ALTER = 287,
     ALWAYS = 288,
     ANALYSE = 289,
     ANALYZE = 290,
     AND = 291,
     ANTI = 292,
     ANY = 293,
     ARRAY = 294,
     AS = 295,
     ASC_P = 296,
     ASOF = 297,
     ASSERTION = 298,
     ASSIGNMENT = 299,
     ASYMMETRIC = 300,
     AT = 301,
     ATTACH = 302,
     ATTRIBUTE = 303,
     AUTHORIZATION = 304,
     BACKWARD = 305,
     BEFORE = 306,
     BEGIN_P = 307,
     BETWEEN = 308,
     BIGINT = 309,
     BINARY = 310,
     BIT = 311,
     BOOLEAN_P = 312,
     BOTH = 313,
     BY = 314,
     CACHE = 315,
     CALL_P = 316,
     CALLED = 317,
     CASCADE = 318,
     CASCADED = 319,
     CASE = 320,
     CAST = 321,
     CATALOG_P = 322,
     CHAIN = 323,
     CHAR_P = 324,
     CHARACTER = 325,
     CHARACTERISTICS = 326,
     CHECK_P = 327,
     CHECKPOINT = 328,
     CLASS = 329,
     CLOSE = 330,
     CLUSTER = 331,
     COALESCE = 332,
     COLLATE = 333,
     COLLATION = 334,
     COLUMN = 335,
     COLUMNS = 336,
     COMMENT = 337,
     COMMENTS = 338,
     COMMIT = 339,
     COMMITTED = 340,
     COMPRESSION = 341,
     CONCURRENTLY = 342,
     CONFIGURATION = 343,
     CONFLICT = 344,
     CONNECTION = 345,
     CONSTRAINT = 346,
     CONSTRAINTS = 347,
     CONTENT_P = 348,
     CONTINUE_P = 349,
     CONVERSION_P = 350,
     COPY = 351,
     COST = 352,
     CREATE_P = 353,
     CROSS = 354,
     CSV = 355,
     CUBE = 356,
     CURRENT_P = 357,
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
     EXPORT_STATE = 406,
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
     GROUPING_ID = 433,
     GROUPS = 434,
     HANDLER = 435,
     HAVING = 436,
     HEADER_P = 437,
     HOLD = 438,
     HOUR_P = 439,
     HOURS_P = 440,
     IDENTITY_P = 441,
     IF_P = 442,
     IGNORE_P = 443,
     ILIKE = 444,
     IMMEDIATE = 445,
     IMMUTABLE = 446,
     IMPLICIT_P = 447,
     IMPORT_P = 448,
     IN_P = 449,
     INCLUDE_P = 450,
     INCLUDING = 451,
     INCREMENT = 452,
     INDEX = 453,
     INDEXES = 454,
     INHERIT = 455,
     INHERITS = 456,
     INITIALLY = 457,
     INLINE_P = 458,
     INNER_P = 459,
     INOUT = 460,
     INPUT_P = 461,
     INSENSITIVE = 462,
     INSERT = 463,
     INSTALL = 464,
     INSTEAD = 465,
     INT_P = 466,
     INTEGER = 467,
     INTERSECT = 468,
     INTERVAL = 469,
     INTO = 470,
     INVOKER = 471,
     IS = 472,
     ISNULL = 473,
     ISOLATION = 474,
     JOIN = 475,
     JSON = 476,
     KEY = 477,
     LABEL = 478,
     LANGUAGE = 479,
     LARGE_P = 480,
     LAST_P = 481,
     LATERAL_P = 482,
     LEADING = 483,
     LEAKPROOF = 484,
     LEFT = 485,
     LEVEL = 486,
     LIKE = 487,
     LIMIT = 488,
     LISTEN = 489,
     LOAD = 490,
     LOCAL = 491,
     LOCATION = 492,
     LOCK_P = 493,
     LOCKED = 494,
     LOGGED = 495,
     MACRO = 496,
     MAP = 497,
     MAPPING = 498,
     MATCH = 499,
     MATERIALIZED = 500,
     MAXVALUE = 501,
     METHOD = 502,
     MICROSECOND_P = 503,
     MICROSECONDS_P = 504,
     MILLISECOND_P = 505,
     MILLISECONDS_P = 506,
     MINUTE_P = 507,
     MINUTES_P = 508,
     MINVALUE = 509,
     MODE = 510,
     MONTH_P = 511,
     MONTHS_P = 512,
     MOVE = 513,
     NAME_P = 514,
     NAMES = 515,
     NATIONAL = 516,
     NATURAL = 517,
     NCHAR = 518,
     NEW = 519,
     NEXT = 520,
     NO = 521,
     NONE = 522,
     NOT = 523,
     NOTHING = 524,
     NOTIFY = 525,
     NOTNULL = 526,
     NOWAIT = 527,
     NULL_P = 528,
     NULLIF = 529,
     NULLS_P = 530,
     NUMERIC = 531,
     OBJECT_P = 532,
     OF = 533,
     OFF = 534,
     OFFSET = 535,
     OIDS = 536,
     OLD = 537,
     ON = 538,
     ONLY = 539,
     OPERATOR = 540,
     OPTION = 541,
     OPTIONS = 542,
     OR = 543,
     ORDER = 544,
     ORDINALITY = 545,
     OTHERS = 546,
     OUT_P = 547,
     OUTER_P = 548,
     OVER = 549,
     OVERLAPS = 550,
     OVERLAY = 551,
     OVERRIDING = 552,
     OWNED = 553,
     OWNER = 554,
     PARALLEL = 555,
     PARSER = 556,
     PARTIAL = 557,
     PARTITION = 558,
     PASSING = 559,
     PASSWORD = 560,
     PERCENT = 561,
     PIVOT = 562,
     PIVOT_LONGER = 563,
     PIVOT_WIDER = 564,
     PLACING = 565,
     PLANS = 566,
     POLICY = 567,
     POSITION = 568,
     POSITIONAL = 569,
     PRAGMA_P = 570,
     PRECEDING = 571,
     PRECISION = 572,
     PREPARE = 573,
     PREPARED = 574,
     PRESERVE = 575,
     PRIMARY = 576,
     PRIOR = 577,
     PRIVILEGES = 578,
     PROCEDURAL = 579,
     PROCEDURE = 580,
     PROGRAM = 581,
     PUBLICATION = 582,
     QUALIFY = 583,
     QUOTE = 584,
     RANGE = 585,
     READ_P = 586,
     REAL = 587,
     REASSIGN = 588,
     RECHECK = 589,
     RECURSIVE = 590,
     REF = 591,
     REFERENCES = 592,
     REFERENCING = 593,
     REFRESH = 594,
     REINDEX = 595,
     RELATIVE_P = 596,
     RELEASE = 597,
     RENAME = 598,
     REPEATABLE = 599,
     REPLACE = 600,
     REPLICA = 601,
     RESET = 602,
     RESPECT_P = 603,
     RESTART = 604,
     RESTRICT = 605,
     RETURNING = 606,
     RETURNS = 607,
     REVOKE = 608,
     RIGHT = 609,
     ROLE = 610,
     ROLLBACK = 611,
     ROLLUP = 612,
     ROW = 613,
     ROWS = 614,
     RULE = 615,
     SAMPLE = 616,
     SAVEPOINT = 617,
     SCHEMA = 618,
     SCHEMAS = 619,
     SCROLL = 620,
     SEARCH = 621,
     SECOND_P = 622,
     SECONDS_P = 623,
     SECURITY = 624,
     SELECT = 625,
     SEMI = 626,
     SEQUENCE = 627,
     SEQUENCES = 628,
     SERIALIZABLE = 629,
     SERVER = 630,
     SESSION = 631,
     SET = 632,
     SETOF = 633,
     SETS = 634,
     SHARE = 635,
     SHOW = 636,
     SIMILAR = 637,
     SIMPLE = 638,
     SKIP = 639,
     SMALLINT = 640,
     SNAPSHOT = 641,
     SOME = 642,
     SQL_P = 643,
     STABLE = 644,
     STANDALONE_P = 645,
     START = 646,
     STATEMENT = 647,
     STATISTICS = 648,
     STDIN = 649,
     STDOUT = 650,
     STORAGE = 651,
     STORED = 652,
     STRICT_P = 653,
     STRIP_P = 654,
     STRUCT = 655,
     SUBSCRIPTION = 656,
     SUBSTRING = 657,
     SUMMARIZE = 658,
     SYMMETRIC = 659,
     SYSID = 660,
     SYSTEM_P = 661,
     TABLE = 662,
     TABLES = 663,
     TABLESAMPLE = 664,
     TABLESPACE = 665,
     TEMP = 666,
     TEMPLATE = 667,
     TEMPORARY = 668,
     TEXT_P = 669,
     THEN = 670,
     TIES = 671,
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
     UNPIVOT = 695,
     UNTIL = 696,
     UPDATE = 697,
     USE_P = 698,
     USER = 699,
     USING = 700,
     VACUUM = 701,
     VALID = 702,
     VALIDATE = 703,
     VALIDATOR = 704,
     VALUE_P = 705,
     VALUES = 706,
     VARCHAR = 707,
     VARIADIC = 708,
     VARYING = 709,
     VERBOSE = 710,
     VERSION_P = 711,
     VIEW = 712,
     VIEWS = 713,
     VIRTUAL = 714,
     VOLATILE = 715,
     WHEN = 716,
     WHERE = 717,
     WHITESPACE_P = 718,
     WINDOW = 719,
     WITH = 720,
     WITHIN = 721,
     WITHOUT = 722,
     WORK = 723,
     WRAPPER = 724,
     WRITE_P = 725,
     XML_P = 726,
     XMLATTRIBUTES = 727,
     XMLCONCAT = 728,
     XMLELEMENT = 729,
     XMLEXISTS = 730,
     XMLFOREST = 731,
     XMLNAMESPACES = 732,
     XMLPARSE = 733,
     XMLPI = 734,
     XMLROOT = 735,
     XMLSERIALIZE = 736,
     XMLTABLE = 737,
     YEAR_P = 738,
     YEARS_P = 739,
     YES_P = 740,
     ZONE = 741,
     NOT_LA = 742,
     NULLS_LA = 743,
     WITH_LA = 744,
     POSTFIXOP = 745,
     UMINUS = 746
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
#define INTEGER_DIVISION 270
#define POWER_OF 271
#define LAMBDA_ARROW 272
#define DOUBLE_ARROW 273
#define LESS_EQUALS 274
#define GREATER_EQUALS 275
#define NOT_EQUALS 276
#define ABORT_P 277
#define ABSOLUTE_P 278
#define ACCESS 279
#define ACTION 280
#define ADD_P 281
#define ADMIN 282
#define AFTER 283
#define AGGREGATE 284
#define ALL 285
#define ALSO 286
#define ALTER 287
#define ALWAYS 288
#define ANALYSE 289
#define ANALYZE 290
#define AND 291
#define ANTI 292
#define ANY 293
#define ARRAY 294
#define AS 295
#define ASC_P 296
#define ASOF 297
#define ASSERTION 298
#define ASSIGNMENT 299
#define ASYMMETRIC 300
#define AT 301
#define ATTACH 302
#define ATTRIBUTE 303
#define AUTHORIZATION 304
#define BACKWARD 305
#define BEFORE 306
#define BEGIN_P 307
#define BETWEEN 308
#define BIGINT 309
#define BINARY 310
#define BIT 311
#define BOOLEAN_P 312
#define BOTH 313
#define BY 314
#define CACHE 315
#define CALL_P 316
#define CALLED 317
#define CASCADE 318
#define CASCADED 319
#define CASE 320
#define CAST 321
#define CATALOG_P 322
#define CHAIN 323
#define CHAR_P 324
#define CHARACTER 325
#define CHARACTERISTICS 326
#define CHECK_P 327
#define CHECKPOINT 328
#define CLASS 329
#define CLOSE 330
#define CLUSTER 331
#define COALESCE 332
#define COLLATE 333
#define COLLATION 334
#define COLUMN 335
#define COLUMNS 336
#define COMMENT 337
#define COMMENTS 338
#define COMMIT 339
#define COMMITTED 340
#define COMPRESSION 341
#define CONCURRENTLY 342
#define CONFIGURATION 343
#define CONFLICT 344
#define CONNECTION 345
#define CONSTRAINT 346
#define CONSTRAINTS 347
#define CONTENT_P 348
#define CONTINUE_P 349
#define CONVERSION_P 350
#define COPY 351
#define COST 352
#define CREATE_P 353
#define CROSS 354
#define CSV 355
#define CUBE 356
#define CURRENT_P 357
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
#define EXPORT_STATE 406
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
#define GROUPING_ID 433
#define GROUPS 434
#define HANDLER 435
#define HAVING 436
#define HEADER_P 437
#define HOLD 438
#define HOUR_P 439
#define HOURS_P 440
#define IDENTITY_P 441
#define IF_P 442
#define IGNORE_P 443
#define ILIKE 444
#define IMMEDIATE 445
#define IMMUTABLE 446
#define IMPLICIT_P 447
#define IMPORT_P 448
#define IN_P 449
#define INCLUDE_P 450
#define INCLUDING 451
#define INCREMENT 452
#define INDEX 453
#define INDEXES 454
#define INHERIT 455
#define INHERITS 456
#define INITIALLY 457
#define INLINE_P 458
#define INNER_P 459
#define INOUT 460
#define INPUT_P 461
#define INSENSITIVE 462
#define INSERT 463
#define INSTALL 464
#define INSTEAD 465
#define INT_P 466
#define INTEGER 467
#define INTERSECT 468
#define INTERVAL 469
#define INTO 470
#define INVOKER 471
#define IS 472
#define ISNULL 473
#define ISOLATION 474
#define JOIN 475
#define JSON 476
#define KEY 477
#define LABEL 478
#define LANGUAGE 479
#define LARGE_P 480
#define LAST_P 481
#define LATERAL_P 482
#define LEADING 483
#define LEAKPROOF 484
#define LEFT 485
#define LEVEL 486
#define LIKE 487
#define LIMIT 488
#define LISTEN 489
#define LOAD 490
#define LOCAL 491
#define LOCATION 492
#define LOCK_P 493
#define LOCKED 494
#define LOGGED 495
#define MACRO 496
#define MAP 497
#define MAPPING 498
#define MATCH 499
#define MATERIALIZED 500
#define MAXVALUE 501
#define METHOD 502
#define MICROSECOND_P 503
#define MICROSECONDS_P 504
#define MILLISECOND_P 505
#define MILLISECONDS_P 506
#define MINUTE_P 507
#define MINUTES_P 508
#define MINVALUE 509
#define MODE 510
#define MONTH_P 511
#define MONTHS_P 512
#define MOVE 513
#define NAME_P 514
#define NAMES 515
#define NATIONAL 516
#define NATURAL 517
#define NCHAR 518
#define NEW 519
#define NEXT 520
#define NO 521
#define NONE 522
#define NOT 523
#define NOTHING 524
#define NOTIFY 525
#define NOTNULL 526
#define NOWAIT 527
#define NULL_P 528
#define NULLIF 529
#define NULLS_P 530
#define NUMERIC 531
#define OBJECT_P 532
#define OF 533
#define OFF 534
#define OFFSET 535
#define OIDS 536
#define OLD 537
#define ON 538
#define ONLY 539
#define OPERATOR 540
#define OPTION 541
#define OPTIONS 542
#define OR 543
#define ORDER 544
#define ORDINALITY 545
#define OTHERS 546
#define OUT_P 547
#define OUTER_P 548
#define OVER 549
#define OVERLAPS 550
#define OVERLAY 551
#define OVERRIDING 552
#define OWNED 553
#define OWNER 554
#define PARALLEL 555
#define PARSER 556
#define PARTIAL 557
#define PARTITION 558
#define PASSING 559
#define PASSWORD 560
#define PERCENT 561
#define PIVOT 562
#define PIVOT_LONGER 563
#define PIVOT_WIDER 564
#define PLACING 565
#define PLANS 566
#define POLICY 567
#define POSITION 568
#define POSITIONAL 569
#define PRAGMA_P 570
#define PRECEDING 571
#define PRECISION 572
#define PREPARE 573
#define PREPARED 574
#define PRESERVE 575
#define PRIMARY 576
#define PRIOR 577
#define PRIVILEGES 578
#define PROCEDURAL 579
#define PROCEDURE 580
#define PROGRAM 581
#define PUBLICATION 582
#define QUALIFY 583
#define QUOTE 584
#define RANGE 585
#define READ_P 586
#define REAL 587
#define REASSIGN 588
#define RECHECK 589
#define RECURSIVE 590
#define REF 591
#define REFERENCES 592
#define REFERENCING 593
#define REFRESH 594
#define REINDEX 595
#define RELATIVE_P 596
#define RELEASE 597
#define RENAME 598
#define REPEATABLE 599
#define REPLACE 600
#define REPLICA 601
#define RESET 602
#define RESPECT_P 603
#define RESTART 604
#define RESTRICT 605
#define RETURNING 606
#define RETURNS 607
#define REVOKE 608
#define RIGHT 609
#define ROLE 610
#define ROLLBACK 611
#define ROLLUP 612
#define ROW 613
#define ROWS 614
#define RULE 615
#define SAMPLE 616
#define SAVEPOINT 617
#define SCHEMA 618
#define SCHEMAS 619
#define SCROLL 620
#define SEARCH 621
#define SECOND_P 622
#define SECONDS_P 623
#define SECURITY 624
#define SELECT 625
#define SEMI 626
#define SEQUENCE 627
#define SEQUENCES 628
#define SERIALIZABLE 629
#define SERVER 630
#define SESSION 631
#define SET 632
#define SETOF 633
#define SETS 634
#define SHARE 635
#define SHOW 636
#define SIMILAR 637
#define SIMPLE 638
#define SKIP 639
#define SMALLINT 640
#define SNAPSHOT 641
#define SOME 642
#define SQL_P 643
#define STABLE 644
#define STANDALONE_P 645
#define START 646
#define STATEMENT 647
#define STATISTICS 648
#define STDIN 649
#define STDOUT 650
#define STORAGE 651
#define STORED 652
#define STRICT_P 653
#define STRIP_P 654
#define STRUCT 655
#define SUBSCRIPTION 656
#define SUBSTRING 657
#define SUMMARIZE 658
#define SYMMETRIC 659
#define SYSID 660
#define SYSTEM_P 661
#define TABLE 662
#define TABLES 663
#define TABLESAMPLE 664
#define TABLESPACE 665
#define TEMP 666
#define TEMPLATE 667
#define TEMPORARY 668
#define TEXT_P 669
#define THEN 670
#define TIES 671
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
#define UNPIVOT 695
#define UNTIL 696
#define UPDATE 697
#define USE_P 698
#define USER 699
#define USING 700
#define VACUUM 701
#define VALID 702
#define VALIDATE 703
#define VALIDATOR 704
#define VALUE_P 705
#define VALUES 706
#define VARCHAR 707
#define VARIADIC 708
#define VARYING 709
#define VERBOSE 710
#define VERSION_P 711
#define VIEW 712
#define VIEWS 713
#define VIRTUAL 714
#define VOLATILE 715
#define WHEN 716
#define WHERE 717
#define WHITESPACE_P 718
#define WINDOW 719
#define WITH 720
#define WITHIN 721
#define WITHOUT 722
#define WORK 723
#define WRAPPER 724
#define WRITE_P 725
#define XML_P 726
#define XMLATTRIBUTES 727
#define XMLCONCAT 728
#define XMLELEMENT 729
#define XMLEXISTS 730
#define XMLFOREST 731
#define XMLNAMESPACES 732
#define XMLPARSE 733
#define XMLPI 734
#define XMLROOT 735
#define XMLSERIALIZE 736
#define XMLTABLE 737
#define YEAR_P 738
#define YEARS_P 739
#define YES_P 740
#define ZONE 741
#define NOT_LA 742
#define NULLS_LA 743
#define WITH_LA 744
#define POSTFIXOP 745
#define UMINUS 746




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
	PGCTEMaterialize			ctematerialize;
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
	PGInsertColumnOrder bynameorposition;
}
/* Line 1529 of yacc.c.  */
#line 1079 "third_party/libpg_query/grammar/grammar_out.hpp"
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


