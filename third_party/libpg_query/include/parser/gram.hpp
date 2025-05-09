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
     SINGLE_ARROW = 272,
     DOUBLE_ARROW = 273,
     SINGLE_COLON = 274,
     LESS_EQUALS = 275,
     GREATER_EQUALS = 276,
     NOT_EQUALS = 277,
     ABORT_P = 278,
     ABSOLUTE_P = 279,
     ACCESS = 280,
     ACTION = 281,
     ADD_P = 282,
     ADMIN = 283,
     AFTER = 284,
     AGGREGATE = 285,
     ALL = 286,
     ALSO = 287,
     ALTER = 288,
     ALWAYS = 289,
     ANALYSE = 290,
     ANALYZE = 291,
     AND = 292,
     ANTI = 293,
     ANY = 294,
     ARRAY = 295,
     AS = 296,
     ASC_P = 297,
     ASOF = 298,
     ASSERTION = 299,
     ASSIGNMENT = 300,
     ASYMMETRIC = 301,
     AT = 302,
     ATTACH = 303,
     ATTRIBUTE = 304,
     AUTHORIZATION = 305,
     BACKWARD = 306,
     BEFORE = 307,
     BEGIN_P = 308,
     BETWEEN = 309,
     BIGINT = 310,
     BINARY = 311,
     BIT = 312,
     BOOLEAN_P = 313,
     BOTH = 314,
     BY = 315,
     CACHE = 316,
     CALL_P = 317,
     CALLED = 318,
     CASCADE = 319,
     CASCADED = 320,
     CASE = 321,
     CAST = 322,
     CATALOG_P = 323,
     CENTURIES_P = 324,
     CENTURY_P = 325,
     CHAIN = 326,
     CHAR_P = 327,
     CHARACTER = 328,
     CHARACTERISTICS = 329,
     CHECK_P = 330,
     CHECKPOINT = 331,
     CLASS = 332,
     CLOSE = 333,
     CLUSTER = 334,
     COALESCE = 335,
     COLLATE = 336,
     COLLATION = 337,
     COLUMN = 338,
     COLUMNS = 339,
     COMMENT = 340,
     COMMENTS = 341,
     COMMIT = 342,
     COMMITTED = 343,
     COMPRESSION = 344,
     CONCURRENTLY = 345,
     CONFIGURATION = 346,
     CONFLICT = 347,
     CONNECTION = 348,
     CONSTRAINT = 349,
     CONSTRAINTS = 350,
     CONTENT_P = 351,
     CONTINUE_P = 352,
     CONVERSION_P = 353,
     COPY = 354,
     COST = 355,
     CREATE_P = 356,
     CROSS = 357,
     CSV = 358,
     CUBE = 359,
     CURRENT_P = 360,
     CURSOR = 361,
     CYCLE = 362,
     DATA_P = 363,
     DATABASE = 364,
     DAY_P = 365,
     DAYS_P = 366,
     DEALLOCATE = 367,
     DEC = 368,
     DECADE_P = 369,
     DECADES_P = 370,
     DECIMAL_P = 371,
     DECLARE = 372,
     DEFAULT = 373,
     DEFAULTS = 374,
     DEFERRABLE = 375,
     DEFERRED = 376,
     DEFINER = 377,
     DELETE_P = 378,
     DELIMITER = 379,
     DELIMITERS = 380,
     DEPENDS = 381,
     DESC_P = 382,
     DESCRIBE = 383,
     DETACH = 384,
     DICTIONARY = 385,
     DISABLE_P = 386,
     DISCARD = 387,
     DISTINCT = 388,
     DO = 389,
     DOCUMENT_P = 390,
     DOMAIN_P = 391,
     DOUBLE_P = 392,
     DROP = 393,
     EACH = 394,
     ELSE = 395,
     ENABLE_P = 396,
     ENCODING = 397,
     ENCRYPTED = 398,
     END_P = 399,
     ENUM_P = 400,
     ESCAPE = 401,
     EVENT = 402,
     EXCEPT = 403,
     EXCLUDE = 404,
     EXCLUDING = 405,
     EXCLUSIVE = 406,
     EXECUTE = 407,
     EXISTS = 408,
     EXPLAIN = 409,
     EXPORT_P = 410,
     EXPORT_STATE = 411,
     EXTENSION = 412,
     EXTENSIONS = 413,
     EXTERNAL = 414,
     EXTRACT = 415,
     FALSE_P = 416,
     FAMILY = 417,
     FETCH = 418,
     FILTER = 419,
     FIRST_P = 420,
     FLOAT_P = 421,
     FOLLOWING = 422,
     FOR = 423,
     FORCE = 424,
     FOREIGN = 425,
     FORWARD = 426,
     FREEZE = 427,
     FROM = 428,
     FULL = 429,
     FUNCTION = 430,
     FUNCTIONS = 431,
     GENERATED = 432,
     GLOB = 433,
     GLOBAL = 434,
     GRANT = 435,
     GRANTED = 436,
     GROUP_P = 437,
     GROUPING = 438,
     GROUPING_ID = 439,
     GROUPS = 440,
     HANDLER = 441,
     HAVING = 442,
     HEADER_P = 443,
     HOLD = 444,
     HOUR_P = 445,
     HOURS_P = 446,
     IDENTITY_P = 447,
     IF_P = 448,
     IGNORE_P = 449,
     ILIKE = 450,
     IMMEDIATE = 451,
     IMMUTABLE = 452,
     IMPLICIT_P = 453,
     IMPORT_P = 454,
     IN_P = 455,
     INCLUDE_P = 456,
     INCLUDING = 457,
     INCREMENT = 458,
     INDEX = 459,
     INDEXES = 460,
     INHERIT = 461,
     INHERITS = 462,
     INITIALLY = 463,
     INLINE_P = 464,
     INNER_P = 465,
     INOUT = 466,
     INPUT_P = 467,
     INSENSITIVE = 468,
     INSERT = 469,
     INSTALL = 470,
     INSTEAD = 471,
     INT_P = 472,
     INTEGER = 473,
     INTERSECT = 474,
     INTERVAL = 475,
     INTO = 476,
     INVOKER = 477,
     IS = 478,
     ISNULL = 479,
     ISOLATION = 480,
     JOIN = 481,
     JSON = 482,
     KEY = 483,
     LABEL = 484,
     LAMBDA = 485,
     LANGUAGE = 486,
     LARGE_P = 487,
     LAST_P = 488,
     LATERAL_P = 489,
     LEADING = 490,
     LEAKPROOF = 491,
     LEFT = 492,
     LEVEL = 493,
     LIKE = 494,
     LIMIT = 495,
     LISTEN = 496,
     LOAD = 497,
     LOCAL = 498,
     LOCATION = 499,
     LOCK_P = 500,
     LOCKED = 501,
     LOGGED = 502,
     MACRO = 503,
     MAP = 504,
     MAPPING = 505,
     MATCH = 506,
     MATERIALIZED = 507,
     MAXVALUE = 508,
     METHOD = 509,
     MICROSECOND_P = 510,
     MICROSECONDS_P = 511,
     MILLENNIA_P = 512,
     MILLENNIUM_P = 513,
     MILLISECOND_P = 514,
     MILLISECONDS_P = 515,
     MINUTE_P = 516,
     MINUTES_P = 517,
     MINVALUE = 518,
     MODE = 519,
     MONTH_P = 520,
     MONTHS_P = 521,
     MOVE = 522,
     NAME_P = 523,
     NAMES = 524,
     NATIONAL = 525,
     NATURAL = 526,
     NCHAR = 527,
     NEW = 528,
     NEXT = 529,
     NO = 530,
     NONE = 531,
     NOT = 532,
     NOTHING = 533,
     NOTIFY = 534,
     NOTNULL = 535,
     NOWAIT = 536,
     NULL_P = 537,
     NULLIF = 538,
     NULLS_P = 539,
     NUMERIC = 540,
     OBJECT_P = 541,
     OF = 542,
     OFF = 543,
     OFFSET = 544,
     OIDS = 545,
     OLD = 546,
     ON = 547,
     ONLY = 548,
     OPERATOR = 549,
     OPTION = 550,
     OPTIONS = 551,
     OR = 552,
     ORDER = 553,
     ORDINALITY = 554,
     OTHERS = 555,
     OUT_P = 556,
     OUTER_P = 557,
     OVER = 558,
     OVERLAPS = 559,
     OVERLAY = 560,
     OVERRIDING = 561,
     OWNED = 562,
     OWNER = 563,
     PARALLEL = 564,
     PARSER = 565,
     PARTIAL = 566,
     PARTITION = 567,
     PARTITIONED = 568,
     PASSING = 569,
     PASSWORD = 570,
     PERCENT = 571,
     PERSISTENT = 572,
     PIVOT = 573,
     PIVOT_LONGER = 574,
     PIVOT_WIDER = 575,
     PLACING = 576,
     PLANS = 577,
     POLICY = 578,
     POSITION = 579,
     POSITIONAL = 580,
     PRAGMA_P = 581,
     PRECEDING = 582,
     PRECISION = 583,
     PREPARE = 584,
     PREPARED = 585,
     PRESERVE = 586,
     PRIMARY = 587,
     PRIOR = 588,
     PRIVILEGES = 589,
     PROCEDURAL = 590,
     PROCEDURE = 591,
     PROGRAM = 592,
     PUBLICATION = 593,
     QUALIFY = 594,
     QUARTER_P = 595,
     QUARTERS_P = 596,
     QUOTE = 597,
     RANGE = 598,
     READ_P = 599,
     REAL = 600,
     REASSIGN = 601,
     RECHECK = 602,
     RECURSIVE = 603,
     REF = 604,
     REFERENCES = 605,
     REFERENCING = 606,
     REFRESH = 607,
     REINDEX = 608,
     RELATIVE_P = 609,
     RELEASE = 610,
     RENAME = 611,
     REPEATABLE = 612,
     REPLACE = 613,
     REPLICA = 614,
     RESET = 615,
     RESPECT_P = 616,
     RESTART = 617,
     RESTRICT = 618,
     RETURNING = 619,
     RETURNS = 620,
     REVOKE = 621,
     RIGHT = 622,
     ROLE = 623,
     ROLLBACK = 624,
     ROLLUP = 625,
     ROW = 626,
     ROWS = 627,
     RULE = 628,
     SAMPLE = 629,
     SAVEPOINT = 630,
     SCHEMA = 631,
     SCHEMAS = 632,
     SCOPE = 633,
     SCROLL = 634,
     SEARCH = 635,
     SECOND_P = 636,
     SECONDS_P = 637,
     SECRET = 638,
     SECURITY = 639,
     SELECT = 640,
     SEMI = 641,
     SEQUENCE = 642,
     SEQUENCES = 643,
     SERIALIZABLE = 644,
     SERVER = 645,
     SESSION = 646,
     SET = 647,
     SETOF = 648,
     SETS = 649,
     SHARE = 650,
     SHOW = 651,
     SIMILAR = 652,
     SIMPLE = 653,
     SKIP = 654,
     SMALLINT = 655,
     SNAPSHOT = 656,
     SOME = 657,
     SORTED = 658,
     SQL_P = 659,
     STABLE = 660,
     STANDALONE_P = 661,
     START = 662,
     STATEMENT = 663,
     STATISTICS = 664,
     STDIN = 665,
     STDOUT = 666,
     STORAGE = 667,
     STORED = 668,
     STRICT_P = 669,
     STRIP_P = 670,
     STRUCT = 671,
     SUBSCRIPTION = 672,
     SUBSTRING = 673,
     SUMMARIZE = 674,
     SYMMETRIC = 675,
     SYSID = 676,
     SYSTEM_P = 677,
     TABLE = 678,
     TABLES = 679,
     TABLESAMPLE = 680,
     TABLESPACE = 681,
     TEMP = 682,
     TEMPLATE = 683,
     TEMPORARY = 684,
     TEXT_P = 685,
     THEN = 686,
     TIES = 687,
     TIME = 688,
     TIMESTAMP = 689,
     TO = 690,
     TRAILING = 691,
     TRANSACTION = 692,
     TRANSFORM = 693,
     TREAT = 694,
     TRIGGER = 695,
     TRIM = 696,
     TRUE_P = 697,
     TRUNCATE = 698,
     TRUSTED = 699,
     TRY_CAST = 700,
     TYPE_P = 701,
     TYPES_P = 702,
     UNBOUNDED = 703,
     UNCOMMITTED = 704,
     UNENCRYPTED = 705,
     UNION = 706,
     UNIQUE = 707,
     UNKNOWN = 708,
     UNLISTEN = 709,
     UNLOGGED = 710,
     UNPACK = 711,
     UNPIVOT = 712,
     UNTIL = 713,
     UPDATE = 714,
     USE_P = 715,
     USER = 716,
     USING = 717,
     VACUUM = 718,
     VALID = 719,
     VALIDATE = 720,
     VALIDATOR = 721,
     VALUE_P = 722,
     VALUES = 723,
     VARCHAR = 724,
     VARIABLE_P = 725,
     VARIADIC = 726,
     VARYING = 727,
     VERBOSE = 728,
     VERSION_P = 729,
     VIEW = 730,
     VIEWS = 731,
     VIRTUAL = 732,
     VOLATILE = 733,
     WEEK_P = 734,
     WEEKS_P = 735,
     WHEN = 736,
     WHERE = 737,
     WHITESPACE_P = 738,
     WINDOW = 739,
     WITH = 740,
     WITHIN = 741,
     WITHOUT = 742,
     WORK = 743,
     WRAPPER = 744,
     WRITE_P = 745,
     XML_P = 746,
     XMLATTRIBUTES = 747,
     XMLCONCAT = 748,
     XMLELEMENT = 749,
     XMLEXISTS = 750,
     XMLFOREST = 751,
     XMLNAMESPACES = 752,
     XMLPARSE = 753,
     XMLPI = 754,
     XMLROOT = 755,
     XMLSERIALIZE = 756,
     XMLTABLE = 757,
     YEAR_P = 758,
     YEARS_P = 759,
     YES_P = 760,
     ZONE = 761,
     NOT_LA = 762,
     NULLS_LA = 763,
     WITH_LA = 764,
     POSTFIXOP = 765,
     UMINUS = 766
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
#define SINGLE_ARROW 272
#define DOUBLE_ARROW 273
#define SINGLE_COLON 274
#define LESS_EQUALS 275
#define GREATER_EQUALS 276
#define NOT_EQUALS 277
#define ABORT_P 278
#define ABSOLUTE_P 279
#define ACCESS 280
#define ACTION 281
#define ADD_P 282
#define ADMIN 283
#define AFTER 284
#define AGGREGATE 285
#define ALL 286
#define ALSO 287
#define ALTER 288
#define ALWAYS 289
#define ANALYSE 290
#define ANALYZE 291
#define AND 292
#define ANTI 293
#define ANY 294
#define ARRAY 295
#define AS 296
#define ASC_P 297
#define ASOF 298
#define ASSERTION 299
#define ASSIGNMENT 300
#define ASYMMETRIC 301
#define AT 302
#define ATTACH 303
#define ATTRIBUTE 304
#define AUTHORIZATION 305
#define BACKWARD 306
#define BEFORE 307
#define BEGIN_P 308
#define BETWEEN 309
#define BIGINT 310
#define BINARY 311
#define BIT 312
#define BOOLEAN_P 313
#define BOTH 314
#define BY 315
#define CACHE 316
#define CALL_P 317
#define CALLED 318
#define CASCADE 319
#define CASCADED 320
#define CASE 321
#define CAST 322
#define CATALOG_P 323
#define CENTURIES_P 324
#define CENTURY_P 325
#define CHAIN 326
#define CHAR_P 327
#define CHARACTER 328
#define CHARACTERISTICS 329
#define CHECK_P 330
#define CHECKPOINT 331
#define CLASS 332
#define CLOSE 333
#define CLUSTER 334
#define COALESCE 335
#define COLLATE 336
#define COLLATION 337
#define COLUMN 338
#define COLUMNS 339
#define COMMENT 340
#define COMMENTS 341
#define COMMIT 342
#define COMMITTED 343
#define COMPRESSION 344
#define CONCURRENTLY 345
#define CONFIGURATION 346
#define CONFLICT 347
#define CONNECTION 348
#define CONSTRAINT 349
#define CONSTRAINTS 350
#define CONTENT_P 351
#define CONTINUE_P 352
#define CONVERSION_P 353
#define COPY 354
#define COST 355
#define CREATE_P 356
#define CROSS 357
#define CSV 358
#define CUBE 359
#define CURRENT_P 360
#define CURSOR 361
#define CYCLE 362
#define DATA_P 363
#define DATABASE 364
#define DAY_P 365
#define DAYS_P 366
#define DEALLOCATE 367
#define DEC 368
#define DECADE_P 369
#define DECADES_P 370
#define DECIMAL_P 371
#define DECLARE 372
#define DEFAULT 373
#define DEFAULTS 374
#define DEFERRABLE 375
#define DEFERRED 376
#define DEFINER 377
#define DELETE_P 378
#define DELIMITER 379
#define DELIMITERS 380
#define DEPENDS 381
#define DESC_P 382
#define DESCRIBE 383
#define DETACH 384
#define DICTIONARY 385
#define DISABLE_P 386
#define DISCARD 387
#define DISTINCT 388
#define DO 389
#define DOCUMENT_P 390
#define DOMAIN_P 391
#define DOUBLE_P 392
#define DROP 393
#define EACH 394
#define ELSE 395
#define ENABLE_P 396
#define ENCODING 397
#define ENCRYPTED 398
#define END_P 399
#define ENUM_P 400
#define ESCAPE 401
#define EVENT 402
#define EXCEPT 403
#define EXCLUDE 404
#define EXCLUDING 405
#define EXCLUSIVE 406
#define EXECUTE 407
#define EXISTS 408
#define EXPLAIN 409
#define EXPORT_P 410
#define EXPORT_STATE 411
#define EXTENSION 412
#define EXTENSIONS 413
#define EXTERNAL 414
#define EXTRACT 415
#define FALSE_P 416
#define FAMILY 417
#define FETCH 418
#define FILTER 419
#define FIRST_P 420
#define FLOAT_P 421
#define FOLLOWING 422
#define FOR 423
#define FORCE 424
#define FOREIGN 425
#define FORWARD 426
#define FREEZE 427
#define FROM 428
#define FULL 429
#define FUNCTION 430
#define FUNCTIONS 431
#define GENERATED 432
#define GLOB 433
#define GLOBAL 434
#define GRANT 435
#define GRANTED 436
#define GROUP_P 437
#define GROUPING 438
#define GROUPING_ID 439
#define GROUPS 440
#define HANDLER 441
#define HAVING 442
#define HEADER_P 443
#define HOLD 444
#define HOUR_P 445
#define HOURS_P 446
#define IDENTITY_P 447
#define IF_P 448
#define IGNORE_P 449
#define ILIKE 450
#define IMMEDIATE 451
#define IMMUTABLE 452
#define IMPLICIT_P 453
#define IMPORT_P 454
#define IN_P 455
#define INCLUDE_P 456
#define INCLUDING 457
#define INCREMENT 458
#define INDEX 459
#define INDEXES 460
#define INHERIT 461
#define INHERITS 462
#define INITIALLY 463
#define INLINE_P 464
#define INNER_P 465
#define INOUT 466
#define INPUT_P 467
#define INSENSITIVE 468
#define INSERT 469
#define INSTALL 470
#define INSTEAD 471
#define INT_P 472
#define INTEGER 473
#define INTERSECT 474
#define INTERVAL 475
#define INTO 476
#define INVOKER 477
#define IS 478
#define ISNULL 479
#define ISOLATION 480
#define JOIN 481
#define JSON 482
#define KEY 483
#define LABEL 484
#define LAMBDA 485
#define LANGUAGE 486
#define LARGE_P 487
#define LAST_P 488
#define LATERAL_P 489
#define LEADING 490
#define LEAKPROOF 491
#define LEFT 492
#define LEVEL 493
#define LIKE 494
#define LIMIT 495
#define LISTEN 496
#define LOAD 497
#define LOCAL 498
#define LOCATION 499
#define LOCK_P 500
#define LOCKED 501
#define LOGGED 502
#define MACRO 503
#define MAP 504
#define MAPPING 505
#define MATCH 506
#define MATERIALIZED 507
#define MAXVALUE 508
#define METHOD 509
#define MICROSECOND_P 510
#define MICROSECONDS_P 511
#define MILLENNIA_P 512
#define MILLENNIUM_P 513
#define MILLISECOND_P 514
#define MILLISECONDS_P 515
#define MINUTE_P 516
#define MINUTES_P 517
#define MINVALUE 518
#define MODE 519
#define MONTH_P 520
#define MONTHS_P 521
#define MOVE 522
#define NAME_P 523
#define NAMES 524
#define NATIONAL 525
#define NATURAL 526
#define NCHAR 527
#define NEW 528
#define NEXT 529
#define NO 530
#define NONE 531
#define NOT 532
#define NOTHING 533
#define NOTIFY 534
#define NOTNULL 535
#define NOWAIT 536
#define NULL_P 537
#define NULLIF 538
#define NULLS_P 539
#define NUMERIC 540
#define OBJECT_P 541
#define OF 542
#define OFF 543
#define OFFSET 544
#define OIDS 545
#define OLD 546
#define ON 547
#define ONLY 548
#define OPERATOR 549
#define OPTION 550
#define OPTIONS 551
#define OR 552
#define ORDER 553
#define ORDINALITY 554
#define OTHERS 555
#define OUT_P 556
#define OUTER_P 557
#define OVER 558
#define OVERLAPS 559
#define OVERLAY 560
#define OVERRIDING 561
#define OWNED 562
#define OWNER 563
#define PARALLEL 564
#define PARSER 565
#define PARTIAL 566
#define PARTITION 567
#define PARTITIONED 568
#define PASSING 569
#define PASSWORD 570
#define PERCENT 571
#define PERSISTENT 572
#define PIVOT 573
#define PIVOT_LONGER 574
#define PIVOT_WIDER 575
#define PLACING 576
#define PLANS 577
#define POLICY 578
#define POSITION 579
#define POSITIONAL 580
#define PRAGMA_P 581
#define PRECEDING 582
#define PRECISION 583
#define PREPARE 584
#define PREPARED 585
#define PRESERVE 586
#define PRIMARY 587
#define PRIOR 588
#define PRIVILEGES 589
#define PROCEDURAL 590
#define PROCEDURE 591
#define PROGRAM 592
#define PUBLICATION 593
#define QUALIFY 594
#define QUARTER_P 595
#define QUARTERS_P 596
#define QUOTE 597
#define RANGE 598
#define READ_P 599
#define REAL 600
#define REASSIGN 601
#define RECHECK 602
#define RECURSIVE 603
#define REF 604
#define REFERENCES 605
#define REFERENCING 606
#define REFRESH 607
#define REINDEX 608
#define RELATIVE_P 609
#define RELEASE 610
#define RENAME 611
#define REPEATABLE 612
#define REPLACE 613
#define REPLICA 614
#define RESET 615
#define RESPECT_P 616
#define RESTART 617
#define RESTRICT 618
#define RETURNING 619
#define RETURNS 620
#define REVOKE 621
#define RIGHT 622
#define ROLE 623
#define ROLLBACK 624
#define ROLLUP 625
#define ROW 626
#define ROWS 627
#define RULE 628
#define SAMPLE 629
#define SAVEPOINT 630
#define SCHEMA 631
#define SCHEMAS 632
#define SCOPE 633
#define SCROLL 634
#define SEARCH 635
#define SECOND_P 636
#define SECONDS_P 637
#define SECRET 638
#define SECURITY 639
#define SELECT 640
#define SEMI 641
#define SEQUENCE 642
#define SEQUENCES 643
#define SERIALIZABLE 644
#define SERVER 645
#define SESSION 646
#define SET 647
#define SETOF 648
#define SETS 649
#define SHARE 650
#define SHOW 651
#define SIMILAR 652
#define SIMPLE 653
#define SKIP 654
#define SMALLINT 655
#define SNAPSHOT 656
#define SOME 657
#define SORTED 658
#define SQL_P 659
#define STABLE 660
#define STANDALONE_P 661
#define START 662
#define STATEMENT 663
#define STATISTICS 664
#define STDIN 665
#define STDOUT 666
#define STORAGE 667
#define STORED 668
#define STRICT_P 669
#define STRIP_P 670
#define STRUCT 671
#define SUBSCRIPTION 672
#define SUBSTRING 673
#define SUMMARIZE 674
#define SYMMETRIC 675
#define SYSID 676
#define SYSTEM_P 677
#define TABLE 678
#define TABLES 679
#define TABLESAMPLE 680
#define TABLESPACE 681
#define TEMP 682
#define TEMPLATE 683
#define TEMPORARY 684
#define TEXT_P 685
#define THEN 686
#define TIES 687
#define TIME 688
#define TIMESTAMP 689
#define TO 690
#define TRAILING 691
#define TRANSACTION 692
#define TRANSFORM 693
#define TREAT 694
#define TRIGGER 695
#define TRIM 696
#define TRUE_P 697
#define TRUNCATE 698
#define TRUSTED 699
#define TRY_CAST 700
#define TYPE_P 701
#define TYPES_P 702
#define UNBOUNDED 703
#define UNCOMMITTED 704
#define UNENCRYPTED 705
#define UNION 706
#define UNIQUE 707
#define UNKNOWN 708
#define UNLISTEN 709
#define UNLOGGED 710
#define UNPACK 711
#define UNPIVOT 712
#define UNTIL 713
#define UPDATE 714
#define USE_P 715
#define USER 716
#define USING 717
#define VACUUM 718
#define VALID 719
#define VALIDATE 720
#define VALIDATOR 721
#define VALUE_P 722
#define VALUES 723
#define VARCHAR 724
#define VARIABLE_P 725
#define VARIADIC 726
#define VARYING 727
#define VERBOSE 728
#define VERSION_P 729
#define VIEW 730
#define VIEWS 731
#define VIRTUAL 732
#define VOLATILE 733
#define WEEK_P 734
#define WEEKS_P 735
#define WHEN 736
#define WHERE 737
#define WHITESPACE_P 738
#define WINDOW 739
#define WITH 740
#define WITHIN 741
#define WITHOUT 742
#define WORK 743
#define WRAPPER 744
#define WRITE_P 745
#define XML_P 746
#define XMLATTRIBUTES 747
#define XMLCONCAT 748
#define XMLELEMENT 749
#define XMLEXISTS 750
#define XMLFOREST 751
#define XMLNAMESPACES 752
#define XMLPARSE 753
#define XMLPI 754
#define XMLROOT 755
#define XMLSERIALIZE 756
#define XMLTABLE 757
#define YEAR_P 758
#define YEARS_P 759
#define YES_P 760
#define ZONE 761
#define NOT_LA 762
#define NULLS_LA 763
#define WITH_LA 764
#define POSTFIXOP 765
#define UMINUS 766




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
	PGIgnoreNulls          ignorenulls;
	PGConstrType           constr;
	PGLockClauseStrength lockstrength;
	PGLockWaitPolicy lockwaitpolicy;
	PGSubLinkType subquerytype;
	PGViewCheckOption viewcheckoption;
	PGInsertColumnOrder bynameorposition;
	PGLoadInstallType loadinstalltype;
	PGTransactionStmtType transactiontype;
}
/* Line 1529 of yacc.c.  */
#line 1122 "third_party/libpg_query/grammar/grammar_out.hpp"
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


