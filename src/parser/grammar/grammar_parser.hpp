// A Bison parser, made by GNU Bison 3.4.2.

// Skeleton interface for Bison LALR(1) parsers in C++

// Copyright (C) 2002-2015, 2018-2019 Free Software Foundation, Inc.

// This program is free software: you can redistribute it and/or modify
// it under the terms of the GNU General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.

// This program is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU General Public License for more details.

// You should have received a copy of the GNU General Public License
// along with this program.  If not, see <http://www.gnu.org/licenses/>.

// As a special exception, you may create a larger work that contains
// part or all of the Bison parser skeleton and distribute that work
// under terms of your choice, so long as that work isn't itself a
// parser generator using the skeleton or a modified version thereof
// as a parser skeleton.  Alternatively, if you modify or redistribute
// the parser skeleton itself, you may (at your option) remove this
// special exception, which will cause the skeleton and the resulting
// Bison output files to be licensed under the GNU General Public
// License without this special exception.

// This special exception was added by the Free Software Foundation in
// version 2.2 of Bison.


/**
 ** \file src/parser/grammar/grammar_parser.hpp
 ** Define the yy::parser class.
 */

// C++ LALR(1) parser skeleton written by Akim Demaille.

// Undocumented macros, especially those whose name start with YY_,
// are private implementation details.  Do not rely on them.

#ifndef YY_YY_SRC_PARSER_GRAMMAR_GRAMMAR_PARSER_HPP_INCLUDED
# define YY_YY_SRC_PARSER_GRAMMAR_GRAMMAR_PARSER_HPP_INCLUDED


# include <cstdlib> // std::abort
# include <iostream>
# include <stdexcept>
# include <string>
# include <vector>

#if defined __cplusplus
# define YY_CPLUSPLUS __cplusplus
#else
# define YY_CPLUSPLUS 199711L
#endif

// Support move semantics when possible.
#if 201103L <= YY_CPLUSPLUS
# define YY_MOVE           std::move
# define YY_MOVE_OR_COPY   move
# define YY_MOVE_REF(Type) Type&&
# define YY_RVREF(Type)    Type&&
# define YY_COPY(Type)     Type
#else
# define YY_MOVE
# define YY_MOVE_OR_COPY   copy
# define YY_MOVE_REF(Type) Type&
# define YY_RVREF(Type)    const Type&
# define YY_COPY(Type)     const Type&
#endif

// Support noexcept when possible.
#if 201103L <= YY_CPLUSPLUS
# define YY_NOEXCEPT noexcept
# define YY_NOTHROW
#else
# define YY_NOEXCEPT
# define YY_NOTHROW throw ()
#endif

// Support constexpr when possible.
#if 201703 <= YY_CPLUSPLUS
# define YY_CONSTEXPR constexpr
#else
# define YY_CONSTEXPR
#endif
# include "location.hh"


#ifndef YY_ATTRIBUTE
# if (defined __GNUC__                                               \
      && (2 < __GNUC__ || (__GNUC__ == 2 && 96 <= __GNUC_MINOR__)))  \
     || defined __SUNPRO_C && 0x5110 <= __SUNPRO_C
#  define YY_ATTRIBUTE(Spec) __attribute__(Spec)
# else
#  define YY_ATTRIBUTE(Spec) /* empty */
# endif
#endif

#ifndef YY_ATTRIBUTE_PURE
# define YY_ATTRIBUTE_PURE   YY_ATTRIBUTE ((__pure__))
#endif

#ifndef YY_ATTRIBUTE_UNUSED
# define YY_ATTRIBUTE_UNUSED YY_ATTRIBUTE ((__unused__))
#endif

/* Suppress unused-variable warnings by "using" E.  */
#if ! defined lint || defined __GNUC__
# define YYUSE(E) ((void) (E))
#else
# define YYUSE(E) /* empty */
#endif

#if defined __GNUC__ && ! defined __ICC && 407 <= __GNUC__ * 100 + __GNUC_MINOR__
/* Suppress an incorrect diagnostic about yylval being uninitialized.  */
# define YY_IGNORE_MAYBE_UNINITIALIZED_BEGIN \
    _Pragma ("GCC diagnostic push") \
    _Pragma ("GCC diagnostic ignored \"-Wuninitialized\"")\
    _Pragma ("GCC diagnostic ignored \"-Wmaybe-uninitialized\"")
# define YY_IGNORE_MAYBE_UNINITIALIZED_END \
    _Pragma ("GCC diagnostic pop")
#else
# define YY_INITIAL_VALUE(Value) Value
#endif
#ifndef YY_IGNORE_MAYBE_UNINITIALIZED_BEGIN
# define YY_IGNORE_MAYBE_UNINITIALIZED_BEGIN
# define YY_IGNORE_MAYBE_UNINITIALIZED_END
#endif
#ifndef YY_INITIAL_VALUE
# define YY_INITIAL_VALUE(Value) /* Nothing. */
#endif

# ifndef YY_NULLPTR
#  if defined __cplusplus
#   if 201103L <= __cplusplus
#    define YY_NULLPTR nullptr
#   else
#    define YY_NULLPTR 0
#   endif
#  else
#   define YY_NULLPTR ((void*)0)
#  endif
# endif

/* Debug traces.  */
#ifndef YYDEBUG
# define YYDEBUG 0
#endif

namespace yy {
#line 156 "src/parser/grammar/grammar_parser.hpp"




  /// A Bison parser.
  class parser
  {
  public:
#ifndef YYSTYPE
    /// Symbol semantic values.
    union semantic_type
    {
#line 127 "src/parser/grammar/main.y.tmp"

	core_YYSTYPE		core_yystype;
	/* these fields must match core_YYSTYPE: */
	int					ival;
	char				*str;
	const char			*keyword;

	char				chr;
	bool				boolean;
	JoinType			jtype;
	DropBehavior		dbehavior;
	OnCommitAction		oncommit;
	List				*list;
	Node				*node;
	Value				*value;
	ObjectType			objtype;
	TypeName			*typnam;
	FunctionParameter   *fun_param;
	FunctionParameterMode fun_param_mode;
	ObjectWithArgs		*objwithargs;
	DefElem				*defelt;
	SortBy				*sortby;
	WindowDef			*windef;
	JoinExpr			*jexpr;
	IndexElem			*ielem;
	Alias				*alias;
	RangeVar			*range;
	IntoClause			*into;
	WithClause			*with;
	InferClause			*infer;
	OnConflictClause	*onconflict;
	ResTarget			*target;
	InsertStmt			*istmt;
	VariableSetStmt		*vsetstmt;

#line 205 "src/parser/grammar/grammar_parser.hpp"

    };
#else
    typedef YYSTYPE semantic_type;
#endif
    /// Symbol locations.
    typedef location location_type;

    /// Syntax errors thrown from user actions.
    struct syntax_error : std::runtime_error
    {
      syntax_error (const location_type& l, const std::string& m)
        : std::runtime_error (m)
        , location (l)
      {}

      syntax_error (const syntax_error& s)
        : std::runtime_error (s.what ())
        , location (s.location)
      {}

      ~syntax_error () YY_NOEXCEPT YY_NOTHROW;

      location_type location;
    };

    /// Tokens.
    struct token
    {
      enum yytokentype
      {
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
        ASC = 291,
        ASSERTION = 292,
        ASSIGNMENT = 293,
        ASYMMETRIC = 294,
        AT = 295,
        ATTACH = 296,
        ATTRIBUTE = 297,
        BACKWARD = 298,
        BEFORE = 299,
        BEGIN_P = 300,
        BETWEEN = 301,
        BIGINT = 302,
        BINARY = 303,
        BIT = 304,
        BOOLEAN_P = 305,
        BOTH = 306,
        BY = 307,
        CACHE = 308,
        CALL = 309,
        CALLED = 310,
        CASCADE = 311,
        CASCADED = 312,
        CASE = 313,
        CAST = 314,
        CATALOG_P = 315,
        CHAIN = 316,
        CHARACTER = 317,
        CHARACTERISTICS = 318,
        CHAR_P = 319,
        CHECK = 320,
        CHECKPOINT = 321,
        CLASS = 322,
        CLOSE = 323,
        CLUSTER = 324,
        COALESCE = 325,
        COLLATE = 326,
        COLLATION = 327,
        COLUMN = 328,
        COLUMNS = 329,
        COMMIT = 330,
        COMMITTED = 331,
        CONCURRENTLY = 332,
        CONFIGURATION = 333,
        CONFLICT = 334,
        CONNECTION = 335,
        CONSTRAINT = 336,
        CONSTRAINTS = 337,
        CONTENT_P = 338,
        CONTINUE_P = 339,
        CONVERSION_P = 340,
        COPY = 341,
        COST = 342,
        CREATE = 343,
        CROSS = 344,
        CSV = 345,
        CUBE = 346,
        CURRENT_CATALOG = 347,
        CURRENT_DATE = 348,
        CURRENT_P = 349,
        CURRENT_ROLE = 350,
        CURRENT_SCHEMA = 351,
        CURRENT_TIME = 352,
        CURRENT_TIMESTAMP = 353,
        CURRENT_USER = 354,
        CURSOR = 355,
        CYCLE = 356,
        DATABASE = 357,
        DATA_P = 358,
        DAY_P = 359,
        DEALLOCATE = 360,
        DEC = 361,
        DECIMAL_P = 362,
        DECLARE = 363,
        DEFAULT = 364,
        DEFAULTS = 365,
        DEFERRABLE = 366,
        DEFERRED = 367,
        DEFINER = 368,
        DELETE_P = 369,
        DELIMITER = 370,
        DELIMITERS = 371,
        DEPENDS = 372,
        DESC = 373,
        DETACH = 374,
        DICTIONARY = 375,
        DISABLE_P = 376,
        DISCARD = 377,
        DISTINCT = 378,
        DO = 379,
        DOCUMENT_P = 380,
        DOMAIN_P = 381,
        DOUBLE_P = 382,
        DROP = 383,
        EACH = 384,
        ELSE = 385,
        ENABLE_P = 386,
        ENCODING = 387,
        ENCRYPTED = 388,
        END_P = 389,
        ENUM_P = 390,
        ESCAPE = 391,
        EVENT = 392,
        EXCEPT = 393,
        EXCLUDE = 394,
        EXCLUDING = 395,
        EXCLUSIVE = 396,
        EXECUTE = 397,
        EXISTS = 398,
        EXPLAIN = 399,
        EXTENSION = 400,
        EXTERNAL = 401,
        EXTRACT = 402,
        FALSE_P = 403,
        FAMILY = 404,
        FETCH = 405,
        FILTER = 406,
        FIRST_P = 407,
        FLOAT_P = 408,
        FOLLOWING = 409,
        FOR = 410,
        FORCE = 411,
        FOREIGN = 412,
        FORWARD = 413,
        FREEZE = 414,
        FROM = 415,
        FULL = 416,
        FUNCTION = 417,
        FUNCTIONS = 418,
        GENERATED = 419,
        GLOBAL = 420,
        GRANT = 421,
        GRANTED = 422,
        GREATEST = 423,
        GROUPING = 424,
        GROUPS = 425,
        GROUP_P = 426,
        HANDLER = 427,
        HAVING = 428,
        HEADER_P = 429,
        HOLD = 430,
        HOUR_P = 431,
        IDENTITY_P = 432,
        IF_P = 433,
        ILIKE = 434,
        IMMEDIATE = 435,
        IMMUTABLE = 436,
        IMPLICIT_P = 437,
        IMPORT_P = 438,
        INCLUDE = 439,
        INCLUDING = 440,
        INCREMENT = 441,
        INDEX = 442,
        INDEXES = 443,
        INHERIT = 444,
        INHERITS = 445,
        INITIALLY = 446,
        INLINE_P = 447,
        INNER_P = 448,
        INOUT = 449,
        INPUT_P = 450,
        INSENSITIVE = 451,
        INSERT = 452,
        INSTEAD = 453,
        INTEGER = 454,
        INTERSECT = 455,
        INTERVAL = 456,
        INTO = 457,
        INT_P = 458,
        INVOKER = 459,
        IN_P = 460,
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
        LEAST = 473,
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
        LOCKED = 484,
        LOCK_P = 485,
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
        NAMES = 497,
        NAME_P = 498,
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
        NULLIF = 511,
        NULLS_P = 512,
        NULL_P = 513,
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
        OTHERS = 529,
        OUTER_P = 530,
        OUT_P = 531,
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
        PROCEDURES = 558,
        PROGRAM = 559,
        PUBLICATION = 560,
        QUOTE = 561,
        RANGE = 562,
        READ = 563,
        REAL = 564,
        REASSIGN = 565,
        RECHECK = 566,
        RECURSIVE = 567,
        REF = 568,
        REFERENCES = 569,
        REFERENCING = 570,
        REFRESH = 571,
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
        ROUTINE = 588,
        ROUTINES = 589,
        ROW = 590,
        ROWS = 591,
        RULE = 592,
        SAVEPOINT = 593,
        SCHEMA = 594,
        SCHEMAS = 595,
        SCROLL = 596,
        SEARCH = 597,
        SECOND_P = 598,
        SELECT = 599,
        SEQUENCE = 600,
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
        STORED = 625,
        STRICT_P = 626,
        STRIP_P = 627,
        SUBSCRIPTION = 628,
        SUBSTRING = 629,
        SUPPORT = 630,
        SYMMETRIC = 631,
        SYSID = 632,
        SYSTEM_P = 633,
        TABLE = 634,
        TABLES = 635,
        TABLESAMPLE = 636,
        TEMP = 637,
        TEMPLATE = 638,
        TEMPORARY = 639,
        TEXT_P = 640,
        THEN = 641,
        TIES = 642,
        TIME = 643,
        TIMESTAMP = 644,
        TO = 645,
        TRAILING = 646,
        TRANSACTION = 647,
        TRANSFORM = 648,
        TREAT = 649,
        TRIM = 650,
        TRUE_P = 651,
        TRUNCATE = 652,
        TRUSTED = 653,
        TYPES_P = 654,
        TYPE_P = 655,
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
        VALUES = 672,
        VALUE_P = 673,
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
        WRITE = 691,
        YEAR_P = 692,
        YES_P = 693,
        ZONE = 694,
        NOT_LA = 695,
        NULLS_LA = 696,
        WITH_LA = 697,
        POSTFIXOP = 698,
        UMINUS = 699
      };
    };

    /// (External) token type, as returned by yylex.
    typedef token::yytokentype token_type;

    /// Symbol type: an internal symbol number.
    typedef int symbol_number_type;

    /// The symbol type number to denote an empty symbol.
    enum { empty_symbol = -2 };

    /// Internal symbol number for tokens (subsumed by symbol_number_type).
    typedef unsigned short token_number_type;

    /// A complete symbol.
    ///
    /// Expects its Base type to provide access to the symbol type
    /// via type_get ().
    ///
    /// Provide access to semantic value and location.
    template <typename Base>
    struct basic_symbol : Base
    {
      /// Alias to Base.
      typedef Base super_type;

      /// Default constructor.
      basic_symbol ()
        : value ()
        , location ()
      {}

#if 201103L <= YY_CPLUSPLUS
      /// Move constructor.
      basic_symbol (basic_symbol&& that);
#endif

      /// Copy constructor.
      basic_symbol (const basic_symbol& that);
      /// Constructor for valueless symbols.
      basic_symbol (typename Base::kind_type t,
                    YY_MOVE_REF (location_type) l);

      /// Constructor for symbols with semantic value.
      basic_symbol (typename Base::kind_type t,
                    YY_RVREF (semantic_type) v,
                    YY_RVREF (location_type) l);

      /// Destroy the symbol.
      ~basic_symbol ()
      {
        clear ();
      }

      /// Destroy contents, and record that is empty.
      void clear ()
      {
        Base::clear ();
      }

      /// Whether empty.
      bool empty () const YY_NOEXCEPT;

      /// Destructive move, \a s is emptied into this.
      void move (basic_symbol& s);

      /// The semantic value.
      semantic_type value;

      /// The location.
      location_type location;

    private:
#if YY_CPLUSPLUS < 201103L
      /// Assignment operator.
      basic_symbol& operator= (const basic_symbol& that);
#endif
    };

    /// Type access provider for token (enum) based symbols.
    struct by_type
    {
      /// Default constructor.
      by_type ();

#if 201103L <= YY_CPLUSPLUS
      /// Move constructor.
      by_type (by_type&& that);
#endif

      /// Copy constructor.
      by_type (const by_type& that);

      /// The symbol type as needed by the constructor.
      typedef token_type kind_type;

      /// Constructor from (external) token numbers.
      by_type (kind_type t);

      /// Record that this symbol is empty.
      void clear ();

      /// Steal the symbol type from \a that.
      void move (by_type& that);

      /// The (internal) type number (corresponding to \a type).
      /// \a empty when empty.
      symbol_number_type type_get () const YY_NOEXCEPT;

      /// The token.
      token_type token () const YY_NOEXCEPT;

      /// The symbol type.
      /// \a empty_symbol when empty.
      /// An int, not token_number_type, to be able to store empty_symbol.
      int type;
    };

    /// "External" symbols: returned by the scanner.
    struct symbol_type : basic_symbol<by_type>
    {};

    /// Build a parser object.
    parser (core_yyscan_t yyscanner_yyarg);
    virtual ~parser ();

    /// Parse.  An alias for parse ().
    /// \returns  0 iff parsing succeeded.
    int operator() ();

    /// Parse.
    /// \returns  0 iff parsing succeeded.
    virtual int parse ();

#if YYDEBUG
    /// The current debugging stream.
    std::ostream& debug_stream () const YY_ATTRIBUTE_PURE;
    /// Set the current debugging stream.
    void set_debug_stream (std::ostream &);

    /// Type for debugging levels.
    typedef int debug_level_type;
    /// The current debugging level.
    debug_level_type debug_level () const YY_ATTRIBUTE_PURE;
    /// Set the current debugging level.
    void set_debug_level (debug_level_type l);
#endif

    /// Report a syntax error.
    /// \param loc    where the syntax error is found.
    /// \param msg    a description of the syntax error.
    virtual void error (const location_type& loc, const std::string& msg);

    /// Report a syntax error.
    void error (const syntax_error& err);



  private:
    /// This class is not copyable.
    parser (const parser&);
    parser& operator= (const parser&);

    /// State numbers.
    typedef int state_type;

    /// Generate an error message.
    /// \param yystate   the state where the error occurred.
    /// \param yyla      the lookahead token.
    virtual std::string yysyntax_error_ (state_type yystate,
                                         const symbol_type& yyla) const;

    /// Compute post-reduction state.
    /// \param yystate   the current state
    /// \param yysym     the nonterminal to push on the stack
    state_type yy_lr_goto_state_ (state_type yystate, int yysym);

    /// Whether the given \c yypact_ value indicates a defaulted state.
    /// \param yyvalue   the value to check
    static bool yy_pact_value_is_default_ (int yyvalue);

    /// Whether the given \c yytable_ value indicates a syntax error.
    /// \param yyvalue   the value to check
    static bool yy_table_value_is_error_ (int yyvalue);

    static const short yypact_ninf_;
    static const short yytable_ninf_;

    /// Convert a scanner token number \a t to a symbol number.
    static token_number_type yytranslate_ (int t);

    // Tables.
  // YYPACT[STATE-NUM] -- Index in YYTABLE of the portion describing
  // STATE-NUM.
  static const int yypact_[];

  // YYDEFACT[STATE-NUM] -- Default reduction number in state STATE-NUM.
  // Performed when YYTABLE does not specify something else to do.  Zero
  // means the default is an error.
  static const unsigned short yydefact_[];

  // YYPGOTO[NTERM-NUM].
  static const short yypgoto_[];

  // YYDEFGOTO[NTERM-NUM].
  static const short yydefgoto_[];

  // YYTABLE[YYPACT[STATE-NUM]] -- What to do in state STATE-NUM.  If
  // positive, shift that token.  If negative, reduce the rule whose
  // number is the opposite.  If YYTABLE_NINF, syntax error.
  static const short yytable_[];

  static const short yycheck_[];

  // YYSTOS[STATE-NUM] -- The (internal number of the) accessing
  // symbol of state STATE-NUM.
  static const unsigned short yystos_[];

  // YYR1[YYN] -- Symbol number of symbol that rule YYN derives.
  static const unsigned short yyr1_[];

  // YYR2[YYN] -- Number of symbols on the right hand side of rule YYN.
  static const unsigned char yyr2_[];


#if YYDEBUG
    /// For a symbol, its name in clear.
    static const char* const yytname_[];

  // YYRLINE[YYN] -- Source line where rule number YYN was defined.
  static const unsigned short yyrline_[];
    /// Report on the debug stream that the rule \a r is going to be reduced.
    virtual void yy_reduce_print_ (int r);
    /// Print the state stack on the debug stream.
    virtual void yystack_print_ ();

    /// Debugging level.
    int yydebug_;
    /// Debug stream.
    std::ostream* yycdebug_;

    /// \brief Display a symbol type, value and location.
    /// \param yyo    The output stream.
    /// \param yysym  The symbol.
    template <typename Base>
    void yy_print_ (std::ostream& yyo, const basic_symbol<Base>& yysym) const;
#endif

    /// \brief Reclaim the memory associated to a symbol.
    /// \param yymsg     Why this token is reclaimed.
    ///                  If null, print nothing.
    /// \param yysym     The symbol.
    template <typename Base>
    void yy_destroy_ (const char* yymsg, basic_symbol<Base>& yysym) const;

  private:
    /// Type access provider for state based symbols.
    struct by_state
    {
      /// Default constructor.
      by_state () YY_NOEXCEPT;

      /// The symbol type as needed by the constructor.
      typedef state_type kind_type;

      /// Constructor.
      by_state (kind_type s) YY_NOEXCEPT;

      /// Copy constructor.
      by_state (const by_state& that) YY_NOEXCEPT;

      /// Record that this symbol is empty.
      void clear () YY_NOEXCEPT;

      /// Steal the symbol type from \a that.
      void move (by_state& that);

      /// The (internal) type number (corresponding to \a state).
      /// \a empty_symbol when empty.
      symbol_number_type type_get () const YY_NOEXCEPT;

      /// The state number used to denote an empty symbol.
      enum { empty_state = -1 };

      /// The state.
      /// \a empty when empty.
      state_type state;
    };

    /// "Internal" symbol: element of the stack.
    struct stack_symbol_type : basic_symbol<by_state>
    {
      /// Superclass.
      typedef basic_symbol<by_state> super_type;
      /// Construct an empty symbol.
      stack_symbol_type ();
      /// Move or copy construction.
      stack_symbol_type (YY_RVREF (stack_symbol_type) that);
      /// Steal the contents from \a sym to build this.
      stack_symbol_type (state_type s, YY_MOVE_REF (symbol_type) sym);
#if YY_CPLUSPLUS < 201103L
      /// Assignment, needed by push_back by some old implementations.
      /// Moves the contents of that.
      stack_symbol_type& operator= (stack_symbol_type& that);
#endif
    };

    /// A stack with random access from its top.
    template <typename T, typename S = std::vector<T> >
    class stack
    {
    public:
      // Hide our reversed order.
      typedef typename S::reverse_iterator iterator;
      typedef typename S::const_reverse_iterator const_iterator;
      typedef typename S::size_type size_type;

      stack (size_type n = 200)
        : seq_ (n)
      {}

      /// Random access.
      ///
      /// Index 0 returns the topmost element.
      T&
      operator[] (size_type i)
      {
        return seq_[size () - 1 - i];
      }

      /// Random access.
      ///
      /// Index 0 returns the topmost element.
      T&
      operator[] (int i)
      {
        return operator[] (size_type (i));
      }

      /// Random access.
      ///
      /// Index 0 returns the topmost element.
      const T&
      operator[] (size_type i) const
      {
        return seq_[size () - 1 - i];
      }

      /// Random access.
      ///
      /// Index 0 returns the topmost element.
      const T&
      operator[] (int i) const
      {
        return operator[] (size_type (i));
      }

      /// Steal the contents of \a t.
      ///
      /// Close to move-semantics.
      void
      push (YY_MOVE_REF (T) t)
      {
        seq_.push_back (T ());
        operator[] (0).move (t);
      }

      /// Pop elements from the stack.
      void
      pop (int n = 1) YY_NOEXCEPT
      {
        for (; 0 < n; --n)
          seq_.pop_back ();
      }

      /// Pop all elements from the stack.
      void
      clear () YY_NOEXCEPT
      {
        seq_.clear ();
      }

      /// Number of elements on the stack.
      size_type
      size () const YY_NOEXCEPT
      {
        return seq_.size ();
      }

      /// Iterator on top of the stack (going downwards).
      const_iterator
      begin () const YY_NOEXCEPT
      {
        return seq_.rbegin ();
      }

      /// Bottom of the stack.
      const_iterator
      end () const YY_NOEXCEPT
      {
        return seq_.rend ();
      }

      /// Present a slice of the top of a stack.
      class slice
      {
      public:
        slice (const stack& stack, int range)
          : stack_ (stack)
          , range_ (range)
        {}

        const T&
        operator[] (int i) const
        {
          return stack_[range_ - i];
        }

      private:
        const stack& stack_;
        int range_;
      };

    private:
      stack (const stack&);
      stack& operator= (const stack&);
      /// The wrapped container.
      S seq_;
    };


    /// Stack type.
    typedef stack<stack_symbol_type> stack_type;

    /// The stack.
    stack_type yystack_;

    /// Push a new state on the stack.
    /// \param m    a debug message to display
    ///             if null, no trace is output.
    /// \param sym  the symbol
    /// \warning the contents of \a s.value is stolen.
    void yypush_ (const char* m, YY_MOVE_REF (stack_symbol_type) sym);

    /// Push a new look ahead token on the state on the stack.
    /// \param m    a debug message to display
    ///             if null, no trace is output.
    /// \param s    the state
    /// \param sym  the symbol (for its value and location).
    /// \warning the contents of \a sym.value is stolen.
    void yypush_ (const char* m, state_type s, YY_MOVE_REF (symbol_type) sym);

    /// Pop \a n symbols from the stack.
    void yypop_ (int n = 1);

    /// Constants.
    enum
    {
      yyeof_ = 0,
      yylast_ = 44310,     ///< Last index in yytable_.
      yynnts_ = 344,  ///< Number of nonterminal symbols.
      yyfinal_ = 486, ///< Termination state number.
      yyterror_ = 1,
      yyerrcode_ = 256,
      yyntokens_ = 462  ///< Number of tokens.
    };


    // User arguments.
    core_yyscan_t yyscanner;
  };


} // yy
#line 1154 "src/parser/grammar/grammar_parser.hpp"





#endif // !YY_YY_SRC_PARSER_GRAMMAR_GRAMMAR_PARSER_HPP_INCLUDED
