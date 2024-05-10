//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/extension/inet/include/html_charref.hpp
//
//
//===----------------------------------------------------------------------===//

#include "duckdb/common/string_map_set.hpp"
namespace duckdb {

// see https://html.spec.whatwg.org/multipage/parsing.html#numeric-character-reference-end-state
const unordered_map<uint32_t, const char *> ReturnInvalidCharrefs() {
	return {
	    {0x80, "\u20ac"}, // EURO SIGN
	    {0x82, "\u201a"}, // SINGLE LOW-9 QUOTATION MARK
	    {0x83, "\u0192"}, // LATIN SMALL LETTER F WITH HOOK
	    {0x84, "\u201e"}, // DOUBLE LOW-9 QUOTATION MARK
	    {0x85, "\u2026"}, // HORIZONTAL ELLIPSIS
	    {0x86, "\u2020"}, // DAGGER
	    {0x87, "\u2021"}, // DOUBLE DAGGER
	    {0x88, "\u02c6"}, // MODIFIER LETTER CIRCUMFLEX ACCENT
	    {0x89, "\u2030"}, // PER MILLE SIGN
	    {0x8a, "\u0160"}, // LATIN CAPITAL LETTER S WITH CARON
	    {0x8b, "\u2039"}, // SINGLE LEFT-POINTING ANGLE QUOTATION MARK
	    {0x8c, "\u0152"}, // LATIN CAPITAL LIGATURE OE
	    {0x8e, "\u017d"}, // LATIN CAPITAL LETTER Z WITH CARON
	    {0x91, "\u2018"}, // LEFT SINGLE QUOTATION MARK
	    {0x92, "\u2019"}, // RIGHT SINGLE QUOTATION MARK
	    {0x93, "\u201c"}, // LEFT DOUBLE QUOTATION MARK
	    {0x94, "\u201d"}, // RIGHT DOUBLE QUOTATION MARK
	    {0x95, "\u2022"}, // BULLET
	    {0x96, "\u2013"}, // EN DASH
	    {0x97, "\u2014"}, // EM DASH
	    {0x98, "\u02dc"}, // SMALL TILDE
	    {0x99, "\u2122"}, // TRADE MARK SIGN
	    {0x9a, "\u0161"}, // LATIN SMALL LETTER S WITH CARON
	    {0x9b, "\u203a"}, // SINGLE RIGHT-POINTING ANGLE QUOTATION MARK
	    {0x9c, "\u0153"}, // LATIN SMALL LIGATURE OE
	    {0x9e, "\u017e"}, // LATIN SMALL LETTER Z WITH CARON
	    {0x9f, "\u0178"}  // LATIN CAPITAL LETTER Y WITH DIAERESIS
	};
}

const unordered_set<uint32_t> ReturnInvalidCodepoints() {
	return {// 0x0001 to 0x0008 - control characters
	        0x1, 0x2, 0x3, 0x4, 0x5, 0x6, 0x7, 0x8,
	        // 0x000E to 0x001F - control characters
	        0xe, 0xf, 0x10, 0x11, 0x12, 0x13, 0x14, 0x15, 0x16, 0x17, 0x18, 0x19, 0x1a, 0x1b, 0x1c, 0x1d, 0x1e, 0x1f,
	        // 0x007F to 0x009F
	        0x7f, 0x80, 0x81, 0x82, 0x83, 0x84, 0x85, 0x86, 0x87, 0x88, 0x89, 0x8a, 0x8b, 0x8c, 0x8d, 0x8e, 0x8f, 0x90,
	        0x91, 0x92, 0x93, 0x94, 0x95, 0x96, 0x97, 0x98, 0x99, 0x9a, 0x9b, 0x9c, 0x9d, 0x9e, 0x9f,
	        // 0xFDD0 to 0xFDEF - Unicode non-character code points which are reserved for internal use
	        0xfdd0, 0xfdd1, 0xfdd2, 0xfdd3, 0xfdd4, 0xfdd5, 0xfdd6, 0xfdd7, 0xfdd8, 0xfdd9, 0xfdda, 0xfddb, 0xfddc,
	        0xfddd, 0xfdde, 0xfddf, 0xfde0, 0xfde1, 0xfde2, 0xfde3, 0xfde4, 0xfde5, 0xfde6, 0xfde7, 0xfde8, 0xfde9,
	        0xfdea, 0xfdeb, 0xfdec, 0xfded, 0xfdee, 0xfdef,
	        // others - surrogate code points
	        0xb, 0xfffe, 0xffff, 0x1fffe, 0x1ffff, 0x2fffe, 0x2ffff, 0x3fffe, 0x3ffff, 0x4fffe, 0x4ffff, 0x5fffe,
	        0x5ffff, 0x6fffe, 0x6ffff, 0x7fffe, 0x7ffff, 0x8fffe, 0x8ffff, 0x9fffe, 0x9ffff, 0xafffe, 0xaffff, 0xbfffe,
	        0xbffff, 0xcfffe, 0xcffff, 0xdfffe, 0xdffff, 0xefffe, 0xeffff, 0xffffe, 0xfffff, 0x10fffe, 0x10ffff};
}

// HTML5 named character references
// see https://html.spec.whatwg.org/multipage/named-characters.html.
// This maps HTML5 named character references to the equivalent Unicode character(s).
// copied from https://html.spec.whatwg.org/entities.json
struct HTML5NameCharrefs {
	static const string_map_t<uint32_t> mapped_strings;

private:
	static vector<string> initialize_vector() {
		return {"AElig",
		        "AElig;",
		        "AMP",
		        "AMP;",
		        "Aacute",
		        "Aacute;",
		        "Abreve;",
		        "Acirc",
		        "Acirc;",
		        "Acy;",
		        "Afr;",
		        "Agrave",
		        "Agrave;",
		        "Alpha;",
		        "Amacr;",
		        "And;",
		        "Aogon;",
		        "Aopf;",
		        "ApplyFunction;",
		        "Aring",
		        "Aring;",
		        "Ascr;",
		        "Assign;",
		        "Atilde",
		        "Atilde;",
		        "Auml",
		        "Auml;",
		        "Backslash;",
		        "Barv;",
		        "Barwed;",
		        "Bcy;",
		        "Because;",
		        "Bernoullis;",
		        "Beta;",
		        "Bfr;",
		        "Bopf;",
		        "Breve;",
		        "Bscr;",
		        "Bumpeq;",
		        "CHcy;",
		        "COPY",
		        "COPY;",
		        "Cacute;",
		        "Cap;",
		        "CapitalDifferentialD;",
		        "Cayleys;",
		        "Ccaron;",
		        "Ccedil",
		        "Ccedil;",
		        "Ccirc;",
		        "Cconint;",
		        "Cdot;",
		        "Cedilla;",
		        "CenterDot;",
		        "Cfr;",
		        "Chi;",
		        "CircleDot;",
		        "CircleMinus;",
		        "CirclePlus;",
		        "CircleTimes;",
		        "ClockwiseContourIntegral;",
		        "CloseCurlyDoubleQuote;",
		        "CloseCurlyQuote;",
		        "Colon;",
		        "Colone;",
		        "Congruent;",
		        "Conint;",
		        "ContourIntegral;",
		        "Copf;",
		        "Coproduct;",
		        "CounterClockwiseContourIntegral;",
		        "Cross;",
		        "Cscr;",
		        "Cup;",
		        "CupCap;",
		        "DD;",
		        "DDotrahd;",
		        "DJcy;",
		        "DScy;",
		        "DZcy;",
		        "Dagger;",
		        "Darr;",
		        "Dashv;",
		        "Dcaron;",
		        "Dcy;",
		        "Del;",
		        "Delta;",
		        "Dfr;",
		        "DiacriticalAcute;",
		        "DiacriticalDot;",
		        "DiacriticalDoubleAcute;",
		        "DiacriticalGrave;",
		        "DiacriticalTilde;",
		        "Diamond;",
		        "DifferentialD;",
		        "Dopf;",
		        "Dot;",
		        "DotDot;",
		        "DotEqual;",
		        "DoubleContourIntegral;",
		        "DoubleDot;",
		        "DoubleDownArrow;",
		        "DoubleLeftArrow;",
		        "DoubleLeftRightArrow;",
		        "DoubleLeftTee;",
		        "DoubleLongLeftArrow;",
		        "DoubleLongLeftRightArrow;",
		        "DoubleLongRightArrow;",
		        "DoubleRightArrow;",
		        "DoubleRightTee;",
		        "DoubleUpArrow;",
		        "DoubleUpDownArrow;",
		        "DoubleVerticalBar;",
		        "DownArrow;",
		        "DownArrowBar;",
		        "DownArrowUpArrow;",
		        "DownBreve;",
		        "DownLeftRightVector;",
		        "DownLeftTeeVector;",
		        "DownLeftVector;",
		        "DownLeftVectorBar;",
		        "DownRightTeeVector;",
		        "DownRightVector;",
		        "DownRightVectorBar;",
		        "DownTee;",
		        "DownTeeArrow;",
		        "Downarrow;",
		        "Dscr;",
		        "Dstrok;",
		        "ENG;",
		        "ETH",
		        "ETH;",
		        "Eacute",
		        "Eacute;",
		        "Ecaron;",
		        "Ecirc",
		        "Ecirc;",
		        "Ecy;",
		        "Edot;",
		        "Efr;",
		        "Egrave",
		        "Egrave;",
		        "Element;",
		        "Emacr;",
		        "EmptySmallSquare;",
		        "EmptyVerySmallSquare;",
		        "Eogon;",
		        "Eopf;",
		        "Epsilon;",
		        "Equal;",
		        "EqualTilde;",
		        "Equilibrium;",
		        "Escr;",
		        "Esim;",
		        "Eta;",
		        "Euml",
		        "Euml;",
		        "Exists;",
		        "ExponentialE;",
		        "Fcy;",
		        "Ffr;",
		        "FilledSmallSquare;",
		        "FilledVerySmallSquare;",
		        "Fopf;",
		        "ForAll;",
		        "Fouriertrf;",
		        "Fscr;",
		        "GJcy;",
		        "GT",
		        "GT;",
		        "Gamma;",
		        "Gammad;",
		        "Gbreve;",
		        "Gcedil;",
		        "Gcirc;",
		        "Gcy;",
		        "Gdot;",
		        "Gfr;",
		        "Gg;",
		        "Gopf;",
		        "GreaterEqual;",
		        "GreaterEqualLess;",
		        "GreaterFullEqual;",
		        "GreaterGreater;",
		        "GreaterLess;",
		        "GreaterSlantEqual;",
		        "GreaterTilde;",
		        "Gscr;",
		        "Gt;",
		        "HARDcy;",
		        "Hacek;",
		        "Hat;",
		        "Hcirc;",
		        "Hfr;",
		        "HilbertSpace;",
		        "Hopf;",
		        "HorizontalLine;",
		        "Hscr;",
		        "Hstrok;",
		        "HumpDownHump;",
		        "HumpEqual;",
		        "IEcy;",
		        "IJlig;",
		        "IOcy;",
		        "Iacute",
		        "Iacute;",
		        "Icirc",
		        "Icirc;",
		        "Icy;",
		        "Idot;",
		        "Ifr;",
		        "Igrave",
		        "Igrave;",
		        "Im;",
		        "Imacr;",
		        "ImaginaryI;",
		        "Implies;",
		        "Int;",
		        "Integral;",
		        "Intersection;",
		        "InvisibleComma;",
		        "InvisibleTimes;",
		        "Iogon;",
		        "Iopf;",
		        "Iota;",
		        "Iscr;",
		        "Itilde;",
		        "Iukcy;",
		        "Iuml",
		        "Iuml;",
		        "Jcirc;",
		        "Jcy;",
		        "Jfr;",
		        "Jopf;",
		        "Jscr;",
		        "Jsercy;",
		        "Jukcy;",
		        "KHcy;",
		        "KJcy;",
		        "Kappa;",
		        "Kcedil;",
		        "Kcy;",
		        "Kfr;",
		        "Kopf;",
		        "Kscr;",
		        "LJcy;",
		        "LT",
		        "LT;",
		        "Lacute;",
		        "Lambda;",
		        "Lang;",
		        "Laplacetrf;",
		        "Larr;",
		        "Lcaron;",
		        "Lcedil;",
		        "Lcy;",
		        "LeftAngleBracket;",
		        "LeftArrow;",
		        "LeftArrowBar;",
		        "LeftArrowRightArrow;",
		        "LeftCeiling;",
		        "LeftDoubleBracket;",
		        "LeftDownTeeVector;",
		        "LeftDownVector;",
		        "LeftDownVectorBar;",
		        "LeftFloor;",
		        "LeftRightArrow;",
		        "LeftRightVector;",
		        "LeftTee;",
		        "LeftTeeArrow;",
		        "LeftTeeVector;",
		        "LeftTriangle;",
		        "LeftTriangleBar;",
		        "LeftTriangleEqual;",
		        "LeftUpDownVector;",
		        "LeftUpTeeVector;",
		        "LeftUpVector;",
		        "LeftUpVectorBar;",
		        "LeftVector;",
		        "LeftVectorBar;",
		        "Leftarrow;",
		        "Leftrightarrow;",
		        "LessEqualGreater;",
		        "LessFullEqual;",
		        "LessGreater;",
		        "LessLess;",
		        "LessSlantEqual;",
		        "LessTilde;",
		        "Lfr;",
		        "Ll;",
		        "Lleftarrow;",
		        "Lmidot;",
		        "LongLeftArrow;",
		        "LongLeftRightArrow;",
		        "LongRightArrow;",
		        "Longleftarrow;",
		        "Longleftrightarrow;",
		        "Longrightarrow;",
		        "Lopf;",
		        "LowerLeftArrow;",
		        "LowerRightArrow;",
		        "Lscr;",
		        "Lsh;",
		        "Lstrok;",
		        "Lt;",
		        "Map;",
		        "Mcy;",
		        "MediumSpace;",
		        "Mellintrf;",
		        "Mfr;",
		        "MinusPlus;",
		        "Mopf;",
		        "Mscr;",
		        "Mu;",
		        "NJcy;",
		        "Nacute;",
		        "Ncaron;",
		        "Ncedil;",
		        "Ncy;",
		        "NegativeMediumSpace;",
		        "NegativeThickSpace;",
		        "NegativeThinSpace;",
		        "NegativeVeryThinSpace;",
		        "NestedGreaterGreater;",
		        "NestedLessLess;",
		        "NewLine;",
		        "Nfr;",
		        "NoBreak;",
		        "NonBreakingSpace;",
		        "Nopf;",
		        "Not;",
		        "NotCongruent;",
		        "NotCupCap;",
		        "NotDoubleVerticalBar;",
		        "NotElement;",
		        "NotEqual;",
		        "NotEqualTilde;",
		        "NotExists;",
		        "NotGreater;",
		        "NotGreaterEqual;",
		        "NotGreaterFullEqual;",
		        "NotGreaterGreater;",
		        "NotGreaterLess;",
		        "NotGreaterSlantEqual;",
		        "NotGreaterTilde;",
		        "NotHumpDownHump;",
		        "NotHumpEqual;",
		        "NotLeftTriangle;",
		        "NotLeftTriangleBar;",
		        "NotLeftTriangleEqual;",
		        "NotLess;",
		        "NotLessEqual;",
		        "NotLessGreater;",
		        "NotLessLess;",
		        "NotLessSlantEqual;",
		        "NotLessTilde;",
		        "NotNestedGreaterGreater;",
		        "NotNestedLessLess;",
		        "NotPrecedes;",
		        "NotPrecedesEqual;",
		        "NotPrecedesSlantEqual;",
		        "NotReverseElement;",
		        "NotRightTriangle;",
		        "NotRightTriangleBar;",
		        "NotRightTriangleEqual;",
		        "NotSquareSubset;",
		        "NotSquareSubsetEqual;",
		        "NotSquareSuperset;",
		        "NotSquareSupersetEqual;",
		        "NotSubset;",
		        "NotSubsetEqual;",
		        "NotSucceeds;",
		        "NotSucceedsEqual;",
		        "NotSucceedsSlantEqual;",
		        "NotSucceedsTilde;",
		        "NotSuperset;",
		        "NotSupersetEqual;",
		        "NotTilde;",
		        "NotTildeEqual;",
		        "NotTildeFullEqual;",
		        "NotTildeTilde;",
		        "NotVerticalBar;",
		        "Nscr;",
		        "Ntilde",
		        "Ntilde;",
		        "Nu;",
		        "OElig;",
		        "Oacute",
		        "Oacute;",
		        "Ocirc",
		        "Ocirc;",
		        "Ocy;",
		        "Odblac;",
		        "Ofr;",
		        "Ograve",
		        "Ograve;",
		        "Omacr;",
		        "Omega;",
		        "Omicron;",
		        "Oopf;",
		        "OpenCurlyDoubleQuote;",
		        "OpenCurlyQuote;",
		        "Or;",
		        "Oscr;",
		        "Oslash",
		        "Oslash;",
		        "Otilde",
		        "Otilde;",
		        "Otimes;",
		        "Ouml",
		        "Ouml;",
		        "OverBar;",
		        "OverBrace;",
		        "OverBracket;",
		        "OverParenthesis;",
		        "PartialD;",
		        "Pcy;",
		        "Pfr;",
		        "Phi;",
		        "Pi;",
		        "PlusMinus;",
		        "Poincareplane;",
		        "Popf;",
		        "Pr;",
		        "Precedes;",
		        "PrecedesEqual;",
		        "PrecedesSlantEqual;",
		        "PrecedesTilde;",
		        "Prime;",
		        "Product;",
		        "Proportion;",
		        "Proportional;",
		        "Pscr;",
		        "Psi;",
		        "QUOT",
		        "QUOT;",
		        "Qfr;",
		        "Qopf;",
		        "Qscr;",
		        "RBarr;",
		        "REG",
		        "REG;",
		        "Racute;",
		        "Rang;",
		        "Rarr;",
		        "Rarrtl;",
		        "Rcaron;",
		        "Rcedil;",
		        "Rcy;",
		        "Re;",
		        "ReverseElement;",
		        "ReverseEquilibrium;",
		        "ReverseUpEquilibrium;",
		        "Rfr;",
		        "Rho;",
		        "RightAngleBracket;",
		        "RightArrow;",
		        "RightArrowBar;",
		        "RightArrowLeftArrow;",
		        "RightCeiling;",
		        "RightDoubleBracket;",
		        "RightDownTeeVector;",
		        "RightDownVector;",
		        "RightDownVectorBar;",
		        "RightFloor;",
		        "RightTee;",
		        "RightTeeArrow;",
		        "RightTeeVector;",
		        "RightTriangle;",
		        "RightTriangleBar;",
		        "RightTriangleEqual;",
		        "RightUpDownVector;",
		        "RightUpTeeVector;",
		        "RightUpVector;",
		        "RightUpVectorBar;",
		        "RightVector;",
		        "RightVectorBar;",
		        "Rightarrow;",
		        "Ropf;",
		        "RoundImplies;",
		        "Rrightarrow;",
		        "Rscr;",
		        "Rsh;",
		        "RuleDelayed;",
		        "SHCHcy;",
		        "SHcy;",
		        "SOFTcy;",
		        "Sacute;",
		        "Sc;",
		        "Scaron;",
		        "Scedil;",
		        "Scirc;",
		        "Scy;",
		        "Sfr;",
		        "ShortDownArrow;",
		        "ShortLeftArrow;",
		        "ShortRightArrow;",
		        "ShortUpArrow;",
		        "Sigma;",
		        "SmallCircle;",
		        "Sopf;",
		        "Sqrt;",
		        "Square;",
		        "SquareIntersection;",
		        "SquareSubset;",
		        "SquareSubsetEqual;",
		        "SquareSuperset;",
		        "SquareSupersetEqual;",
		        "SquareUnion;",
		        "Sscr;",
		        "Star;",
		        "Sub;",
		        "Subset;",
		        "SubsetEqual;",
		        "Succeeds;",
		        "SucceedsEqual;",
		        "SucceedsSlantEqual;",
		        "SucceedsTilde;",
		        "SuchThat;",
		        "Sum;",
		        "Sup;",
		        "Superset;",
		        "SupersetEqual;",
		        "Supset;",
		        "THORN",
		        "THORN;",
		        "TRADE;",
		        "TSHcy;",
		        "TScy;",
		        "Tab;",
		        "Tau;",
		        "Tcaron;",
		        "Tcedil;",
		        "Tcy;",
		        "Tfr;",
		        "Therefore;",
		        "Theta;",
		        "ThickSpace;",
		        "ThinSpace;",
		        "Tilde;",
		        "TildeEqual;",
		        "TildeFullEqual;",
		        "TildeTilde;",
		        "Topf;",
		        "TripleDot;",
		        "Tscr;",
		        "Tstrok;",
		        "Uacute",
		        "Uacute;",
		        "Uarr;",
		        "Uarrocir;",
		        "Ubrcy;",
		        "Ubreve;",
		        "Ucirc",
		        "Ucirc;",
		        "Ucy;",
		        "Udblac;",
		        "Ufr;",
		        "Ugrave",
		        "Ugrave;",
		        "Umacr;",
		        "UnderBar;",
		        "UnderBrace;",
		        "UnderBracket;",
		        "UnderParenthesis;",
		        "Union;",
		        "UnionPlus;",
		        "Uogon;",
		        "Uopf;",
		        "UpArrow;",
		        "UpArrowBar;",
		        "UpArrowDownArrow;",
		        "UpDownArrow;",
		        "UpEquilibrium;",
		        "UpTee;",
		        "UpTeeArrow;",
		        "Uparrow;",
		        "Updownarrow;",
		        "UpperLeftArrow;",
		        "UpperRightArrow;",
		        "Upsi;",
		        "Upsilon;",
		        "Uring;",
		        "Uscr;",
		        "Utilde;",
		        "Uuml",
		        "Uuml;",
		        "VDash;",
		        "Vbar;",
		        "Vcy;",
		        "Vdash;",
		        "Vdashl;",
		        "Vee;",
		        "Verbar;",
		        "Vert;",
		        "VerticalBar;",
		        "VerticalLine;",
		        "VerticalSeparator;",
		        "VerticalTilde;",
		        "VeryThinSpace;",
		        "Vfr;",
		        "Vopf;",
		        "Vscr;",
		        "Vvdash;",
		        "Wcirc;",
		        "Wedge;",
		        "Wfr;",
		        "Wopf;",
		        "Wscr;",
		        "Xfr;",
		        "Xi;",
		        "Xopf;",
		        "Xscr;",
		        "YAcy;",
		        "YIcy;",
		        "YUcy;",
		        "Yacute",
		        "Yacute;",
		        "Ycirc;",
		        "Ycy;",
		        "Yfr;",
		        "Yopf;",
		        "Yscr;",
		        "Yuml;",
		        "ZHcy;",
		        "Zacute;",
		        "Zcaron;",
		        "Zcy;",
		        "Zdot;",
		        "ZeroWidthSpace;",
		        "Zeta;",
		        "Zfr;",
		        "Zopf;",
		        "Zscr;",
		        "aacute",
		        "aacute;",
		        "abreve;",
		        "ac;",
		        "acE;",
		        "acd;",
		        "acirc",
		        "acirc;",
		        "acute",
		        "acute;",
		        "acy;",
		        "aelig",
		        "aelig;",
		        "af;",
		        "afr;",
		        "agrave",
		        "agrave;",
		        "alefsym;",
		        "aleph;",
		        "alpha;",
		        "amacr;",
		        "amalg;",
		        "amp",
		        "amp;",
		        "and;",
		        "andand;",
		        "andd;",
		        "andslope;",
		        "andv;",
		        "ang;",
		        "ange;",
		        "angle;",
		        "angmsd;",
		        "angmsdaa;",
		        "angmsdab;",
		        "angmsdac;",
		        "angmsdad;",
		        "angmsdae;",
		        "angmsdaf;",
		        "angmsdag;",
		        "angmsdah;",
		        "angrt;",
		        "angrtvb;",
		        "angrtvbd;",
		        "angsph;",
		        "angst;",
		        "angzarr;",
		        "aogon;",
		        "aopf;",
		        "ap;",
		        "apE;",
		        "apacir;",
		        "ape;",
		        "apid;",
		        "apos;",
		        "approx;",
		        "approxeq;",
		        "aring",
		        "aring;",
		        "ascr;",
		        "ast;",
		        "asymp;",
		        "asympeq;",
		        "atilde",
		        "atilde;",
		        "auml",
		        "auml;",
		        "awconint;",
		        "awint;",
		        "bNot;",
		        "backcong;",
		        "backepsilon;",
		        "backprime;",
		        "backsim;",
		        "backsimeq;",
		        "barvee;",
		        "barwed;",
		        "barwedge;",
		        "bbrk;",
		        "bbrktbrk;",
		        "bcong;",
		        "bcy;",
		        "bdquo;",
		        "becaus;",
		        "because;",
		        "bemptyv;",
		        "bepsi;",
		        "bernou;",
		        "beta;",
		        "beth;",
		        "between;",
		        "bfr;",
		        "bigcap;",
		        "bigcirc;",
		        "bigcup;",
		        "bigodot;",
		        "bigoplus;",
		        "bigotimes;",
		        "bigsqcup;",
		        "bigstar;",
		        "bigtriangledown;",
		        "bigtriangleup;",
		        "biguplus;",
		        "bigvee;",
		        "bigwedge;",
		        "bkarow;",
		        "blacklozenge;",
		        "blacksquare;",
		        "blacktriangle;",
		        "blacktriangledown;",
		        "blacktriangleleft;",
		        "blacktriangleright;",
		        "blank;",
		        "blk12;",
		        "blk14;",
		        "blk34;",
		        "block;",
		        "bne;",
		        "bnequiv;",
		        "bnot;",
		        "bopf;",
		        "bot;",
		        "bottom;",
		        "bowtie;",
		        "boxDL;",
		        "boxDR;",
		        "boxDl;",
		        "boxDr;",
		        "boxH;",
		        "boxHD;",
		        "boxHU;",
		        "boxHd;",
		        "boxHu;",
		        "boxUL;",
		        "boxUR;",
		        "boxUl;",
		        "boxUr;",
		        "boxV;",
		        "boxVH;",
		        "boxVL;",
		        "boxVR;",
		        "boxVh;",
		        "boxVl;",
		        "boxVr;",
		        "boxbox;",
		        "boxdL;",
		        "boxdR;",
		        "boxdl;",
		        "boxdr;",
		        "boxh;",
		        "boxhD;",
		        "boxhU;",
		        "boxhd;",
		        "boxhu;",
		        "boxminus;",
		        "boxplus;",
		        "boxtimes;",
		        "boxuL;",
		        "boxuR;",
		        "boxul;",
		        "boxur;",
		        "boxv;",
		        "boxvH;",
		        "boxvL;",
		        "boxvR;",
		        "boxvh;",
		        "boxvl;",
		        "boxvr;",
		        "bprime;",
		        "breve;",
		        "brvbar",
		        "brvbar;",
		        "bscr;",
		        "bsemi;",
		        "bsim;",
		        "bsime;",
		        "bsol;",
		        "bsolb;",
		        "bsolhsub;",
		        "bull;",
		        "bullet;",
		        "bump;",
		        "bumpE;",
		        "bumpe;",
		        "bumpeq;",
		        "cacute;",
		        "cap;",
		        "capand;",
		        "capbrcup;",
		        "capcap;",
		        "capcup;",
		        "capdot;",
		        "caps;",
		        "caret;",
		        "caron;",
		        "ccaps;",
		        "ccaron;",
		        "ccedil",
		        "ccedil;",
		        "ccirc;",
		        "ccups;",
		        "ccupssm;",
		        "cdot;",
		        "cedil",
		        "cedil;",
		        "cemptyv;",
		        "cent",
		        "cent;",
		        "centerdot;",
		        "cfr;",
		        "chcy;",
		        "check;",
		        "checkmark;",
		        "chi;",
		        "cir;",
		        "cirE;",
		        "circ;",
		        "circeq;",
		        "circlearrowleft;",
		        "circlearrowright;",
		        "circledR;",
		        "circledS;",
		        "circledast;",
		        "circledcirc;",
		        "circleddash;",
		        "cire;",
		        "cirfnint;",
		        "cirmid;",
		        "cirscir;",
		        "clubs;",
		        "clubsuit;",
		        "colon;",
		        "colone;",
		        "coloneq;",
		        "comma;",
		        "commat;",
		        "comp;",
		        "compfn;",
		        "complement;",
		        "complexes;",
		        "cong;",
		        "congdot;",
		        "conint;",
		        "copf;",
		        "coprod;",
		        "copy",
		        "copy;",
		        "copysr;",
		        "crarr;",
		        "cross;",
		        "cscr;",
		        "csub;",
		        "csube;",
		        "csup;",
		        "csupe;",
		        "ctdot;",
		        "cudarrl;",
		        "cudarrr;",
		        "cuepr;",
		        "cuesc;",
		        "cularr;",
		        "cularrp;",
		        "cup;",
		        "cupbrcap;",
		        "cupcap;",
		        "cupcup;",
		        "cupdot;",
		        "cupor;",
		        "cups;",
		        "curarr;",
		        "curarrm;",
		        "curlyeqprec;",
		        "curlyeqsucc;",
		        "curlyvee;",
		        "curlywedge;",
		        "curren",
		        "curren;",
		        "curvearrowleft;",
		        "curvearrowright;",
		        "cuvee;",
		        "cuwed;",
		        "cwconint;",
		        "cwint;",
		        "cylcty;",
		        "dArr;",
		        "dHar;",
		        "dagger;",
		        "daleth;",
		        "darr;",
		        "dash;",
		        "dashv;",
		        "dbkarow;",
		        "dblac;",
		        "dcaron;",
		        "dcy;",
		        "dd;",
		        "ddagger;",
		        "ddarr;",
		        "ddotseq;",
		        "deg",
		        "deg;",
		        "delta;",
		        "demptyv;",
		        "dfisht;",
		        "dfr;",
		        "dharl;",
		        "dharr;",
		        "diam;",
		        "diamond;",
		        "diamondsuit;",
		        "diams;",
		        "die;",
		        "digamma;",
		        "disin;",
		        "div;",
		        "divide",
		        "divide;",
		        "divideontimes;",
		        "divonx;",
		        "djcy;",
		        "dlcorn;",
		        "dlcrop;",
		        "dollar;",
		        "dopf;",
		        "dot;",
		        "doteq;",
		        "doteqdot;",
		        "dotminus;",
		        "dotplus;",
		        "dotsquare;",
		        "doublebarwedge;",
		        "downarrow;",
		        "downdownarrows;",
		        "downharpoonleft;",
		        "downharpoonright;",
		        "drbkarow;",
		        "drcorn;",
		        "drcrop;",
		        "dscr;",
		        "dscy;",
		        "dsol;",
		        "dstrok;",
		        "dtdot;",
		        "dtri;",
		        "dtrif;",
		        "duarr;",
		        "duhar;",
		        "dwangle;",
		        "dzcy;",
		        "dzigrarr;",
		        "eDDot;",
		        "eDot;",
		        "eacute",
		        "eacute;",
		        "easter;",
		        "ecaron;",
		        "ecir;",
		        "ecirc",
		        "ecirc;",
		        "ecolon;",
		        "ecy;",
		        "edot;",
		        "ee;",
		        "efDot;",
		        "efr;",
		        "eg;",
		        "egrave",
		        "egrave;",
		        "egs;",
		        "egsdot;",
		        "el;",
		        "elinters;",
		        "ell;",
		        "els;",
		        "elsdot;",
		        "emacr;",
		        "empty;",
		        "emptyset;",
		        "emptyv;",
		        "emsp13;",
		        "emsp14;",
		        "emsp;",
		        "eng;",
		        "ensp;",
		        "eogon;",
		        "eopf;",
		        "epar;",
		        "eparsl;",
		        "eplus;",
		        "epsi;",
		        "epsilon;",
		        "epsiv;",
		        "eqcirc;",
		        "eqcolon;",
		        "eqsim;",
		        "eqslantgtr;",
		        "eqslantless;",
		        "equals;",
		        "equest;",
		        "equiv;",
		        "equivDD;",
		        "eqvparsl;",
		        "erDot;",
		        "erarr;",
		        "escr;",
		        "esdot;",
		        "esim;",
		        "eta;",
		        "eth",
		        "eth;",
		        "euml",
		        "euml;",
		        "euro;",
		        "excl;",
		        "exist;",
		        "expectation;",
		        "exponentiale;",
		        "fallingdotseq;",
		        "fcy;",
		        "female;",
		        "ffilig;",
		        "fflig;",
		        "ffllig;",
		        "ffr;",
		        "filig;",
		        "fjlig;",
		        "flat;",
		        "fllig;",
		        "fltns;",
		        "fnof;",
		        "fopf;",
		        "forall;",
		        "fork;",
		        "forkv;",
		        "fpartint;",
		        "frac12",
		        "frac12;",
		        "frac13;",
		        "frac14",
		        "frac14;",
		        "frac15;",
		        "frac16;",
		        "frac18;",
		        "frac23;",
		        "frac25;",
		        "frac34",
		        "frac34;",
		        "frac35;",
		        "frac38;",
		        "frac45;",
		        "frac56;",
		        "frac58;",
		        "frac78;",
		        "frasl;",
		        "frown;",
		        "fscr;",
		        "gE;",
		        "gEl;",
		        "gacute;",
		        "gamma;",
		        "gammad;",
		        "gap;",
		        "gbreve;",
		        "gcirc;",
		        "gcy;",
		        "gdot;",
		        "ge;",
		        "gel;",
		        "geq;",
		        "geqq;",
		        "geqslant;",
		        "ges;",
		        "gescc;",
		        "gesdot;",
		        "gesdoto;",
		        "gesdotol;",
		        "gesl;",
		        "gesles;",
		        "gfr;",
		        "gg;",
		        "ggg;",
		        "gimel;",
		        "gjcy;",
		        "gl;",
		        "glE;",
		        "gla;",
		        "glj;",
		        "gnE;",
		        "gnap;",
		        "gnapprox;",
		        "gne;",
		        "gneq;",
		        "gneqq;",
		        "gnsim;",
		        "gopf;",
		        "grave;",
		        "gscr;",
		        "gsim;",
		        "gsime;",
		        "gsiml;",
		        "gt",
		        "gt;",
		        "gtcc;",
		        "gtcir;",
		        "gtdot;",
		        "gtlPar;",
		        "gtquest;",
		        "gtrapprox;",
		        "gtrarr;",
		        "gtrdot;",
		        "gtreqless;",
		        "gtreqqless;",
		        "gtrless;",
		        "gtrsim;",
		        "gvertneqq;",
		        "gvnE;",
		        "hArr;",
		        "hairsp;",
		        "half;",
		        "hamilt;",
		        "hardcy;",
		        "harr;",
		        "harrcir;",
		        "harrw;",
		        "hbar;",
		        "hcirc;",
		        "hearts;",
		        "heartsuit;",
		        "hellip;",
		        "hercon;",
		        "hfr;",
		        "hksearow;",
		        "hkswarow;",
		        "hoarr;",
		        "homtht;",
		        "hookleftarrow;",
		        "hookrightarrow;",
		        "hopf;",
		        "horbar;",
		        "hscr;",
		        "hslash;",
		        "hstrok;",
		        "hybull;",
		        "hyphen;",
		        "iacute",
		        "iacute;",
		        "ic;",
		        "icirc",
		        "icirc;",
		        "icy;",
		        "iecy;",
		        "iexcl",
		        "iexcl;",
		        "iff;",
		        "ifr;",
		        "igrave",
		        "igrave;",
		        "ii;",
		        "iiiint;",
		        "iiint;",
		        "iinfin;",
		        "iiota;",
		        "ijlig;",
		        "imacr;",
		        "image;",
		        "imagline;",
		        "imagpart;",
		        "imath;",
		        "imof;",
		        "imped;",
		        "in;",
		        "incare;",
		        "infin;",
		        "infintie;",
		        "inodot;",
		        "int;",
		        "intcal;",
		        "integers;",
		        "intercal;",
		        "intlarhk;",
		        "intprod;",
		        "iocy;",
		        "iogon;",
		        "iopf;",
		        "iota;",
		        "iprod;",
		        "iquest",
		        "iquest;",
		        "iscr;",
		        "isin;",
		        "isinE;",
		        "isindot;",
		        "isins;",
		        "isinsv;",
		        "isinv;",
		        "it;",
		        "itilde;",
		        "iukcy;",
		        "iuml",
		        "iuml;",
		        "jcirc;",
		        "jcy;",
		        "jfr;",
		        "jmath;",
		        "jopf;",
		        "jscr;",
		        "jsercy;",
		        "jukcy;",
		        "kappa;",
		        "kappav;",
		        "kcedil;",
		        "kcy;",
		        "kfr;",
		        "kgreen;",
		        "khcy;",
		        "kjcy;",
		        "kopf;",
		        "kscr;",
		        "lAarr;",
		        "lArr;",
		        "lAtail;",
		        "lBarr;",
		        "lE;",
		        "lEg;",
		        "lHar;",
		        "lacute;",
		        "laemptyv;",
		        "lagran;",
		        "lambda;",
		        "lang;",
		        "langd;",
		        "langle;",
		        "lap;",
		        "laquo",
		        "laquo;",
		        "larr;",
		        "larrb;",
		        "larrbfs;",
		        "larrfs;",
		        "larrhk;",
		        "larrlp;",
		        "larrpl;",
		        "larrsim;",
		        "larrtl;",
		        "lat;",
		        "latail;",
		        "late;",
		        "lates;",
		        "lbarr;",
		        "lbbrk;",
		        "lbrace;",
		        "lbrack;",
		        "lbrke;",
		        "lbrksld;",
		        "lbrkslu;",
		        "lcaron;",
		        "lcedil;",
		        "lceil;",
		        "lcub;",
		        "lcy;",
		        "ldca;",
		        "ldquo;",
		        "ldquor;",
		        "ldrdhar;",
		        "ldrushar;",
		        "ldsh;",
		        "le;",
		        "leftarrow;",
		        "leftarrowtail;",
		        "leftharpoondown;",
		        "leftharpoonup;",
		        "leftleftarrows;",
		        "leftrightarrow;",
		        "leftrightarrows;",
		        "leftrightharpoons;",
		        "leftrightsquigarrow;",
		        "leftthreetimes;",
		        "leg;",
		        "leq;",
		        "leqq;",
		        "leqslant;",
		        "les;",
		        "lescc;",
		        "lesdot;",
		        "lesdoto;",
		        "lesdotor;",
		        "lesg;",
		        "lesges;",
		        "lessapprox;",
		        "lessdot;",
		        "lesseqgtr;",
		        "lesseqqgtr;",
		        "lessgtr;",
		        "lesssim;",
		        "lfisht;",
		        "lfloor;",
		        "lfr;",
		        "lg;",
		        "lgE;",
		        "lhard;",
		        "lharu;",
		        "lharul;",
		        "lhblk;",
		        "ljcy;",
		        "ll;",
		        "llarr;",
		        "llcorner;",
		        "llhard;",
		        "lltri;",
		        "lmidot;",
		        "lmoust;",
		        "lmoustache;",
		        "lnE;",
		        "lnap;",
		        "lnapprox;",
		        "lne;",
		        "lneq;",
		        "lneqq;",
		        "lnsim;",
		        "loang;",
		        "loarr;",
		        "lobrk;",
		        "longleftarrow;",
		        "longleftrightarrow;",
		        "longmapsto;",
		        "longrightarrow;",
		        "looparrowleft;",
		        "looparrowright;",
		        "lopar;",
		        "lopf;",
		        "loplus;",
		        "lotimes;",
		        "lowast;",
		        "lowbar;",
		        "loz;",
		        "lozenge;",
		        "lozf;",
		        "lpar;",
		        "lparlt;",
		        "lrarr;",
		        "lrcorner;",
		        "lrhar;",
		        "lrhard;",
		        "lrm;",
		        "lrtri;",
		        "lsaquo;",
		        "lscr;",
		        "lsh;",
		        "lsim;",
		        "lsime;",
		        "lsimg;",
		        "lsqb;",
		        "lsquo;",
		        "lsquor;",
		        "lstrok;",
		        "lt",
		        "lt;",
		        "ltcc;",
		        "ltcir;",
		        "ltdot;",
		        "lthree;",
		        "ltimes;",
		        "ltlarr;",
		        "ltquest;",
		        "ltrPar;",
		        "ltri;",
		        "ltrie;",
		        "ltrif;",
		        "lurdshar;",
		        "luruhar;",
		        "lvertneqq;",
		        "lvnE;",
		        "mDDot;",
		        "macr",
		        "macr;",
		        "male;",
		        "malt;",
		        "maltese;",
		        "map;",
		        "mapsto;",
		        "mapstodown;",
		        "mapstoleft;",
		        "mapstoup;",
		        "marker;",
		        "mcomma;",
		        "mcy;",
		        "mdash;",
		        "measuredangle;",
		        "mfr;",
		        "mho;",
		        "micro",
		        "micro;",
		        "mid;",
		        "midast;",
		        "midcir;",
		        "middot",
		        "middot;",
		        "minus;",
		        "minusb;",
		        "minusd;",
		        "minusdu;",
		        "mlcp;",
		        "mldr;",
		        "mnplus;",
		        "models;",
		        "mopf;",
		        "mp;",
		        "mscr;",
		        "mstpos;",
		        "mu;",
		        "multimap;",
		        "mumap;",
		        "nGg;",
		        "nGt;",
		        "nGtv;",
		        "nLeftarrow;",
		        "nLeftrightarrow;",
		        "nLl;",
		        "nLt;",
		        "nLtv;",
		        "nRightarrow;",
		        "nVDash;",
		        "nVdash;",
		        "nabla;",
		        "nacute;",
		        "nang;",
		        "nap;",
		        "napE;",
		        "napid;",
		        "napos;",
		        "napprox;",
		        "natur;",
		        "natural;",
		        "naturals;",
		        "nbsp",
		        "nbsp;",
		        "nbump;",
		        "nbumpe;",
		        "ncap;",
		        "ncaron;",
		        "ncedil;",
		        "ncong;",
		        "ncongdot;",
		        "ncup;",
		        "ncy;",
		        "ndash;",
		        "ne;",
		        "neArr;",
		        "nearhk;",
		        "nearr;",
		        "nearrow;",
		        "nedot;",
		        "nequiv;",
		        "nesear;",
		        "nesim;",
		        "nexist;",
		        "nexists;",
		        "nfr;",
		        "ngE;",
		        "nge;",
		        "ngeq;",
		        "ngeqq;",
		        "ngeqslant;",
		        "nges;",
		        "ngsim;",
		        "ngt;",
		        "ngtr;",
		        "nhArr;",
		        "nharr;",
		        "nhpar;",
		        "ni;",
		        "nis;",
		        "nisd;",
		        "niv;",
		        "njcy;",
		        "nlArr;",
		        "nlE;",
		        "nlarr;",
		        "nldr;",
		        "nle;",
		        "nleftarrow;",
		        "nleftrightarrow;",
		        "nleq;",
		        "nleqq;",
		        "nleqslant;",
		        "nles;",
		        "nless;",
		        "nlsim;",
		        "nlt;",
		        "nltri;",
		        "nltrie;",
		        "nmid;",
		        "nopf;",
		        "not",
		        "not;",
		        "notin;",
		        "notinE;",
		        "notindot;",
		        "notinva;",
		        "notinvb;",
		        "notinvc;",
		        "notni;",
		        "notniva;",
		        "notnivb;",
		        "notnivc;",
		        "npar;",
		        "nparallel;",
		        "nparsl;",
		        "npart;",
		        "npolint;",
		        "npr;",
		        "nprcue;",
		        "npre;",
		        "nprec;",
		        "npreceq;",
		        "nrArr;",
		        "nrarr;",
		        "nrarrc;",
		        "nrarrw;",
		        "nrightarrow;",
		        "nrtri;",
		        "nrtrie;",
		        "nsc;",
		        "nsccue;",
		        "nsce;",
		        "nscr;",
		        "nshortmid;",
		        "nshortparallel;",
		        "nsim;",
		        "nsime;",
		        "nsimeq;",
		        "nsmid;",
		        "nspar;",
		        "nsqsube;",
		        "nsqsupe;",
		        "nsub;",
		        "nsubE;",
		        "nsube;",
		        "nsubset;",
		        "nsubseteq;",
		        "nsubseteqq;",
		        "nsucc;",
		        "nsucceq;",
		        "nsup;",
		        "nsupE;",
		        "nsupe;",
		        "nsupset;",
		        "nsupseteq;",
		        "nsupseteqq;",
		        "ntgl;",
		        "ntilde",
		        "ntilde;",
		        "ntlg;",
		        "ntriangleleft;",
		        "ntrianglelefteq;",
		        "ntriangleright;",
		        "ntrianglerighteq;",
		        "nu;",
		        "num;",
		        "numero;",
		        "numsp;",
		        "nvDash;",
		        "nvHarr;",
		        "nvap;",
		        "nvdash;",
		        "nvge;",
		        "nvgt;",
		        "nvinfin;",
		        "nvlArr;",
		        "nvle;",
		        "nvlt;",
		        "nvltrie;",
		        "nvrArr;",
		        "nvrtrie;",
		        "nvsim;",
		        "nwArr;",
		        "nwarhk;",
		        "nwarr;",
		        "nwarrow;",
		        "nwnear;",
		        "oS;",
		        "oacute",
		        "oacute;",
		        "oast;",
		        "ocir;",
		        "ocirc",
		        "ocirc;",
		        "ocy;",
		        "odash;",
		        "odblac;",
		        "odiv;",
		        "odot;",
		        "odsold;",
		        "oelig;",
		        "ofcir;",
		        "ofr;",
		        "ogon;",
		        "ograve",
		        "ograve;",
		        "ogt;",
		        "ohbar;",
		        "ohm;",
		        "oint;",
		        "olarr;",
		        "olcir;",
		        "olcross;",
		        "oline;",
		        "olt;",
		        "omacr;",
		        "omega;",
		        "omicron;",
		        "omid;",
		        "ominus;",
		        "oopf;",
		        "opar;",
		        "operp;",
		        "oplus;",
		        "or;",
		        "orarr;",
		        "ord;",
		        "order;",
		        "orderof;",
		        "ordf",
		        "ordf;",
		        "ordm",
		        "ordm;",
		        "origof;",
		        "oror;",
		        "orslope;",
		        "orv;",
		        "oscr;",
		        "oslash",
		        "oslash;",
		        "osol;",
		        "otilde",
		        "otilde;",
		        "otimes;",
		        "otimesas;",
		        "ouml",
		        "ouml;",
		        "ovbar;",
		        "par;",
		        "para",
		        "para;",
		        "parallel;",
		        "parsim;",
		        "parsl;",
		        "part;",
		        "pcy;",
		        "percnt;",
		        "period;",
		        "permil;",
		        "perp;",
		        "pertenk;",
		        "pfr;",
		        "phi;",
		        "phiv;",
		        "phmmat;",
		        "phone;",
		        "pi;",
		        "pitchfork;",
		        "piv;",
		        "planck;",
		        "planckh;",
		        "plankv;",
		        "plus;",
		        "plusacir;",
		        "plusb;",
		        "pluscir;",
		        "plusdo;",
		        "plusdu;",
		        "pluse;",
		        "plusmn",
		        "plusmn;",
		        "plussim;",
		        "plustwo;",
		        "pm;",
		        "pointint;",
		        "popf;",
		        "pound",
		        "pound;",
		        "pr;",
		        "prE;",
		        "prap;",
		        "prcue;",
		        "pre;",
		        "prec;",
		        "precapprox;",
		        "preccurlyeq;",
		        "preceq;",
		        "precnapprox;",
		        "precneqq;",
		        "precnsim;",
		        "precsim;",
		        "prime;",
		        "primes;",
		        "prnE;",
		        "prnap;",
		        "prnsim;",
		        "prod;",
		        "profalar;",
		        "profline;",
		        "profsurf;",
		        "prop;",
		        "propto;",
		        "prsim;",
		        "prurel;",
		        "pscr;",
		        "psi;",
		        "puncsp;",
		        "qfr;",
		        "qint;",
		        "qopf;",
		        "qprime;",
		        "qscr;",
		        "quaternions;",
		        "quatint;",
		        "quest;",
		        "questeq;",
		        "quot",
		        "quot;",
		        "rAarr;",
		        "rArr;",
		        "rAtail;",
		        "rBarr;",
		        "rHar;",
		        "race;",
		        "racute;",
		        "radic;",
		        "raemptyv;",
		        "rang;",
		        "rangd;",
		        "range;",
		        "rangle;",
		        "raquo",
		        "raquo;",
		        "rarr;",
		        "rarrap;",
		        "rarrb;",
		        "rarrbfs;",
		        "rarrc;",
		        "rarrfs;",
		        "rarrhk;",
		        "rarrlp;",
		        "rarrpl;",
		        "rarrsim;",
		        "rarrtl;",
		        "rarrw;",
		        "ratail;",
		        "ratio;",
		        "rationals;",
		        "rbarr;",
		        "rbbrk;",
		        "rbrace;",
		        "rbrack;",
		        "rbrke;",
		        "rbrksld;",
		        "rbrkslu;",
		        "rcaron;",
		        "rcedil;",
		        "rceil;",
		        "rcub;",
		        "rcy;",
		        "rdca;",
		        "rdldhar;",
		        "rdquo;",
		        "rdquor;",
		        "rdsh;",
		        "real;",
		        "realine;",
		        "realpart;",
		        "reals;",
		        "rect;",
		        "reg",
		        "reg;",
		        "rfisht;",
		        "rfloor;",
		        "rfr;",
		        "rhard;",
		        "rharu;",
		        "rharul;",
		        "rho;",
		        "rhov;",
		        "rightarrow;",
		        "rightarrowtail;",
		        "rightharpoondown;",
		        "rightharpoonup;",
		        "rightleftarrows;",
		        "rightleftharpoons;",
		        "rightrightarrows;",
		        "rightsquigarrow;",
		        "rightthreetimes;",
		        "ring;",
		        "risingdotseq;",
		        "rlarr;",
		        "rlhar;",
		        "rlm;",
		        "rmoust;",
		        "rmoustache;",
		        "rnmid;",
		        "roang;",
		        "roarr;",
		        "robrk;",
		        "ropar;",
		        "ropf;",
		        "roplus;",
		        "rotimes;",
		        "rpar;",
		        "rpargt;",
		        "rppolint;",
		        "rrarr;",
		        "rsaquo;",
		        "rscr;",
		        "rsh;",
		        "rsqb;",
		        "rsquo;",
		        "rsquor;",
		        "rthree;",
		        "rtimes;",
		        "rtri;",
		        "rtrie;",
		        "rtrif;",
		        "rtriltri;",
		        "ruluhar;",
		        "rx;",
		        "sacute;",
		        "sbquo;",
		        "sc;",
		        "scE;",
		        "scap;",
		        "scaron;",
		        "sccue;",
		        "sce;",
		        "scedil;",
		        "scirc;",
		        "scnE;",
		        "scnap;",
		        "scnsim;",
		        "scpolint;",
		        "scsim;",
		        "scy;",
		        "sdot;",
		        "sdotb;",
		        "sdote;",
		        "seArr;",
		        "searhk;",
		        "searr;",
		        "searrow;",
		        "sect",
		        "sect;",
		        "semi;",
		        "seswar;",
		        "setminus;",
		        "setmn;",
		        "sext;",
		        "sfr;",
		        "sfrown;",
		        "sharp;",
		        "shchcy;",
		        "shcy;",
		        "shortmid;",
		        "shortparallel;",
		        "shy",
		        "shy;",
		        "sigma;",
		        "sigmaf;",
		        "sigmav;",
		        "sim;",
		        "simdot;",
		        "sime;",
		        "simeq;",
		        "simg;",
		        "simgE;",
		        "siml;",
		        "simlE;",
		        "simne;",
		        "simplus;",
		        "simrarr;",
		        "slarr;",
		        "smallsetminus;",
		        "smashp;",
		        "smeparsl;",
		        "smid;",
		        "smile;",
		        "smt;",
		        "smte;",
		        "smtes;",
		        "softcy;",
		        "sol;",
		        "solb;",
		        "solbar;",
		        "sopf;",
		        "spades;",
		        "spadesuit;",
		        "spar;",
		        "sqcap;",
		        "sqcaps;",
		        "sqcup;",
		        "sqcups;",
		        "sqsub;",
		        "sqsube;",
		        "sqsubset;",
		        "sqsubseteq;",
		        "sqsup;",
		        "sqsupe;",
		        "sqsupset;",
		        "sqsupseteq;",
		        "squ;",
		        "square;",
		        "squarf;",
		        "squf;",
		        "srarr;",
		        "sscr;",
		        "ssetmn;",
		        "ssmile;",
		        "sstarf;",
		        "star;",
		        "starf;",
		        "straightepsilon;",
		        "straightphi;",
		        "strns;",
		        "sub;",
		        "subE;",
		        "subdot;",
		        "sube;",
		        "subedot;",
		        "submult;",
		        "subnE;",
		        "subne;",
		        "subplus;",
		        "subrarr;",
		        "subset;",
		        "subseteq;",
		        "subseteqq;",
		        "subsetneq;",
		        "subsetneqq;",
		        "subsim;",
		        "subsub;",
		        "subsup;",
		        "succ;",
		        "succapprox;",
		        "succcurlyeq;",
		        "succeq;",
		        "succnapprox;",
		        "succneqq;",
		        "succnsim;",
		        "succsim;",
		        "sum;",
		        "sung;",
		        "sup1",
		        "sup1;",
		        "sup2",
		        "sup2;",
		        "sup3",
		        "sup3;",
		        "sup;",
		        "supE;",
		        "supdot;",
		        "supdsub;",
		        "supe;",
		        "supedot;",
		        "suphsol;",
		        "suphsub;",
		        "suplarr;",
		        "supmult;",
		        "supnE;",
		        "supne;",
		        "supplus;",
		        "supset;",
		        "supseteq;",
		        "supseteqq;",
		        "supsetneq;",
		        "supsetneqq;",
		        "supsim;",
		        "supsub;",
		        "supsup;",
		        "swArr;",
		        "swarhk;",
		        "swarr;",
		        "swarrow;",
		        "swnwar;",
		        "szlig",
		        "szlig;",
		        "target;",
		        "tau;",
		        "tbrk;",
		        "tcaron;",
		        "tcedil;",
		        "tcy;",
		        "tdot;",
		        "telrec;",
		        "tfr;",
		        "there4;",
		        "therefore;",
		        "theta;",
		        "thetasym;",
		        "thetav;",
		        "thickapprox;",
		        "thicksim;",
		        "thinsp;",
		        "thkap;",
		        "thksim;",
		        "thorn",
		        "thorn;",
		        "tilde;",
		        "times",
		        "times;",
		        "timesb;",
		        "timesbar;",
		        "timesd;",
		        "tint;",
		        "toea;",
		        "top;",
		        "topbot;",
		        "topcir;",
		        "topf;",
		        "topfork;",
		        "tosa;",
		        "tprime;",
		        "trade;",
		        "triangle;",
		        "triangledown;",
		        "triangleleft;",
		        "trianglelefteq;",
		        "triangleq;",
		        "triangleright;",
		        "trianglerighteq;",
		        "tridot;",
		        "trie;",
		        "triminus;",
		        "triplus;",
		        "trisb;",
		        "tritime;",
		        "trpezium;",
		        "tscr;",
		        "tscy;",
		        "tshcy;",
		        "tstrok;",
		        "twixt;",
		        "twoheadleftarrow;",
		        "twoheadrightarrow;",
		        "uArr;",
		        "uHar;",
		        "uacute",
		        "uacute;",
		        "uarr;",
		        "ubrcy;",
		        "ubreve;",
		        "ucirc",
		        "ucirc;",
		        "ucy;",
		        "udarr;",
		        "udblac;",
		        "udhar;",
		        "ufisht;",
		        "ufr;",
		        "ugrave",
		        "ugrave;",
		        "uharl;",
		        "uharr;",
		        "uhblk;",
		        "ulcorn;",
		        "ulcorner;",
		        "ulcrop;",
		        "ultri;",
		        "umacr;",
		        "uml",
		        "uml;",
		        "uogon;",
		        "uopf;",
		        "uparrow;",
		        "updownarrow;",
		        "upharpoonleft;",
		        "upharpoonright;",
		        "uplus;",
		        "upsi;",
		        "upsih;",
		        "upsilon;",
		        "upuparrows;",
		        "urcorn;",
		        "urcorner;",
		        "urcrop;",
		        "uring;",
		        "urtri;",
		        "uscr;",
		        "utdot;",
		        "utilde;",
		        "utri;",
		        "utrif;",
		        "uuarr;",
		        "uuml",
		        "uuml;",
		        "uwangle;",
		        "vArr;",
		        "vBar;",
		        "vBarv;",
		        "vDash;",
		        "vangrt;",
		        "varepsilon;",
		        "varkappa;",
		        "varnothing;",
		        "varphi;",
		        "varpi;",
		        "varpropto;",
		        "varr;",
		        "varrho;",
		        "varsigma;",
		        "varsubsetneq;",
		        "varsubsetneqq;",
		        "varsupsetneq;",
		        "varsupsetneqq;",
		        "vartheta;",
		        "vartriangleleft;",
		        "vartriangleright;",
		        "vcy;",
		        "vdash;",
		        "vee;",
		        "veebar;",
		        "veeeq;",
		        "vellip;",
		        "verbar;",
		        "vert;",
		        "vfr;",
		        "vltri;",
		        "vnsub;",
		        "vnsup;",
		        "vopf;",
		        "vprop;",
		        "vrtri;",
		        "vscr;",
		        "vsubnE;",
		        "vsubne;",
		        "vsupnE;",
		        "vsupne;",
		        "vzigzag;",
		        "wcirc;",
		        "wedbar;",
		        "wedge;",
		        "wedgeq;",
		        "weierp;",
		        "wfr;",
		        "wopf;",
		        "wp;",
		        "wr;",
		        "wreath;",
		        "wscr;",
		        "xcap;",
		        "xcirc;",
		        "xcup;",
		        "xdtri;",
		        "xfr;",
		        "xhArr;",
		        "xharr;",
		        "xi;",
		        "xlArr;",
		        "xlarr;",
		        "xmap;",
		        "xnis;",
		        "xodot;",
		        "xopf;",
		        "xoplus;",
		        "xotime;",
		        "xrArr;",
		        "xrarr;",
		        "xscr;",
		        "xsqcup;",
		        "xuplus;",
		        "xutri;",
		        "xvee;",
		        "xwedge;",
		        "yacute",
		        "yacute;",
		        "yacy;",
		        "ycirc;",
		        "ycy;",
		        "yen",
		        "yen;",
		        "yfr;",
		        "yicy;",
		        "yopf;",
		        "yscr;",
		        "yucy;",
		        "yuml",
		        "yuml;",
		        "zacute;",
		        "zcaron;",
		        "zcy;",
		        "zdot;",
		        "zeetrf;",
		        "zeta;",
		        "zfr;",
		        "zhcy;",
		        "zigrarr;",
		        "zopf;",
		        "zscr;",
		        "zwj;",
		        "zwnj;"};
	}

	static string_map_t<uint32_t> initialize_map() {
		string_map_t<uint32_t> mapped_strings;
		mapped_strings.reserve(2231);
		mapped_strings[string_t(owned_strings[0])] = 198;
		mapped_strings[string_t(owned_strings[1])] = 198;
		mapped_strings[string_t(owned_strings[2])] = 38;
		mapped_strings[string_t(owned_strings[3])] = 38;
		mapped_strings[string_t(owned_strings[4])] = 193;
		mapped_strings[string_t(owned_strings[5])] = 193;
		mapped_strings[string_t(owned_strings[6])] = 258;
		mapped_strings[string_t(owned_strings[7])] = 194;
		mapped_strings[string_t(owned_strings[8])] = 194;
		mapped_strings[string_t(owned_strings[9])] = 1040;
		mapped_strings[string_t(owned_strings[10])] = 120068;
		mapped_strings[string_t(owned_strings[11])] = 192;
		mapped_strings[string_t(owned_strings[12])] = 192;
		mapped_strings[string_t(owned_strings[13])] = 913;
		mapped_strings[string_t(owned_strings[14])] = 256;
		mapped_strings[string_t(owned_strings[15])] = 10835;
		mapped_strings[string_t(owned_strings[16])] = 260;
		mapped_strings[string_t(owned_strings[17])] = 120120;
		mapped_strings[string_t(owned_strings[18])] = 8289;
		mapped_strings[string_t(owned_strings[19])] = 197;
		mapped_strings[string_t(owned_strings[20])] = 19;
		mapped_strings[string_t(owned_strings[21])] = 119964;
		mapped_strings[string_t(owned_strings[22])] = 8788;
		mapped_strings[string_t(owned_strings[23])] = 19;
		mapped_strings[string_t(owned_strings[24])] = 195;
		mapped_strings[string_t(owned_strings[25])] = 196;
		mapped_strings[string_t(owned_strings[26])] = 19;
		mapped_strings[string_t(owned_strings[27])] = 8726;
		mapped_strings[string_t(owned_strings[28])] = 10983;
		mapped_strings[string_t(owned_strings[29])] = 896;
		mapped_strings[string_t(owned_strings[30])] = 1041;
		mapped_strings[string_t(owned_strings[31])] = 8757;
		mapped_strings[string_t(owned_strings[32])] = 849;
		mapped_strings[string_t(owned_strings[33])] = 914;
		mapped_strings[string_t(owned_strings[34])] = 120069;
		mapped_strings[string_t(owned_strings[35])] = 12012;
		mapped_strings[string_t(owned_strings[36])] = 728;
		mapped_strings[string_t(owned_strings[37])] = 8492;
		mapped_strings[string_t(owned_strings[38])] = 878;
		mapped_strings[string_t(owned_strings[39])] = 1063;
		mapped_strings[string_t(owned_strings[40])] = 169;
		mapped_strings[string_t(owned_strings[41])] = 16;
		mapped_strings[string_t(owned_strings[42])] = 262;
		mapped_strings[string_t(owned_strings[43])] = 8914;
		mapped_strings[string_t(owned_strings[44])] = 851;
		mapped_strings[string_t(owned_strings[45])] = 8493;
		mapped_strings[string_t(owned_strings[46])] = 268;
		mapped_strings[string_t(owned_strings[47])] = 19;
		mapped_strings[string_t(owned_strings[48])] = 199;
		mapped_strings[string_t(owned_strings[49])] = 264;
		mapped_strings[string_t(owned_strings[50])] = 875;
		mapped_strings[string_t(owned_strings[51])] = 266;
		mapped_strings[string_t(owned_strings[52])] = 184;
		mapped_strings[string_t(owned_strings[53])] = 18;
		mapped_strings[string_t(owned_strings[54])] = 8493;
		mapped_strings[string_t(owned_strings[55])] = 935;
		mapped_strings[string_t(owned_strings[56])] = 885;
		mapped_strings[string_t(owned_strings[57])] = 8854;
		mapped_strings[string_t(owned_strings[58])] = 8853;
		mapped_strings[string_t(owned_strings[59])] = 885;
		mapped_strings[string_t(owned_strings[60])] = 8754;
		mapped_strings[string_t(owned_strings[61])] = 8221;
		mapped_strings[string_t(owned_strings[62])] = 821;
		mapped_strings[string_t(owned_strings[63])] = 8759;
		mapped_strings[string_t(owned_strings[64])] = 10868;
		mapped_strings[string_t(owned_strings[65])] = 880;
		mapped_strings[string_t(owned_strings[66])] = 8751;
		mapped_strings[string_t(owned_strings[67])] = 8750;
		mapped_strings[string_t(owned_strings[68])] = 845;
		mapped_strings[string_t(owned_strings[69])] = 8720;
		mapped_strings[string_t(owned_strings[70])] = 8755;
		mapped_strings[string_t(owned_strings[71])] = 1079;
		mapped_strings[string_t(owned_strings[72])] = 119966;
		mapped_strings[string_t(owned_strings[73])] = 8915;
		mapped_strings[string_t(owned_strings[74])] = 878;
		mapped_strings[string_t(owned_strings[75])] = 8517;
		mapped_strings[string_t(owned_strings[76])] = 10513;
		mapped_strings[string_t(owned_strings[77])] = 102;
		mapped_strings[string_t(owned_strings[78])] = 1029;
		mapped_strings[string_t(owned_strings[79])] = 1039;
		mapped_strings[string_t(owned_strings[80])] = 822;
		mapped_strings[string_t(owned_strings[81])] = 8609;
		mapped_strings[string_t(owned_strings[82])] = 10980;
		mapped_strings[string_t(owned_strings[83])] = 27;
		mapped_strings[string_t(owned_strings[84])] = 1044;
		mapped_strings[string_t(owned_strings[85])] = 8711;
		mapped_strings[string_t(owned_strings[86])] = 91;
		mapped_strings[string_t(owned_strings[87])] = 120071;
		mapped_strings[string_t(owned_strings[88])] = 180;
		mapped_strings[string_t(owned_strings[89])] = 72;
		mapped_strings[string_t(owned_strings[90])] = 733;
		mapped_strings[string_t(owned_strings[91])] = 96;
		mapped_strings[string_t(owned_strings[92])] = 73;
		mapped_strings[string_t(owned_strings[93])] = 8900;
		mapped_strings[string_t(owned_strings[94])] = 8518;
		mapped_strings[string_t(owned_strings[95])] = 12012;
		mapped_strings[string_t(owned_strings[96])] = 168;
		mapped_strings[string_t(owned_strings[97])] = 8412;
		mapped_strings[string_t(owned_strings[98])] = 878;
		mapped_strings[string_t(owned_strings[99])] = 8751;
		mapped_strings[string_t(owned_strings[100])] = 168;
		mapped_strings[string_t(owned_strings[101])] = 865;
		mapped_strings[string_t(owned_strings[102])] = 8656;
		mapped_strings[string_t(owned_strings[103])] = 8660;
		mapped_strings[string_t(owned_strings[104])] = 1098;
		mapped_strings[string_t(owned_strings[105])] = 10232;
		mapped_strings[string_t(owned_strings[106])] = 10234;
		mapped_strings[string_t(owned_strings[107])] = 1023;
		mapped_strings[string_t(owned_strings[108])] = 8658;
		mapped_strings[string_t(owned_strings[109])] = 8872;
		mapped_strings[string_t(owned_strings[110])] = 865;
		mapped_strings[string_t(owned_strings[111])] = 8661;
		mapped_strings[string_t(owned_strings[112])] = 8741;
		mapped_strings[string_t(owned_strings[113])] = 859;
		mapped_strings[string_t(owned_strings[114])] = 10515;
		mapped_strings[string_t(owned_strings[115])] = 8693;
		mapped_strings[string_t(owned_strings[116])] = 78;
		mapped_strings[string_t(owned_strings[117])] = 10576;
		mapped_strings[string_t(owned_strings[118])] = 10590;
		mapped_strings[string_t(owned_strings[119])] = 863;
		mapped_strings[string_t(owned_strings[120])] = 10582;
		mapped_strings[string_t(owned_strings[121])] = 10591;
		mapped_strings[string_t(owned_strings[122])] = 864;
		mapped_strings[string_t(owned_strings[123])] = 10583;
		mapped_strings[string_t(owned_strings[124])] = 8868;
		mapped_strings[string_t(owned_strings[125])] = 861;
		mapped_strings[string_t(owned_strings[126])] = 8659;
		mapped_strings[string_t(owned_strings[127])] = 119967;
		mapped_strings[string_t(owned_strings[128])] = 27;
		mapped_strings[string_t(owned_strings[129])] = 330;
		mapped_strings[string_t(owned_strings[130])] = 208;
		mapped_strings[string_t(owned_strings[131])] = 20;
		mapped_strings[string_t(owned_strings[132])] = 201;
		mapped_strings[string_t(owned_strings[133])] = 201;
		mapped_strings[string_t(owned_strings[134])] = 28;
		mapped_strings[string_t(owned_strings[135])] = 202;
		mapped_strings[string_t(owned_strings[136])] = 202;
		mapped_strings[string_t(owned_strings[137])] = 106;
		mapped_strings[string_t(owned_strings[138])] = 278;
		mapped_strings[string_t(owned_strings[139])] = 120072;
		mapped_strings[string_t(owned_strings[140])] = 20;
		mapped_strings[string_t(owned_strings[141])] = 200;
		mapped_strings[string_t(owned_strings[142])] = 8712;
		mapped_strings[string_t(owned_strings[143])] = 27;
		mapped_strings[string_t(owned_strings[144])] = 9723;
		mapped_strings[string_t(owned_strings[145])] = 9643;
		mapped_strings[string_t(owned_strings[146])] = 28;
		mapped_strings[string_t(owned_strings[147])] = 120124;
		mapped_strings[string_t(owned_strings[148])] = 917;
		mapped_strings[string_t(owned_strings[149])] = 1086;
		mapped_strings[string_t(owned_strings[150])] = 8770;
		mapped_strings[string_t(owned_strings[151])] = 8652;
		mapped_strings[string_t(owned_strings[152])] = 849;
		mapped_strings[string_t(owned_strings[153])] = 10867;
		mapped_strings[string_t(owned_strings[154])] = 919;
		mapped_strings[string_t(owned_strings[155])] = 20;
		mapped_strings[string_t(owned_strings[156])] = 203;
		mapped_strings[string_t(owned_strings[157])] = 8707;
		mapped_strings[string_t(owned_strings[158])] = 851;
		mapped_strings[string_t(owned_strings[159])] = 1060;
		mapped_strings[string_t(owned_strings[160])] = 120073;
		mapped_strings[string_t(owned_strings[161])] = 972;
		mapped_strings[string_t(owned_strings[162])] = 9642;
		mapped_strings[string_t(owned_strings[163])] = 120125;
		mapped_strings[string_t(owned_strings[164])] = 870;
		mapped_strings[string_t(owned_strings[165])] = 8497;
		mapped_strings[string_t(owned_strings[166])] = 8497;
		mapped_strings[string_t(owned_strings[167])] = 102;
		mapped_strings[string_t(owned_strings[168])] = 62;
		mapped_strings[string_t(owned_strings[169])] = 62;
		mapped_strings[string_t(owned_strings[170])] = 91;
		mapped_strings[string_t(owned_strings[171])] = 988;
		mapped_strings[string_t(owned_strings[172])] = 286;
		mapped_strings[string_t(owned_strings[173])] = 29;
		mapped_strings[string_t(owned_strings[174])] = 284;
		mapped_strings[string_t(owned_strings[175])] = 1043;
		mapped_strings[string_t(owned_strings[176])] = 28;
		mapped_strings[string_t(owned_strings[177])] = 120074;
		mapped_strings[string_t(owned_strings[178])] = 8921;
		mapped_strings[string_t(owned_strings[179])] = 12012;
		mapped_strings[string_t(owned_strings[180])] = 8805;
		mapped_strings[string_t(owned_strings[181])] = 8923;
		mapped_strings[string_t(owned_strings[182])] = 880;
		mapped_strings[string_t(owned_strings[183])] = 10914;
		mapped_strings[string_t(owned_strings[184])] = 8823;
		mapped_strings[string_t(owned_strings[185])] = 1087;
		mapped_strings[string_t(owned_strings[186])] = 8819;
		mapped_strings[string_t(owned_strings[187])] = 119970;
		mapped_strings[string_t(owned_strings[188])] = 881;
		mapped_strings[string_t(owned_strings[189])] = 1066;
		mapped_strings[string_t(owned_strings[190])] = 711;
		mapped_strings[string_t(owned_strings[191])] = 9;
		mapped_strings[string_t(owned_strings[192])] = 292;
		mapped_strings[string_t(owned_strings[193])] = 8460;
		mapped_strings[string_t(owned_strings[194])] = 845;
		mapped_strings[string_t(owned_strings[195])] = 8461;
		mapped_strings[string_t(owned_strings[196])] = 9472;
		mapped_strings[string_t(owned_strings[197])] = 845;
		mapped_strings[string_t(owned_strings[198])] = 294;
		mapped_strings[string_t(owned_strings[199])] = 8782;
		mapped_strings[string_t(owned_strings[200])] = 878;
		mapped_strings[string_t(owned_strings[201])] = 1045;
		mapped_strings[string_t(owned_strings[202])] = 306;
		mapped_strings[string_t(owned_strings[203])] = 102;
		mapped_strings[string_t(owned_strings[204])] = 205;
		mapped_strings[string_t(owned_strings[205])] = 205;
		mapped_strings[string_t(owned_strings[206])] = 20;
		mapped_strings[string_t(owned_strings[207])] = 206;
		mapped_strings[string_t(owned_strings[208])] = 1048;
		mapped_strings[string_t(owned_strings[209])] = 30;
		mapped_strings[string_t(owned_strings[210])] = 8465;
		mapped_strings[string_t(owned_strings[211])] = 204;
		mapped_strings[string_t(owned_strings[212])] = 20;
		mapped_strings[string_t(owned_strings[213])] = 8465;
		mapped_strings[string_t(owned_strings[214])] = 298;
		mapped_strings[string_t(owned_strings[215])] = 852;
		mapped_strings[string_t(owned_strings[216])] = 8658;
		mapped_strings[string_t(owned_strings[217])] = 8748;
		mapped_strings[string_t(owned_strings[218])] = 874;
		mapped_strings[string_t(owned_strings[219])] = 8898;
		mapped_strings[string_t(owned_strings[220])] = 8291;
		mapped_strings[string_t(owned_strings[221])] = 829;
		mapped_strings[string_t(owned_strings[222])] = 302;
		mapped_strings[string_t(owned_strings[223])] = 120128;
		mapped_strings[string_t(owned_strings[224])] = 92;
		mapped_strings[string_t(owned_strings[225])] = 8464;
		mapped_strings[string_t(owned_strings[226])] = 296;
		mapped_strings[string_t(owned_strings[227])] = 103;
		mapped_strings[string_t(owned_strings[228])] = 207;
		mapped_strings[string_t(owned_strings[229])] = 207;
		mapped_strings[string_t(owned_strings[230])] = 30;
		mapped_strings[string_t(owned_strings[231])] = 1049;
		mapped_strings[string_t(owned_strings[232])] = 120077;
		mapped_strings[string_t(owned_strings[233])] = 12012;
		mapped_strings[string_t(owned_strings[234])] = 119973;
		mapped_strings[string_t(owned_strings[235])] = 1032;
		mapped_strings[string_t(owned_strings[236])] = 102;
		mapped_strings[string_t(owned_strings[237])] = 1061;
		mapped_strings[string_t(owned_strings[238])] = 1036;
		mapped_strings[string_t(owned_strings[239])] = 92;
		mapped_strings[string_t(owned_strings[240])] = 310;
		mapped_strings[string_t(owned_strings[241])] = 1050;
		mapped_strings[string_t(owned_strings[242])] = 12007;
		mapped_strings[string_t(owned_strings[243])] = 120130;
		mapped_strings[string_t(owned_strings[244])] = 119974;
		mapped_strings[string_t(owned_strings[245])] = 103;
		mapped_strings[string_t(owned_strings[246])] = 60;
		mapped_strings[string_t(owned_strings[247])] = 60;
		mapped_strings[string_t(owned_strings[248])] = 31;
		mapped_strings[string_t(owned_strings[249])] = 923;
		mapped_strings[string_t(owned_strings[250])] = 10218;
		mapped_strings[string_t(owned_strings[251])] = 846;
		mapped_strings[string_t(owned_strings[252])] = 8606;
		mapped_strings[string_t(owned_strings[253])] = 317;
		mapped_strings[string_t(owned_strings[254])] = 31;
		mapped_strings[string_t(owned_strings[255])] = 1051;
		mapped_strings[string_t(owned_strings[256])] = 10216;
		mapped_strings[string_t(owned_strings[257])] = 859;
		mapped_strings[string_t(owned_strings[258])] = 8676;
		mapped_strings[string_t(owned_strings[259])] = 8646;
		mapped_strings[string_t(owned_strings[260])] = 896;
		mapped_strings[string_t(owned_strings[261])] = 10214;
		mapped_strings[string_t(owned_strings[262])] = 10593;
		mapped_strings[string_t(owned_strings[263])] = 864;
		mapped_strings[string_t(owned_strings[264])] = 10585;
		mapped_strings[string_t(owned_strings[265])] = 8970;
		mapped_strings[string_t(owned_strings[266])] = 859;
		mapped_strings[string_t(owned_strings[267])] = 10574;
		mapped_strings[string_t(owned_strings[268])] = 8867;
		mapped_strings[string_t(owned_strings[269])] = 861;
		mapped_strings[string_t(owned_strings[270])] = 10586;
		mapped_strings[string_t(owned_strings[271])] = 8882;
		mapped_strings[string_t(owned_strings[272])] = 1070;
		mapped_strings[string_t(owned_strings[273])] = 8884;
		mapped_strings[string_t(owned_strings[274])] = 10577;
		mapped_strings[string_t(owned_strings[275])] = 1059;
		mapped_strings[string_t(owned_strings[276])] = 8639;
		mapped_strings[string_t(owned_strings[277])] = 10584;
		mapped_strings[string_t(owned_strings[278])] = 863;
		mapped_strings[string_t(owned_strings[279])] = 10578;
		mapped_strings[string_t(owned_strings[280])] = 8656;
		mapped_strings[string_t(owned_strings[281])] = 866;
		mapped_strings[string_t(owned_strings[282])] = 8922;
		mapped_strings[string_t(owned_strings[283])] = 8806;
		mapped_strings[string_t(owned_strings[284])] = 882;
		mapped_strings[string_t(owned_strings[285])] = 10913;
		mapped_strings[string_t(owned_strings[286])] = 10877;
		mapped_strings[string_t(owned_strings[287])] = 881;
		mapped_strings[string_t(owned_strings[288])] = 120079;
		mapped_strings[string_t(owned_strings[289])] = 8920;
		mapped_strings[string_t(owned_strings[290])] = 866;
		mapped_strings[string_t(owned_strings[291])] = 319;
		mapped_strings[string_t(owned_strings[292])] = 10229;
		mapped_strings[string_t(owned_strings[293])] = 1023;
		mapped_strings[string_t(owned_strings[294])] = 10230;
		mapped_strings[string_t(owned_strings[295])] = 10232;
		mapped_strings[string_t(owned_strings[296])] = 1023;
		mapped_strings[string_t(owned_strings[297])] = 10233;
		mapped_strings[string_t(owned_strings[298])] = 120131;
		mapped_strings[string_t(owned_strings[299])] = 860;
		mapped_strings[string_t(owned_strings[300])] = 8600;
		mapped_strings[string_t(owned_strings[301])] = 8466;
		mapped_strings[string_t(owned_strings[302])] = 862;
		mapped_strings[string_t(owned_strings[303])] = 321;
		mapped_strings[string_t(owned_strings[304])] = 8810;
		mapped_strings[string_t(owned_strings[305])] = 1050;
		mapped_strings[string_t(owned_strings[306])] = 1052;
		mapped_strings[string_t(owned_strings[307])] = 8287;
		mapped_strings[string_t(owned_strings[308])] = 849;
		mapped_strings[string_t(owned_strings[309])] = 120080;
		mapped_strings[string_t(owned_strings[310])] = 8723;
		mapped_strings[string_t(owned_strings[311])] = 12013;
		mapped_strings[string_t(owned_strings[312])] = 8499;
		mapped_strings[string_t(owned_strings[313])] = 924;
		mapped_strings[string_t(owned_strings[314])] = 103;
		mapped_strings[string_t(owned_strings[315])] = 323;
		mapped_strings[string_t(owned_strings[316])] = 327;
		mapped_strings[string_t(owned_strings[317])] = 32;
		mapped_strings[string_t(owned_strings[318])] = 1053;
		mapped_strings[string_t(owned_strings[319])] = 8203;
		mapped_strings[string_t(owned_strings[320])] = 820;
		mapped_strings[string_t(owned_strings[321])] = 8203;
		mapped_strings[string_t(owned_strings[322])] = 8203;
		mapped_strings[string_t(owned_strings[323])] = 881;
		mapped_strings[string_t(owned_strings[324])] = 8810;
		mapped_strings[string_t(owned_strings[325])] = 10;
		mapped_strings[string_t(owned_strings[326])] = 12008;
		mapped_strings[string_t(owned_strings[327])] = 8288;
		mapped_strings[string_t(owned_strings[328])] = 160;
		mapped_strings[string_t(owned_strings[329])] = 846;
		mapped_strings[string_t(owned_strings[330])] = 10988;
		mapped_strings[string_t(owned_strings[331])] = 8802;
		mapped_strings[string_t(owned_strings[332])] = 881;
		mapped_strings[string_t(owned_strings[333])] = 8742;
		mapped_strings[string_t(owned_strings[334])] = 8713;
		mapped_strings[string_t(owned_strings[335])] = 880;
		mapped_strings[string_t(owned_strings[336])] = 8770;
		mapped_strings[string_t(owned_strings[337])] = 8708;
		mapped_strings[string_t(owned_strings[338])] = 881;
		mapped_strings[string_t(owned_strings[339])] = 8817;
		mapped_strings[string_t(owned_strings[340])] = 8807;
		mapped_strings[string_t(owned_strings[341])] = 881;
		mapped_strings[string_t(owned_strings[342])] = 8825;
		mapped_strings[string_t(owned_strings[343])] = 10878;
		mapped_strings[string_t(owned_strings[344])] = 882;
		mapped_strings[string_t(owned_strings[345])] = 8782;
		mapped_strings[string_t(owned_strings[346])] = 8783;
		mapped_strings[string_t(owned_strings[347])] = 893;
		mapped_strings[string_t(owned_strings[348])] = 10703;
		mapped_strings[string_t(owned_strings[349])] = 8940;
		mapped_strings[string_t(owned_strings[350])] = 881;
		mapped_strings[string_t(owned_strings[351])] = 8816;
		mapped_strings[string_t(owned_strings[352])] = 8824;
		mapped_strings[string_t(owned_strings[353])] = 881;
		mapped_strings[string_t(owned_strings[354])] = 10877;
		mapped_strings[string_t(owned_strings[355])] = 8820;
		mapped_strings[string_t(owned_strings[356])] = 1091;
		mapped_strings[string_t(owned_strings[357])] = 10913;
		mapped_strings[string_t(owned_strings[358])] = 8832;
		mapped_strings[string_t(owned_strings[359])] = 1092;
		mapped_strings[string_t(owned_strings[360])] = 8928;
		mapped_strings[string_t(owned_strings[361])] = 8716;
		mapped_strings[string_t(owned_strings[362])] = 893;
		mapped_strings[string_t(owned_strings[363])] = 10704;
		mapped_strings[string_t(owned_strings[364])] = 8941;
		mapped_strings[string_t(owned_strings[365])] = 884;
		mapped_strings[string_t(owned_strings[366])] = 8930;
		mapped_strings[string_t(owned_strings[367])] = 8848;
		mapped_strings[string_t(owned_strings[368])] = 893;
		mapped_strings[string_t(owned_strings[369])] = 8834;
		mapped_strings[string_t(owned_strings[370])] = 8840;
		mapped_strings[string_t(owned_strings[371])] = 883;
		mapped_strings[string_t(owned_strings[372])] = 10928;
		mapped_strings[string_t(owned_strings[373])] = 8929;
		mapped_strings[string_t(owned_strings[374])] = 883;
		mapped_strings[string_t(owned_strings[375])] = 8835;
		mapped_strings[string_t(owned_strings[376])] = 8841;
		mapped_strings[string_t(owned_strings[377])] = 876;
		mapped_strings[string_t(owned_strings[378])] = 8772;
		mapped_strings[string_t(owned_strings[379])] = 8775;
		mapped_strings[string_t(owned_strings[380])] = 877;
		mapped_strings[string_t(owned_strings[381])] = 8740;
		mapped_strings[string_t(owned_strings[382])] = 119977;
		mapped_strings[string_t(owned_strings[383])] = 20;
		mapped_strings[string_t(owned_strings[384])] = 209;
		mapped_strings[string_t(owned_strings[385])] = 925;
		mapped_strings[string_t(owned_strings[386])] = 33;
		mapped_strings[string_t(owned_strings[387])] = 211;
		mapped_strings[string_t(owned_strings[388])] = 211;
		mapped_strings[string_t(owned_strings[389])] = 21;
		mapped_strings[string_t(owned_strings[390])] = 212;
		mapped_strings[string_t(owned_strings[391])] = 1054;
		mapped_strings[string_t(owned_strings[392])] = 33;
		mapped_strings[string_t(owned_strings[393])] = 120082;
		mapped_strings[string_t(owned_strings[394])] = 210;
		mapped_strings[string_t(owned_strings[395])] = 21;
		mapped_strings[string_t(owned_strings[396])] = 332;
		mapped_strings[string_t(owned_strings[397])] = 937;
		mapped_strings[string_t(owned_strings[398])] = 92;
		mapped_strings[string_t(owned_strings[399])] = 120134;
		mapped_strings[string_t(owned_strings[400])] = 8220;
		mapped_strings[string_t(owned_strings[401])] = 821;
		mapped_strings[string_t(owned_strings[402])] = 10836;
		mapped_strings[string_t(owned_strings[403])] = 119978;
		mapped_strings[string_t(owned_strings[404])] = 21;
		mapped_strings[string_t(owned_strings[405])] = 216;
		mapped_strings[string_t(owned_strings[406])] = 213;
		mapped_strings[string_t(owned_strings[407])] = 21;
		mapped_strings[string_t(owned_strings[408])] = 10807;
		mapped_strings[string_t(owned_strings[409])] = 214;
		mapped_strings[string_t(owned_strings[410])] = 21;
		mapped_strings[string_t(owned_strings[411])] = 8254;
		mapped_strings[string_t(owned_strings[412])] = 9182;
		mapped_strings[string_t(owned_strings[413])] = 914;
		mapped_strings[string_t(owned_strings[414])] = 9180;
		mapped_strings[string_t(owned_strings[415])] = 8706;
		mapped_strings[string_t(owned_strings[416])] = 105;
		mapped_strings[string_t(owned_strings[417])] = 120083;
		mapped_strings[string_t(owned_strings[418])] = 934;
		mapped_strings[string_t(owned_strings[419])] = 92;
		mapped_strings[string_t(owned_strings[420])] = 177;
		mapped_strings[string_t(owned_strings[421])] = 8460;
		mapped_strings[string_t(owned_strings[422])] = 847;
		mapped_strings[string_t(owned_strings[423])] = 10939;
		mapped_strings[string_t(owned_strings[424])] = 8826;
		mapped_strings[string_t(owned_strings[425])] = 1092;
		mapped_strings[string_t(owned_strings[426])] = 8828;
		mapped_strings[string_t(owned_strings[427])] = 8830;
		mapped_strings[string_t(owned_strings[428])] = 824;
		mapped_strings[string_t(owned_strings[429])] = 8719;
		mapped_strings[string_t(owned_strings[430])] = 8759;
		mapped_strings[string_t(owned_strings[431])] = 873;
		mapped_strings[string_t(owned_strings[432])] = 119979;
		mapped_strings[string_t(owned_strings[433])] = 936;
		mapped_strings[string_t(owned_strings[434])] = 3;
		mapped_strings[string_t(owned_strings[435])] = 34;
		mapped_strings[string_t(owned_strings[436])] = 120084;
		mapped_strings[string_t(owned_strings[437])] = 847;
		mapped_strings[string_t(owned_strings[438])] = 119980;
		mapped_strings[string_t(owned_strings[439])] = 10512;
		mapped_strings[string_t(owned_strings[440])] = 17;
		mapped_strings[string_t(owned_strings[441])] = 174;
		mapped_strings[string_t(owned_strings[442])] = 340;
		mapped_strings[string_t(owned_strings[443])] = 1021;
		mapped_strings[string_t(owned_strings[444])] = 8608;
		mapped_strings[string_t(owned_strings[445])] = 10518;
		mapped_strings[string_t(owned_strings[446])] = 344;
		mapped_strings[string_t(owned_strings[447])] = 342;
		mapped_strings[string_t(owned_strings[448])] = 1056;
		mapped_strings[string_t(owned_strings[449])] = 8476;
		mapped_strings[string_t(owned_strings[450])] = 8715;
		mapped_strings[string_t(owned_strings[451])] = 8651;
		mapped_strings[string_t(owned_strings[452])] = 10607;
		mapped_strings[string_t(owned_strings[453])] = 8476;
		mapped_strings[string_t(owned_strings[454])] = 929;
		mapped_strings[string_t(owned_strings[455])] = 10217;
		mapped_strings[string_t(owned_strings[456])] = 8594;
		mapped_strings[string_t(owned_strings[457])] = 8677;
		mapped_strings[string_t(owned_strings[458])] = 8644;
		mapped_strings[string_t(owned_strings[459])] = 8969;
		mapped_strings[string_t(owned_strings[460])] = 10215;
		mapped_strings[string_t(owned_strings[461])] = 10589;
		mapped_strings[string_t(owned_strings[462])] = 8642;
		mapped_strings[string_t(owned_strings[463])] = 10581;
		mapped_strings[string_t(owned_strings[464])] = 8971;
		mapped_strings[string_t(owned_strings[465])] = 8866;
		mapped_strings[string_t(owned_strings[466])] = 8614;
		mapped_strings[string_t(owned_strings[467])] = 10587;
		mapped_strings[string_t(owned_strings[468])] = 8883;
		mapped_strings[string_t(owned_strings[469])] = 10704;
		mapped_strings[string_t(owned_strings[470])] = 8885;
		mapped_strings[string_t(owned_strings[471])] = 10575;
		mapped_strings[string_t(owned_strings[472])] = 10588;
		mapped_strings[string_t(owned_strings[473])] = 8638;
		mapped_strings[string_t(owned_strings[474])] = 10580;
		mapped_strings[string_t(owned_strings[475])] = 8640;
		mapped_strings[string_t(owned_strings[476])] = 10579;
		mapped_strings[string_t(owned_strings[477])] = 8658;
		mapped_strings[string_t(owned_strings[478])] = 8477;
		mapped_strings[string_t(owned_strings[479])] = 10608;
		mapped_strings[string_t(owned_strings[480])] = 8667;
		mapped_strings[string_t(owned_strings[481])] = 8475;
		mapped_strings[string_t(owned_strings[482])] = 8625;
		mapped_strings[string_t(owned_strings[483])] = 10740;
		mapped_strings[string_t(owned_strings[484])] = 1065;
		mapped_strings[string_t(owned_strings[485])] = 1064;
		mapped_strings[string_t(owned_strings[486])] = 1068;
		mapped_strings[string_t(owned_strings[487])] = 346;
		mapped_strings[string_t(owned_strings[488])] = 10940;
		mapped_strings[string_t(owned_strings[489])] = 352;
		mapped_strings[string_t(owned_strings[490])] = 350;
		mapped_strings[string_t(owned_strings[491])] = 348;
		mapped_strings[string_t(owned_strings[492])] = 1057;
		mapped_strings[string_t(owned_strings[493])] = 120086;
		mapped_strings[string_t(owned_strings[494])] = 8595;
		mapped_strings[string_t(owned_strings[495])] = 8592;
		mapped_strings[string_t(owned_strings[496])] = 8594;
		mapped_strings[string_t(owned_strings[497])] = 8593;
		mapped_strings[string_t(owned_strings[498])] = 931;
		mapped_strings[string_t(owned_strings[499])] = 8728;
		mapped_strings[string_t(owned_strings[500])] = 120138;
		mapped_strings[string_t(owned_strings[501])] = 8730;
		mapped_strings[string_t(owned_strings[502])] = 9633;
		mapped_strings[string_t(owned_strings[503])] = 8851;
		mapped_strings[string_t(owned_strings[504])] = 8847;
		mapped_strings[string_t(owned_strings[505])] = 8849;
		mapped_strings[string_t(owned_strings[506])] = 8848;
		mapped_strings[string_t(owned_strings[507])] = 8850;
		mapped_strings[string_t(owned_strings[508])] = 8852;
		mapped_strings[string_t(owned_strings[509])] = 119982;
		mapped_strings[string_t(owned_strings[510])] = 8902;
		mapped_strings[string_t(owned_strings[511])] = 8912;
		mapped_strings[string_t(owned_strings[512])] = 8912;
		mapped_strings[string_t(owned_strings[513])] = 8838;
		mapped_strings[string_t(owned_strings[514])] = 8827;
		mapped_strings[string_t(owned_strings[515])] = 10928;
		mapped_strings[string_t(owned_strings[516])] = 8829;
		mapped_strings[string_t(owned_strings[517])] = 8831;
		mapped_strings[string_t(owned_strings[518])] = 8715;
		mapped_strings[string_t(owned_strings[519])] = 8721;
		mapped_strings[string_t(owned_strings[520])] = 8913;
		mapped_strings[string_t(owned_strings[521])] = 8835;
		mapped_strings[string_t(owned_strings[522])] = 8839;
		mapped_strings[string_t(owned_strings[523])] = 8913;
		mapped_strings[string_t(owned_strings[524])] = 222;
		mapped_strings[string_t(owned_strings[525])] = 222;
		mapped_strings[string_t(owned_strings[526])] = 8482;
		mapped_strings[string_t(owned_strings[527])] = 1035;
		mapped_strings[string_t(owned_strings[528])] = 1062;
		mapped_strings[string_t(owned_strings[529])] = 9;
		mapped_strings[string_t(owned_strings[530])] = 932;
		mapped_strings[string_t(owned_strings[531])] = 356;
		mapped_strings[string_t(owned_strings[532])] = 354;
		mapped_strings[string_t(owned_strings[533])] = 1058;
		mapped_strings[string_t(owned_strings[534])] = 120087;
		mapped_strings[string_t(owned_strings[535])] = 8756;
		mapped_strings[string_t(owned_strings[536])] = 920;
		mapped_strings[string_t(owned_strings[537])] = 8287;
		mapped_strings[string_t(owned_strings[538])] = 8201;
		mapped_strings[string_t(owned_strings[539])] = 8764;
		mapped_strings[string_t(owned_strings[540])] = 8771;
		mapped_strings[string_t(owned_strings[541])] = 8773;
		mapped_strings[string_t(owned_strings[542])] = 8776;
		mapped_strings[string_t(owned_strings[543])] = 120139;
		mapped_strings[string_t(owned_strings[544])] = 8411;
		mapped_strings[string_t(owned_strings[545])] = 119983;
		mapped_strings[string_t(owned_strings[546])] = 358;
		mapped_strings[string_t(owned_strings[547])] = 218;
		mapped_strings[string_t(owned_strings[548])] = 218;
		mapped_strings[string_t(owned_strings[549])] = 8607;
		mapped_strings[string_t(owned_strings[550])] = 10569;
		mapped_strings[string_t(owned_strings[551])] = 1038;
		mapped_strings[string_t(owned_strings[552])] = 364;
		mapped_strings[string_t(owned_strings[553])] = 219;
		mapped_strings[string_t(owned_strings[554])] = 219;
		mapped_strings[string_t(owned_strings[555])] = 1059;
		mapped_strings[string_t(owned_strings[556])] = 368;
		mapped_strings[string_t(owned_strings[557])] = 120088;
		mapped_strings[string_t(owned_strings[558])] = 217;
		mapped_strings[string_t(owned_strings[559])] = 217;
		mapped_strings[string_t(owned_strings[560])] = 362;
		mapped_strings[string_t(owned_strings[561])] = 95;
		mapped_strings[string_t(owned_strings[562])] = 9183;
		mapped_strings[string_t(owned_strings[563])] = 9141;
		mapped_strings[string_t(owned_strings[564])] = 9181;
		mapped_strings[string_t(owned_strings[565])] = 8899;
		mapped_strings[string_t(owned_strings[566])] = 8846;
		mapped_strings[string_t(owned_strings[567])] = 370;
		mapped_strings[string_t(owned_strings[568])] = 120140;
		mapped_strings[string_t(owned_strings[569])] = 8593;
		mapped_strings[string_t(owned_strings[570])] = 10514;
		mapped_strings[string_t(owned_strings[571])] = 8645;
		mapped_strings[string_t(owned_strings[572])] = 8597;
		mapped_strings[string_t(owned_strings[573])] = 10606;
		mapped_strings[string_t(owned_strings[574])] = 8869;
		mapped_strings[string_t(owned_strings[575])] = 8613;
		mapped_strings[string_t(owned_strings[576])] = 8657;
		mapped_strings[string_t(owned_strings[577])] = 8661;
		mapped_strings[string_t(owned_strings[578])] = 8598;
		mapped_strings[string_t(owned_strings[579])] = 8599;
		mapped_strings[string_t(owned_strings[580])] = 978;
		mapped_strings[string_t(owned_strings[581])] = 933;
		mapped_strings[string_t(owned_strings[582])] = 366;
		mapped_strings[string_t(owned_strings[583])] = 119984;
		mapped_strings[string_t(owned_strings[584])] = 360;
		mapped_strings[string_t(owned_strings[585])] = 220;
		mapped_strings[string_t(owned_strings[586])] = 220;
		mapped_strings[string_t(owned_strings[587])] = 8875;
		mapped_strings[string_t(owned_strings[588])] = 10987;
		mapped_strings[string_t(owned_strings[589])] = 1042;
		mapped_strings[string_t(owned_strings[590])] = 8873;
		mapped_strings[string_t(owned_strings[591])] = 10982;
		mapped_strings[string_t(owned_strings[592])] = 8897;
		mapped_strings[string_t(owned_strings[593])] = 8214;
		mapped_strings[string_t(owned_strings[594])] = 8214;
		mapped_strings[string_t(owned_strings[595])] = 8739;
		mapped_strings[string_t(owned_strings[596])] = 124;
		mapped_strings[string_t(owned_strings[597])] = 10072;
		mapped_strings[string_t(owned_strings[598])] = 8768;
		mapped_strings[string_t(owned_strings[599])] = 8202;
		mapped_strings[string_t(owned_strings[600])] = 120089;
		mapped_strings[string_t(owned_strings[601])] = 120141;
		mapped_strings[string_t(owned_strings[602])] = 119985;
		mapped_strings[string_t(owned_strings[603])] = 8874;
		mapped_strings[string_t(owned_strings[604])] = 372;
		mapped_strings[string_t(owned_strings[605])] = 8896;
		mapped_strings[string_t(owned_strings[606])] = 120090;
		mapped_strings[string_t(owned_strings[607])] = 120142;
		mapped_strings[string_t(owned_strings[608])] = 119986;
		mapped_strings[string_t(owned_strings[609])] = 120091;
		mapped_strings[string_t(owned_strings[610])] = 926;
		mapped_strings[string_t(owned_strings[611])] = 120143;
		mapped_strings[string_t(owned_strings[612])] = 119987;
		mapped_strings[string_t(owned_strings[613])] = 1071;
		mapped_strings[string_t(owned_strings[614])] = 1031;
		mapped_strings[string_t(owned_strings[615])] = 1070;
		mapped_strings[string_t(owned_strings[616])] = 221;
		mapped_strings[string_t(owned_strings[617])] = 221;
		mapped_strings[string_t(owned_strings[618])] = 374;
		mapped_strings[string_t(owned_strings[619])] = 1067;
		mapped_strings[string_t(owned_strings[620])] = 120092;
		mapped_strings[string_t(owned_strings[621])] = 120144;
		mapped_strings[string_t(owned_strings[622])] = 119988;
		mapped_strings[string_t(owned_strings[623])] = 376;
		mapped_strings[string_t(owned_strings[624])] = 1046;
		mapped_strings[string_t(owned_strings[625])] = 377;
		mapped_strings[string_t(owned_strings[626])] = 381;
		mapped_strings[string_t(owned_strings[627])] = 1047;
		mapped_strings[string_t(owned_strings[628])] = 379;
		mapped_strings[string_t(owned_strings[629])] = 8203;
		mapped_strings[string_t(owned_strings[630])] = 918;
		mapped_strings[string_t(owned_strings[631])] = 8488;
		mapped_strings[string_t(owned_strings[632])] = 8484;
		mapped_strings[string_t(owned_strings[633])] = 119989;
		mapped_strings[string_t(owned_strings[634])] = 225;
		mapped_strings[string_t(owned_strings[635])] = 225;
		mapped_strings[string_t(owned_strings[636])] = 259;
		mapped_strings[string_t(owned_strings[637])] = 8766;
		mapped_strings[string_t(owned_strings[638])] = 8766;
		mapped_strings[string_t(owned_strings[639])] = 8767;
		mapped_strings[string_t(owned_strings[640])] = 226;
		mapped_strings[string_t(owned_strings[641])] = 226;
		mapped_strings[string_t(owned_strings[642])] = 180;
		mapped_strings[string_t(owned_strings[643])] = 180;
		mapped_strings[string_t(owned_strings[644])] = 1072;
		mapped_strings[string_t(owned_strings[645])] = 230;
		mapped_strings[string_t(owned_strings[646])] = 230;
		mapped_strings[string_t(owned_strings[647])] = 8289;
		mapped_strings[string_t(owned_strings[648])] = 120094;
		mapped_strings[string_t(owned_strings[649])] = 224;
		mapped_strings[string_t(owned_strings[650])] = 224;
		mapped_strings[string_t(owned_strings[651])] = 8501;
		mapped_strings[string_t(owned_strings[652])] = 8501;
		mapped_strings[string_t(owned_strings[653])] = 945;
		mapped_strings[string_t(owned_strings[654])] = 257;
		mapped_strings[string_t(owned_strings[655])] = 10815;
		mapped_strings[string_t(owned_strings[656])] = 38;
		mapped_strings[string_t(owned_strings[657])] = 38;
		mapped_strings[string_t(owned_strings[658])] = 8743;
		mapped_strings[string_t(owned_strings[659])] = 10837;
		mapped_strings[string_t(owned_strings[660])] = 10844;
		mapped_strings[string_t(owned_strings[661])] = 10840;
		mapped_strings[string_t(owned_strings[662])] = 10842;
		mapped_strings[string_t(owned_strings[663])] = 8736;
		mapped_strings[string_t(owned_strings[664])] = 10660;
		mapped_strings[string_t(owned_strings[665])] = 8736;
		mapped_strings[string_t(owned_strings[666])] = 8737;
		mapped_strings[string_t(owned_strings[667])] = 10664;
		mapped_strings[string_t(owned_strings[668])] = 10665;
		mapped_strings[string_t(owned_strings[669])] = 10666;
		mapped_strings[string_t(owned_strings[670])] = 10667;
		mapped_strings[string_t(owned_strings[671])] = 10668;
		mapped_strings[string_t(owned_strings[672])] = 10669;
		mapped_strings[string_t(owned_strings[673])] = 10670;
		mapped_strings[string_t(owned_strings[674])] = 10671;
		mapped_strings[string_t(owned_strings[675])] = 8735;
		mapped_strings[string_t(owned_strings[676])] = 8894;
		mapped_strings[string_t(owned_strings[677])] = 10653;
		mapped_strings[string_t(owned_strings[678])] = 8738;
		mapped_strings[string_t(owned_strings[679])] = 197;
		mapped_strings[string_t(owned_strings[680])] = 9084;
		mapped_strings[string_t(owned_strings[681])] = 261;
		mapped_strings[string_t(owned_strings[682])] = 120146;
		mapped_strings[string_t(owned_strings[683])] = 8776;
		mapped_strings[string_t(owned_strings[684])] = 10864;
		mapped_strings[string_t(owned_strings[685])] = 10863;
		mapped_strings[string_t(owned_strings[686])] = 8778;
		mapped_strings[string_t(owned_strings[687])] = 8779;
		mapped_strings[string_t(owned_strings[688])] = 39;
		mapped_strings[string_t(owned_strings[689])] = 8776;
		mapped_strings[string_t(owned_strings[690])] = 8778;
		mapped_strings[string_t(owned_strings[691])] = 229;
		mapped_strings[string_t(owned_strings[692])] = 229;
		mapped_strings[string_t(owned_strings[693])] = 119990;
		mapped_strings[string_t(owned_strings[694])] = 42;
		mapped_strings[string_t(owned_strings[695])] = 8776;
		mapped_strings[string_t(owned_strings[696])] = 8781;
		mapped_strings[string_t(owned_strings[697])] = 227;
		mapped_strings[string_t(owned_strings[698])] = 227;
		mapped_strings[string_t(owned_strings[699])] = 228;
		mapped_strings[string_t(owned_strings[700])] = 228;
		mapped_strings[string_t(owned_strings[701])] = 8755;
		mapped_strings[string_t(owned_strings[702])] = 10769;
		mapped_strings[string_t(owned_strings[703])] = 10989;
		mapped_strings[string_t(owned_strings[704])] = 8780;
		mapped_strings[string_t(owned_strings[705])] = 1014;
		mapped_strings[string_t(owned_strings[706])] = 8245;
		mapped_strings[string_t(owned_strings[707])] = 8765;
		mapped_strings[string_t(owned_strings[708])] = 8909;
		mapped_strings[string_t(owned_strings[709])] = 8893;
		mapped_strings[string_t(owned_strings[710])] = 8965;
		mapped_strings[string_t(owned_strings[711])] = 8965;
		mapped_strings[string_t(owned_strings[712])] = 9141;
		mapped_strings[string_t(owned_strings[713])] = 9142;
		mapped_strings[string_t(owned_strings[714])] = 8780;
		mapped_strings[string_t(owned_strings[715])] = 1073;
		mapped_strings[string_t(owned_strings[716])] = 8222;
		mapped_strings[string_t(owned_strings[717])] = 8757;
		mapped_strings[string_t(owned_strings[718])] = 8757;
		mapped_strings[string_t(owned_strings[719])] = 10672;
		mapped_strings[string_t(owned_strings[720])] = 1014;
		mapped_strings[string_t(owned_strings[721])] = 8492;
		mapped_strings[string_t(owned_strings[722])] = 946;
		mapped_strings[string_t(owned_strings[723])] = 8502;
		mapped_strings[string_t(owned_strings[724])] = 8812;
		mapped_strings[string_t(owned_strings[725])] = 120095;
		mapped_strings[string_t(owned_strings[726])] = 8898;
		mapped_strings[string_t(owned_strings[727])] = 9711;
		mapped_strings[string_t(owned_strings[728])] = 8899;
		mapped_strings[string_t(owned_strings[729])] = 10752;
		mapped_strings[string_t(owned_strings[730])] = 10753;
		mapped_strings[string_t(owned_strings[731])] = 10754;
		mapped_strings[string_t(owned_strings[732])] = 10758;
		mapped_strings[string_t(owned_strings[733])] = 9733;
		mapped_strings[string_t(owned_strings[734])] = 9661;
		mapped_strings[string_t(owned_strings[735])] = 9651;
		mapped_strings[string_t(owned_strings[736])] = 10756;
		mapped_strings[string_t(owned_strings[737])] = 8897;
		mapped_strings[string_t(owned_strings[738])] = 8896;
		mapped_strings[string_t(owned_strings[739])] = 10509;
		mapped_strings[string_t(owned_strings[740])] = 10731;
		mapped_strings[string_t(owned_strings[741])] = 9642;
		mapped_strings[string_t(owned_strings[742])] = 9652;
		mapped_strings[string_t(owned_strings[743])] = 9662;
		mapped_strings[string_t(owned_strings[744])] = 9666;
		mapped_strings[string_t(owned_strings[745])] = 9656;
		mapped_strings[string_t(owned_strings[746])] = 9251;
		mapped_strings[string_t(owned_strings[747])] = 9618;
		mapped_strings[string_t(owned_strings[748])] = 9617;
		mapped_strings[string_t(owned_strings[749])] = 9619;
		mapped_strings[string_t(owned_strings[750])] = 9608;
		mapped_strings[string_t(owned_strings[751])] = 61;
		mapped_strings[string_t(owned_strings[752])] = 8801;
		mapped_strings[string_t(owned_strings[753])] = 8976;
		mapped_strings[string_t(owned_strings[754])] = 120147;
		mapped_strings[string_t(owned_strings[755])] = 8869;
		mapped_strings[string_t(owned_strings[756])] = 8869;
		mapped_strings[string_t(owned_strings[757])] = 8904;
		mapped_strings[string_t(owned_strings[758])] = 9559;
		mapped_strings[string_t(owned_strings[759])] = 9556;
		mapped_strings[string_t(owned_strings[760])] = 9558;
		mapped_strings[string_t(owned_strings[761])] = 9555;
		mapped_strings[string_t(owned_strings[762])] = 9552;
		mapped_strings[string_t(owned_strings[763])] = 9574;
		mapped_strings[string_t(owned_strings[764])] = 9577;
		mapped_strings[string_t(owned_strings[765])] = 9572;
		mapped_strings[string_t(owned_strings[766])] = 9575;
		mapped_strings[string_t(owned_strings[767])] = 9565;
		mapped_strings[string_t(owned_strings[768])] = 9562;
		mapped_strings[string_t(owned_strings[769])] = 9564;
		mapped_strings[string_t(owned_strings[770])] = 9561;
		mapped_strings[string_t(owned_strings[771])] = 9553;
		mapped_strings[string_t(owned_strings[772])] = 9580;
		mapped_strings[string_t(owned_strings[773])] = 9571;
		mapped_strings[string_t(owned_strings[774])] = 9568;
		mapped_strings[string_t(owned_strings[775])] = 9579;
		mapped_strings[string_t(owned_strings[776])] = 9570;
		mapped_strings[string_t(owned_strings[777])] = 9567;
		mapped_strings[string_t(owned_strings[778])] = 10697;
		mapped_strings[string_t(owned_strings[779])] = 9557;
		mapped_strings[string_t(owned_strings[780])] = 9554;
		mapped_strings[string_t(owned_strings[781])] = 9488;
		mapped_strings[string_t(owned_strings[782])] = 9484;
		mapped_strings[string_t(owned_strings[783])] = 9472;
		mapped_strings[string_t(owned_strings[784])] = 9573;
		mapped_strings[string_t(owned_strings[785])] = 9576;
		mapped_strings[string_t(owned_strings[786])] = 9516;
		mapped_strings[string_t(owned_strings[787])] = 9524;
		mapped_strings[string_t(owned_strings[788])] = 8863;
		mapped_strings[string_t(owned_strings[789])] = 8862;
		mapped_strings[string_t(owned_strings[790])] = 8864;
		mapped_strings[string_t(owned_strings[791])] = 9563;
		mapped_strings[string_t(owned_strings[792])] = 9560;
		mapped_strings[string_t(owned_strings[793])] = 9496;
		mapped_strings[string_t(owned_strings[794])] = 9492;
		mapped_strings[string_t(owned_strings[795])] = 9474;
		mapped_strings[string_t(owned_strings[796])] = 9578;
		mapped_strings[string_t(owned_strings[797])] = 9569;
		mapped_strings[string_t(owned_strings[798])] = 9566;
		mapped_strings[string_t(owned_strings[799])] = 9532;
		mapped_strings[string_t(owned_strings[800])] = 9508;
		mapped_strings[string_t(owned_strings[801])] = 9500;
		mapped_strings[string_t(owned_strings[802])] = 8245;
		mapped_strings[string_t(owned_strings[803])] = 728;
		mapped_strings[string_t(owned_strings[804])] = 166;
		mapped_strings[string_t(owned_strings[805])] = 166;
		mapped_strings[string_t(owned_strings[806])] = 119991;
		mapped_strings[string_t(owned_strings[807])] = 8271;
		mapped_strings[string_t(owned_strings[808])] = 8765;
		mapped_strings[string_t(owned_strings[809])] = 8909;
		mapped_strings[string_t(owned_strings[810])] = 92;
		mapped_strings[string_t(owned_strings[811])] = 10693;
		mapped_strings[string_t(owned_strings[812])] = 10184;
		mapped_strings[string_t(owned_strings[813])] = 8226;
		mapped_strings[string_t(owned_strings[814])] = 8226;
		mapped_strings[string_t(owned_strings[815])] = 8782;
		mapped_strings[string_t(owned_strings[816])] = 10926;
		mapped_strings[string_t(owned_strings[817])] = 8783;
		mapped_strings[string_t(owned_strings[818])] = 8783;
		mapped_strings[string_t(owned_strings[819])] = 263;
		mapped_strings[string_t(owned_strings[820])] = 8745;
		mapped_strings[string_t(owned_strings[821])] = 10820;
		mapped_strings[string_t(owned_strings[822])] = 10825;
		mapped_strings[string_t(owned_strings[823])] = 10827;
		mapped_strings[string_t(owned_strings[824])] = 10823;
		mapped_strings[string_t(owned_strings[825])] = 10816;
		mapped_strings[string_t(owned_strings[826])] = 8745;
		mapped_strings[string_t(owned_strings[827])] = 8257;
		mapped_strings[string_t(owned_strings[828])] = 711;
		mapped_strings[string_t(owned_strings[829])] = 10829;
		mapped_strings[string_t(owned_strings[830])] = 269;
		mapped_strings[string_t(owned_strings[831])] = 231;
		mapped_strings[string_t(owned_strings[832])] = 231;
		mapped_strings[string_t(owned_strings[833])] = 265;
		mapped_strings[string_t(owned_strings[834])] = 10828;
		mapped_strings[string_t(owned_strings[835])] = 10832;
		mapped_strings[string_t(owned_strings[836])] = 267;
		mapped_strings[string_t(owned_strings[837])] = 184;
		mapped_strings[string_t(owned_strings[838])] = 184;
		mapped_strings[string_t(owned_strings[839])] = 10674;
		mapped_strings[string_t(owned_strings[840])] = 162;
		mapped_strings[string_t(owned_strings[841])] = 162;
		mapped_strings[string_t(owned_strings[842])] = 183;
		mapped_strings[string_t(owned_strings[843])] = 120096;
		mapped_strings[string_t(owned_strings[844])] = 1095;
		mapped_strings[string_t(owned_strings[845])] = 10003;
		mapped_strings[string_t(owned_strings[846])] = 10003;
		mapped_strings[string_t(owned_strings[847])] = 967;
		mapped_strings[string_t(owned_strings[848])] = 9675;
		mapped_strings[string_t(owned_strings[849])] = 10691;
		mapped_strings[string_t(owned_strings[850])] = 710;
		mapped_strings[string_t(owned_strings[851])] = 8791;
		mapped_strings[string_t(owned_strings[852])] = 8634;
		mapped_strings[string_t(owned_strings[853])] = 8635;
		mapped_strings[string_t(owned_strings[854])] = 174;
		mapped_strings[string_t(owned_strings[855])] = 9416;
		mapped_strings[string_t(owned_strings[856])] = 8859;
		mapped_strings[string_t(owned_strings[857])] = 8858;
		mapped_strings[string_t(owned_strings[858])] = 8861;
		mapped_strings[string_t(owned_strings[859])] = 8791;
		mapped_strings[string_t(owned_strings[860])] = 10768;
		mapped_strings[string_t(owned_strings[861])] = 10991;
		mapped_strings[string_t(owned_strings[862])] = 10690;
		mapped_strings[string_t(owned_strings[863])] = 9827;
		mapped_strings[string_t(owned_strings[864])] = 9827;
		mapped_strings[string_t(owned_strings[865])] = 58;
		mapped_strings[string_t(owned_strings[866])] = 8788;
		mapped_strings[string_t(owned_strings[867])] = 8788;
		mapped_strings[string_t(owned_strings[868])] = 44;
		mapped_strings[string_t(owned_strings[869])] = 64;
		mapped_strings[string_t(owned_strings[870])] = 8705;
		mapped_strings[string_t(owned_strings[871])] = 8728;
		mapped_strings[string_t(owned_strings[872])] = 8705;
		mapped_strings[string_t(owned_strings[873])] = 8450;
		mapped_strings[string_t(owned_strings[874])] = 8773;
		mapped_strings[string_t(owned_strings[875])] = 10861;
		mapped_strings[string_t(owned_strings[876])] = 8750;
		mapped_strings[string_t(owned_strings[877])] = 120148;
		mapped_strings[string_t(owned_strings[878])] = 8720;
		mapped_strings[string_t(owned_strings[879])] = 169;
		mapped_strings[string_t(owned_strings[880])] = 169;
		mapped_strings[string_t(owned_strings[881])] = 8471;
		mapped_strings[string_t(owned_strings[882])] = 8629;
		mapped_strings[string_t(owned_strings[883])] = 10007;
		mapped_strings[string_t(owned_strings[884])] = 119992;
		mapped_strings[string_t(owned_strings[885])] = 10959;
		mapped_strings[string_t(owned_strings[886])] = 10961;
		mapped_strings[string_t(owned_strings[887])] = 10960;
		mapped_strings[string_t(owned_strings[888])] = 10962;
		mapped_strings[string_t(owned_strings[889])] = 8943;
		mapped_strings[string_t(owned_strings[890])] = 10552;
		mapped_strings[string_t(owned_strings[891])] = 10549;
		mapped_strings[string_t(owned_strings[892])] = 8926;
		mapped_strings[string_t(owned_strings[893])] = 8927;
		mapped_strings[string_t(owned_strings[894])] = 8630;
		mapped_strings[string_t(owned_strings[895])] = 10557;
		mapped_strings[string_t(owned_strings[896])] = 8746;
		mapped_strings[string_t(owned_strings[897])] = 10824;
		mapped_strings[string_t(owned_strings[898])] = 10822;
		mapped_strings[string_t(owned_strings[899])] = 10826;
		mapped_strings[string_t(owned_strings[900])] = 8845;
		mapped_strings[string_t(owned_strings[901])] = 10821;
		mapped_strings[string_t(owned_strings[902])] = 8746;
		mapped_strings[string_t(owned_strings[903])] = 8631;
		mapped_strings[string_t(owned_strings[904])] = 10556;
		mapped_strings[string_t(owned_strings[905])] = 8926;
		mapped_strings[string_t(owned_strings[906])] = 8927;
		mapped_strings[string_t(owned_strings[907])] = 8910;
		mapped_strings[string_t(owned_strings[908])] = 8911;
		mapped_strings[string_t(owned_strings[909])] = 164;
		mapped_strings[string_t(owned_strings[910])] = 164;
		mapped_strings[string_t(owned_strings[911])] = 8630;
		mapped_strings[string_t(owned_strings[912])] = 8631;
		mapped_strings[string_t(owned_strings[913])] = 8910;
		mapped_strings[string_t(owned_strings[914])] = 8911;
		mapped_strings[string_t(owned_strings[915])] = 8754;
		mapped_strings[string_t(owned_strings[916])] = 8753;
		mapped_strings[string_t(owned_strings[917])] = 9005;
		mapped_strings[string_t(owned_strings[918])] = 8659;
		mapped_strings[string_t(owned_strings[919])] = 10597;
		mapped_strings[string_t(owned_strings[920])] = 8224;
		mapped_strings[string_t(owned_strings[921])] = 8504;
		mapped_strings[string_t(owned_strings[922])] = 8595;
		mapped_strings[string_t(owned_strings[923])] = 8208;
		mapped_strings[string_t(owned_strings[924])] = 8867;
		mapped_strings[string_t(owned_strings[925])] = 10511;
		mapped_strings[string_t(owned_strings[926])] = 733;
		mapped_strings[string_t(owned_strings[927])] = 271;
		mapped_strings[string_t(owned_strings[928])] = 1076;
		mapped_strings[string_t(owned_strings[929])] = 8518;
		mapped_strings[string_t(owned_strings[930])] = 8225;
		mapped_strings[string_t(owned_strings[931])] = 8650;
		mapped_strings[string_t(owned_strings[932])] = 10871;
		mapped_strings[string_t(owned_strings[933])] = 176;
		mapped_strings[string_t(owned_strings[934])] = 176;
		mapped_strings[string_t(owned_strings[935])] = 948;
		mapped_strings[string_t(owned_strings[936])] = 10673;
		mapped_strings[string_t(owned_strings[937])] = 10623;
		mapped_strings[string_t(owned_strings[938])] = 120097;
		mapped_strings[string_t(owned_strings[939])] = 8643;
		mapped_strings[string_t(owned_strings[940])] = 8642;
		mapped_strings[string_t(owned_strings[941])] = 8900;
		mapped_strings[string_t(owned_strings[942])] = 8900;
		mapped_strings[string_t(owned_strings[943])] = 9830;
		mapped_strings[string_t(owned_strings[944])] = 9830;
		mapped_strings[string_t(owned_strings[945])] = 168;
		mapped_strings[string_t(owned_strings[946])] = 989;
		mapped_strings[string_t(owned_strings[947])] = 8946;
		mapped_strings[string_t(owned_strings[948])] = 247;
		mapped_strings[string_t(owned_strings[949])] = 247;
		mapped_strings[string_t(owned_strings[950])] = 247;
		mapped_strings[string_t(owned_strings[951])] = 8903;
		mapped_strings[string_t(owned_strings[952])] = 8903;
		mapped_strings[string_t(owned_strings[953])] = 1106;
		mapped_strings[string_t(owned_strings[954])] = 8990;
		mapped_strings[string_t(owned_strings[955])] = 8973;
		mapped_strings[string_t(owned_strings[956])] = 36;
		mapped_strings[string_t(owned_strings[957])] = 120149;
		mapped_strings[string_t(owned_strings[958])] = 729;
		mapped_strings[string_t(owned_strings[959])] = 8784;
		mapped_strings[string_t(owned_strings[960])] = 8785;
		mapped_strings[string_t(owned_strings[961])] = 8760;
		mapped_strings[string_t(owned_strings[962])] = 8724;
		mapped_strings[string_t(owned_strings[963])] = 8865;
		mapped_strings[string_t(owned_strings[964])] = 8966;
		mapped_strings[string_t(owned_strings[965])] = 8595;
		mapped_strings[string_t(owned_strings[966])] = 8650;
		mapped_strings[string_t(owned_strings[967])] = 8643;
		mapped_strings[string_t(owned_strings[968])] = 8642;
		mapped_strings[string_t(owned_strings[969])] = 10512;
		mapped_strings[string_t(owned_strings[970])] = 8991;
		mapped_strings[string_t(owned_strings[971])] = 8972;
		mapped_strings[string_t(owned_strings[972])] = 119993;
		mapped_strings[string_t(owned_strings[973])] = 1109;
		mapped_strings[string_t(owned_strings[974])] = 10742;
		mapped_strings[string_t(owned_strings[975])] = 273;
		mapped_strings[string_t(owned_strings[976])] = 8945;
		mapped_strings[string_t(owned_strings[977])] = 9663;
		mapped_strings[string_t(owned_strings[978])] = 9662;
		mapped_strings[string_t(owned_strings[979])] = 8693;
		mapped_strings[string_t(owned_strings[980])] = 10607;
		mapped_strings[string_t(owned_strings[981])] = 10662;
		mapped_strings[string_t(owned_strings[982])] = 1119;
		mapped_strings[string_t(owned_strings[983])] = 10239;
		mapped_strings[string_t(owned_strings[984])] = 10871;
		mapped_strings[string_t(owned_strings[985])] = 8785;
		mapped_strings[string_t(owned_strings[986])] = 233;
		mapped_strings[string_t(owned_strings[987])] = 233;
		mapped_strings[string_t(owned_strings[988])] = 10862;
		mapped_strings[string_t(owned_strings[989])] = 283;
		mapped_strings[string_t(owned_strings[990])] = 8790;
		mapped_strings[string_t(owned_strings[991])] = 234;
		mapped_strings[string_t(owned_strings[992])] = 234;
		mapped_strings[string_t(owned_strings[993])] = 8789;
		mapped_strings[string_t(owned_strings[994])] = 1101;
		mapped_strings[string_t(owned_strings[995])] = 279;
		mapped_strings[string_t(owned_strings[996])] = 8519;
		mapped_strings[string_t(owned_strings[997])] = 8786;
		mapped_strings[string_t(owned_strings[998])] = 120098;
		mapped_strings[string_t(owned_strings[999])] = 10906;
		mapped_strings[string_t(owned_strings[1000])] = 232;
		mapped_strings[string_t(owned_strings[1001])] = 232;
		mapped_strings[string_t(owned_strings[1002])] = 10902;
		mapped_strings[string_t(owned_strings[1003])] = 10904;
		mapped_strings[string_t(owned_strings[1004])] = 10905;
		mapped_strings[string_t(owned_strings[1005])] = 9191;
		mapped_strings[string_t(owned_strings[1006])] = 8467;
		mapped_strings[string_t(owned_strings[1007])] = 10901;
		mapped_strings[string_t(owned_strings[1008])] = 10903;
		mapped_strings[string_t(owned_strings[1009])] = 275;
		mapped_strings[string_t(owned_strings[1010])] = 8709;
		mapped_strings[string_t(owned_strings[1011])] = 8709;
		mapped_strings[string_t(owned_strings[1012])] = 8709;
		mapped_strings[string_t(owned_strings[1013])] = 8196;
		mapped_strings[string_t(owned_strings[1014])] = 8197;
		mapped_strings[string_t(owned_strings[1015])] = 8195;
		mapped_strings[string_t(owned_strings[1016])] = 331;
		mapped_strings[string_t(owned_strings[1017])] = 8194;
		mapped_strings[string_t(owned_strings[1018])] = 281;
		mapped_strings[string_t(owned_strings[1019])] = 120150;
		mapped_strings[string_t(owned_strings[1020])] = 8917;
		mapped_strings[string_t(owned_strings[1021])] = 10723;
		mapped_strings[string_t(owned_strings[1022])] = 10865;
		mapped_strings[string_t(owned_strings[1023])] = 949;
		mapped_strings[string_t(owned_strings[1024])] = 949;
		mapped_strings[string_t(owned_strings[1025])] = 1013;
		mapped_strings[string_t(owned_strings[1026])] = 8790;
		mapped_strings[string_t(owned_strings[1027])] = 8789;
		mapped_strings[string_t(owned_strings[1028])] = 8770;
		mapped_strings[string_t(owned_strings[1029])] = 10902;
		mapped_strings[string_t(owned_strings[1030])] = 10901;
		mapped_strings[string_t(owned_strings[1031])] = 61;
		mapped_strings[string_t(owned_strings[1032])] = 8799;
		mapped_strings[string_t(owned_strings[1033])] = 8801;
		mapped_strings[string_t(owned_strings[1034])] = 10872;
		mapped_strings[string_t(owned_strings[1035])] = 10725;
		mapped_strings[string_t(owned_strings[1036])] = 8787;
		mapped_strings[string_t(owned_strings[1037])] = 10609;
		mapped_strings[string_t(owned_strings[1038])] = 8495;
		mapped_strings[string_t(owned_strings[1039])] = 8784;
		mapped_strings[string_t(owned_strings[1040])] = 8770;
		mapped_strings[string_t(owned_strings[1041])] = 951;
		mapped_strings[string_t(owned_strings[1042])] = 240;
		mapped_strings[string_t(owned_strings[1043])] = 240;
		mapped_strings[string_t(owned_strings[1044])] = 235;
		mapped_strings[string_t(owned_strings[1045])] = 235;
		mapped_strings[string_t(owned_strings[1046])] = 8364;
		mapped_strings[string_t(owned_strings[1047])] = 33;
		mapped_strings[string_t(owned_strings[1048])] = 8707;
		mapped_strings[string_t(owned_strings[1049])] = 8496;
		mapped_strings[string_t(owned_strings[1050])] = 8519;
		mapped_strings[string_t(owned_strings[1051])] = 8786;
		mapped_strings[string_t(owned_strings[1052])] = 1092;
		mapped_strings[string_t(owned_strings[1053])] = 9792;
		mapped_strings[string_t(owned_strings[1054])] = 64259;
		mapped_strings[string_t(owned_strings[1055])] = 64256;
		mapped_strings[string_t(owned_strings[1056])] = 64260;
		mapped_strings[string_t(owned_strings[1057])] = 120099;
		mapped_strings[string_t(owned_strings[1058])] = 64257;
		mapped_strings[string_t(owned_strings[1059])] = 102;
		mapped_strings[string_t(owned_strings[1060])] = 9837;
		mapped_strings[string_t(owned_strings[1061])] = 64258;
		mapped_strings[string_t(owned_strings[1062])] = 9649;
		mapped_strings[string_t(owned_strings[1063])] = 402;
		mapped_strings[string_t(owned_strings[1064])] = 120151;
		mapped_strings[string_t(owned_strings[1065])] = 8704;
		mapped_strings[string_t(owned_strings[1066])] = 8916;
		mapped_strings[string_t(owned_strings[1067])] = 10969;
		mapped_strings[string_t(owned_strings[1068])] = 10765;
		mapped_strings[string_t(owned_strings[1069])] = 189;
		mapped_strings[string_t(owned_strings[1070])] = 189;
		mapped_strings[string_t(owned_strings[1071])] = 8531;
		mapped_strings[string_t(owned_strings[1072])] = 188;
		mapped_strings[string_t(owned_strings[1073])] = 188;
		mapped_strings[string_t(owned_strings[1074])] = 8533;
		mapped_strings[string_t(owned_strings[1075])] = 8537;
		mapped_strings[string_t(owned_strings[1076])] = 8539;
		mapped_strings[string_t(owned_strings[1077])] = 8532;
		mapped_strings[string_t(owned_strings[1078])] = 8534;
		mapped_strings[string_t(owned_strings[1079])] = 190;
		mapped_strings[string_t(owned_strings[1080])] = 190;
		mapped_strings[string_t(owned_strings[1081])] = 8535;
		mapped_strings[string_t(owned_strings[1082])] = 8540;
		mapped_strings[string_t(owned_strings[1083])] = 8536;
		mapped_strings[string_t(owned_strings[1084])] = 8538;
		mapped_strings[string_t(owned_strings[1085])] = 8541;
		mapped_strings[string_t(owned_strings[1086])] = 8542;
		mapped_strings[string_t(owned_strings[1087])] = 8260;
		mapped_strings[string_t(owned_strings[1088])] = 8994;
		mapped_strings[string_t(owned_strings[1089])] = 119995;
		mapped_strings[string_t(owned_strings[1090])] = 8807;
		mapped_strings[string_t(owned_strings[1091])] = 10892;
		mapped_strings[string_t(owned_strings[1092])] = 501;
		mapped_strings[string_t(owned_strings[1093])] = 947;
		mapped_strings[string_t(owned_strings[1094])] = 989;
		mapped_strings[string_t(owned_strings[1095])] = 10886;
		mapped_strings[string_t(owned_strings[1096])] = 287;
		mapped_strings[string_t(owned_strings[1097])] = 285;
		mapped_strings[string_t(owned_strings[1098])] = 1075;
		mapped_strings[string_t(owned_strings[1099])] = 289;
		mapped_strings[string_t(owned_strings[1100])] = 8805;
		mapped_strings[string_t(owned_strings[1101])] = 8923;
		mapped_strings[string_t(owned_strings[1102])] = 8805;
		mapped_strings[string_t(owned_strings[1103])] = 8807;
		mapped_strings[string_t(owned_strings[1104])] = 10878;
		mapped_strings[string_t(owned_strings[1105])] = 10878;
		mapped_strings[string_t(owned_strings[1106])] = 10921;
		mapped_strings[string_t(owned_strings[1107])] = 10880;
		mapped_strings[string_t(owned_strings[1108])] = 10882;
		mapped_strings[string_t(owned_strings[1109])] = 10884;
		mapped_strings[string_t(owned_strings[1110])] = 8923;
		mapped_strings[string_t(owned_strings[1111])] = 10900;
		mapped_strings[string_t(owned_strings[1112])] = 120100;
		mapped_strings[string_t(owned_strings[1113])] = 8811;
		mapped_strings[string_t(owned_strings[1114])] = 8921;
		mapped_strings[string_t(owned_strings[1115])] = 8503;
		mapped_strings[string_t(owned_strings[1116])] = 1107;
		mapped_strings[string_t(owned_strings[1117])] = 8823;
		mapped_strings[string_t(owned_strings[1118])] = 10898;
		mapped_strings[string_t(owned_strings[1119])] = 10917;
		mapped_strings[string_t(owned_strings[1120])] = 10916;
		mapped_strings[string_t(owned_strings[1121])] = 8809;
		mapped_strings[string_t(owned_strings[1122])] = 10890;
		mapped_strings[string_t(owned_strings[1123])] = 10890;
		mapped_strings[string_t(owned_strings[1124])] = 10888;
		mapped_strings[string_t(owned_strings[1125])] = 10888;
		mapped_strings[string_t(owned_strings[1126])] = 8809;
		mapped_strings[string_t(owned_strings[1127])] = 8935;
		mapped_strings[string_t(owned_strings[1128])] = 120152;
		mapped_strings[string_t(owned_strings[1129])] = 96;
		mapped_strings[string_t(owned_strings[1130])] = 8458;
		mapped_strings[string_t(owned_strings[1131])] = 8819;
		mapped_strings[string_t(owned_strings[1132])] = 10894;
		mapped_strings[string_t(owned_strings[1133])] = 10896;
		mapped_strings[string_t(owned_strings[1134])] = 62;
		mapped_strings[string_t(owned_strings[1135])] = 62;
		mapped_strings[string_t(owned_strings[1136])] = 10919;
		mapped_strings[string_t(owned_strings[1137])] = 10874;
		mapped_strings[string_t(owned_strings[1138])] = 8919;
		mapped_strings[string_t(owned_strings[1139])] = 10645;
		mapped_strings[string_t(owned_strings[1140])] = 10876;
		mapped_strings[string_t(owned_strings[1141])] = 10886;
		mapped_strings[string_t(owned_strings[1142])] = 10616;
		mapped_strings[string_t(owned_strings[1143])] = 8919;
		mapped_strings[string_t(owned_strings[1144])] = 8923;
		mapped_strings[string_t(owned_strings[1145])] = 10892;
		mapped_strings[string_t(owned_strings[1146])] = 8823;
		mapped_strings[string_t(owned_strings[1147])] = 8819;
		mapped_strings[string_t(owned_strings[1148])] = 8809;
		mapped_strings[string_t(owned_strings[1149])] = 8809;
		mapped_strings[string_t(owned_strings[1150])] = 8660;
		mapped_strings[string_t(owned_strings[1151])] = 8202;
		mapped_strings[string_t(owned_strings[1152])] = 189;
		mapped_strings[string_t(owned_strings[1153])] = 8459;
		mapped_strings[string_t(owned_strings[1154])] = 1098;
		mapped_strings[string_t(owned_strings[1155])] = 8596;
		mapped_strings[string_t(owned_strings[1156])] = 10568;
		mapped_strings[string_t(owned_strings[1157])] = 8621;
		mapped_strings[string_t(owned_strings[1158])] = 8463;
		mapped_strings[string_t(owned_strings[1159])] = 293;
		mapped_strings[string_t(owned_strings[1160])] = 9829;
		mapped_strings[string_t(owned_strings[1161])] = 9829;
		mapped_strings[string_t(owned_strings[1162])] = 8230;
		mapped_strings[string_t(owned_strings[1163])] = 8889;
		mapped_strings[string_t(owned_strings[1164])] = 120101;
		mapped_strings[string_t(owned_strings[1165])] = 10533;
		mapped_strings[string_t(owned_strings[1166])] = 10534;
		mapped_strings[string_t(owned_strings[1167])] = 8703;
		mapped_strings[string_t(owned_strings[1168])] = 8763;
		mapped_strings[string_t(owned_strings[1169])] = 8617;
		mapped_strings[string_t(owned_strings[1170])] = 8618;
		mapped_strings[string_t(owned_strings[1171])] = 120153;
		mapped_strings[string_t(owned_strings[1172])] = 8213;
		mapped_strings[string_t(owned_strings[1173])] = 119997;
		mapped_strings[string_t(owned_strings[1174])] = 8463;
		mapped_strings[string_t(owned_strings[1175])] = 295;
		mapped_strings[string_t(owned_strings[1176])] = 8259;
		mapped_strings[string_t(owned_strings[1177])] = 8208;
		mapped_strings[string_t(owned_strings[1178])] = 237;
		mapped_strings[string_t(owned_strings[1179])] = 237;
		mapped_strings[string_t(owned_strings[1180])] = 8291;
		mapped_strings[string_t(owned_strings[1181])] = 238;
		mapped_strings[string_t(owned_strings[1182])] = 238;
		mapped_strings[string_t(owned_strings[1183])] = 1080;
		mapped_strings[string_t(owned_strings[1184])] = 1077;
		mapped_strings[string_t(owned_strings[1185])] = 161;
		mapped_strings[string_t(owned_strings[1186])] = 161;
		mapped_strings[string_t(owned_strings[1187])] = 8660;
		mapped_strings[string_t(owned_strings[1188])] = 120102;
		mapped_strings[string_t(owned_strings[1189])] = 236;
		mapped_strings[string_t(owned_strings[1190])] = 236;
		mapped_strings[string_t(owned_strings[1191])] = 8520;
		mapped_strings[string_t(owned_strings[1192])] = 10764;
		mapped_strings[string_t(owned_strings[1193])] = 8749;
		mapped_strings[string_t(owned_strings[1194])] = 10716;
		mapped_strings[string_t(owned_strings[1195])] = 8489;
		mapped_strings[string_t(owned_strings[1196])] = 307;
		mapped_strings[string_t(owned_strings[1197])] = 299;
		mapped_strings[string_t(owned_strings[1198])] = 8465;
		mapped_strings[string_t(owned_strings[1199])] = 8464;
		mapped_strings[string_t(owned_strings[1200])] = 8465;
		mapped_strings[string_t(owned_strings[1201])] = 305;
		mapped_strings[string_t(owned_strings[1202])] = 8887;
		mapped_strings[string_t(owned_strings[1203])] = 437;
		mapped_strings[string_t(owned_strings[1204])] = 8712;
		mapped_strings[string_t(owned_strings[1205])] = 8453;
		mapped_strings[string_t(owned_strings[1206])] = 8734;
		mapped_strings[string_t(owned_strings[1207])] = 10717;
		mapped_strings[string_t(owned_strings[1208])] = 305;
		mapped_strings[string_t(owned_strings[1209])] = 8747;
		mapped_strings[string_t(owned_strings[1210])] = 8890;
		mapped_strings[string_t(owned_strings[1211])] = 8484;
		mapped_strings[string_t(owned_strings[1212])] = 8890;
		mapped_strings[string_t(owned_strings[1213])] = 10775;
		mapped_strings[string_t(owned_strings[1214])] = 10812;
		mapped_strings[string_t(owned_strings[1215])] = 1105;
		mapped_strings[string_t(owned_strings[1216])] = 303;
		mapped_strings[string_t(owned_strings[1217])] = 120154;
		mapped_strings[string_t(owned_strings[1218])] = 953;
		mapped_strings[string_t(owned_strings[1219])] = 10812;
		mapped_strings[string_t(owned_strings[1220])] = 191;
		mapped_strings[string_t(owned_strings[1221])] = 191;
		mapped_strings[string_t(owned_strings[1222])] = 119998;
		mapped_strings[string_t(owned_strings[1223])] = 8712;
		mapped_strings[string_t(owned_strings[1224])] = 8953;
		mapped_strings[string_t(owned_strings[1225])] = 8949;
		mapped_strings[string_t(owned_strings[1226])] = 8948;
		mapped_strings[string_t(owned_strings[1227])] = 8947;
		mapped_strings[string_t(owned_strings[1228])] = 8712;
		mapped_strings[string_t(owned_strings[1229])] = 8290;
		mapped_strings[string_t(owned_strings[1230])] = 297;
		mapped_strings[string_t(owned_strings[1231])] = 1110;
		mapped_strings[string_t(owned_strings[1232])] = 239;
		mapped_strings[string_t(owned_strings[1233])] = 239;
		mapped_strings[string_t(owned_strings[1234])] = 309;
		mapped_strings[string_t(owned_strings[1235])] = 1081;
		mapped_strings[string_t(owned_strings[1236])] = 120103;
		mapped_strings[string_t(owned_strings[1237])] = 567;
		mapped_strings[string_t(owned_strings[1238])] = 120155;
		mapped_strings[string_t(owned_strings[1239])] = 119999;
		mapped_strings[string_t(owned_strings[1240])] = 1112;
		mapped_strings[string_t(owned_strings[1241])] = 1108;
		mapped_strings[string_t(owned_strings[1242])] = 954;
		mapped_strings[string_t(owned_strings[1243])] = 1008;
		mapped_strings[string_t(owned_strings[1244])] = 311;
		mapped_strings[string_t(owned_strings[1245])] = 1082;
		mapped_strings[string_t(owned_strings[1246])] = 120104;
		mapped_strings[string_t(owned_strings[1247])] = 312;
		mapped_strings[string_t(owned_strings[1248])] = 1093;
		mapped_strings[string_t(owned_strings[1249])] = 1116;
		mapped_strings[string_t(owned_strings[1250])] = 120156;
		mapped_strings[string_t(owned_strings[1251])] = 120000;
		mapped_strings[string_t(owned_strings[1252])] = 8666;
		mapped_strings[string_t(owned_strings[1253])] = 8656;
		mapped_strings[string_t(owned_strings[1254])] = 10523;
		mapped_strings[string_t(owned_strings[1255])] = 10510;
		mapped_strings[string_t(owned_strings[1256])] = 8806;
		mapped_strings[string_t(owned_strings[1257])] = 10891;
		mapped_strings[string_t(owned_strings[1258])] = 10594;
		mapped_strings[string_t(owned_strings[1259])] = 314;
		mapped_strings[string_t(owned_strings[1260])] = 10676;
		mapped_strings[string_t(owned_strings[1261])] = 8466;
		mapped_strings[string_t(owned_strings[1262])] = 955;
		mapped_strings[string_t(owned_strings[1263])] = 10216;
		mapped_strings[string_t(owned_strings[1264])] = 10641;
		mapped_strings[string_t(owned_strings[1265])] = 10216;
		mapped_strings[string_t(owned_strings[1266])] = 10885;
		mapped_strings[string_t(owned_strings[1267])] = 171;
		mapped_strings[string_t(owned_strings[1268])] = 171;
		mapped_strings[string_t(owned_strings[1269])] = 8592;
		mapped_strings[string_t(owned_strings[1270])] = 8676;
		mapped_strings[string_t(owned_strings[1271])] = 10527;
		mapped_strings[string_t(owned_strings[1272])] = 10525;
		mapped_strings[string_t(owned_strings[1273])] = 8617;
		mapped_strings[string_t(owned_strings[1274])] = 8619;
		mapped_strings[string_t(owned_strings[1275])] = 10553;
		mapped_strings[string_t(owned_strings[1276])] = 10611;
		mapped_strings[string_t(owned_strings[1277])] = 8610;
		mapped_strings[string_t(owned_strings[1278])] = 10923;
		mapped_strings[string_t(owned_strings[1279])] = 10521;
		mapped_strings[string_t(owned_strings[1280])] = 10925;
		mapped_strings[string_t(owned_strings[1281])] = 10925;
		mapped_strings[string_t(owned_strings[1282])] = 10508;
		mapped_strings[string_t(owned_strings[1283])] = 10098;
		mapped_strings[string_t(owned_strings[1284])] = 123;
		mapped_strings[string_t(owned_strings[1285])] = 91;
		mapped_strings[string_t(owned_strings[1286])] = 10635;
		mapped_strings[string_t(owned_strings[1287])] = 10639;
		mapped_strings[string_t(owned_strings[1288])] = 10637;
		mapped_strings[string_t(owned_strings[1289])] = 318;
		mapped_strings[string_t(owned_strings[1290])] = 316;
		mapped_strings[string_t(owned_strings[1291])] = 8968;
		mapped_strings[string_t(owned_strings[1292])] = 123;
		mapped_strings[string_t(owned_strings[1293])] = 1083;
		mapped_strings[string_t(owned_strings[1294])] = 10550;
		mapped_strings[string_t(owned_strings[1295])] = 8220;
		mapped_strings[string_t(owned_strings[1296])] = 8222;
		mapped_strings[string_t(owned_strings[1297])] = 10599;
		mapped_strings[string_t(owned_strings[1298])] = 10571;
		mapped_strings[string_t(owned_strings[1299])] = 8626;
		mapped_strings[string_t(owned_strings[1300])] = 8804;
		mapped_strings[string_t(owned_strings[1301])] = 8592;
		mapped_strings[string_t(owned_strings[1302])] = 8610;
		mapped_strings[string_t(owned_strings[1303])] = 8637;
		mapped_strings[string_t(owned_strings[1304])] = 8636;
		mapped_strings[string_t(owned_strings[1305])] = 8647;
		mapped_strings[string_t(owned_strings[1306])] = 8596;
		mapped_strings[string_t(owned_strings[1307])] = 8646;
		mapped_strings[string_t(owned_strings[1308])] = 8651;
		mapped_strings[string_t(owned_strings[1309])] = 8621;
		mapped_strings[string_t(owned_strings[1310])] = 8907;
		mapped_strings[string_t(owned_strings[1311])] = 8922;
		mapped_strings[string_t(owned_strings[1312])] = 8804;
		mapped_strings[string_t(owned_strings[1313])] = 8806;
		mapped_strings[string_t(owned_strings[1314])] = 10877;
		mapped_strings[string_t(owned_strings[1315])] = 10877;
		mapped_strings[string_t(owned_strings[1316])] = 10920;
		mapped_strings[string_t(owned_strings[1317])] = 10879;
		mapped_strings[string_t(owned_strings[1318])] = 10881;
		mapped_strings[string_t(owned_strings[1319])] = 10883;
		mapped_strings[string_t(owned_strings[1320])] = 8922;
		mapped_strings[string_t(owned_strings[1321])] = 10899;
		mapped_strings[string_t(owned_strings[1322])] = 10885;
		mapped_strings[string_t(owned_strings[1323])] = 8918;
		mapped_strings[string_t(owned_strings[1324])] = 8922;
		mapped_strings[string_t(owned_strings[1325])] = 10891;
		mapped_strings[string_t(owned_strings[1326])] = 8822;
		mapped_strings[string_t(owned_strings[1327])] = 8818;
		mapped_strings[string_t(owned_strings[1328])] = 10620;
		mapped_strings[string_t(owned_strings[1329])] = 8970;
		mapped_strings[string_t(owned_strings[1330])] = 120105;
		mapped_strings[string_t(owned_strings[1331])] = 8822;
		mapped_strings[string_t(owned_strings[1332])] = 10897;
		mapped_strings[string_t(owned_strings[1333])] = 8637;
		mapped_strings[string_t(owned_strings[1334])] = 8636;
		mapped_strings[string_t(owned_strings[1335])] = 10602;
		mapped_strings[string_t(owned_strings[1336])] = 9604;
		mapped_strings[string_t(owned_strings[1337])] = 1113;
		mapped_strings[string_t(owned_strings[1338])] = 8810;
		mapped_strings[string_t(owned_strings[1339])] = 8647;
		mapped_strings[string_t(owned_strings[1340])] = 8990;
		mapped_strings[string_t(owned_strings[1341])] = 10603;
		mapped_strings[string_t(owned_strings[1342])] = 9722;
		mapped_strings[string_t(owned_strings[1343])] = 320;
		mapped_strings[string_t(owned_strings[1344])] = 9136;
		mapped_strings[string_t(owned_strings[1345])] = 9136;
		mapped_strings[string_t(owned_strings[1346])] = 8808;
		mapped_strings[string_t(owned_strings[1347])] = 10889;
		mapped_strings[string_t(owned_strings[1348])] = 10889;
		mapped_strings[string_t(owned_strings[1349])] = 10887;
		mapped_strings[string_t(owned_strings[1350])] = 10887;
		mapped_strings[string_t(owned_strings[1351])] = 8808;
		mapped_strings[string_t(owned_strings[1352])] = 8934;
		mapped_strings[string_t(owned_strings[1353])] = 10220;
		mapped_strings[string_t(owned_strings[1354])] = 8701;
		mapped_strings[string_t(owned_strings[1355])] = 10214;
		mapped_strings[string_t(owned_strings[1356])] = 10229;
		mapped_strings[string_t(owned_strings[1357])] = 10231;
		mapped_strings[string_t(owned_strings[1358])] = 10236;
		mapped_strings[string_t(owned_strings[1359])] = 10230;
		mapped_strings[string_t(owned_strings[1360])] = 8619;
		mapped_strings[string_t(owned_strings[1361])] = 8620;
		mapped_strings[string_t(owned_strings[1362])] = 10629;
		mapped_strings[string_t(owned_strings[1363])] = 120157;
		mapped_strings[string_t(owned_strings[1364])] = 10797;
		mapped_strings[string_t(owned_strings[1365])] = 10804;
		mapped_strings[string_t(owned_strings[1366])] = 8727;
		mapped_strings[string_t(owned_strings[1367])] = 95;
		mapped_strings[string_t(owned_strings[1368])] = 9674;
		mapped_strings[string_t(owned_strings[1369])] = 9674;
		mapped_strings[string_t(owned_strings[1370])] = 10731;
		mapped_strings[string_t(owned_strings[1371])] = 40;
		mapped_strings[string_t(owned_strings[1372])] = 10643;
		mapped_strings[string_t(owned_strings[1373])] = 8646;
		mapped_strings[string_t(owned_strings[1374])] = 8991;
		mapped_strings[string_t(owned_strings[1375])] = 8651;
		mapped_strings[string_t(owned_strings[1376])] = 10605;
		mapped_strings[string_t(owned_strings[1377])] = 8206;
		mapped_strings[string_t(owned_strings[1378])] = 8895;
		mapped_strings[string_t(owned_strings[1379])] = 8249;
		mapped_strings[string_t(owned_strings[1380])] = 120001;
		mapped_strings[string_t(owned_strings[1381])] = 8624;
		mapped_strings[string_t(owned_strings[1382])] = 8818;
		mapped_strings[string_t(owned_strings[1383])] = 10893;
		mapped_strings[string_t(owned_strings[1384])] = 10895;
		mapped_strings[string_t(owned_strings[1385])] = 91;
		mapped_strings[string_t(owned_strings[1386])] = 8216;
		mapped_strings[string_t(owned_strings[1387])] = 8218;
		mapped_strings[string_t(owned_strings[1388])] = 322;
		mapped_strings[string_t(owned_strings[1389])] = 60;
		mapped_strings[string_t(owned_strings[1390])] = 60;
		mapped_strings[string_t(owned_strings[1391])] = 10918;
		mapped_strings[string_t(owned_strings[1392])] = 10873;
		mapped_strings[string_t(owned_strings[1393])] = 8918;
		mapped_strings[string_t(owned_strings[1394])] = 8907;
		mapped_strings[string_t(owned_strings[1395])] = 8905;
		mapped_strings[string_t(owned_strings[1396])] = 10614;
		mapped_strings[string_t(owned_strings[1397])] = 10875;
		mapped_strings[string_t(owned_strings[1398])] = 10646;
		mapped_strings[string_t(owned_strings[1399])] = 9667;
		mapped_strings[string_t(owned_strings[1400])] = 8884;
		mapped_strings[string_t(owned_strings[1401])] = 9666;
		mapped_strings[string_t(owned_strings[1402])] = 10570;
		mapped_strings[string_t(owned_strings[1403])] = 10598;
		mapped_strings[string_t(owned_strings[1404])] = 8808;
		mapped_strings[string_t(owned_strings[1405])] = 8808;
		mapped_strings[string_t(owned_strings[1406])] = 8762;
		mapped_strings[string_t(owned_strings[1407])] = 175;
		mapped_strings[string_t(owned_strings[1408])] = 175;
		mapped_strings[string_t(owned_strings[1409])] = 9794;
		mapped_strings[string_t(owned_strings[1410])] = 10016;
		mapped_strings[string_t(owned_strings[1411])] = 10016;
		mapped_strings[string_t(owned_strings[1412])] = 8614;
		mapped_strings[string_t(owned_strings[1413])] = 8614;
		mapped_strings[string_t(owned_strings[1414])] = 8615;
		mapped_strings[string_t(owned_strings[1415])] = 8612;
		mapped_strings[string_t(owned_strings[1416])] = 8613;
		mapped_strings[string_t(owned_strings[1417])] = 9646;
		mapped_strings[string_t(owned_strings[1418])] = 10793;
		mapped_strings[string_t(owned_strings[1419])] = 1084;
		mapped_strings[string_t(owned_strings[1420])] = 8212;
		mapped_strings[string_t(owned_strings[1421])] = 8737;
		mapped_strings[string_t(owned_strings[1422])] = 120106;
		mapped_strings[string_t(owned_strings[1423])] = 8487;
		mapped_strings[string_t(owned_strings[1424])] = 181;
		mapped_strings[string_t(owned_strings[1425])] = 181;
		mapped_strings[string_t(owned_strings[1426])] = 8739;
		mapped_strings[string_t(owned_strings[1427])] = 42;
		mapped_strings[string_t(owned_strings[1428])] = 10992;
		mapped_strings[string_t(owned_strings[1429])] = 183;
		mapped_strings[string_t(owned_strings[1430])] = 183;
		mapped_strings[string_t(owned_strings[1431])] = 8722;
		mapped_strings[string_t(owned_strings[1432])] = 8863;
		mapped_strings[string_t(owned_strings[1433])] = 8760;
		mapped_strings[string_t(owned_strings[1434])] = 10794;
		mapped_strings[string_t(owned_strings[1435])] = 10971;
		mapped_strings[string_t(owned_strings[1436])] = 8230;
		mapped_strings[string_t(owned_strings[1437])] = 8723;
		mapped_strings[string_t(owned_strings[1438])] = 8871;
		mapped_strings[string_t(owned_strings[1439])] = 120158;
		mapped_strings[string_t(owned_strings[1440])] = 8723;
		mapped_strings[string_t(owned_strings[1441])] = 120002;
		mapped_strings[string_t(owned_strings[1442])] = 8766;
		mapped_strings[string_t(owned_strings[1443])] = 956;
		mapped_strings[string_t(owned_strings[1444])] = 8888;
		mapped_strings[string_t(owned_strings[1445])] = 8888;
		mapped_strings[string_t(owned_strings[1446])] = 8921;
		mapped_strings[string_t(owned_strings[1447])] = 8811;
		mapped_strings[string_t(owned_strings[1448])] = 8811;
		mapped_strings[string_t(owned_strings[1449])] = 8653;
		mapped_strings[string_t(owned_strings[1450])] = 8654;
		mapped_strings[string_t(owned_strings[1451])] = 8920;
		mapped_strings[string_t(owned_strings[1452])] = 8810;
		mapped_strings[string_t(owned_strings[1453])] = 8810;
		mapped_strings[string_t(owned_strings[1454])] = 8655;
		mapped_strings[string_t(owned_strings[1455])] = 8879;
		mapped_strings[string_t(owned_strings[1456])] = 8878;
		mapped_strings[string_t(owned_strings[1457])] = 8711;
		mapped_strings[string_t(owned_strings[1458])] = 324;
		mapped_strings[string_t(owned_strings[1459])] = 8736;
		mapped_strings[string_t(owned_strings[1460])] = 8777;
		mapped_strings[string_t(owned_strings[1461])] = 10864;
		mapped_strings[string_t(owned_strings[1462])] = 8779;
		mapped_strings[string_t(owned_strings[1463])] = 329;
		mapped_strings[string_t(owned_strings[1464])] = 8777;
		mapped_strings[string_t(owned_strings[1465])] = 9838;
		mapped_strings[string_t(owned_strings[1466])] = 9838;
		mapped_strings[string_t(owned_strings[1467])] = 8469;
		mapped_strings[string_t(owned_strings[1468])] = 160;
		mapped_strings[string_t(owned_strings[1469])] = 160;
		mapped_strings[string_t(owned_strings[1470])] = 8782;
		mapped_strings[string_t(owned_strings[1471])] = 8783;
		mapped_strings[string_t(owned_strings[1472])] = 10819;
		mapped_strings[string_t(owned_strings[1473])] = 328;
		mapped_strings[string_t(owned_strings[1474])] = 326;
		mapped_strings[string_t(owned_strings[1475])] = 8775;
		mapped_strings[string_t(owned_strings[1476])] = 10861;
		mapped_strings[string_t(owned_strings[1477])] = 10818;
		mapped_strings[string_t(owned_strings[1478])] = 1085;
		mapped_strings[string_t(owned_strings[1479])] = 8211;
		mapped_strings[string_t(owned_strings[1480])] = 8800;
		mapped_strings[string_t(owned_strings[1481])] = 8663;
		mapped_strings[string_t(owned_strings[1482])] = 10532;
		mapped_strings[string_t(owned_strings[1483])] = 8599;
		mapped_strings[string_t(owned_strings[1484])] = 8599;
		mapped_strings[string_t(owned_strings[1485])] = 8784;
		mapped_strings[string_t(owned_strings[1486])] = 8802;
		mapped_strings[string_t(owned_strings[1487])] = 10536;
		mapped_strings[string_t(owned_strings[1488])] = 8770;
		mapped_strings[string_t(owned_strings[1489])] = 8708;
		mapped_strings[string_t(owned_strings[1490])] = 8708;
		mapped_strings[string_t(owned_strings[1491])] = 120107;
		mapped_strings[string_t(owned_strings[1492])] = 8807;
		mapped_strings[string_t(owned_strings[1493])] = 8817;
		mapped_strings[string_t(owned_strings[1494])] = 8817;
		mapped_strings[string_t(owned_strings[1495])] = 8807;
		mapped_strings[string_t(owned_strings[1496])] = 10878;
		mapped_strings[string_t(owned_strings[1497])] = 10878;
		mapped_strings[string_t(owned_strings[1498])] = 8821;
		mapped_strings[string_t(owned_strings[1499])] = 8815;
		mapped_strings[string_t(owned_strings[1500])] = 8815;
		mapped_strings[string_t(owned_strings[1501])] = 8654;
		mapped_strings[string_t(owned_strings[1502])] = 8622;
		mapped_strings[string_t(owned_strings[1503])] = 10994;
		mapped_strings[string_t(owned_strings[1504])] = 8715;
		mapped_strings[string_t(owned_strings[1505])] = 8956;
		mapped_strings[string_t(owned_strings[1506])] = 8954;
		mapped_strings[string_t(owned_strings[1507])] = 8715;
		mapped_strings[string_t(owned_strings[1508])] = 1114;
		mapped_strings[string_t(owned_strings[1509])] = 8653;
		mapped_strings[string_t(owned_strings[1510])] = 8806;
		mapped_strings[string_t(owned_strings[1511])] = 8602;
		mapped_strings[string_t(owned_strings[1512])] = 8229;
		mapped_strings[string_t(owned_strings[1513])] = 8816;
		mapped_strings[string_t(owned_strings[1514])] = 8602;
		mapped_strings[string_t(owned_strings[1515])] = 8622;
		mapped_strings[string_t(owned_strings[1516])] = 8816;
		mapped_strings[string_t(owned_strings[1517])] = 8806;
		mapped_strings[string_t(owned_strings[1518])] = 10877;
		mapped_strings[string_t(owned_strings[1519])] = 10877;
		mapped_strings[string_t(owned_strings[1520])] = 8814;
		mapped_strings[string_t(owned_strings[1521])] = 8820;
		mapped_strings[string_t(owned_strings[1522])] = 8814;
		mapped_strings[string_t(owned_strings[1523])] = 8938;
		mapped_strings[string_t(owned_strings[1524])] = 8940;
		mapped_strings[string_t(owned_strings[1525])] = 8740;
		mapped_strings[string_t(owned_strings[1526])] = 120159;
		mapped_strings[string_t(owned_strings[1527])] = 172;
		mapped_strings[string_t(owned_strings[1528])] = 172;
		mapped_strings[string_t(owned_strings[1529])] = 8713;
		mapped_strings[string_t(owned_strings[1530])] = 8953;
		mapped_strings[string_t(owned_strings[1531])] = 8949;
		mapped_strings[string_t(owned_strings[1532])] = 8713;
		mapped_strings[string_t(owned_strings[1533])] = 8951;
		mapped_strings[string_t(owned_strings[1534])] = 8950;
		mapped_strings[string_t(owned_strings[1535])] = 8716;
		mapped_strings[string_t(owned_strings[1536])] = 8716;
		mapped_strings[string_t(owned_strings[1537])] = 8958;
		mapped_strings[string_t(owned_strings[1538])] = 8957;
		mapped_strings[string_t(owned_strings[1539])] = 8742;
		mapped_strings[string_t(owned_strings[1540])] = 8742;
		mapped_strings[string_t(owned_strings[1541])] = 11005;
		mapped_strings[string_t(owned_strings[1542])] = 8706;
		mapped_strings[string_t(owned_strings[1543])] = 10772;
		mapped_strings[string_t(owned_strings[1544])] = 8832;
		mapped_strings[string_t(owned_strings[1545])] = 8928;
		mapped_strings[string_t(owned_strings[1546])] = 10927;
		mapped_strings[string_t(owned_strings[1547])] = 8832;
		mapped_strings[string_t(owned_strings[1548])] = 10927;
		mapped_strings[string_t(owned_strings[1549])] = 8655;
		mapped_strings[string_t(owned_strings[1550])] = 8603;
		mapped_strings[string_t(owned_strings[1551])] = 10547;
		mapped_strings[string_t(owned_strings[1552])] = 8605;
		mapped_strings[string_t(owned_strings[1553])] = 8603;
		mapped_strings[string_t(owned_strings[1554])] = 8939;
		mapped_strings[string_t(owned_strings[1555])] = 8941;
		mapped_strings[string_t(owned_strings[1556])] = 8833;
		mapped_strings[string_t(owned_strings[1557])] = 8929;
		mapped_strings[string_t(owned_strings[1558])] = 10928;
		mapped_strings[string_t(owned_strings[1559])] = 120003;
		mapped_strings[string_t(owned_strings[1560])] = 8740;
		mapped_strings[string_t(owned_strings[1561])] = 8742;
		mapped_strings[string_t(owned_strings[1562])] = 8769;
		mapped_strings[string_t(owned_strings[1563])] = 8772;
		mapped_strings[string_t(owned_strings[1564])] = 8772;
		mapped_strings[string_t(owned_strings[1565])] = 8740;
		mapped_strings[string_t(owned_strings[1566])] = 8742;
		mapped_strings[string_t(owned_strings[1567])] = 8930;
		mapped_strings[string_t(owned_strings[1568])] = 8931;
		mapped_strings[string_t(owned_strings[1569])] = 8836;
		mapped_strings[string_t(owned_strings[1570])] = 10949;
		mapped_strings[string_t(owned_strings[1571])] = 8840;
		mapped_strings[string_t(owned_strings[1572])] = 8834;
		mapped_strings[string_t(owned_strings[1573])] = 8840;
		mapped_strings[string_t(owned_strings[1574])] = 10949;
		mapped_strings[string_t(owned_strings[1575])] = 8833;
		mapped_strings[string_t(owned_strings[1576])] = 10928;
		mapped_strings[string_t(owned_strings[1577])] = 8837;
		mapped_strings[string_t(owned_strings[1578])] = 10950;
		mapped_strings[string_t(owned_strings[1579])] = 8841;
		mapped_strings[string_t(owned_strings[1580])] = 8835;
		mapped_strings[string_t(owned_strings[1581])] = 8841;
		mapped_strings[string_t(owned_strings[1582])] = 10950;
		mapped_strings[string_t(owned_strings[1583])] = 8825;
		mapped_strings[string_t(owned_strings[1584])] = 241;
		mapped_strings[string_t(owned_strings[1585])] = 241;
		mapped_strings[string_t(owned_strings[1586])] = 8824;
		mapped_strings[string_t(owned_strings[1587])] = 8938;
		mapped_strings[string_t(owned_strings[1588])] = 8940;
		mapped_strings[string_t(owned_strings[1589])] = 8939;
		mapped_strings[string_t(owned_strings[1590])] = 8941;
		mapped_strings[string_t(owned_strings[1591])] = 957;
		mapped_strings[string_t(owned_strings[1592])] = 35;
		mapped_strings[string_t(owned_strings[1593])] = 8470;
		mapped_strings[string_t(owned_strings[1594])] = 8199;
		mapped_strings[string_t(owned_strings[1595])] = 8877;
		mapped_strings[string_t(owned_strings[1596])] = 10500;
		mapped_strings[string_t(owned_strings[1597])] = 8781;
		mapped_strings[string_t(owned_strings[1598])] = 8876;
		mapped_strings[string_t(owned_strings[1599])] = 8805;
		mapped_strings[string_t(owned_strings[1600])] = 62;
		mapped_strings[string_t(owned_strings[1601])] = 10718;
		mapped_strings[string_t(owned_strings[1602])] = 10498;
		mapped_strings[string_t(owned_strings[1603])] = 8804;
		mapped_strings[string_t(owned_strings[1604])] = 60;
		mapped_strings[string_t(owned_strings[1605])] = 8884;
		mapped_strings[string_t(owned_strings[1606])] = 10499;
		mapped_strings[string_t(owned_strings[1607])] = 8885;
		mapped_strings[string_t(owned_strings[1608])] = 8764;
		mapped_strings[string_t(owned_strings[1609])] = 8662;
		mapped_strings[string_t(owned_strings[1610])] = 10531;
		mapped_strings[string_t(owned_strings[1611])] = 8598;
		mapped_strings[string_t(owned_strings[1612])] = 8598;
		mapped_strings[string_t(owned_strings[1613])] = 10535;
		mapped_strings[string_t(owned_strings[1614])] = 9416;
		mapped_strings[string_t(owned_strings[1615])] = 243;
		mapped_strings[string_t(owned_strings[1616])] = 243;
		mapped_strings[string_t(owned_strings[1617])] = 8859;
		mapped_strings[string_t(owned_strings[1618])] = 8858;
		mapped_strings[string_t(owned_strings[1619])] = 244;
		mapped_strings[string_t(owned_strings[1620])] = 244;
		mapped_strings[string_t(owned_strings[1621])] = 1086;
		mapped_strings[string_t(owned_strings[1622])] = 8861;
		mapped_strings[string_t(owned_strings[1623])] = 337;
		mapped_strings[string_t(owned_strings[1624])] = 10808;
		mapped_strings[string_t(owned_strings[1625])] = 8857;
		mapped_strings[string_t(owned_strings[1626])] = 10684;
		mapped_strings[string_t(owned_strings[1627])] = 339;
		mapped_strings[string_t(owned_strings[1628])] = 10687;
		mapped_strings[string_t(owned_strings[1629])] = 120108;
		mapped_strings[string_t(owned_strings[1630])] = 731;
		mapped_strings[string_t(owned_strings[1631])] = 242;
		mapped_strings[string_t(owned_strings[1632])] = 242;
		mapped_strings[string_t(owned_strings[1633])] = 10689;
		mapped_strings[string_t(owned_strings[1634])] = 10677;
		mapped_strings[string_t(owned_strings[1635])] = 937;
		mapped_strings[string_t(owned_strings[1636])] = 8750;
		mapped_strings[string_t(owned_strings[1637])] = 8634;
		mapped_strings[string_t(owned_strings[1638])] = 10686;
		mapped_strings[string_t(owned_strings[1639])] = 10683;
		mapped_strings[string_t(owned_strings[1640])] = 8254;
		mapped_strings[string_t(owned_strings[1641])] = 10688;
		mapped_strings[string_t(owned_strings[1642])] = 333;
		mapped_strings[string_t(owned_strings[1643])] = 969;
		mapped_strings[string_t(owned_strings[1644])] = 959;
		mapped_strings[string_t(owned_strings[1645])] = 10678;
		mapped_strings[string_t(owned_strings[1646])] = 8854;
		mapped_strings[string_t(owned_strings[1647])] = 120160;
		mapped_strings[string_t(owned_strings[1648])] = 10679;
		mapped_strings[string_t(owned_strings[1649])] = 10681;
		mapped_strings[string_t(owned_strings[1650])] = 8853;
		mapped_strings[string_t(owned_strings[1651])] = 8744;
		mapped_strings[string_t(owned_strings[1652])] = 8635;
		mapped_strings[string_t(owned_strings[1653])] = 10845;
		mapped_strings[string_t(owned_strings[1654])] = 8500;
		mapped_strings[string_t(owned_strings[1655])] = 8500;
		mapped_strings[string_t(owned_strings[1656])] = 170;
		mapped_strings[string_t(owned_strings[1657])] = 170;
		mapped_strings[string_t(owned_strings[1658])] = 186;
		mapped_strings[string_t(owned_strings[1659])] = 186;
		mapped_strings[string_t(owned_strings[1660])] = 8886;
		mapped_strings[string_t(owned_strings[1661])] = 10838;
		mapped_strings[string_t(owned_strings[1662])] = 10839;
		mapped_strings[string_t(owned_strings[1663])] = 10843;
		mapped_strings[string_t(owned_strings[1664])] = 8500;
		mapped_strings[string_t(owned_strings[1665])] = 248;
		mapped_strings[string_t(owned_strings[1666])] = 248;
		mapped_strings[string_t(owned_strings[1667])] = 8856;
		mapped_strings[string_t(owned_strings[1668])] = 245;
		mapped_strings[string_t(owned_strings[1669])] = 245;
		mapped_strings[string_t(owned_strings[1670])] = 8855;
		mapped_strings[string_t(owned_strings[1671])] = 10806;
		mapped_strings[string_t(owned_strings[1672])] = 246;
		mapped_strings[string_t(owned_strings[1673])] = 246;
		mapped_strings[string_t(owned_strings[1674])] = 9021;
		mapped_strings[string_t(owned_strings[1675])] = 8741;
		mapped_strings[string_t(owned_strings[1676])] = 182;
		mapped_strings[string_t(owned_strings[1677])] = 182;
		mapped_strings[string_t(owned_strings[1678])] = 8741;
		mapped_strings[string_t(owned_strings[1679])] = 10995;
		mapped_strings[string_t(owned_strings[1680])] = 11005;
		mapped_strings[string_t(owned_strings[1681])] = 8706;
		mapped_strings[string_t(owned_strings[1682])] = 1087;
		mapped_strings[string_t(owned_strings[1683])] = 37;
		mapped_strings[string_t(owned_strings[1684])] = 46;
		mapped_strings[string_t(owned_strings[1685])] = 8240;
		mapped_strings[string_t(owned_strings[1686])] = 8869;
		mapped_strings[string_t(owned_strings[1687])] = 8241;
		mapped_strings[string_t(owned_strings[1688])] = 120109;
		mapped_strings[string_t(owned_strings[1689])] = 966;
		mapped_strings[string_t(owned_strings[1690])] = 981;
		mapped_strings[string_t(owned_strings[1691])] = 8499;
		mapped_strings[string_t(owned_strings[1692])] = 9742;
		mapped_strings[string_t(owned_strings[1693])] = 960;
		mapped_strings[string_t(owned_strings[1694])] = 8916;
		mapped_strings[string_t(owned_strings[1695])] = 982;
		mapped_strings[string_t(owned_strings[1696])] = 8463;
		mapped_strings[string_t(owned_strings[1697])] = 8462;
		mapped_strings[string_t(owned_strings[1698])] = 8463;
		mapped_strings[string_t(owned_strings[1699])] = 43;
		mapped_strings[string_t(owned_strings[1700])] = 10787;
		mapped_strings[string_t(owned_strings[1701])] = 8862;
		mapped_strings[string_t(owned_strings[1702])] = 10786;
		mapped_strings[string_t(owned_strings[1703])] = 8724;
		mapped_strings[string_t(owned_strings[1704])] = 10789;
		mapped_strings[string_t(owned_strings[1705])] = 10866;
		mapped_strings[string_t(owned_strings[1706])] = 177;
		mapped_strings[string_t(owned_strings[1707])] = 177;
		mapped_strings[string_t(owned_strings[1708])] = 10790;
		mapped_strings[string_t(owned_strings[1709])] = 10791;
		mapped_strings[string_t(owned_strings[1710])] = 177;
		mapped_strings[string_t(owned_strings[1711])] = 10773;
		mapped_strings[string_t(owned_strings[1712])] = 120161;
		mapped_strings[string_t(owned_strings[1713])] = 163;
		mapped_strings[string_t(owned_strings[1714])] = 163;
		mapped_strings[string_t(owned_strings[1715])] = 8826;
		mapped_strings[string_t(owned_strings[1716])] = 10931;
		mapped_strings[string_t(owned_strings[1717])] = 10935;
		mapped_strings[string_t(owned_strings[1718])] = 8828;
		mapped_strings[string_t(owned_strings[1719])] = 10927;
		mapped_strings[string_t(owned_strings[1720])] = 8826;
		mapped_strings[string_t(owned_strings[1721])] = 10935;
		mapped_strings[string_t(owned_strings[1722])] = 8828;
		mapped_strings[string_t(owned_strings[1723])] = 10927;
		mapped_strings[string_t(owned_strings[1724])] = 10937;
		mapped_strings[string_t(owned_strings[1725])] = 10933;
		mapped_strings[string_t(owned_strings[1726])] = 8936;
		mapped_strings[string_t(owned_strings[1727])] = 8830;
		mapped_strings[string_t(owned_strings[1728])] = 8242;
		mapped_strings[string_t(owned_strings[1729])] = 8473;
		mapped_strings[string_t(owned_strings[1730])] = 10933;
		mapped_strings[string_t(owned_strings[1731])] = 10937;
		mapped_strings[string_t(owned_strings[1732])] = 8936;
		mapped_strings[string_t(owned_strings[1733])] = 8719;
		mapped_strings[string_t(owned_strings[1734])] = 9006;
		mapped_strings[string_t(owned_strings[1735])] = 8978;
		mapped_strings[string_t(owned_strings[1736])] = 8979;
		mapped_strings[string_t(owned_strings[1737])] = 8733;
		mapped_strings[string_t(owned_strings[1738])] = 8733;
		mapped_strings[string_t(owned_strings[1739])] = 8830;
		mapped_strings[string_t(owned_strings[1740])] = 8880;
		mapped_strings[string_t(owned_strings[1741])] = 120005;
		mapped_strings[string_t(owned_strings[1742])] = 968;
		mapped_strings[string_t(owned_strings[1743])] = 8200;
		mapped_strings[string_t(owned_strings[1744])] = 120110;
		mapped_strings[string_t(owned_strings[1745])] = 10764;
		mapped_strings[string_t(owned_strings[1746])] = 120162;
		mapped_strings[string_t(owned_strings[1747])] = 8279;
		mapped_strings[string_t(owned_strings[1748])] = 120006;
		mapped_strings[string_t(owned_strings[1749])] = 8461;
		mapped_strings[string_t(owned_strings[1750])] = 10774;
		mapped_strings[string_t(owned_strings[1751])] = 63;
		mapped_strings[string_t(owned_strings[1752])] = 8799;
		mapped_strings[string_t(owned_strings[1753])] = 34;
		mapped_strings[string_t(owned_strings[1754])] = 34;
		mapped_strings[string_t(owned_strings[1755])] = 8667;
		mapped_strings[string_t(owned_strings[1756])] = 8658;
		mapped_strings[string_t(owned_strings[1757])] = 10524;
		mapped_strings[string_t(owned_strings[1758])] = 10511;
		mapped_strings[string_t(owned_strings[1759])] = 10596;
		mapped_strings[string_t(owned_strings[1760])] = 8765;
		mapped_strings[string_t(owned_strings[1761])] = 341;
		mapped_strings[string_t(owned_strings[1762])] = 8730;
		mapped_strings[string_t(owned_strings[1763])] = 10675;
		mapped_strings[string_t(owned_strings[1764])] = 10217;
		mapped_strings[string_t(owned_strings[1765])] = 10642;
		mapped_strings[string_t(owned_strings[1766])] = 10661;
		mapped_strings[string_t(owned_strings[1767])] = 10217;
		mapped_strings[string_t(owned_strings[1768])] = 187;
		mapped_strings[string_t(owned_strings[1769])] = 187;
		mapped_strings[string_t(owned_strings[1770])] = 8594;
		mapped_strings[string_t(owned_strings[1771])] = 10613;
		mapped_strings[string_t(owned_strings[1772])] = 8677;
		mapped_strings[string_t(owned_strings[1773])] = 10528;
		mapped_strings[string_t(owned_strings[1774])] = 10547;
		mapped_strings[string_t(owned_strings[1775])] = 10526;
		mapped_strings[string_t(owned_strings[1776])] = 8618;
		mapped_strings[string_t(owned_strings[1777])] = 8620;
		mapped_strings[string_t(owned_strings[1778])] = 10565;
		mapped_strings[string_t(owned_strings[1779])] = 10612;
		mapped_strings[string_t(owned_strings[1780])] = 8611;
		mapped_strings[string_t(owned_strings[1781])] = 8605;
		mapped_strings[string_t(owned_strings[1782])] = 10522;
		mapped_strings[string_t(owned_strings[1783])] = 8758;
		mapped_strings[string_t(owned_strings[1784])] = 8474;
		mapped_strings[string_t(owned_strings[1785])] = 10509;
		mapped_strings[string_t(owned_strings[1786])] = 10099;
		mapped_strings[string_t(owned_strings[1787])] = 125;
		mapped_strings[string_t(owned_strings[1788])] = 93;
		mapped_strings[string_t(owned_strings[1789])] = 10636;
		mapped_strings[string_t(owned_strings[1790])] = 10638;
		mapped_strings[string_t(owned_strings[1791])] = 10640;
		mapped_strings[string_t(owned_strings[1792])] = 345;
		mapped_strings[string_t(owned_strings[1793])] = 343;
		mapped_strings[string_t(owned_strings[1794])] = 8969;
		mapped_strings[string_t(owned_strings[1795])] = 125;
		mapped_strings[string_t(owned_strings[1796])] = 1088;
		mapped_strings[string_t(owned_strings[1797])] = 10551;
		mapped_strings[string_t(owned_strings[1798])] = 10601;
		mapped_strings[string_t(owned_strings[1799])] = 8221;
		mapped_strings[string_t(owned_strings[1800])] = 8221;
		mapped_strings[string_t(owned_strings[1801])] = 8627;
		mapped_strings[string_t(owned_strings[1802])] = 8476;
		mapped_strings[string_t(owned_strings[1803])] = 8475;
		mapped_strings[string_t(owned_strings[1804])] = 8476;
		mapped_strings[string_t(owned_strings[1805])] = 8477;
		mapped_strings[string_t(owned_strings[1806])] = 9645;
		mapped_strings[string_t(owned_strings[1807])] = 174;
		mapped_strings[string_t(owned_strings[1808])] = 174;
		mapped_strings[string_t(owned_strings[1809])] = 10621;
		mapped_strings[string_t(owned_strings[1810])] = 8971;
		mapped_strings[string_t(owned_strings[1811])] = 120111;
		mapped_strings[string_t(owned_strings[1812])] = 8641;
		mapped_strings[string_t(owned_strings[1813])] = 8640;
		mapped_strings[string_t(owned_strings[1814])] = 10604;
		mapped_strings[string_t(owned_strings[1815])] = 961;
		mapped_strings[string_t(owned_strings[1816])] = 1009;
		mapped_strings[string_t(owned_strings[1817])] = 8594;
		mapped_strings[string_t(owned_strings[1818])] = 8611;
		mapped_strings[string_t(owned_strings[1819])] = 8641;
		mapped_strings[string_t(owned_strings[1820])] = 8640;
		mapped_strings[string_t(owned_strings[1821])] = 8644;
		mapped_strings[string_t(owned_strings[1822])] = 8652;
		mapped_strings[string_t(owned_strings[1823])] = 8649;
		mapped_strings[string_t(owned_strings[1824])] = 8605;
		mapped_strings[string_t(owned_strings[1825])] = 8908;
		mapped_strings[string_t(owned_strings[1826])] = 730;
		mapped_strings[string_t(owned_strings[1827])] = 8787;
		mapped_strings[string_t(owned_strings[1828])] = 8644;
		mapped_strings[string_t(owned_strings[1829])] = 8652;
		mapped_strings[string_t(owned_strings[1830])] = 8207;
		mapped_strings[string_t(owned_strings[1831])] = 9137;
		mapped_strings[string_t(owned_strings[1832])] = 9137;
		mapped_strings[string_t(owned_strings[1833])] = 10990;
		mapped_strings[string_t(owned_strings[1834])] = 10221;
		mapped_strings[string_t(owned_strings[1835])] = 8702;
		mapped_strings[string_t(owned_strings[1836])] = 10215;
		mapped_strings[string_t(owned_strings[1837])] = 10630;
		mapped_strings[string_t(owned_strings[1838])] = 120163;
		mapped_strings[string_t(owned_strings[1839])] = 10798;
		mapped_strings[string_t(owned_strings[1840])] = 10805;
		mapped_strings[string_t(owned_strings[1841])] = 41;
		mapped_strings[string_t(owned_strings[1842])] = 10644;
		mapped_strings[string_t(owned_strings[1843])] = 10770;
		mapped_strings[string_t(owned_strings[1844])] = 8649;
		mapped_strings[string_t(owned_strings[1845])] = 8250;
		mapped_strings[string_t(owned_strings[1846])] = 120007;
		mapped_strings[string_t(owned_strings[1847])] = 8625;
		mapped_strings[string_t(owned_strings[1848])] = 93;
		mapped_strings[string_t(owned_strings[1849])] = 8217;
		mapped_strings[string_t(owned_strings[1850])] = 8217;
		mapped_strings[string_t(owned_strings[1851])] = 8908;
		mapped_strings[string_t(owned_strings[1852])] = 8906;
		mapped_strings[string_t(owned_strings[1853])] = 9657;
		mapped_strings[string_t(owned_strings[1854])] = 8885;
		mapped_strings[string_t(owned_strings[1855])] = 9656;
		mapped_strings[string_t(owned_strings[1856])] = 10702;
		mapped_strings[string_t(owned_strings[1857])] = 10600;
		mapped_strings[string_t(owned_strings[1858])] = 8478;
		mapped_strings[string_t(owned_strings[1859])] = 347;
		mapped_strings[string_t(owned_strings[1860])] = 8218;
		mapped_strings[string_t(owned_strings[1861])] = 8827;
		mapped_strings[string_t(owned_strings[1862])] = 10932;
		mapped_strings[string_t(owned_strings[1863])] = 10936;
		mapped_strings[string_t(owned_strings[1864])] = 353;
		mapped_strings[string_t(owned_strings[1865])] = 8829;
		mapped_strings[string_t(owned_strings[1866])] = 10928;
		mapped_strings[string_t(owned_strings[1867])] = 351;
		mapped_strings[string_t(owned_strings[1868])] = 349;
		mapped_strings[string_t(owned_strings[1869])] = 10934;
		mapped_strings[string_t(owned_strings[1870])] = 10938;
		mapped_strings[string_t(owned_strings[1871])] = 8937;
		mapped_strings[string_t(owned_strings[1872])] = 10771;
		mapped_strings[string_t(owned_strings[1873])] = 8831;
		mapped_strings[string_t(owned_strings[1874])] = 1089;
		mapped_strings[string_t(owned_strings[1875])] = 8901;
		mapped_strings[string_t(owned_strings[1876])] = 8865;
		mapped_strings[string_t(owned_strings[1877])] = 10854;
		mapped_strings[string_t(owned_strings[1878])] = 8664;
		mapped_strings[string_t(owned_strings[1879])] = 10533;
		mapped_strings[string_t(owned_strings[1880])] = 8600;
		mapped_strings[string_t(owned_strings[1881])] = 8600;
		mapped_strings[string_t(owned_strings[1882])] = 167;
		mapped_strings[string_t(owned_strings[1883])] = 167;
		mapped_strings[string_t(owned_strings[1884])] = 59;
		mapped_strings[string_t(owned_strings[1885])] = 10537;
		mapped_strings[string_t(owned_strings[1886])] = 8726;
		mapped_strings[string_t(owned_strings[1887])] = 8726;
		mapped_strings[string_t(owned_strings[1888])] = 10038;
		mapped_strings[string_t(owned_strings[1889])] = 120112;
		mapped_strings[string_t(owned_strings[1890])] = 8994;
		mapped_strings[string_t(owned_strings[1891])] = 9839;
		mapped_strings[string_t(owned_strings[1892])] = 1097;
		mapped_strings[string_t(owned_strings[1893])] = 1096;
		mapped_strings[string_t(owned_strings[1894])] = 8739;
		mapped_strings[string_t(owned_strings[1895])] = 8741;
		mapped_strings[string_t(owned_strings[1896])] = 173;
		mapped_strings[string_t(owned_strings[1897])] = 173;
		mapped_strings[string_t(owned_strings[1898])] = 963;
		mapped_strings[string_t(owned_strings[1899])] = 962;
		mapped_strings[string_t(owned_strings[1900])] = 962;
		mapped_strings[string_t(owned_strings[1901])] = 8764;
		mapped_strings[string_t(owned_strings[1902])] = 10858;
		mapped_strings[string_t(owned_strings[1903])] = 8771;
		mapped_strings[string_t(owned_strings[1904])] = 8771;
		mapped_strings[string_t(owned_strings[1905])] = 10910;
		mapped_strings[string_t(owned_strings[1906])] = 10912;
		mapped_strings[string_t(owned_strings[1907])] = 10909;
		mapped_strings[string_t(owned_strings[1908])] = 10911;
		mapped_strings[string_t(owned_strings[1909])] = 8774;
		mapped_strings[string_t(owned_strings[1910])] = 10788;
		mapped_strings[string_t(owned_strings[1911])] = 10610;
		mapped_strings[string_t(owned_strings[1912])] = 8592;
		mapped_strings[string_t(owned_strings[1913])] = 8726;
		mapped_strings[string_t(owned_strings[1914])] = 10803;
		mapped_strings[string_t(owned_strings[1915])] = 10724;
		mapped_strings[string_t(owned_strings[1916])] = 8739;
		mapped_strings[string_t(owned_strings[1917])] = 8995;
		mapped_strings[string_t(owned_strings[1918])] = 10922;
		mapped_strings[string_t(owned_strings[1919])] = 10924;
		mapped_strings[string_t(owned_strings[1920])] = 10924;
		mapped_strings[string_t(owned_strings[1921])] = 1100;
		mapped_strings[string_t(owned_strings[1922])] = 47;
		mapped_strings[string_t(owned_strings[1923])] = 10692;
		mapped_strings[string_t(owned_strings[1924])] = 9023;
		mapped_strings[string_t(owned_strings[1925])] = 120164;
		mapped_strings[string_t(owned_strings[1926])] = 9824;
		mapped_strings[string_t(owned_strings[1927])] = 9824;
		mapped_strings[string_t(owned_strings[1928])] = 8741;
		mapped_strings[string_t(owned_strings[1929])] = 8851;
		mapped_strings[string_t(owned_strings[1930])] = 8851;
		mapped_strings[string_t(owned_strings[1931])] = 8852;
		mapped_strings[string_t(owned_strings[1932])] = 8852;
		mapped_strings[string_t(owned_strings[1933])] = 8847;
		mapped_strings[string_t(owned_strings[1934])] = 8849;
		mapped_strings[string_t(owned_strings[1935])] = 8847;
		mapped_strings[string_t(owned_strings[1936])] = 8849;
		mapped_strings[string_t(owned_strings[1937])] = 8848;
		mapped_strings[string_t(owned_strings[1938])] = 8850;
		mapped_strings[string_t(owned_strings[1939])] = 8848;
		mapped_strings[string_t(owned_strings[1940])] = 8850;
		mapped_strings[string_t(owned_strings[1941])] = 9633;
		mapped_strings[string_t(owned_strings[1942])] = 9633;
		mapped_strings[string_t(owned_strings[1943])] = 9642;
		mapped_strings[string_t(owned_strings[1944])] = 9642;
		mapped_strings[string_t(owned_strings[1945])] = 8594;
		mapped_strings[string_t(owned_strings[1946])] = 120008;
		mapped_strings[string_t(owned_strings[1947])] = 8726;
		mapped_strings[string_t(owned_strings[1948])] = 8995;
		mapped_strings[string_t(owned_strings[1949])] = 8902;
		mapped_strings[string_t(owned_strings[1950])] = 9734;
		mapped_strings[string_t(owned_strings[1951])] = 9733;
		mapped_strings[string_t(owned_strings[1952])] = 1013;
		mapped_strings[string_t(owned_strings[1953])] = 981;
		mapped_strings[string_t(owned_strings[1954])] = 175;
		mapped_strings[string_t(owned_strings[1955])] = 8834;
		mapped_strings[string_t(owned_strings[1956])] = 10949;
		mapped_strings[string_t(owned_strings[1957])] = 10941;
		mapped_strings[string_t(owned_strings[1958])] = 8838;
		mapped_strings[string_t(owned_strings[1959])] = 10947;
		mapped_strings[string_t(owned_strings[1960])] = 10945;
		mapped_strings[string_t(owned_strings[1961])] = 10955;
		mapped_strings[string_t(owned_strings[1962])] = 8842;
		mapped_strings[string_t(owned_strings[1963])] = 10943;
		mapped_strings[string_t(owned_strings[1964])] = 10617;
		mapped_strings[string_t(owned_strings[1965])] = 8834;
		mapped_strings[string_t(owned_strings[1966])] = 8838;
		mapped_strings[string_t(owned_strings[1967])] = 10949;
		mapped_strings[string_t(owned_strings[1968])] = 8842;
		mapped_strings[string_t(owned_strings[1969])] = 10955;
		mapped_strings[string_t(owned_strings[1970])] = 10951;
		mapped_strings[string_t(owned_strings[1971])] = 10965;
		mapped_strings[string_t(owned_strings[1972])] = 10963;
		mapped_strings[string_t(owned_strings[1973])] = 8827;
		mapped_strings[string_t(owned_strings[1974])] = 10936;
		mapped_strings[string_t(owned_strings[1975])] = 8829;
		mapped_strings[string_t(owned_strings[1976])] = 10928;
		mapped_strings[string_t(owned_strings[1977])] = 10938;
		mapped_strings[string_t(owned_strings[1978])] = 10934;
		mapped_strings[string_t(owned_strings[1979])] = 8937;
		mapped_strings[string_t(owned_strings[1980])] = 8831;
		mapped_strings[string_t(owned_strings[1981])] = 8721;
		mapped_strings[string_t(owned_strings[1982])] = 9834;
		mapped_strings[string_t(owned_strings[1983])] = 185;
		mapped_strings[string_t(owned_strings[1984])] = 185;
		mapped_strings[string_t(owned_strings[1985])] = 178;
		mapped_strings[string_t(owned_strings[1986])] = 178;
		mapped_strings[string_t(owned_strings[1987])] = 179;
		mapped_strings[string_t(owned_strings[1988])] = 179;
		mapped_strings[string_t(owned_strings[1989])] = 8835;
		mapped_strings[string_t(owned_strings[1990])] = 10950;
		mapped_strings[string_t(owned_strings[1991])] = 10942;
		mapped_strings[string_t(owned_strings[1992])] = 10968;
		mapped_strings[string_t(owned_strings[1993])] = 8839;
		mapped_strings[string_t(owned_strings[1994])] = 10948;
		mapped_strings[string_t(owned_strings[1995])] = 10185;
		mapped_strings[string_t(owned_strings[1996])] = 10967;
		mapped_strings[string_t(owned_strings[1997])] = 10619;
		mapped_strings[string_t(owned_strings[1998])] = 10946;
		mapped_strings[string_t(owned_strings[1999])] = 10956;
		mapped_strings[string_t(owned_strings[2000])] = 8843;
		mapped_strings[string_t(owned_strings[2001])] = 10944;
		mapped_strings[string_t(owned_strings[2002])] = 8835;
		mapped_strings[string_t(owned_strings[2003])] = 8839;
		mapped_strings[string_t(owned_strings[2004])] = 10950;
		mapped_strings[string_t(owned_strings[2005])] = 8843;
		mapped_strings[string_t(owned_strings[2006])] = 10956;
		mapped_strings[string_t(owned_strings[2007])] = 10952;
		mapped_strings[string_t(owned_strings[2008])] = 10964;
		mapped_strings[string_t(owned_strings[2009])] = 10966;
		mapped_strings[string_t(owned_strings[2010])] = 8665;
		mapped_strings[string_t(owned_strings[2011])] = 10534;
		mapped_strings[string_t(owned_strings[2012])] = 8601;
		mapped_strings[string_t(owned_strings[2013])] = 8601;
		mapped_strings[string_t(owned_strings[2014])] = 10538;
		mapped_strings[string_t(owned_strings[2015])] = 223;
		mapped_strings[string_t(owned_strings[2016])] = 223;
		mapped_strings[string_t(owned_strings[2017])] = 8982;
		mapped_strings[string_t(owned_strings[2018])] = 964;
		mapped_strings[string_t(owned_strings[2019])] = 9140;
		mapped_strings[string_t(owned_strings[2020])] = 357;
		mapped_strings[string_t(owned_strings[2021])] = 355;
		mapped_strings[string_t(owned_strings[2022])] = 1090;
		mapped_strings[string_t(owned_strings[2023])] = 8411;
		mapped_strings[string_t(owned_strings[2024])] = 8981;
		mapped_strings[string_t(owned_strings[2025])] = 120113;
		mapped_strings[string_t(owned_strings[2026])] = 8756;
		mapped_strings[string_t(owned_strings[2027])] = 8756;
		mapped_strings[string_t(owned_strings[2028])] = 952;
		mapped_strings[string_t(owned_strings[2029])] = 977;
		mapped_strings[string_t(owned_strings[2030])] = 977;
		mapped_strings[string_t(owned_strings[2031])] = 8776;
		mapped_strings[string_t(owned_strings[2032])] = 8764;
		mapped_strings[string_t(owned_strings[2033])] = 8201;
		mapped_strings[string_t(owned_strings[2034])] = 8776;
		mapped_strings[string_t(owned_strings[2035])] = 8764;
		mapped_strings[string_t(owned_strings[2036])] = 254;
		mapped_strings[string_t(owned_strings[2037])] = 254;
		mapped_strings[string_t(owned_strings[2038])] = 732;
		mapped_strings[string_t(owned_strings[2039])] = 215;
		mapped_strings[string_t(owned_strings[2040])] = 215;
		mapped_strings[string_t(owned_strings[2041])] = 8864;
		mapped_strings[string_t(owned_strings[2042])] = 10801;
		mapped_strings[string_t(owned_strings[2043])] = 10800;
		mapped_strings[string_t(owned_strings[2044])] = 8749;
		mapped_strings[string_t(owned_strings[2045])] = 10536;
		mapped_strings[string_t(owned_strings[2046])] = 8868;
		mapped_strings[string_t(owned_strings[2047])] = 9014;
		mapped_strings[string_t(owned_strings[2048])] = 10993;
		mapped_strings[string_t(owned_strings[2049])] = 120165;
		mapped_strings[string_t(owned_strings[2050])] = 10970;
		mapped_strings[string_t(owned_strings[2051])] = 10537;
		mapped_strings[string_t(owned_strings[2052])] = 8244;
		mapped_strings[string_t(owned_strings[2053])] = 8482;
		mapped_strings[string_t(owned_strings[2054])] = 9653;
		mapped_strings[string_t(owned_strings[2055])] = 9663;
		mapped_strings[string_t(owned_strings[2056])] = 9667;
		mapped_strings[string_t(owned_strings[2057])] = 8884;
		mapped_strings[string_t(owned_strings[2058])] = 8796;
		mapped_strings[string_t(owned_strings[2059])] = 9657;
		mapped_strings[string_t(owned_strings[2060])] = 8885;
		mapped_strings[string_t(owned_strings[2061])] = 9708;
		mapped_strings[string_t(owned_strings[2062])] = 8796;
		mapped_strings[string_t(owned_strings[2063])] = 10810;
		mapped_strings[string_t(owned_strings[2064])] = 10809;
		mapped_strings[string_t(owned_strings[2065])] = 10701;
		mapped_strings[string_t(owned_strings[2066])] = 10811;
		mapped_strings[string_t(owned_strings[2067])] = 9186;
		mapped_strings[string_t(owned_strings[2068])] = 120009;
		mapped_strings[string_t(owned_strings[2069])] = 1094;
		mapped_strings[string_t(owned_strings[2070])] = 1115;
		mapped_strings[string_t(owned_strings[2071])] = 359;
		mapped_strings[string_t(owned_strings[2072])] = 8812;
		mapped_strings[string_t(owned_strings[2073])] = 8606;
		mapped_strings[string_t(owned_strings[2074])] = 8608;
		mapped_strings[string_t(owned_strings[2075])] = 8657;
		mapped_strings[string_t(owned_strings[2076])] = 10595;
		mapped_strings[string_t(owned_strings[2077])] = 250;
		mapped_strings[string_t(owned_strings[2078])] = 250;
		mapped_strings[string_t(owned_strings[2079])] = 8593;
		mapped_strings[string_t(owned_strings[2080])] = 1118;
		mapped_strings[string_t(owned_strings[2081])] = 365;
		mapped_strings[string_t(owned_strings[2082])] = 251;
		mapped_strings[string_t(owned_strings[2083])] = 251;
		mapped_strings[string_t(owned_strings[2084])] = 1091;
		mapped_strings[string_t(owned_strings[2085])] = 8645;
		mapped_strings[string_t(owned_strings[2086])] = 369;
		mapped_strings[string_t(owned_strings[2087])] = 10606;
		mapped_strings[string_t(owned_strings[2088])] = 10622;
		mapped_strings[string_t(owned_strings[2089])] = 120114;
		mapped_strings[string_t(owned_strings[2090])] = 249;
		mapped_strings[string_t(owned_strings[2091])] = 249;
		mapped_strings[string_t(owned_strings[2092])] = 8639;
		mapped_strings[string_t(owned_strings[2093])] = 8638;
		mapped_strings[string_t(owned_strings[2094])] = 9600;
		mapped_strings[string_t(owned_strings[2095])] = 8988;
		mapped_strings[string_t(owned_strings[2096])] = 8988;
		mapped_strings[string_t(owned_strings[2097])] = 8975;
		mapped_strings[string_t(owned_strings[2098])] = 9720;
		mapped_strings[string_t(owned_strings[2099])] = 363;
		mapped_strings[string_t(owned_strings[2100])] = 168;
		mapped_strings[string_t(owned_strings[2101])] = 168;
		mapped_strings[string_t(owned_strings[2102])] = 371;
		mapped_strings[string_t(owned_strings[2103])] = 120166;
		mapped_strings[string_t(owned_strings[2104])] = 8593;
		mapped_strings[string_t(owned_strings[2105])] = 8597;
		mapped_strings[string_t(owned_strings[2106])] = 8639;
		mapped_strings[string_t(owned_strings[2107])] = 8638;
		mapped_strings[string_t(owned_strings[2108])] = 8846;
		mapped_strings[string_t(owned_strings[2109])] = 965;
		mapped_strings[string_t(owned_strings[2110])] = 978;
		mapped_strings[string_t(owned_strings[2111])] = 965;
		mapped_strings[string_t(owned_strings[2112])] = 8648;
		mapped_strings[string_t(owned_strings[2113])] = 8989;
		mapped_strings[string_t(owned_strings[2114])] = 8989;
		mapped_strings[string_t(owned_strings[2115])] = 8974;
		mapped_strings[string_t(owned_strings[2116])] = 367;
		mapped_strings[string_t(owned_strings[2117])] = 9721;
		mapped_strings[string_t(owned_strings[2118])] = 120010;
		mapped_strings[string_t(owned_strings[2119])] = 8944;
		mapped_strings[string_t(owned_strings[2120])] = 361;
		mapped_strings[string_t(owned_strings[2121])] = 9653;
		mapped_strings[string_t(owned_strings[2122])] = 9652;
		mapped_strings[string_t(owned_strings[2123])] = 8648;
		mapped_strings[string_t(owned_strings[2124])] = 252;
		mapped_strings[string_t(owned_strings[2125])] = 252;
		mapped_strings[string_t(owned_strings[2126])] = 10663;
		mapped_strings[string_t(owned_strings[2127])] = 8661;
		mapped_strings[string_t(owned_strings[2128])] = 10984;
		mapped_strings[string_t(owned_strings[2129])] = 10985;
		mapped_strings[string_t(owned_strings[2130])] = 8872;
		mapped_strings[string_t(owned_strings[2131])] = 10652;
		mapped_strings[string_t(owned_strings[2132])] = 1013;
		mapped_strings[string_t(owned_strings[2133])] = 1008;
		mapped_strings[string_t(owned_strings[2134])] = 8709;
		mapped_strings[string_t(owned_strings[2135])] = 981;
		mapped_strings[string_t(owned_strings[2136])] = 982;
		mapped_strings[string_t(owned_strings[2137])] = 8733;
		mapped_strings[string_t(owned_strings[2138])] = 8597;
		mapped_strings[string_t(owned_strings[2139])] = 1009;
		mapped_strings[string_t(owned_strings[2140])] = 962;
		mapped_strings[string_t(owned_strings[2141])] = 8842;
		mapped_strings[string_t(owned_strings[2142])] = 10955;
		mapped_strings[string_t(owned_strings[2143])] = 8843;
		mapped_strings[string_t(owned_strings[2144])] = 10956;
		mapped_strings[string_t(owned_strings[2145])] = 977;
		mapped_strings[string_t(owned_strings[2146])] = 8882;
		mapped_strings[string_t(owned_strings[2147])] = 8883;
		mapped_strings[string_t(owned_strings[2148])] = 1074;
		mapped_strings[string_t(owned_strings[2149])] = 8866;
		mapped_strings[string_t(owned_strings[2150])] = 8744;
		mapped_strings[string_t(owned_strings[2151])] = 8891;
		mapped_strings[string_t(owned_strings[2152])] = 8794;
		mapped_strings[string_t(owned_strings[2153])] = 8942;
		mapped_strings[string_t(owned_strings[2154])] = 124;
		mapped_strings[string_t(owned_strings[2155])] = 124;
		mapped_strings[string_t(owned_strings[2156])] = 120115;
		mapped_strings[string_t(owned_strings[2157])] = 8882;
		mapped_strings[string_t(owned_strings[2158])] = 8834;
		mapped_strings[string_t(owned_strings[2159])] = 8835;
		mapped_strings[string_t(owned_strings[2160])] = 120167;
		mapped_strings[string_t(owned_strings[2161])] = 8733;
		mapped_strings[string_t(owned_strings[2162])] = 8883;
		mapped_strings[string_t(owned_strings[2163])] = 120011;
		mapped_strings[string_t(owned_strings[2164])] = 10955;
		mapped_strings[string_t(owned_strings[2165])] = 8842;
		mapped_strings[string_t(owned_strings[2166])] = 10956;
		mapped_strings[string_t(owned_strings[2167])] = 8843;
		mapped_strings[string_t(owned_strings[2168])] = 10650;
		mapped_strings[string_t(owned_strings[2169])] = 373;
		mapped_strings[string_t(owned_strings[2170])] = 10847;
		mapped_strings[string_t(owned_strings[2171])] = 8743;
		mapped_strings[string_t(owned_strings[2172])] = 8793;
		mapped_strings[string_t(owned_strings[2173])] = 8472;
		mapped_strings[string_t(owned_strings[2174])] = 120116;
		mapped_strings[string_t(owned_strings[2175])] = 120168;
		mapped_strings[string_t(owned_strings[2176])] = 8472;
		mapped_strings[string_t(owned_strings[2177])] = 8768;
		mapped_strings[string_t(owned_strings[2178])] = 8768;
		mapped_strings[string_t(owned_strings[2179])] = 120012;
		mapped_strings[string_t(owned_strings[2180])] = 8898;
		mapped_strings[string_t(owned_strings[2181])] = 9711;
		mapped_strings[string_t(owned_strings[2182])] = 8899;
		mapped_strings[string_t(owned_strings[2183])] = 9661;
		mapped_strings[string_t(owned_strings[2184])] = 120117;
		mapped_strings[string_t(owned_strings[2185])] = 10234;
		mapped_strings[string_t(owned_strings[2186])] = 10231;
		mapped_strings[string_t(owned_strings[2187])] = 958;
		mapped_strings[string_t(owned_strings[2188])] = 10232;
		mapped_strings[string_t(owned_strings[2189])] = 10229;
		mapped_strings[string_t(owned_strings[2190])] = 10236;
		mapped_strings[string_t(owned_strings[2191])] = 8955;
		mapped_strings[string_t(owned_strings[2192])] = 10752;
		mapped_strings[string_t(owned_strings[2193])] = 120169;
		mapped_strings[string_t(owned_strings[2194])] = 10753;
		mapped_strings[string_t(owned_strings[2195])] = 10754;
		mapped_strings[string_t(owned_strings[2196])] = 10233;
		mapped_strings[string_t(owned_strings[2197])] = 10230;
		mapped_strings[string_t(owned_strings[2198])] = 120013;
		mapped_strings[string_t(owned_strings[2199])] = 10758;
		mapped_strings[string_t(owned_strings[2200])] = 10756;
		mapped_strings[string_t(owned_strings[2201])] = 9651;
		mapped_strings[string_t(owned_strings[2202])] = 8897;
		mapped_strings[string_t(owned_strings[2203])] = 8896;
		mapped_strings[string_t(owned_strings[2204])] = 253;
		mapped_strings[string_t(owned_strings[2205])] = 253;
		mapped_strings[string_t(owned_strings[2206])] = 1103;
		mapped_strings[string_t(owned_strings[2207])] = 375;
		mapped_strings[string_t(owned_strings[2208])] = 1099;
		mapped_strings[string_t(owned_strings[2209])] = 165;
		mapped_strings[string_t(owned_strings[2210])] = 165;
		mapped_strings[string_t(owned_strings[2211])] = 120118;
		mapped_strings[string_t(owned_strings[2212])] = 1111;
		mapped_strings[string_t(owned_strings[2213])] = 120170;
		mapped_strings[string_t(owned_strings[2214])] = 120014;
		mapped_strings[string_t(owned_strings[2215])] = 1102;
		mapped_strings[string_t(owned_strings[2216])] = 255;
		mapped_strings[string_t(owned_strings[2217])] = 255;
		mapped_strings[string_t(owned_strings[2218])] = 378;
		mapped_strings[string_t(owned_strings[2219])] = 382;
		mapped_strings[string_t(owned_strings[2220])] = 1079;
		mapped_strings[string_t(owned_strings[2221])] = 380;
		mapped_strings[string_t(owned_strings[2222])] = 8488;
		mapped_strings[string_t(owned_strings[2223])] = 950;
		mapped_strings[string_t(owned_strings[2224])] = 120119;
		mapped_strings[string_t(owned_strings[2225])] = 1078;
		mapped_strings[string_t(owned_strings[2226])] = 8669;
		mapped_strings[string_t(owned_strings[2227])] = 120171;
		mapped_strings[string_t(owned_strings[2228])] = 120015;
		mapped_strings[string_t(owned_strings[2229])] = 8205;
		mapped_strings[string_t(owned_strings[2230])] = 8204;
		return mapped_strings;
	}

private:
	static const vector<string> owned_strings;
};

// definition of the static members
const vector<string> HTML5NameCharrefs::owned_strings = HTML5NameCharrefs::initialize_vector();
const string_map_t<uint32_t> HTML5NameCharrefs::mapped_strings = HTML5NameCharrefs::initialize_map();

} // namespace duckdb
