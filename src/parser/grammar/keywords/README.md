The type of a keyword determines how the keyword can be used outside of its use as a keyword. Every keyword present in the grammar should appear in exactly one of these lists. Put a new keyword into the first list that it can go into without causing shift or reduce conflicts. The earlier lists define "less reserved" categories of keywords.

There are four different kinds of keywords:

* Unreserved Keywords
* Column identifier keywords
* Type/function identifier keywords
* Reserved keywords

#### "Unreserved" keywords
Available for use as any kind of name.

#### Column identifier --- keywords that can be column, table, etc names.
Many of these keywords will in fact be recognized as type or function names too; but they have special productions for the purpose, and so can't be treated as "generic" type or function names.

The type names appearing here are not usable as function names because they can be followed by '(' in typename productions, which looks too much like a function call for an LR(1) parser.

#### Type/function identifier --- keywords that can be type or function names.
Most of these are keywords that are used as operators in expressions; in general such keywords can't be column names because they would be ambiguous with variables, but they are unambiguous as function identifiers.

#### Reserved keyword --- these keywords are usable only as a ColLabel.
Keywords appear here if they could not be distinguished from variable, type, or function names in some contexts.  Don't put things here unless forced to.
