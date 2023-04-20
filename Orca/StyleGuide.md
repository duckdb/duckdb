# Style Guidelines

This style guideline is roughly based on Google C++ style guide that can be
found [here](https://google.github.io/styleguide/cppguide.html); with some
modifications that are enumerated in this document.

## Guiding Principles

1. Although ORCA is maintained in a separate repository, a lot of development
	 on ORCA is closely related work in Greenplum database. To reduce context
	 switch costs arising from such a such a development model, we would like to
	 keep the style in ORCA as close as possible to GPDB. This includes using
	 tabs instead of spaces for indentation, all cap global constants etc.
1. The ORCA code base previously was written completely in a version of
	 [Hungarian notation](https://en.wikipedia.org/wiki/Hungarian_notation). To
	 prevent large number of file renames, we also decided to keep the names of
	 classes and files the same as much as possible.

That said, the specific style guidelines follow:

## Naming conventions

1. File names
	1. File names should match the main class in the file, whenever possible.

1. Type names
	1. Typenames start with a capital letter and have a capital letter for every
		 new word, with no underscores. For example, `MyExcitingClass` & `MyExcitingEnum`
	1. For typedefs of basic data structures (such as HashMaps), the typename
		 should include the type as a suffix. eg:  `typedef CDynamicPtrArray
		 <CBitSetLink, CleanupNULL> BitSetLinkArray;`

1. Variable names
	1. The names of variables (including function parameters) and data members
		 are all lowercase, with underscores between words.
		 For example, `variable_name`
	1. Data members of classes and structs additionally have an additional prefix
		 of `m_`.
		 For example, `myobj->m_variable_name`

1. Constant names & macros
	1. All macros definitions are written in all capital letters with underscore
		 between words. Use the same format for variables declared const in a
		 global scope.
		 For example, `#define MYMACRO` and `const int GLOBAL_CONSTANT = 4;`
	1. However, variables declared as const as local variable in a function or
		 class should follow the same convention as a non-const variable would in
		 that context.
		 For example, `const int CMyClass::m_my_const = 5;`

1. Function names
	1. All functions should start with a capital letter and have a capital letter
		 for each new word.
		 For example: `CallFunction()`
	1. Accessors and mutators may be prefixed with `Get` and `Set` respectively.
		 But, this is not necessary for some trivial functions, such as
		 `array->Size()` vs `array->GetSize()`.
	1. Ideally, theses functions should start with a verb describing the
		 action(s) the functions is going to perform, rather than a noun. For
		 example, prefer `ReadBook()` over `BookReader()`.
	1. Since the `Get` and `Set` prefixes suggest the method is a simple accessor
		 or mutator, avoid using those prefixes for complex methods and do a lot of
		 computations, and prefer a `Compute` or `Calculate` prefix for them
		 instead .

1. Namespace names
	1. Namespace names are all lower-case.

1. Class, enumeration & struct names
	1. Class names should start with the capital letter 'C' and have a capital
		 letter for each new word. For example, `class CMyClass`.
	1. Struct names should start with the capital letter 'S' and have a capital
		 letter for each new word. For example, `struct SMyStruct`.
	1. Enumeration names should start with the capital letter 'E' and have a
		 capital letter for each new word. For example, `enum EOperatorId`. Each
		 enumeration name may contain a Hungarian notational prefix for legacy
		 reasons. For example, `enum EOperatorId { EopLogicalGet, ... }`

