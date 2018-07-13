/*--------------------------------------------------------------------
 * Symbols referenced in this file:
 * - lappend
 * - new_list
 * - new_tail_cell
 * - lcons
 * - new_head_cell
 * - list_concat
 * - list_nth
 * - list_nth_cell
 * - list_delete_cell
 * - list_free
 * - list_free_private
 * - list_copy
 * - list_copy_tail
 * - list_truncate
 *--------------------------------------------------------------------
 */

/*-------------------------------------------------------------------------
 *
 * list.c
 *	  implementation for PostgreSQL generic linked list package
 *
 *
 * Portions Copyright (c) 1996-2015, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * IDENTIFICATION
 *	  src/backend/nodes/list.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

/* see pg_list.h */
#define PG_LIST_INCLUDE_DEFINITIONS

#include "nodes/pg_list.h"


/*
 * Routines to simplify writing assertions about the type of a list; a
 * NIL list is considered to be an empty list of any type.
 */
#define IsPointerList(l)		((l) == NIL || IsA((l), List))
#define IsIntegerList(l)		((l) == NIL || IsA((l), IntList))
#define IsOidList(l)			((l) == NIL || IsA((l), OidList))

#ifdef USE_ASSERT_CHECKING
/*
 * Check that the specified List is valid (so far as we can tell).
 */
static void
check_list_invariants(const List *list)
{
	if (list == NIL)
		return;

	Assert(list->length > 0);
	Assert(list->head != NULL);
	Assert(list->tail != NULL);

	Assert(list->type == T_List ||
		   list->type == T_IntList ||
		   list->type == T_OidList);

	if (list->length == 1)
		Assert(list->head == list->tail);
	if (list->length == 2)
		Assert(list->head->next == list->tail);
	Assert(list->tail->next == NULL);
}
#else
#define check_list_invariants(l)
#endif   /* USE_ASSERT_CHECKING */

/*
 * Return a freshly allocated List. Since empty non-NIL lists are
 * invalid, new_list() also allocates the head cell of the new list:
 * the caller should be sure to fill in that cell's data.
 */
static List *
new_list(NodeTag type)
{
	List	   *new_list;
	ListCell   *new_head;

	new_head = (ListCell *) palloc(sizeof(*new_head));
	new_head->next = NULL;
	/* new_head->data is left undefined! */

	new_list = (List *) palloc(sizeof(*new_list));
	new_list->type = type;
	new_list->length = 1;
	new_list->head = new_head;
	new_list->tail = new_head;

	return new_list;
}

/*
 * Allocate a new cell and make it the head of the specified
 * list. Assumes the list it is passed is non-NIL.
 *
 * The data in the new head cell is undefined; the caller should be
 * sure to fill it in
 */
static void
new_head_cell(List *list)
{
	ListCell   *new_head;

	new_head = (ListCell *) palloc(sizeof(*new_head));
	new_head->next = list->head;

	list->head = new_head;
	list->length++;
}

/*
 * Allocate a new cell and make it the tail of the specified
 * list. Assumes the list it is passed is non-NIL.
 *
 * The data in the new tail cell is undefined; the caller should be
 * sure to fill it in
 */
static void
new_tail_cell(List *list)
{
	ListCell   *new_tail;

	new_tail = (ListCell *) palloc(sizeof(*new_tail));
	new_tail->next = NULL;

	list->tail->next = new_tail;
	list->tail = new_tail;
	list->length++;
}

/*
 * Append a pointer to the list. A pointer to the modified list is
 * returned. Note that this function may or may not destructively
 * modify the list; callers should always use this function's return
 * value, rather than continuing to use the pointer passed as the
 * first argument.
 */
List *
lappend(List *list, void *datum)
{
	Assert(IsPointerList(list));

	if (list == NIL)
		list = new_list(T_List);
	else
		new_tail_cell(list);

	lfirst(list->tail) = datum;
	check_list_invariants(list);
	return list;
}

/*
 * Append an integer to the specified list. See lappend()
 */


/*
 * Append an OID to the specified list. See lappend()
 */


/*
 * Add a new cell to the list, in the position after 'prev_cell'. The
 * data in the cell is left undefined, and must be filled in by the
 * caller. 'list' is assumed to be non-NIL, and 'prev_cell' is assumed
 * to be non-NULL and a member of 'list'.
 */


/*
 * Add a new cell to the specified list (which must be non-NIL);
 * it will be placed after the list cell 'prev' (which must be
 * non-NULL and a member of 'list'). The data placed in the new cell
 * is 'datum'. The newly-constructed cell is returned.
 */






/*
 * Prepend a new element to the list. A pointer to the modified list
 * is returned. Note that this function may or may not destructively
 * modify the list; callers should always use this function's return
 * value, rather than continuing to use the pointer passed as the
 * second argument.
 *
 * Caution: before Postgres 8.0, the original List was unmodified and
 * could be considered to retain its separate identity.  This is no longer
 * the case.
 */
List *
lcons(void *datum, List *list)
{
	Assert(IsPointerList(list));

	if (list == NIL)
		list = new_list(T_List);
	else
		new_head_cell(list);

	lfirst(list->head) = datum;
	check_list_invariants(list);
	return list;
}

/*
 * Prepend an integer to the list. See lcons()
 */


/*
 * Prepend an OID to the list. See lcons()
 */


/*
 * Concatenate list2 to the end of list1, and return list1. list1 is
 * destructively changed. Callers should be sure to use the return
 * value as the new pointer to the concatenated list: the 'list1'
 * input pointer may or may not be the same as the returned pointer.
 *
 * The nodes in list2 are merely appended to the end of list1 in-place
 * (i.e. they aren't copied; the two lists will share some of the same
 * storage). Therefore, invoking list_free() on list2 will also
 * invalidate a portion of list1.
 */
List *
list_concat(List *list1, List *list2)
{
	if (list1 == NIL)
		return list2;
	if (list2 == NIL)
		return list1;
	if (list1 == list2)
		elog(ERROR, "cannot list_concat() a list to itself");

	Assert(list1->type == list2->type);

	list1->length += list2->length;
	list1->tail->next = list2->head;
	list1->tail = list2->tail;

	check_list_invariants(list1);
	return list1;
}

/*
 * Truncate 'list' to contain no more than 'new_size' elements. This
 * modifies the list in-place! Despite this, callers should use the
 * pointer returned by this function to refer to the newly truncated
 * list -- it may or may not be the same as the pointer that was
 * passed.
 *
 * Note that any cells removed by list_truncate() are NOT pfree'd.
 */
List *
list_truncate(List *list, int new_size)
{
	ListCell   *cell;
	int			n;

	if (new_size <= 0)
		return NIL;				/* truncate to zero length */

	/* If asked to effectively extend the list, do nothing */
	if (new_size >= list_length(list))
		return list;

	n = 1;
	foreach(cell, list)
	{
		if (n == new_size)
		{
			cell->next = NULL;
			list->tail = cell;
			list->length = new_size;
			check_list_invariants(list);
			return list;
		}
		n++;
	}

	/* keep the compiler quiet; never reached */
	Assert(false);
	return list;
}

/*
 * Locate the n'th cell (counting from 0) of the list.  It is an assertion
 * failure if there is no such cell.
 */
ListCell *
list_nth_cell(const List *list, int n)
{
	ListCell   *match;

	Assert(list != NIL);
	Assert(n >= 0);
	Assert(n < list->length);
	check_list_invariants(list);

	/* Does the caller actually mean to fetch the tail? */
	if (n == list->length - 1)
		return list->tail;

	for (match = list->head; n-- > 0; match = match->next)
		;

	return match;
}

/*
 * Return the data value contained in the n'th element of the
 * specified list. (List elements begin at 0.)
 */
void *
list_nth(const List *list, int n)
{
	Assert(IsPointerList(list));
	return lfirst(list_nth_cell(list, n));
}

/*
 * Return the integer value contained in the n'th element of the
 * specified list.
 */


/*
 * Return the OID value contained in the n'th element of the specified
 * list.
 */


/*
 * Return true iff 'datum' is a member of the list. Equality is
 * determined via equal(), so callers should ensure that they pass a
 * Node as 'datum'.
 */


/*
 * Return true iff 'datum' is a member of the list. Equality is
 * determined by using simple pointer comparison.
 */


/*
 * Return true iff the integer 'datum' is a member of the list.
 */


/*
 * Return true iff the OID 'datum' is a member of the list.
 */


/*
 * Delete 'cell' from 'list'; 'prev' is the previous element to 'cell'
 * in 'list', if any (i.e. prev == NULL iff list->head == cell)
 *
 * The cell is pfree'd, as is the List header if this was the last member.
 */
List *
list_delete_cell(List *list, ListCell *cell, ListCell *prev)
{
	check_list_invariants(list);
	Assert(prev != NULL ? lnext(prev) == cell : list_head(list) == cell);

	/*
	 * If we're about to delete the last node from the list, free the whole
	 * list instead and return NIL, which is the only valid representation of
	 * a zero-length list.
	 */
	if (list->length == 1)
	{
		list_free(list);
		return NIL;
	}

	/*
	 * Otherwise, adjust the necessary list links, deallocate the particular
	 * node we have just removed, and return the list we were given.
	 */
	list->length--;

	if (prev)
		prev->next = cell->next;
	else
		list->head = cell->next;

	if (list->tail == cell)
		list->tail = prev;

	pfree(cell);
	return list;
}

/*
 * Delete the first cell in list that matches datum, if any.
 * Equality is determined via equal().
 */


/* As above, but use simple pointer equality */


/* As above, but for integers */


/* As above, but for OIDs */


/*
 * Delete the first element of the list.
 *
 * This is useful to replace the Lisp-y code "list = lnext(list);" in cases
 * where the intent is to alter the list rather than just traverse it.
 * Beware that the removed cell is freed, whereas the lnext() coding leaves
 * the original list head intact if there's another pointer to it.
 */


/*
 * Generate the union of two lists. This is calculated by copying
 * list1 via list_copy(), then adding to it all the members of list2
 * that aren't already in list1.
 *
 * Whether an element is already a member of the list is determined
 * via equal().
 *
 * The returned list is newly-allocated, although the content of the
 * cells is the same (i.e. any pointed-to objects are not copied).
 *
 * NB: this function will NOT remove any duplicates that are present
 * in list1 (so it only performs a "union" if list1 is known unique to
 * start with).  Also, if you are about to write "x = list_union(x, y)"
 * you probably want to use list_concat_unique() instead to avoid wasting
 * the list cells of the old x list.
 *
 * This function could probably be implemented a lot faster if it is a
 * performance bottleneck.
 */


/*
 * This variant of list_union() determines duplicates via simple
 * pointer comparison.
 */


/*
 * This variant of list_union() operates upon lists of integers.
 */


/*
 * This variant of list_union() operates upon lists of OIDs.
 */


/*
 * Return a list that contains all the cells that are in both list1 and
 * list2.  The returned list is freshly allocated via palloc(), but the
 * cells themselves point to the same objects as the cells of the
 * input lists.
 *
 * Duplicate entries in list1 will not be suppressed, so it's only a true
 * "intersection" if list1 is known unique beforehand.
 *
 * This variant works on lists of pointers, and determines list
 * membership via equal().  Note that the list1 member will be pointed
 * to in the result.
 */


/*
 * As list_intersection but operates on lists of integers.
 */


/*
 * Return a list that contains all the cells in list1 that are not in
 * list2. The returned list is freshly allocated via palloc(), but the
 * cells themselves point to the same objects as the cells of the
 * input lists.
 *
 * This variant works on lists of pointers, and determines list
 * membership via equal()
 */


/*
 * This variant of list_difference() determines list membership via
 * simple pointer equality.
 */


/*
 * This variant of list_difference() operates upon lists of integers.
 */


/*
 * This variant of list_difference() operates upon lists of OIDs.
 */


/*
 * Append datum to list, but only if it isn't already in the list.
 *
 * Whether an element is already a member of the list is determined
 * via equal().
 */


/*
 * This variant of list_append_unique() determines list membership via
 * simple pointer equality.
 */


/*
 * This variant of list_append_unique() operates upon lists of integers.
 */


/*
 * This variant of list_append_unique() operates upon lists of OIDs.
 */


/*
 * Append to list1 each member of list2 that isn't already in list1.
 *
 * Whether an element is already a member of the list is determined
 * via equal().
 *
 * This is almost the same functionality as list_union(), but list1 is
 * modified in-place rather than being copied.  Note also that list2's cells
 * are not inserted in list1, so the analogy to list_concat() isn't perfect.
 */


/*
 * This variant of list_concat_unique() determines list membership via
 * simple pointer equality.
 */


/*
 * This variant of list_concat_unique() operates upon lists of integers.
 */


/*
 * This variant of list_concat_unique() operates upon lists of OIDs.
 */


/*
 * Free all storage in a list, and optionally the pointed-to elements
 */
static void
list_free_private(List *list, bool deep)
{
	ListCell   *cell;

	check_list_invariants(list);

	cell = list_head(list);
	while (cell != NULL)
	{
		ListCell   *tmp = cell;

		cell = lnext(cell);
		if (deep)
			pfree(lfirst(tmp));
		pfree(tmp);
	}

	if (list)
		pfree(list);
}

/*
 * Free all the cells of the list, as well as the list itself. Any
 * objects that are pointed-to by the cells of the list are NOT
 * free'd.
 *
 * On return, the argument to this function has been freed, so the
 * caller would be wise to set it to NIL for safety's sake.
 */
void
list_free(List *list)
{
	list_free_private(list, false);
}

/*
 * Free all the cells of the list, the list itself, and all the
 * objects pointed-to by the cells of the list (each element in the
 * list must contain a pointer to a palloc()'d region of memory!)
 *
 * On return, the argument to this function has been freed, so the
 * caller would be wise to set it to NIL for safety's sake.
 */


/*
 * Return a shallow copy of the specified list.
 */
List *
list_copy(const List *oldlist)
{
	List	   *newlist;
	ListCell   *newlist_prev;
	ListCell   *oldlist_cur;

	if (oldlist == NIL)
		return NIL;

	newlist = new_list(oldlist->type);
	newlist->length = oldlist->length;

	/*
	 * Copy over the data in the first cell; new_list() has already allocated
	 * the head cell itself
	 */
	newlist->head->data = oldlist->head->data;

	newlist_prev = newlist->head;
	oldlist_cur = oldlist->head->next;
	while (oldlist_cur)
	{
		ListCell   *newlist_cur;

		newlist_cur = (ListCell *) palloc(sizeof(*newlist_cur));
		newlist_cur->data = oldlist_cur->data;
		newlist_prev->next = newlist_cur;

		newlist_prev = newlist_cur;
		oldlist_cur = oldlist_cur->next;
	}

	newlist_prev->next = NULL;
	newlist->tail = newlist_prev;

	check_list_invariants(newlist);
	return newlist;
}

/*
 * Return a shallow copy of the specified list, without the first N elements.
 */
List *
list_copy_tail(const List *oldlist, int nskip)
{
	List	   *newlist;
	ListCell   *newlist_prev;
	ListCell   *oldlist_cur;

	if (nskip < 0)
		nskip = 0;				/* would it be better to elog? */

	if (oldlist == NIL || nskip >= oldlist->length)
		return NIL;

	newlist = new_list(oldlist->type);
	newlist->length = oldlist->length - nskip;

	/*
	 * Skip over the unwanted elements.
	 */
	oldlist_cur = oldlist->head;
	while (nskip-- > 0)
		oldlist_cur = oldlist_cur->next;

	/*
	 * Copy over the data in the first remaining cell; new_list() has already
	 * allocated the head cell itself
	 */
	newlist->head->data = oldlist_cur->data;

	newlist_prev = newlist->head;
	oldlist_cur = oldlist_cur->next;
	while (oldlist_cur)
	{
		ListCell   *newlist_cur;

		newlist_cur = (ListCell *) palloc(sizeof(*newlist_cur));
		newlist_cur->data = oldlist_cur->data;
		newlist_prev->next = newlist_cur;

		newlist_prev = newlist_cur;
		oldlist_cur = oldlist_cur->next;
	}

	newlist_prev->next = NULL;
	newlist->tail = newlist_prev;

	check_list_invariants(newlist);
	return newlist;
}

/*
 * Temporary compatibility functions
 *
 * In order to avoid warnings for these function definitions, we need
 * to include a prototype here as well as in pg_list.h. That's because
 * we don't enable list API compatibility in list.c, so we
 * don't see the prototypes for these functions.
 */

/*
 * Given a list, return its length. This is merely defined for the
 * sake of backward compatibility: we can't afford to define a macro
 * called "length", so it must be a function. New code should use the
 * list_length() macro in order to avoid the overhead of a function
 * call.
 */
int			length(const List *list);


