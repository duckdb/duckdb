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
 * Portions Copyright (c) 1996-2017, PostgreSQL Global Development PGGroup
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * IDENTIFICATION
 *	  src/backend/nodes/list.c
 *
 *-------------------------------------------------------------------------
 */
#include "pg_functions.hpp"
#include "nodes/pg_list.hpp"


/*
 * Routines to simplify writing assertions about the type of a list; a
 * NIL list is considered to be an empty list of any type.
 */
#define IsPointerList(l)		((l) == NIL || IsA((l), PGList))
#define IsIntegerList(l)		((l) == NIL || IsA((l), IntList))
#define IsOidList(l)			((l) == NIL || IsA((l), OidList))

#ifdef USE_ASSERT_CHECKING
/*
 * Check that the specified PGList is valid (so far as we can tell).
 */
static void
check_list_invariants(const PGList *list)
{
	if (list == NIL)
		return;

	Assert(list->length > 0);
	Assert(list->head != NULL);
	Assert(list->tail != NULL);

	Assert(list->type == T_PGList ||
		   list->type == T_PGIntList ||
		   list->type == T_PGOidList);

	if (list->length == 1)
		Assert(list->head == list->tail);
	if (list->length == 2)
		Assert(list->head->next == list->tail);
	Assert(list->tail->next == NULL);
}
#else
#define check_list_invariants(l)
#endif							/* USE_ASSERT_CHECKING */

/*
 * Return a freshly allocated List. Since empty non-NIL lists are
 * invalid, new_list() also allocates the head cell of the new list:
 * the caller should be sure to fill in that cell's data.
 */
static PGList *
new_list(PGNodeTag type)
{
	PGList	   *new_list;
	PGListCell   *new_head;

	new_head = (PGListCell *) palloc(sizeof(*new_head));
	new_head->next = NULL;
	/* new_head->data is left undefined! */

	new_list = (PGList *) palloc(sizeof(*new_list));
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
new_head_cell(PGList *list)
{
	PGListCell   *new_head;

	new_head = (PGListCell *) palloc(sizeof(*new_head));
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
new_tail_cell(PGList *list)
{
	PGListCell   *new_tail;

	new_tail = (PGListCell *) palloc(sizeof(*new_tail));
	new_tail->next = NULL;

	list->tail->next = new_tail;
	list->tail = new_tail;
	list->length++;
}

/*
 * PGAppend a pointer to the list. A pointer to the modified list is
 * returned. Note that this function may or may not destructively
 * modify the list; callers should always use this function's return
 * value, rather than continuing to use the pointer passed as the
 * first argument.
 */
PGList *
lappend(PGList *list, void *datum)
{
	Assert(IsPointerList(list));

	if (list == NIL)
		list = new_list(T_PGList);
	else
		new_tail_cell(list);

	lfirst(list->tail) = datum;
	check_list_invariants(list);
	return list;
}

/*
 * PGAppend an integer to the specified list. See lappend()
 */


/*
 * PGAppend an OID to the specified list. See lappend()
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
 * Caution: before Postgres 8.0, the original PGList was unmodified and
 * could be considered to retain its separate identity.  This is no longer
 * the case.
 */
PGList *
lcons(void *datum, PGList *list)
{
	Assert(IsPointerList(list));

	if (list == NIL)
		list = new_list(T_PGList);
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
PGList *
list_concat(PGList *list1, PGList *list2)
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
PGList *
list_truncate(PGList *list, int new_size)
{
	PGListCell   *cell;
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
PGListCell *
list_nth_cell(const PGList *list, int n)
{
	PGListCell   *match;

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
 * specified list. (PGList elements begin at 0.)
 */
void *
list_nth(const PGList *list, int n)
{
	Assert(IsPointerList(list));
	return lfirst(list_nth_cell(list, n));
}

/*
 * Delete 'cell' from 'list'; 'prev' is the previous element to 'cell'
 * in 'list', if any (i.e. prev == NULL iff list->head == cell)
 *
 * The cell is pfree'd, as is the PGList header if this was the last member.
 */
PGList *
list_delete_cell(PGList *list, PGListCell *cell, PGListCell *prev)
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
 * Free all storage in a list, and optionally the pointed-to elements
 */
static void
list_free_private(PGList *list, bool deep)
{
	PGListCell   *cell;

	check_list_invariants(list);

	cell = list_head(list);
	while (cell != NULL)
	{
		PGListCell   *tmp = cell;

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
list_free(PGList *list)
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
PGList *
list_copy(const PGList *oldlist)
{
	PGList	   *newlist;
	PGListCell   *newlist_prev;
	PGListCell   *oldlist_cur;

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
		PGListCell   *newlist_cur;

		newlist_cur = (PGListCell *) palloc(sizeof(*newlist_cur));
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
PGList *
list_copy_tail(const PGList *oldlist, int nskip)
{
	PGList	   *newlist;
	PGListCell   *newlist_prev;
	PGListCell   *oldlist_cur;

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
		PGListCell   *newlist_cur;

		newlist_cur = (PGListCell *) palloc(sizeof(*newlist_cur));
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
int			length(const PGList *list);


