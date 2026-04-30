# Optimize `RowGroupCollection::Fetch` #6963

**Status:** Open
**Author:** Mytherin
**Opened:** Dec 19, 2025
**Labels:** feature

## Description

`RowGroupCollection::Fetch` currently does, per row-id:

- Look-up the row group the row-id belongs to
- Check if the row-id is valid for the given transaction
- Fetch the data for the row-id

Often row-ids are co-located within the same row group (or even the same vector within the same row group). We could optimize this by rewriting `RowGroup::Fetch` and `RowGroup::FetchRow` to take a `SelectionVector` of row-ids instead, and having `RowGroupCollection::Fetch` do a more optimized bulk fetch for each row group it is targeting. This could speed this up immensely if the rows are co-located.
