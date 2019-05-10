#include "execution/index/art/art.hpp"
#include "execution/expression_executor.hpp"
#include <algorithm>

using namespace duckdb;
using namespace std;

ART::ART(DataTable &table, vector<column_t> column_ids, vector<TypeId> types, vector<TypeId> expression_types,
         vector<unique_ptr<Expression>> expressions, vector<unique_ptr<Expression>> unbound_expressions)
    : Index(IndexType::ART, move(expressions), move(unbound_expressions)), table(table), column_ids(column_ids),
      types(types) {
	tree = NULL;
	expression_result.Initialize(expression_types);
	int n = 1;
	// little endian if true
	if (*(char *)&n == 1) {
		is_little_endian = true;
	} else {
		is_little_endian = false;
	}
	switch (types[0]) {
	case TypeId::BOOLEAN:
	case TypeId::TINYINT:
		maxPrefix = 1;
		break;
	case TypeId::SMALLINT:
		maxPrefix = 2;
		break;
	case TypeId::INTEGER:
		maxPrefix = 4;
		break;
	case TypeId::BIGINT:
		maxPrefix = 8;
		break;
	default:
		throw InvalidTypeException(types[0], "Invalid type for index");
	}
}

void ART::Insert(DataChunk &input, Vector &row_ids) {
	if (input.column_count > 1) {
		throw NotImplementedException("We only support single dimensional indexes currently");
	}
	assert(row_ids.type == TypeId::BIGINT);
	assert(input.size() == row_ids.count);
	assert(types[0] == input.data[0].type);
	switch (input.data[0].type) {
	case TypeId::BOOLEAN:
	case TypeId::TINYINT:
		templated_insert<int8_t>(input, row_ids);
		break;
	case TypeId::SMALLINT:
		templated_insert<int16_t>(input, row_ids);
		break;
	case TypeId::INTEGER:
		templated_insert<int32_t>(input, row_ids);
		break;
	case TypeId::BIGINT:
		templated_insert<int64_t>(input, row_ids);
		break;
	default:
		throw InvalidTypeException(input.data[0].type, "Invalid type for index");
	}
}

void ART::Delete(DataChunk &input, Vector &row_ids) {
	if (input.column_count > 1) {
		throw NotImplementedException("We only support single dimensional indexes currently");
	}
	lock_guard<mutex> l(lock);
	assert(row_ids.type == TypeId::BIGINT);
	assert(input.size() == row_ids.count);
	assert(types[0] == input.data[0].type);
	switch (input.data[0].type) {
	case TypeId::BOOLEAN:
	case TypeId::TINYINT:
		templated_delete<int8_t>(input, row_ids);
		break;
	case TypeId::SMALLINT:
		templated_delete<int16_t>(input, row_ids);
		break;
	case TypeId::INTEGER:
		templated_delete<int32_t>(input, row_ids);
		break;
	case TypeId::BIGINT:
		templated_delete<int64_t>(input, row_ids);
		break;
	default:
		throw InvalidTypeException(input.data[0].type, "Invalid type for index");
	}
}

unique_ptr<IndexScanState> ART::InitializeScanSinglePredicate(Transaction &transaction, vector<column_t> column_ids,
                                                              Value value, ExpressionType expression_type) {
	auto result = make_unique<ARTIndexScanState>(column_ids);
	result->values[0] = value;
	result->expressions[0] = expression_type;
	return move(result);
}

unique_ptr<IndexScanState> ART::InitializeScanTwoPredicates(Transaction &transaction, vector<column_t> column_ids,
                                                            Value low_value, ExpressionType low_expression_type,
                                                            Value high_value, ExpressionType high_expression_type) {
	auto result = make_unique<ARTIndexScanState>(column_ids);
	result->values[0] = low_value;
	result->expressions[0] = low_expression_type;
	result->values[1] = high_value;
	result->expressions[1] = high_expression_type;
	return move(result);
}

void ART::Append(ClientContext &context, DataChunk &appended_data, uint64_t row_identifier_start) {
	lock_guard<mutex> l(lock);

	// first resolve the expressions
	ExpressionExecutor executor(appended_data);
	executor.Execute(expressions, expression_result);

	// create the row identifiers
	StaticVector<int64_t> row_identifiers;
	auto row_ids = (int64_t *)row_identifiers.data;
	row_identifiers.count = appended_data.size();
	for (size_t i = 0; i < row_identifiers.count; i++) {
		row_ids[i] = row_identifier_start + i;
	}

	Insert(expression_result, row_identifiers);
}

bool ART::leafMatches(bool is_little_endian, Node *node, Key &key, unsigned keyLength, unsigned depth) {
	if (depth != keyLength) {
		auto leaf = static_cast<Leaf *>(node);
		Key &leafKey = *new Key(is_little_endian, types[0], leaf->value,keyLength);
		for (unsigned i = depth; i < keyLength; i++)
			if (leafKey[i] != key[i])
				return false;
	}
	return true;
}

void ART::erase(bool isLittleEndian, Node *node, Node **nodeRef, Key &key, unsigned depth, unsigned maxKeyLength,
                TypeId type, uint64_t row_id) {
	if (!node)
		return;
	// Delete a leaf from a tree
	if (node->type == NodeType::NLeaf) {
		// Make sure we have the right leaf
		if (ART::leafMatches(isLittleEndian, node, key, maxKeyLength, depth))
			*nodeRef = NULL;
		return;
	}

	// Handle prefix
	if (node->prefixLength) {
		if (Node::prefixMismatch(isLittleEndian, node, key, depth, maxKeyLength, type) != node->prefixLength)
			return;
		depth += node->prefixLength;
	}

	Node **child = Node::findChild(key[depth], node);
	if ((*child)->type == NodeType::NLeaf && leafMatches(isLittleEndian, *child, key, maxKeyLength, depth)) {
		// Leaf found, remove entry
		auto leaf = static_cast<Leaf *>(*child);
		if (leaf->num_elements > 1) {
			leaf->remove(leaf, row_id);
			return;
		} else {
			// Leaf only has one element, delete leaf
			switch (node->type) {
			case NodeType::N4:
				Node4::erase(static_cast<Node4 *>(node), nodeRef, child);
				break;

			case NodeType::N16:
				Node16::erase(static_cast<Node16 *>(node), nodeRef, child);
				break;

			case NodeType::N48:
				Node48::erase(static_cast<Node48 *>(node), nodeRef, key[depth]);
				break;

			case NodeType::N256:
				Node256::erase(static_cast<Node256 *>(node), nodeRef, key[depth]);
				break;
			default:
				assert(0);
				break;
			}
		}

	} else {
		// Recurse
		erase(isLittleEndian, *child, child, key, depth + 1, maxKeyLength, type, row_id);
	}
}

void ART::insert(bool isLittleEndian, Node *node, Node **nodeRef, Key &key, unsigned depth, uintptr_t value,
                 unsigned maxKeyLength, TypeId type, uint64_t row_id) {
	if (node == NULL) {
		*nodeRef = new Leaf(value, row_id, maxKeyLength);
		return;
	}

	if (node->type == NodeType::NLeaf) {
		// Replace leaf with Node4 and store both leaves in it
		auto leaf = static_cast<Leaf *>(node);
		Key &existingKey = *new Key(isLittleEndian, type, leaf->value,maxKeyLength);
		unsigned newPrefixLength = 0;
		// Leaf node is already there, update row_id vector
		if (depth + newPrefixLength == maxKeyLength) {
			Leaf::insert(leaf, row_id);
			return;
		}
		while (existingKey[depth + newPrefixLength] == key[depth + newPrefixLength]) {
			newPrefixLength++;
			// Leaf node is already there, update row_id vector
			if (depth + newPrefixLength == maxKeyLength) {
				Leaf::insert(leaf, row_id);
				return;
			}
		}
		Node4 *newNode = new Node4(node->maxPrefixLength);
		newNode->prefixLength = newPrefixLength;
		memcpy(newNode->prefix, &key[depth], Node::min(newPrefixLength, node->maxPrefixLength));
		*nodeRef = newNode;

		Node4::insert(newNode, nodeRef, existingKey[depth + newPrefixLength], node);
		Node4::insert(newNode, nodeRef, key[depth + newPrefixLength], new Leaf(value, row_id, node->maxPrefixLength));
		return;
	}

	// Handle prefix of inner node
	if (node->prefixLength) {
		unsigned mismatchPos = Node::prefixMismatch(isLittleEndian, node, key, depth, maxKeyLength, type);
		if (mismatchPos != node->prefixLength) {
			// Prefix differs, create new node
			Node4 *newNode = new Node4(node->maxPrefixLength);
			*nodeRef = newNode;
			newNode->prefixLength = mismatchPos;
			memcpy(newNode->prefix, node->prefix, Node::min(mismatchPos, node->maxPrefixLength));
			// Break up prefix
			if (node->prefixLength < node->maxPrefixLength) {
				Node4::insert(newNode, nodeRef, node->prefix[mismatchPos], node);
				node->prefixLength -= (mismatchPos + 1);
				memmove(node->prefix, node->prefix + mismatchPos + 1,
				        Node::min(node->prefixLength, node->maxPrefixLength));
			} else {
				node->prefixLength -= (mismatchPos + 1);
				auto leaf = static_cast<Leaf *>(Node::minimum(node));
				Key &minKey = *new Key(isLittleEndian, type, leaf->value,maxKeyLength);
				Node4::insert(newNode, nodeRef, minKey[depth + mismatchPos], node);
				memmove(node->prefix, &minKey[depth + mismatchPos + 1],
				        Node::min(node->prefixLength, node->maxPrefixLength));
			}
			Node4::insert(newNode, nodeRef, key[depth + mismatchPos], new Leaf(value, row_id, node->maxPrefixLength));
			return;
		}
		depth += node->prefixLength;
	}

	// Recurse
	Node **child = Node::findChild(key[depth], node);
	if (child) {
		insert(isLittleEndian, *child, child, key, depth + 1, value, maxKeyLength, type, row_id);
		return;
	}

	Node *newNode = new Leaf(value, row_id, node->maxPrefixLength);
	Node::insertLeaf(node, nodeRef, key[depth], newNode);
}

Node *ART::lookup(Node *node, Key &key, unsigned keyLength, unsigned depth) {

	bool skippedPrefix = false; // Did we optimistically skip some prefix without checking it?

	while (node != NULL) {
		if (node->type == NodeType::NLeaf) {
			if (!skippedPrefix && depth == keyLength) // No check required
				return node;

			if (depth != keyLength) {
				// Check leaf
				auto leaf = static_cast<Leaf *>(node);
				Key &leafKey = *new Key(is_little_endian, types[0], leaf->value,keyLength);

				for (unsigned i = (skippedPrefix ? 0 : depth); i < keyLength; i++)
					if (leafKey[i] != key[i])
						return NULL;
			}
			return node;
		}

		if (node->prefixLength) {
			if (node->prefixLength < node->maxPrefixLength) {
				for (unsigned pos = 0; pos < node->prefixLength; pos++)
					if (key[depth + pos] != node->prefix[pos])
						return NULL;
			} else {
				skippedPrefix = true;
			}
			depth += node->prefixLength;
		}

		auto posNode = Node::findChild(key[depth], node);
		if (posNode)
			node = *posNode;
		else
			return NULL;
		depth++;
	}

	return NULL;
}

bool ART::iteratorNext(Iterator& iter) {
    // Skip leaf
    if ((iter.depth)&&((iter.stack[iter.depth-1].node)->type == NodeType::NLeaf))
        iter.depth--;

    // Look for next leaf
    while (iter.depth) {
        Node* node=iter.stack[iter.depth-1].node;

        // Leaf found
        if (node->type == NodeType::NLeaf) {
			auto leaf = static_cast<Leaf *>(node);
            iter.node=leaf;
            return true;
        }

        // Find next node
        Node* next=nullptr;
        switch (node->type) {
            case NodeType::N4: {
                Node4* n=static_cast<Node4*>(node);
                if (iter.stack[iter.depth-1].pos<node->count)
                    next=n->child[iter.stack[iter.depth-1].pos++];
                break;
            }
            case NodeType::N16: {
                Node16* n=static_cast<Node16*>(node);
                if (iter.stack[iter.depth-1].pos<node->count)
                    next=n->child[iter.stack[iter.depth-1].pos++];
                break;
            }
            case NodeType::N48: {
                Node48* n=static_cast<Node48*>(node);
                unsigned depth=iter.depth-1;
                for (;iter.stack[depth].pos<256;iter.stack[depth].pos++)
                    if (n->childIndex[iter.stack[depth].pos]!=48) {
                        next=n->child[n->childIndex[iter.stack[depth].pos++]];
                        break;
                    }
                break;
            }
            case NodeType::N256: {
                Node256* n=static_cast<Node256*>(node);
                unsigned depth=iter.depth-1;
                for (;iter.stack[depth].pos<256;iter.stack[depth].pos++)
                    if (n->child[iter.stack[depth].pos]) {
                        next=n->child[iter.stack[depth].pos++];
                        break;
                    }
                break;
            }
			default:
				assert(0);
				break;
        }

        if (next) {
            iter.stack[iter.depth].pos=0;
            iter.stack[iter.depth].node=next;
            iter.depth++;
        } else
            iter.depth--;
    }

    return false;
}


bool ART::bound(Node* n,Key &key,unsigned keyLength,Iterator& iterator,unsigned maxKeyLength,bool inclusive, bool isLittleEndian) {
	iterator.depth=0;

	if (!n)
		return false;

	unsigned depth=0;
	while (true) {
		iterator.stack[iterator.depth].node=n;
		int& pos=iterator.stack[iterator.depth].pos;
		iterator.depth++;

		if (n->type == NodeType::NLeaf) {
            auto leaf = static_cast<Leaf *>(n);
			iterator.node=leaf;
			if (depth==keyLength) {
				// Equal
				if (inclusive)
					return true; else
					return iteratorNext(iterator);
			}

			Key &leafKey = *new Key(isLittleEndian, types[0], leaf->value,maxKeyLength);
			for (unsigned i=depth; i<keyLength; i++)
				if (leafKey[i]!=key[i]) {
					if (leafKey[i]<key[i]) {
						// Less
						iterator.depth--;
						return iteratorNext(iterator);
					}
					// Greater
					return true;
				}

			// Equal
			if (inclusive)
				return true; else
				return iteratorNext(iterator);
		}
		unsigned mismatchPos = Node::prefixMismatch(isLittleEndian, n, key, depth, maxKeyLength, types[0]);

		if (mismatchPos!=n->prefixLength) {
			if (n->prefix[mismatchPos]<key[depth+mismatchPos]) {
				// Less
				iterator.depth--;
				return iteratorNext(iterator);
			}
			// Greater
			pos=0;
			return iteratorNext(iterator);
		}
		depth+=n->prefixLength;
		uint8_t keyByte=key[depth];

		Node* next=nullptr;
		switch (n->type) {
			case NodeType::N4: {
				Node4* node=static_cast<Node4*>(n);
				for (pos=0;pos<node->count;pos++)
					if (node->key[pos]==keyByte) {
						next=node->child[pos];
						break;
					} else if (node->key[pos]>keyByte)
						break;
				break;
			}
			case NodeType::N16: {
				Node16* node=static_cast<Node16*>(n);
				for (pos=0;pos<node->count;pos++)
					if (node->key[pos]==keyByte) {
						next=node->child[pos];
						break;
					} else if (node->key[pos]>keyByte)
						break;
				break;
			}
			case NodeType::N48: {
				Node48* node=static_cast<Node48*>(n);
				pos=keyByte;
				if (node->childIndex[keyByte]!=48) {
					next=node->child[node->childIndex[keyByte]];
					break;
				}
				break;
			}
			case NodeType::N256: {
				Node256* node=static_cast<Node256*>(n);
				pos=keyByte;
				next=node->child[keyByte];
				break;
			}
			default:
				assert(0);
				break;
		}

		if (!next)
			return iteratorNext(iterator);

		pos++;
		n=next;
		depth++;
	}
}


void ART::Update(ClientContext &context, vector<column_t> &update_columns, DataChunk &update_data,
                 Vector &row_identifiers) {
	// first check if the columns we use here are updated
	bool index_is_updated = false;
	for (auto &column : update_columns) {
		if (find(column_ids.begin(), column_ids.end(), column) != column_ids.end()) {
			index_is_updated = true;
			break;
		}
	}
	if (!index_is_updated) {
		// none of the indexed columns are updated
		// we can ignore the update
		return;
	}
	// otherwise we need to change the data inside the index
	lock_guard<mutex> l(lock);

	DataChunk temp_chunk;
	temp_chunk.Initialize(table.types);
	temp_chunk.data[0].count = update_data.size();
	for (size_t i = 0; i < column_ids.size(); i++) {
		if (column_ids[i] == COLUMN_IDENTIFIER_ROW_ID) {
			continue;
		}
		bool found_column = false;
		for (size_t j = 0; i < update_columns.size(); j++) {
			if (column_ids[i] == update_columns[j]) {
				temp_chunk.data[column_ids[i]].Reference(update_data.data[update_columns[j]]);
				found_column = true;
				break;
			}
		}
		assert(found_column);
	}

	// now resolve the expressions on the temp_chunk
	ExpressionExecutor executor(temp_chunk);
	executor.Execute(expressions, expression_result);

	// insert the expression result
	Insert(expression_result, row_identifiers);
}

void ART::SearchEqual(StaticVector<int64_t> *result_identifiers,ARTIndexScanState * state){
	auto row_ids = (int64_t *)result_identifiers->data;
	switch (types[0]) {
		case TypeId::BOOLEAN:
			result_identifiers->count = templated_lookup<int8_t>(types[0], state->values[0].value_.boolean, row_ids);
			break;
		case TypeId::TINYINT:
			result_identifiers->count = templated_lookup<int8_t>(types[0], state->values[0].value_.tinyint, row_ids);
			break;
		case TypeId::SMALLINT:
			result_identifiers->count =
					templated_lookup<int16_t>(types[0], state->values[0].value_.smallint, row_ids);
			break;
		case TypeId::INTEGER:
			result_identifiers->count =
					templated_lookup<int32_t>(types[0], state->values[0].value_.integer, row_ids);
			break;
		case TypeId::BIGINT:
			result_identifiers->count = templated_lookup<int64_t>(types[0], state->values[0].value_.bigint, row_ids);
			break;
		default:
			throw InvalidTypeException(types[0], "Invalid type for index");
	}
}

void ART::SearchGreater(StaticVector<int64_t> *result_identifiers,ARTIndexScanState * state, bool inclusive){
	auto row_ids = (int64_t *)result_identifiers->data;
	switch (types[0]) {
		case TypeId::BOOLEAN:
			result_identifiers->count = templated_greater_scan<int8_t>(types[0], state->values[0].value_.boolean, row_ids,inclusive);
			break;
		case TypeId::TINYINT:
			result_identifiers->count = templated_greater_scan<int8_t>(types[0], state->values[0].value_.tinyint, row_ids,inclusive);
			break;
		case TypeId::SMALLINT:
			result_identifiers->count =
					templated_greater_scan<int16_t>(types[0], state->values[0].value_.smallint, row_ids,inclusive);
			break;
		case TypeId::INTEGER:
			result_identifiers->count =
					templated_greater_scan<int32_t>(types[0], state->values[0].value_.integer, row_ids,inclusive);
			break;
		case TypeId::BIGINT:
			result_identifiers->count = templated_greater_scan<int64_t>(types[0], state->values[0].value_.bigint, row_ids,inclusive);
			break;
		default:
			throw InvalidTypeException(types[0], "Invalid type for index");
	}
}

void ART::SearchLess(StaticVector<int64_t> *result_identifiers,ARTIndexScanState * state, bool inclusive){
	auto row_ids = (int64_t *)result_identifiers->data;
	switch (types[0]) {
		case TypeId::BOOLEAN:
			result_identifiers->count = templated_less_scan<int8_t>(types[0], state->values[0].value_.boolean, row_ids,inclusive);
			break;
		case TypeId::TINYINT:
			result_identifiers->count = templated_less_scan<int8_t>(types[0], state->values[0].value_.tinyint, row_ids,inclusive);
			break;
		case TypeId::SMALLINT:
			result_identifiers->count =
					templated_less_scan<int16_t>(types[0], state->values[0].value_.smallint, row_ids,inclusive);
			break;
		case TypeId::INTEGER:
			result_identifiers->count =
					templated_less_scan<int32_t>(types[0], state->values[0].value_.integer, row_ids,inclusive);
			break;
		case TypeId::BIGINT:
			result_identifiers->count = templated_less_scan<int64_t>(types[0], state->values[0].value_.bigint, row_ids,inclusive);
			break;
		default:
			throw InvalidTypeException(types[0], "Invalid type for index");
	}
}

void ART::SearchCloseRange(StaticVector<int64_t> *result_identifiers,ARTIndexScanState * state, bool left_inclusive,bool right_inclusive){
	auto row_ids = (int64_t *)result_identifiers->data;
	switch (types[0]) {
		case TypeId::BOOLEAN:
			result_identifiers->count = templated_close_range<int8_t>(types[0], state->values[0].value_.boolean,state->values[1].value_.boolean, row_ids,left_inclusive,right_inclusive);
			break;
		case TypeId::TINYINT:
			result_identifiers->count = templated_close_range<int8_t>(types[0], state->values[0].value_.tinyint,state->values[1].value_.tinyint, row_ids,left_inclusive,right_inclusive);
			break;
		case TypeId::SMALLINT:
			result_identifiers->count =
					templated_close_range<int16_t>(types[0], state->values[0].value_.smallint,state->values[1].value_.smallint, row_ids,left_inclusive,right_inclusive);
			break;
		case TypeId::INTEGER:
			result_identifiers->count =
					templated_close_range<int32_t>(types[0], state->values[0].value_.integer,state->values[1].value_.integer, row_ids,left_inclusive,right_inclusive);
			break;
		case TypeId::BIGINT:
			result_identifiers->count = templated_close_range<int64_t>(types[0], state->values[0].value_.bigint,state->values[1].value_.bigint, row_ids,left_inclusive,right_inclusive);
			break;
		default:
			throw InvalidTypeException(types[0], "Invalid type for index");
	}
}

//FIXME: Returning one tuple per time so deletes in different chunks do not break.
void ART::Scan(Transaction &transaction, IndexScanState *ss, DataChunk &result) {
	auto state = (ARTIndexScanState *)ss;
    // scan the index
	if (!state->checked){
        assert(state->values[0].type == types[0]);

        //single predicate
        if (state->values[1].is_null){

            switch(state->expressions[0]){
                case ExpressionType::COMPARE_EQUAL:
                    SearchEqual(&state->result_identifiers,state);
                    break;
                case ExpressionType::COMPARE_GREATERTHANOREQUALTO:
                    SearchGreater(&state->result_identifiers,state,true);
                    break;
                case ExpressionType::COMPARE_GREATERTHAN:
                    SearchGreater(&state->result_identifiers,state,false);
                    break;
                case ExpressionType::COMPARE_LESSTHANOREQUALTO:
                    SearchLess(&state->result_identifiers,state,true);
                    break;
                case ExpressionType::COMPARE_LESSTHAN:
                    SearchLess(&state->result_identifiers,state,false);
                    break;
                default:
                    throw NotImplementedException("Operation not implemented");
                    break;
            }
        }
            //two predicates
        else{
            assert(state->values[1].type == types[0]);
            bool left_inclusive = state->expressions[0] == ExpressionType ::COMPARE_GREATERTHANOREQUALTO;
            bool right_inclusive = state->expressions[1] == ExpressionType ::COMPARE_LESSTHANOREQUALTO;
            SearchCloseRange(&state->result_identifiers,state,left_inclusive,right_inclusive);
        }
        state->checked = true;
    }

    // scan the index
    if (state->current_tuple == state->result_identifiers.count)
        return;
    StaticVector<int64_t> result_identifiers_per_tuple;
    auto row_id = (int64_t *)result_identifiers_per_tuple.data;
    auto cur_row_id = (int64_t *)state->result_identifiers.data;

    row_id[0] = cur_row_id[state->current_tuple++];
    result_identifiers_per_tuple.count++;
//	do {
//
//
//
//		if (result_identifiers.count == 0) {
//			return;
//		}
    table.Fetch(transaction, result, state->column_ids, result_identifiers_per_tuple);
//        break;
//	} while (result_identifiers_per_tuple.count == 0);
}
