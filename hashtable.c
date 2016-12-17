/*
 * hashtable.c
 *
 *  Created on: 16 Dec 2016
 *      Author: lena
 */

#include <stdlib.h>
#include <stdio.h>

#include "hashtable.h"

struct node_t {
	int key;
	void* value;
	struct node_t* next;
};

typedef struct node_t Node;
typedef int (*Hash)(int, int);

struct hashtable_t {
	int nr_buckets, nr_elements;
	Hash hash_func;
	Node** table;
	int* buckets_sizes;
};

typedef struct hashtable_t hashtable_t;
//-----------------------------------------------------//
//Auxiliary functions:

/*
 * Auxiliary function:
 * allocate new pair of key-value
 */
Node* entry_alloc(int key, void* value) { //TODO change name to node

	Node* newpair;
	if ((newpair = malloc(sizeof(Node))) == NULL) {
		return NULL;
	}

	newpair->key = key;
	newpair->value = value;
	newpair->next = NULL;

	return newpair;
}

/*
 *  Auxiliary function:
 *  destroys list of node by the head
 */
void list_destroy(Node* node) { //TODO change name to node
	if (!node)
		return;
	list_destroy(node->next);
	free(node);
	node = NULL;
}

/*
 * Auxiliary function:
 * adds element to the tail of the list
 * the malloc is outside of this function
 */
int list_add(Node** dest, Node* element) { //TODO change name to node
	if (!element)
		return -1;

	Node* curr = *dest;
	Node* prev = NULL;
	while (curr) {
		if (curr->key == element->key) {
			return 0;
		}
		prev = curr;
		curr = curr->next;
	}
	//case of empty list, head == NULL
	if (!curr) {
		*dest = element;
	} else {
		prev->next = element;
	}
	return 1;
}

/*
 * Auxiliary function:
 * finding element by the key
 * in one bucket
 */
Node* list_find(Node* node, int key) { //TODO change name to node
	if (!node) {
		return NULL;
	}
	if (node->key == key) {
		return node;
	}
	return list_find(node->next, key);
}

/*
 * Auxiliary function:
 * removes element by the key
 * in one bucket
 */
int list_remove(Node* node, int key) { //TODO change name to node
	Node* next_node;
	if (!node)
		return 0;
	next_node = node->next;
	if (next_node && next_node->key == key) {
		node->next = node->next->next;
		free(next_node);
		return 1;
	}
	return list_remove(node->next, key);
}

/*
 * Auxiliary function:
 * finding element by the key
 * in the hash table
 */
Node* hash_find(hashtable_t* table, int key, int* index) {
	Node* element = NULL;
	int i;
	if (!table)
		return NULL;
	for (i = 0; i < table->nr_buckets; i++) {
		element = list_find(table->table[i], key);
		if (element)
			break;
	}
	if (index)
		*index = i;
	return element;
}

/*
 * Auxiliary function:
 * returns value of element by key
 * or NULL if doesn't exist
 */
void* hash_get_value(hashtable_t* table, int key) {
	if (!table)
		return NULL;
	Node* element = hash_find(table, key, NULL);
	if (!element)
		return NULL;
	return element->value;
}
//-----------------------------------------------------//
//Implementations of requested functions
hashtable_t* hash_alloc(int buckets, int (*hash)(int, int)) {

	hashtable_t* hashtable;

	if (buckets < 1 || hash == NULL)
		return NULL;

	// Allocate the table itself.
	if ((hashtable = malloc(sizeof(hashtable_t))) == NULL) {
		return NULL;
	}

	// Allocate array of nodes
	if ((hashtable->table = malloc(sizeof(Node *) * buckets)) == NULL) {
		free(hashtable);
		return NULL;
	}
	// Allocate array of the buckets sizes
	if ((hashtable->buckets_sizes = malloc(sizeof(int) * buckets)) == NULL) {
		free(hashtable->table);
		free(hashtable);
		return NULL;
	}

	for (int i = 0; i < buckets; i++) {
		hashtable->table[i] = NULL;
		hashtable->buckets_sizes[i] = 0;
	}
	hashtable->hash_func = hash;
	hashtable->nr_buckets = buckets;
	hashtable->nr_elements = 0;
	return hashtable;
}

int hash_free(hashtable_t* ht) {
	if (ht == NULL) {
		return -1;
	}
	for (int i = 0; i < ht->nr_buckets; ++i) {
		list_destroy(ht->table[i]);
	}
	free(ht->table);
	free(ht->buckets_sizes);
	free(ht);
	return 1;
}

int hash_insert(hashtable_t* table, int key, void* val) {
	if (!table)
		return -1;
	Node* new_element = entry_alloc(key, val);
	int hashed_key = table->hash_func(table->nr_buckets, key);
	int retval = list_add(table->table + hashed_key, new_element);

	if (retval == 0) {
		free(new_element);
		return 0;
	} else if (retval == 1) {
		table->nr_elements++;
		table->buckets_sizes[hashed_key]++;
		return 1;
	}
	free(new_element);
	return -1;
}

int hash_update(hashtable_t* table, int key, void *val) {
	if (!table)
		return -1;

	Node* element = hash_find(table, key, NULL);
	if (element == NULL)
		return 0;
	element->value = val;
	return 1;
}

int hash_remove(hashtable_t* table, int key) {
	if (!table)
		return -1;
	int* index;
	Node* element = hash_find(table, key, index);
	if (!element)
		return 0;
	//The element is the head:
	if (table->table[*index] && table->table[*index]->key == key) {
		table->table[*index] = element->next;
		free(element);

	} else {
		if (list_remove(table->table[*index], key) != 1)
			return -1;
	}
	table->nr_elements--;
	table->buckets_sizes[*index]--;
	return 1;
}

int hash_contains(hashtable_t* table, int key) {
	if (!table)
		return -1;
	Node* element = hash_find(table, key, NULL);
	if (!element)
		return 0;
	return 1;
}

int list_node_compute(hashtable_t* table, int key, void* (*compute_func)(void*),
		void** result) {
	if (!table || !compute_func)
		return -1;
	Node* element = hash_find(table, key, NULL);
	if (!element)
		return 0;
	*result = compute_func(element->value);
	return 1;
}

int hash_getbucketsize(hashtable_t* table, int bucket) { //TODO ask if the bucket start from 0?
	if (!table)
		return -1;
	if (bucket < 0 || bucket >= table->nr_buckets)
		return -1;
	return table->buckets_sizes[bucket];
}
