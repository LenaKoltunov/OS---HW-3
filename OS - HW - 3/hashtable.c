/*
 * hashtable.c
 *
 *  Created on: 16 Dec 2016
 *      Author: lena
 */

#include <stdlib.h>
#include <pthread.h>
#include <stdbool.h>
#include <semaphore.h>
#include <stdio.h>

#include "hashtable.h"

//#define _GNU_SOURCE

typedef struct node_t {
	pthread_mutex_t mutex;
	int key;
	void* value;
	struct node_t* next;
}* Node;

typedef int (*Hash)(int, int);

typedef struct hashtable_t {
	int nr_buckets, nr_threads, stopped;
	Hash hash_func;
	Node* table;
	pthread_mutex_t* empty_list_locks; //locks only for dummy node, not entire list
	int* buckets_sizes;
	pthread_mutex_t* sizes_locks;
	pthread_mutex_t empty_threads_list_lock;
	pthread_mutex_t nr_threads_lock;
	pthread_mutex_t stop_lock;
	pthread_cond_t stop_condition;
}* Hashtable;

typedef op_t* Op;

//-----------------------------------------------------//
//Auxiliary functions:

/*
 * Auxiliary function:
 * allocate new pair of key-value
 */
Node node_alloc(int key, void* value) {

	Node newpair;
	if ((newpair = malloc(sizeof(*newpair))) == NULL) {
		return NULL;
	}
	pthread_mutexattr_t attr;
	pthread_mutexattr_init(&attr);
	pthread_mutexattr_settype(&attr, PTHREAD_MUTEX_ERRORCHECK_NP);
	pthread_mutex_init(&newpair->mutex, &attr);
	newpair->key = key;
	newpair->value = value;
	newpair->next = NULL;

	return newpair;
}

/*
 *  Auxiliary function:
 *  destroys list of node by the head
 */
void list_destroy(Node node) {
	if (!node)
		return;
	list_destroy(node->next);
	pthread_mutex_destroy(&node->mutex);
	free(node);
}

/*
 * Auxiliary function:
 * adds element to the tail of the list
 * the malloc is outside of this function
 */
int list_add(Node* dest, Node element, pthread_mutex_t *head_mutex) {
	if (!element)
		return -1;

	Node curr = *dest;
	//case of empty list, head == NULL
	if (!curr) {
		pthread_mutex_lock(head_mutex);
		*dest = element;
		pthread_mutex_unlock(head_mutex);
		return 1;
	}

	Node prev = NULL;
	while (curr) {
		pthread_mutex_lock(&curr->mutex);
		if (curr->key == element->key) {
			if (prev) {
				pthread_mutex_unlock(&prev->mutex);
			}
			pthread_mutex_unlock(&curr->mutex);
			return 0;
		}
		if (prev) {
			pthread_mutex_unlock(&prev->mutex);
		}
		prev = curr;
		curr = curr->next;

	}
	prev->next = element;
	pthread_mutex_unlock(&prev->mutex);
	return 1;
}

int list_update(Node head, int key, void* val) {
	Node curr, prev;
	curr = head;
	prev = NULL;
	if (!head)
		return 0;

	while (curr) {
		pthread_mutex_lock(&curr->mutex);
		if (curr->key == key) {
			curr->value = val;
			if (prev) {
				pthread_mutex_unlock(&prev->mutex);
			}
			pthread_mutex_unlock(&curr->mutex);
			return 1;
		}
		if (prev) {
			pthread_mutex_unlock(&prev->mutex);
		}
		prev = curr;
		curr = curr->next;

	}
	pthread_mutex_unlock(&prev->mutex);
	return 0;

}

/*
 * Auxiliary function:
 * removes element by the key
 * in one bucket
 */
int list_remove(Node head, int key) {
	Node curr, prev;
	curr = head->next;
	prev = head;
	pthread_mutex_lock(&prev->mutex);
	if (!head)
		return 0;

	while (curr) {
		pthread_mutex_lock(&curr->mutex);
		if (curr->key == key) {
			prev->next = curr->next;
			pthread_mutex_unlock(&prev->mutex);
			pthread_mutex_unlock(&curr->mutex);
			pthread_mutex_destroy(&curr->mutex);
			free(curr);
			return 1;
		}
		pthread_mutex_unlock(&prev->mutex);
		prev = curr;
		curr = curr->next;
	}
	pthread_mutex_unlock(&prev->mutex);
	return 0;
}

bool list_contains(Node head, int key) {
	Node curr, prev;
	curr = head;
	prev = NULL;
	if (!head)
		return false;

	while (curr) {
		pthread_mutex_lock(&curr->mutex);
		if (curr->key == key) {
			if (prev) {
				pthread_mutex_unlock(&prev->mutex);
			}
			pthread_mutex_unlock(&curr->mutex);
			return true;
		}
		if (prev) {
			pthread_mutex_unlock(&prev->mutex);
		}
		prev = curr;
		curr = curr->next;
	}
	pthread_mutex_unlock(&prev->mutex);
	return false;
}

//-----------------------------------------------------//
//Implementations of requested functions
//Done
hashtable_t* hash_alloc(int buckets, int (*hash)(int, int)) {

	hashtable_t* hashtable;

	if (buckets < 1 || hash == NULL)
		return NULL;

	// Allocate the table itself.
	if ((hashtable = malloc(sizeof(*hashtable))) == NULL) {
		return NULL;
	}
	// Allocate array of nodes
	if ((hashtable->table = malloc(sizeof(Node) * buckets)) == NULL) {
		free(hashtable);
		return NULL;
	}
	// Allocate array of the buckets sizes
	if ((hashtable->buckets_sizes = malloc(sizeof(int) * buckets)) == NULL) {
		free(hashtable->table);
		free(hashtable);
		return NULL;
	}
	// Allocate array of mutexs for empty lists
	if ((hashtable->empty_list_locks = malloc(sizeof(pthread_mutex_t) * buckets))
			== NULL) {
		free(hashtable->buckets_sizes);
		free(hashtable->table);
		free(hashtable);
		return NULL;
	}

	// Allocate array of mutexs for empty lists
	if ((hashtable->sizes_locks = malloc(sizeof(pthread_mutex_t) * buckets))
			== NULL) {
		hash_free(hashtable);
		return NULL;
	}

	for (int i = 0; i < buckets; i++) {
		hashtable->table[i] = NULL;
		hashtable->buckets_sizes[i] = 0;
		pthread_mutexattr_t attr;
		pthread_mutexattr_init(&attr);
		pthread_mutexattr_settype(&attr, PTHREAD_MUTEX_ERRORCHECK_NP);
		pthread_mutex_init(&hashtable->empty_list_locks[i], &attr);
		pthread_mutex_init(&hashtable->sizes_locks[i], &attr);

	}

	hashtable->hash_func = hash;
	hashtable->nr_buckets = buckets;
	hashtable->nr_threads = 0;
	hashtable->stopped = 0;

	pthread_mutexattr_t attr;
	pthread_mutex_init(&hashtable->empty_threads_list_lock, &attr);
	pthread_mutex_init(&hashtable->nr_threads_lock, &attr);
	pthread_mutex_init(&hashtable->stop_lock, &attr);
	pthread_cond_init(&hashtable->stop_condition, NULL);
	return hashtable;
}

int hash_stop(hashtable_t* table) {
	if (!table)
		return -1;
	if (table->stopped == 1) {
		return -1;
	}
	table->stopped = 1;
	while (table->nr_threads > 0) {

	}
	pthread_mutex_lock(&table->stop_lock);
	pthread_cond_signal(&table->stop_condition);
	pthread_mutex_unlock(&table->stop_lock);
	return 1;
}

int hash_free(hashtable_t* ht) {
	if (ht == NULL) {
		return -1;
	}
	if (!ht->stopped) {
		return 0;
	}
	pthread_mutex_lock(&ht->stop_lock);
	if (ht->nr_threads > 0)
		pthread_cond_wait(&ht->stop_condition, &ht->stop_lock);
	pthread_mutex_unlock(&ht->stop_lock);

	for (int i = 0; i < ht->nr_buckets; ++i) {
		list_destroy(ht->table[i]);
		pthread_mutex_destroy(&ht->empty_list_locks[i]);
		pthread_mutex_destroy(&ht->sizes_locks[i]);
	}
	pthread_mutex_destroy(&ht->empty_threads_list_lock);
	pthread_mutex_destroy(&ht->nr_threads_lock);
	pthread_mutex_destroy(&ht->stop_lock);
	pthread_cond_destroy(&ht->stop_condition);
	free(ht->sizes_locks);
	free(ht->empty_list_locks);
	free(ht->table);
	free(ht->buckets_sizes);
	free(ht);
	return 1;
}

//Done
int hash_insert(hashtable_t* table, int key, void* val) {
	if (!table)
		return -1;
	if (table->stopped) {
		return -1;
	}
	Node new_element = node_alloc(key, val);
	int hashed_key = table->hash_func(table->nr_buckets, key);
	if (hashed_key < 0 || hashed_key >= table->nr_buckets) {
		return -1;
	}

	int retval = list_add(&table->table[hashed_key], new_element,
			&table->empty_list_locks[hashed_key]);

	if (retval == 0) {
		free(new_element);
		return 0;
	} else if (retval == 1) {
		pthread_mutex_lock(&table->sizes_locks[hashed_key]);
		table->buckets_sizes[hashed_key]++;
		pthread_mutex_unlock(&table->sizes_locks[hashed_key]);
		return 1;
	}
	free(new_element);
	return -1;
}

int hash_update(hashtable_t* table, int key, void *val) {
	if (!table)
		return -1;
	if (table->stopped) {
		return -1;
	}
	int hashed_key = table->hash_func(table->nr_buckets, key);
	if (hashed_key < 0 || hashed_key >= table->nr_buckets) {
		return -1;
	}

	return list_update(table->table[hashed_key], key, val);

}

int hash_remove(hashtable_t* table, int key) {
	if (!table)
		return -1;
	if (table->stopped) {
		return -1;
	}

	int hashed_key = table->hash_func(table->nr_buckets, key);
	if (hashed_key < 0 || hashed_key >= table->nr_buckets) {
		return -1;
	}

	Node head = table->table[hashed_key];
	//The element is the head:
	if (head) {
		pthread_mutex_lock(&head->mutex);
		if (head->key == key) {
			table->table[hashed_key] = head->next;
			pthread_mutex_unlock(&head->mutex);
			pthread_mutex_destroy(&head->mutex);
			free(head);
			pthread_mutex_lock(&table->sizes_locks[hashed_key]);
			table->buckets_sizes[hashed_key]--;
			pthread_mutex_unlock(&table->sizes_locks[hashed_key]);
			return 1;
		}
		pthread_mutex_unlock(&head->mutex);
		//----------------------------------------------------
		int ret = list_remove(head, key);
		if (ret == 1) {
			pthread_mutex_lock(&table->sizes_locks[hashed_key]);
			table->buckets_sizes[hashed_key]--;
			pthread_mutex_unlock(&table->sizes_locks[hashed_key]);
			return 1;
		} else if (ret == -1) {
			return -1;
		}
	}
	return 0;
}

int hash_contains(hashtable_t* table, int key) {
	if (!table)
		return -1;
	if (table->stopped) {
		return -1;
	}
	int hashed_key = table->hash_func(table->nr_buckets, key);
	if (hashed_key < 0 || hashed_key >= table->nr_buckets) {
		return -1;
	}

	if (list_contains(table->table[hashed_key], key))
		return 1;
	return 0;
}

int list_node_compute(hashtable_t* table, int key, void* (*compute_func)(void*),
		void** result) {
	if (!table || !compute_func || !result)
		return -1;

	int hashed_key = table->hash_func(table->nr_buckets, key);
	if (hashed_key < 0 || hashed_key >= table->nr_buckets) {
		return -1;
	}

	Node curr = table->table[hashed_key];
	if (!curr)
		return 0;

	Node prev = NULL;
	while (curr) {
		pthread_mutex_lock(&curr->mutex);
		if (curr->key == key) {
			*result = compute_func(curr->value);
			if (prev) {
				pthread_mutex_unlock(&prev->mutex);
			}
			pthread_mutex_unlock(&curr->mutex);
			return 1;
		}
		if (prev) {
			pthread_mutex_unlock(&prev->mutex);
		}
		prev = curr;
		curr = curr->next;

	}
	pthread_mutex_unlock(&prev->mutex);
	return 0;
}

int hash_getbucketsize(hashtable_t* table, int bucket) { //TODO ask if the bucket start from 0?
	if (!table)
		return -1;
	if (table->stopped) {
		return -1;
	}
	if (bucket < 0 || bucket >= table->nr_buckets)
		return -1;
	pthread_mutex_lock(&table->sizes_locks[bucket]);
	int res = table->buckets_sizes[bucket];
	pthread_mutex_unlock(&table->sizes_locks[bucket]);

	return res;
}

typedef struct args_t {
	Hashtable table;
	Op op;
	bool* runThreads;
}* Args;

void* thread_routine(void* args) {
	Args arguments = args;
	pthread_mutex_lock(&arguments->table->nr_threads_lock);
	arguments->table->nr_threads++;
	pthread_mutex_unlock(&arguments->table->nr_threads_lock);

	while (!(arguments->runThreads)) {
	};

	switch (arguments->op->op) {
	case INSERT:
		arguments->op->result = hash_insert(arguments->table,
				arguments->op->key, arguments->op->val);
		break;
	case REMOVE:
		arguments->op->result = hash_remove(arguments->table,
				arguments->op->key);
		break;
	case CONTAINS:
		arguments->op->result = hash_contains(arguments->table,
				arguments->op->key);
		break;
	case UPDATE:
		arguments->op->result = hash_update(arguments->table,
				arguments->op->key, arguments->op->val);
		break;
	case COMPUTE:
		arguments->op->result = list_node_compute(arguments->table,
				arguments->op->key, arguments->op->compute_func,
				&arguments->op->val);
		break;
	}

	pthread_mutex_lock(&arguments->table->nr_threads_lock);
	arguments->table->nr_threads--;
	pthread_mutex_unlock(&arguments->table->nr_threads_lock);

	free(arguments);

	return NULL;
}

void hash_batch(hashtable_t* table, int num_ops, op_t* ops) {
	pthread_t threadArray[num_ops];
	bool runThreads = false;

	for (int i = 0; i < num_ops; ++i) {
		Args args = malloc(sizeof(*args));
		args->table = table;
		args->op = ops + i;
		args->runThreads = &runThreads;

		pthread_create(threadArray + i, NULL, thread_routine, args);

	}

	runThreads = true;

	void** retval = malloc(sizeof(*retval));
	for (int i = 0; i < num_ops; ++i) {
		pthread_join(threadArray[i], retval);
	}
	free(retval);

}
