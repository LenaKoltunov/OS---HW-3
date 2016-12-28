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
	int nr_buckets, nr_elements, nr_threads, stop_flag;
	Hash hash_func;
	Node* table;
	pthread_mutex_t* empty_list_locks; //locks only for dummy node, not entire list
	int* buckets_sizes;
	Node threads_ids;
	pthread_mutex_t empty_threads_list_lock;
	pthread_mutex_t nr_threads_lock;
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
	pthread_mutex_lock(&curr->mutex);
	if (curr->key == element->key) {
		pthread_mutex_unlock(&curr->mutex);
		return 0;
	}
	while (curr->next) {
		if (curr->key == element->key) {
			pthread_mutex_unlock(&curr->mutex);
			return 0;
		}
		if (prev)
			pthread_mutex_unlock(&prev->mutex);
		prev = curr;
		curr = curr->next;
		if (curr)
			pthread_mutex_lock(&curr->mutex);
	}
	curr->next = element;
	pthread_mutex_unlock(&curr->mutex);
	return 1;
}

int list_update(Node head, int key, void* val) {
	Node curr, prev;
	curr = head;
	if (!head)
		return 0;
	pthread_mutex_lock(&curr->mutex);
	prev = NULL;

	while (curr->next) {
		if (prev)
			pthread_mutex_unlock(&prev->mutex);
		prev = curr;
		pthread_mutex_lock(&curr->next->mutex);
		curr = curr->next;

		if (curr->key == key) {
			curr->value = val;
			pthread_mutex_unlock(&curr->mutex);
			return 1;
		}
	}
	pthread_mutex_unlock(&curr->mutex);
	return 0;

}

/*
 * Auxiliary function:
 * removes element by the key
 * in one bucket
 */
int list_remove(Node head, int key) {
	Node curr, prev;
	if (!head)
		return 0;

	curr = head;
	pthread_mutex_lock(&curr->mutex);
	prev = NULL;

	while (curr->next) {
		if (prev)
			pthread_mutex_unlock(&prev->mutex);
		prev = curr;
		pthread_mutex_lock(&curr->next->mutex);
		curr = curr->next;

		if (curr->key == key) {
			prev->next = curr->next;
			pthread_mutex_unlock(&curr->mutex);
			pthread_mutex_destroy(&curr->mutex);

			free(curr);
			return 1;
		}
	}
	pthread_mutex_unlock(&curr->mutex);
	return 0;
}

bool list_contains(Node head, int key) {
	Node curr, prev;
	if (!head)
		return false;

	curr = head;
	pthread_mutex_lock(&curr->mutex);
	prev = NULL;

	if (curr->key == key) {
		pthread_mutex_unlock(&curr->mutex);
		return true;
	}

	while (curr->next) {
		if (prev)
			pthread_mutex_unlock(&prev->mutex);
		prev = curr;
		pthread_mutex_lock(&curr->next->mutex);
		curr = curr->next;

		if (curr->key == key) {
			pthread_mutex_unlock(&curr->mutex);
			return true;
		}
	}
	pthread_mutex_unlock(&curr->mutex);
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

	for (int i = 0; i < buckets; i++) {
		hashtable->table[i] = NULL;
		hashtable->buckets_sizes[i] = 0;
		pthread_mutexattr_t attr;
		pthread_mutexattr_init(&attr);
		pthread_mutexattr_settype(&attr, PTHREAD_MUTEX_ERRORCHECK_NP);
		pthread_mutex_init(&hashtable->empty_list_locks[i], &attr);
	}
	hashtable->hash_func = hash;
	hashtable->nr_buckets = buckets;
	hashtable->nr_elements = 0;
	hashtable->nr_threads = 0;
	hashtable->stop_flag = 0;
	hashtable->threads_ids = NULL;

	pthread_mutexattr_t attr;
	pthread_mutex_init(&hashtable->empty_threads_list_lock, &attr);
	pthread_mutex_init(&hashtable->nr_threads_lock, &attr);

	return hashtable;
}

int hash_stop(hashtable_t* table) {
	if (!table)
		return -1;
	table->stop_flag=1;
	while(table->nr_threads>0){
	}
	return 1;
}

int hash_free(hashtable_t* ht) {
	if (ht == NULL) {
		return -1;
	}
	if(!ht->stop_flag){
		return 0;
	}
	for (int i = 0; i < ht->nr_buckets; ++i) {
		list_destroy(ht->table[i]);
		pthread_mutex_destroy(&ht->empty_list_locks[i]);
	}
	pthread_mutex_destroy(&ht->empty_threads_list_lock);
	pthread_mutex_destroy(&ht->nr_threads_lock);
	list_destroy(ht->threads_ids);
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
	if(table->stop_flag){
		return -1;
	}
	Node new_element = node_alloc(key, val);
	int hashed_key = table->hash_func(table->nr_buckets, key);

	int retval = list_add(&table->table[hashed_key], new_element,
			&table->empty_list_locks[hashed_key]);

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
	if(table->stop_flag){
		return -1;
	}
	int hashed_key = table->hash_func(table->nr_buckets, key);
	return list_update(table->table[hashed_key], key, val);

}

int hash_remove(hashtable_t* table, int key) {
	if (!table)
		return -1;
	if(table->stop_flag){
		return -1;
	}
	int hashed_key = table->hash_func(table->nr_buckets, key);
	Node head = table->table[hashed_key];
	//The element is the head:
	if (head) {
		pthread_mutex_lock(&head->mutex);
		if (head->key == key) {
			table->table[hashed_key] = head->next;
			free(head);
			pthread_mutex_unlock(&head->mutex);
			return 1;
		}
		pthread_mutex_unlock(&head->mutex);
		if (list_remove(head, key) == 1) {
			table->nr_elements--;
			table->buckets_sizes[hashed_key]--;
			return 1;
		} else if (list_remove(head, key) == -1) {
			return -1;
		}
	}
	return 0;
}

int hash_contains(hashtable_t* table, int key) {
	if (!table)
		return -1;
	if(table->stop_flag){
		return -1;
	}
	int hashed_key = table->hash_func(table->nr_buckets, key);
	if (list_contains(table->table[hashed_key], key))
		return 1;
	return 0;
}

int list_node_compute(hashtable_t* table, int key, void* (*compute_func)(void*),
		void** result) {
//	if (!table || !compute_func || !result)
//		return -1;
//
//	int hashed_key = table->hash_func(table->nr_buckets, key);
//	Node curr = table->table[hashed_key];
//	Node prev = NULL;
//
//	pthread_mutex_lock(&curr->mutex);
//
//	if (curr->key == key) {
//		pthread_mutex_unlock(&curr->mutex);
//		return true;
//	}
//
//	while (curr->next) {
//		if (prev)
//			pthread_mutex_unlock(&prev->mutex);
//		prev = curr;
//		pthread_mutex_lock(&curr->next->mutex);
//		curr = curr->next;
//
//		if (curr->key == key) {
//			pthread_mutex_unlock(&curr->mutex);
//			return true;
//		}
//	}
//	pthread_mutex_unlock(&curr->mutex);
//	return false;
}

int hash_getbucketsize(hashtable_t* table, int bucket) { //TODO ask if the bucket start from 0?
	if (!table)
		return -1;
	if(table->stop_flag){
		return -1;
	}
	if (bucket < 0 || bucket >= table->nr_buckets)
		return -1;
	return table->buckets_sizes[bucket];
}

typedef struct args_t {
	Hashtable table;
	Op op;
	bool* runThreads;
}* Args;

void* thread_routine(void* args) {
	Args arguments = args;

	while (!(arguments->runThreads)) {};

	switch (arguments->op->op) {
	case INSERT:
		hash_insert(arguments->table, arguments->op->key, arguments->op->val);
		break;
	case REMOVE:
		hash_remove(arguments->table, arguments->op->key);
		break;
	case CONTAINS:
		hash_contains(arguments->table, arguments->op->key);
		break;
	case UPDATE:
		hash_update(arguments->table, arguments->op->key, arguments->op->val);
		break;
	case COMPUTE:
		list_node_compute(arguments->table, arguments->op->key,
				arguments->op->compute_func, arguments->op->val);
		break;
	}

	free(arguments);
	int pid = pthread_self();
	list_remove(arguments->table->threads_ids, pid);

	pthread_mutex_lock(arguments->table->nr_threads_lock);
	arguments->table->nr_threads--;
	pthread_mutex_unlock(arguments->table->nr_threads_lock);
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

		Node pid_node = node_alloc(threadArray[i], NULL);
		list_add(&table->threads_ids, pid_node, &(table->empty_threads_list_lock));
	}

	runThreads = true;

	void** retval = malloc(sizeof(*retval));
	for (int i = 0; i < num_ops; ++i) {
		pthread_join(threadArray[i], retval);
	}
	free(retval);

}
