/*
 * test_hashtable.c
 *
 *  Created on: 17 Dec 2016
 *      Author: lena
 */
#include <string.h>
#include <stdlib.h>

#include "hashtable.h"
#include "test_utilities.h"

#define ASSERT_TRUE(x,msg) ASSERT_TEST(x,msg);
#define ASSERT_EQUAL(x,y,msg) ASSERT_TEST((x) == (y),msg);
#define ASSERT_EQUAL_STR(x,y,msg) ASSERT_TEST(strcmp((x),(y)) == 0,msg);
#define ASSERT_SUCCESS(x,msg) ASSERT_EQUAL((x), MEMCACHE_SUCCESS,msg)
#define ASSERT_NOT_NULL(x,msg) ASSERT_TRUE(x,msg)
#define ASSERT_NULL(x,msg) ASSERT_TRUE(!x,msg)

//hash function for the table
int hash_func(int nr_buckets, int key) {

	return key % nr_buckets;
}

//compute function for the hash_compute function
void* hash_compute(void* val) {
	char* str = malloc(sizeof(char) * 16);
	if (!str)
		return NULL;
	str = strcpy(str, "Changed element");
	return str;
}

bool test_hash_alloc_and_free() {
	hashtable_t* ht;
	//checking errors
	ht = hash_alloc(0, &hash_func);
	ASSERT_EQUAL(ht, NULL, "The alloc get o buckets\n");
	ht = hash_alloc(-1, &hash_func);
	ASSERT_EQUAL(ht, NULL, "The alloc get -1 buckets\n");
	ht = hash_alloc(10, NULL);
	ASSERT_EQUAL(ht, NULL, "The alloc get NULL hash function\n");

	//allocate hash_table successfully
	ht = hash_alloc(5, &hash_func);
	ASSERT_NOT_NULL(ht, "Checking if the table is allocated\n");

	//check if all the buckets is 0 size
	for (int i = 0; i < 5; i++) {
		ASSERT_EQUAL(hash_getbucketsize(ht, i), 0, "Checking bucket size..\n");
	}

	//checking ht free, twice
	hash_stop(ht);
	hash_free(ht);
	ht = hash_alloc(100, &hash_func);
	ASSERT_NOT_NULL(ht, "Checking if the table is allocated\n");
	hash_stop(ht);
	hash_free(ht);

	return true;
}

bool test_hash_insert() {
	hashtable_t* ht;
	char* element1 = NULL;
	char* element2 = NULL;
	char* element3 = NULL;
	ht = hash_alloc(5, &hash_func);
	ASSERT_NOT_NULL(ht, "Checking if the table is allocated\n");

	//checking errors
	ASSERT_EQUAL(hash_insert(NULL, 1, element1), -1,
			"Checking NULL table argument\n");

	//preparing elements
	element1 = "Lena";
	element2 = "Alex";
	element3 = "Dima";

	//insert elements successfully
	ASSERT_EQUAL(hash_insert(ht, 1, element1), 1, "Insert element 1\n");
	ASSERT_EQUAL(hash_insert(ht, 2, element2), 1, "Insert element 2\n");
	ASSERT_EQUAL(hash_insert(ht, 3, element3), 1, "Insert element 3\n");
	//try to insert the same elements again
	ASSERT_EQUAL(hash_insert(ht, 1, element1), 0, "Insert element 1 again\n");
	ASSERT_EQUAL(hash_insert(ht, 2, element2), 0, "Insert element 2 again\n");
	ASSERT_EQUAL(hash_insert(ht, 3, element3), 0, "Insert element 3 again\n");
	//insert this elements with other key
	ASSERT_EQUAL(hash_insert(ht, 11, element1), 1, "Insert element 1\n");
	ASSERT_EQUAL(hash_insert(ht, 22, element2), 1, "Insert element 2\n");
	ASSERT_EQUAL(hash_insert(ht, 33, element3), 1, "Insert element 3\n");

	hash_free(ht);
	return true;
}

bool test_hash_update() {
	hashtable_t* ht;
	char* element1 = "Lena";
	char* element2 = "Alex";
	char* element3 = "Dima";
	ht = hash_alloc(5, &hash_func);
	ASSERT_NOT_NULL(ht, "Checking if the table is allocated\n");

	//check errors
	ASSERT_EQUAL(hash_update(NULL, 1, element1), -1,
			"Check update NULL hash table\n");
	ASSERT_EQUAL(hash_update(ht, 1, element1), 0,
			"Check update element which doesn't exist\n");

	//insert elements successfully
	ASSERT_EQUAL(hash_insert(ht, 1, element1), 1, "Insert element 1\n");
	ASSERT_EQUAL(hash_insert(ht, 2, element2), 1, "Insert element 2\n");
	ASSERT_EQUAL(hash_insert(ht, 3, element3), 1, "Insert element 3\n");
	//check if the current values is okay

	char* element4 = "Koltonov";
	char* element5 = "Khvolis";

	ASSERT_EQUAL(hash_update(ht, 1, element4), 1, "Update value\n");
	ASSERT_EQUAL(hash_update(ht, 2, element5), 1, "Update value\n");

	hash_free(ht);
	return true;
}

bool test_hash_remove() {
	hashtable_t* ht;
	char* element1 = "Lena";
	char* element2 = "Alex";
	char* element3 = "Dima";
	ht = hash_alloc(5, &hash_func);
	ASSERT_NOT_NULL(ht, "Checking if the table is allocated\n");

	//check errors
	ASSERT_EQUAL(hash_remove(NULL, 1), -1,
			"Check remove from NULL hash table\n");
	ASSERT_EQUAL(hash_remove(ht, 1), 0,
			"Check remove element which doesn't exist\n");

	//insert elements successfully
	ASSERT_EQUAL(hash_insert(ht, 1, element1), 1, "Insert element 1\n");
	ASSERT_EQUAL(hash_insert(ht, 2, element2), 1, "Insert element 2\n");
	ASSERT_EQUAL(hash_insert(ht, 3, element3), 1, "Insert element 3\n");
	//remove elements
	ASSERT_EQUAL(hash_remove(ht, 1), 1, "Remove element 1\n");
	ASSERT_EQUAL(hash_remove(ht, 2), 1, "Remove element 2\n");
	ASSERT_EQUAL(hash_remove(ht, 3), 1, "Remove element 3\n");

	//try to remove those elements again
	ASSERT_EQUAL(hash_remove(ht, 1), 0, "Remove element 1 again\n");
	ASSERT_EQUAL(hash_remove(ht, 2), 0, "Remove element 2 again\n");
	ASSERT_EQUAL(hash_remove(ht, 3), 0, "Remove element 3 again\n");

	hash_free(ht);
	return true;
}

bool test_hash_contains() {
	hashtable_t* ht;
	char* element1 = "Lena";
	char* element2 = "Alex";
	char* element3 = "Dima";
	ht = hash_alloc(5, &hash_func);
	ASSERT_NOT_NULL(ht, "Checking if the table is allocated\n");

	//check errors
	ASSERT_EQUAL(hash_contains(NULL, 1), -1,
			"Check capacity of NULL hash table\n");

	//insert elements successfully
	ASSERT_EQUAL(hash_insert(ht, 1, element1), 1, "Insert element 1\n");
	ASSERT_EQUAL(hash_insert(ht, 2, element2), 1, "Insert element 2\n");
	ASSERT_EQUAL(hash_insert(ht, 3, element3), 1, "Insert element 3\n");
	//check existence of the elements
	ASSERT_EQUAL(hash_contains(ht, 1), 1, "Check if element 1 exist\n");
	ASSERT_EQUAL(hash_contains(ht, 2), 1, "Check if element 2 exist\n");
	ASSERT_EQUAL(hash_contains(ht, 3), 1, "Check if element 3 exist\n");
	//remove elements
	ASSERT_EQUAL(hash_remove(ht, 1), 1, "Remove element 1\n");
	ASSERT_EQUAL(hash_remove(ht, 2), 1, "Remove element 2\n");
	ASSERT_EQUAL(hash_remove(ht, 3), 1, "Remove element 3\n");
	//check existence of removed elements
	ASSERT_EQUAL(hash_contains(ht, 1), 0, "Check if element 1 exist\n");
	ASSERT_EQUAL(hash_contains(ht, 2), 0, "Check if element 2 exist\n");
	ASSERT_EQUAL(hash_contains(ht, 3), 0, "Check if element 3 exist\n");

	hash_free(ht);
	return true;
}

bool test_list_node_compute() {
	hashtable_t* ht;
	char* element1 = "Lena";
	char* element2 = "Alex";
	char* element3 = "Dima";
	ht = hash_alloc(5, &hash_func);
	ASSERT_NOT_NULL(ht, "Checking if the table is allocated\n");

	//insert elements successfully
	ASSERT_EQUAL(hash_insert(ht, 1, element1), 1, "Insert element 1\n");
	ASSERT_EQUAL(hash_insert(ht, 2, element2), 1, "Insert element 2\n");
	ASSERT_EQUAL(hash_insert(ht, 3, element3), 1, "Insert element 3\n");
	//check existence of the elements
	ASSERT_EQUAL(hash_contains(ht, 1), 1, "Check if element 1 exist\n");
	ASSERT_EQUAL(hash_contains(ht, 2), 1, "Check if element 2 exist\n");
	ASSERT_EQUAL(hash_contains(ht, 3), 1, "Check if element 3 exist\n");

	char** str1 = &element1;
	char** str2 = &element2;
	char** str3 = &element3;

	//check errors
	ASSERT_EQUAL(list_node_compute(NULL, 1, &hash_compute, (void** )str1), -1,
			"Compute value in NULL table\n");

	//compute good values
	ASSERT_EQUAL(list_node_compute(ht, 1, &hash_compute, (void** )str1), 1,
			"Compute value of element 1 exist\n");
	ASSERT_EQUAL(list_node_compute(ht, 2, &hash_compute, (void** )str2), 1,
			"Compute value of element 2 exist\n");
	ASSERT_EQUAL(list_node_compute(ht, 2, &hash_compute, (void** )str3), 1,
			"Compute value of element 3 exist\n");

	//check if the computed values is good
	ASSERT_EQUAL(strcmp(element1, "Changed element"), 0, "Compare vals\n");
	ASSERT_EQUAL(strcmp(element2, "Changed element"), 0, "Compare vals\n");
	ASSERT_EQUAL(strcmp(element3, "Changed element"), 0, "Compare vals\n");
	free(element1);
	free(element2);
	free(element3);

	//remove elements
	ASSERT_EQUAL(hash_remove(ht, 1), 1, "Remove element 1\n");
	ASSERT_EQUAL(hash_remove(ht, 2), 1, "Remove element 2\n");
	ASSERT_EQUAL(hash_remove(ht, 3), 1, "Remove element 3\n");

	//compute value that don't exist
	ASSERT_EQUAL(list_node_compute(ht, 1, &hash_compute, (void** )str1), 0,
			"Compute value of element 1 exist\n");
	ASSERT_EQUAL(list_node_compute(ht, 2, &hash_compute, (void** )str2), 0,
			"Compute value of element 2 exist\n");
	ASSERT_EQUAL(list_node_compute(ht, 2, &hash_compute, (void** )str3), 0,
			"Compute value of element 3 exist\n");

	hash_free(ht);
	return true;
}

bool test_hash_getbucketsize() {
	hashtable_t* ht;
	char* element1 = "Lena";
	char* element2 = "Alex";
	char* element3 = "Dima";
	ht = hash_alloc(5, &hash_func);
	ASSERT_NOT_NULL(ht, "Checking if the table is allocated\n");

	//check errors
	ASSERT_EQUAL(hash_getbucketsize(NULL, 1), -1, "Get size from NULL table\n");
	ASSERT_EQUAL(hash_getbucketsize(ht, -1), -1, "Get size from bad bucket\n");
	ASSERT_EQUAL(hash_getbucketsize(ht, 5), -1, "Get size from bad bucket\n");

	//insert elements successfully
	ASSERT_EQUAL(hash_insert(ht, 1, element1), 1, "Insert element 1\n");
	ASSERT_EQUAL(hash_insert(ht, 2, element2), 1, "Insert element 2\n");
	ASSERT_EQUAL(hash_insert(ht, 3, element3), 1, "Insert element 3\n");
	//check existence of the elements
	ASSERT_EQUAL(hash_contains(ht, 1), 1, "Check if element 1 exist\n");
	ASSERT_EQUAL(hash_contains(ht, 2), 1, "Check if element 2 exist\n");
	ASSERT_EQUAL(hash_contains(ht, 3), 1, "Check if element 3 exist\n");
	//get bucket sizes
	ASSERT_EQUAL(hash_getbucketsize(ht, 1), 1, "Get size from 1\n");
	ASSERT_EQUAL(hash_getbucketsize(ht, 2), 1, "Get size from 1\n");
	ASSERT_EQUAL(hash_getbucketsize(ht, 3), 1, "Get size from 1\n");
	//insert elements successfully
	ASSERT_EQUAL(hash_insert(ht, 11, element1), 1, "Insert element 1\n");
	ASSERT_EQUAL(hash_insert(ht, 22, element2), 1, "Insert element 2\n");
	ASSERT_EQUAL(hash_insert(ht, 33, element3), 1, "Insert element 3\n");
	//get bucket sizes
	ASSERT_EQUAL(hash_getbucketsize(ht, 1), 2, "Get size from 1\n");
	ASSERT_EQUAL(hash_getbucketsize(ht, 2), 2, "Get size from 1\n");
	ASSERT_EQUAL(hash_getbucketsize(ht, 3), 2, "Get size from 1\n");


	hash_free(ht);
	return true;
}

int main() {
	RUN_TEST(test_hash_alloc_and_free);
	RUN_TEST(test_hash_insert);
	RUN_TEST(test_hash_update);
	RUN_TEST(test_hash_remove);
	RUN_TEST(test_hash_contains);
//	RUN_TEST(test_list_node_compute);
//	RUN_TEST(test_hash_getbucketsize);

	return 0;
}

