#ifndef TEST_UTILITIES_H_
#define TEST_UTILITIES_H_

#include <stdbool.h>
#include <stdio.h>

#define FRED    "\x1B[31m"
#define FGREEN  "\x1B[32m"
#define NONE  	"\x1B[0m"
#define FCYAN 	"\x1B[36m"
#define NONE  	"\x1B[0m"

/**
 * These macros are here to help you create tests more easily and keep them
 * clear
 *
 * The basic idea with unit-testing is create a test function for every real
 * function and inside the test function declare some variables and execute the
 * function under test.
 *
 * Use the ASSERT_TEST to verify correctness of values.
 */

/**
 * Evaluates b and continues if b is true.
 * If b is false, ends the test by returning false and prints a detailed
 * message about the failure.
 */
#define ASSERT_TEST(b,msg) do { 														\
	printf("%s",msg); 																\
    if (!(b)) { 																		\
        printf("Assertion %sfailed%s at %s:%d %s\n",FRED,NONE,__FILE__,__LINE__,#b); 	\
		return false;																	\
    } else {																			\
   		printf ("Assertion:%s Passed%s\n",FGREEN,NONE);    							\
    }																					\
} while (0)

/**
 * Macro used for running a test from the main function
 */
#define RUN_TEST(test) do { \
        printf("%sRunning "#test"... %s",FCYAN,NONE); \
        if (test()) { \
                printf ("Test Result:%s [OK]%s\n",FGREEN,NONE);\
        } \
} while(0)

#endif /* TEST_UTILITIES_H_ */
