/**************************************************************
* Class::  CSC-415-01 Fall 2025
* Name::Will Young
* Student ID::924230057
* GitHub-Name::Woyoung21
* Project:: Assignment 4 – Processing FLR Data with Threads
*
* File:: young_will_HW4_helper_functions.h
*
* Description:: This file holds the structs and helper 
*       that make the program funciton. 
*
**************************************************************/
#define _POSIX_C_SOURCE 200809L //for strdup
#define _XOPEN_SOURCE           //for strdup
#ifndef YOUNG_WILL_HW4_HELPER_FUNCTIONS_H
#define YOUNG_WILL_HW4_HELPER_FUNCTIONS_H
#include <pthread.h>
#include <time.h>
#include <string.h>

#define HASH_TABLE_SIZE 256


//to hold header file data
typedef struct recordData{

        int fieldWidth;
        int totalRecordLength;
        int indexOffset;
        char* fieldName;

} recordData;

//hold time data
typedef struct timeData{

        long *times;
        int count;
        int capacity;
        pthread_mutex_t lock;
        long min;
        long max;
        long quartile1;
        long quartile3;
        long median;
        long interquartileRange;
        long lowerBound;
        long upperBound;
        double mean;
        double stdDev;

} timeData;

//holds individual event time data
typedef struct callType{

        char *callTypeName;
        timeData dispatchMinusReceived;
        timeData onsceneMinusEnroute;
        timeData onsceneMinusReceived;

} callType;

//node for hash table
typedef struct callTypeNode {
        
        callType data;
        struct callTypeNode *next;

 } callTypeNode;

//aggregate stats
typedef struct aggStats{

        char *catName;
        int calltypeCount;
        pthread_mutex_t lock;
        callType totStats;
        callTypeNode *callTypeTable[HASH_TABLE_SIZE];

} aggStats;

//threads data
typedef struct threadData {

        int threadId;
        int fileDescriptor;
        recordData *recordInfo;
        int recordInfoCount;
        long startRec;
        long endRec;
        char *fieldName;
        char *firstVal;
        char *secondVal;

 } threadData;




extern aggStats globStatArray[3];



//file processing
recordData *readHeader(const char *headerFileName, int *recordCount);
void parseAggrRecord(char *dataBuffer, threadData *threadData);
void* processRecordChunk(void* args); 

//utility functions
int findFieldOffset(const char *fieldName, recordData *newRecs, int recordCount);
time_t convertDateTime(const char *datetimeStr);
void trimWhiteSpace(char *strn);
void insertionSort(long time[], int count);
int hash(const char *strn);

//global data structure MGMT
void initTimeData(timeData *stat);
void addTimeToData(timeData *stat, long timeVal);
callType *clarifyCallTypeH(aggStats *category, const char *callTypeName);

//printing & analysis
void analyzePrintResults();
void printStats(aggStats *category, const char *stat, int reportIndex);
void calcStats(timeData *stat);



#endif