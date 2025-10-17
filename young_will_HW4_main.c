/**************************************************************
* Class::  CSC-415-01 Fall 2025
* Name::Will Young
* Student ID::924230057
* GitHub-Name::Woyoung21
* Project:: Assignment 4 – Processing FLR Data with Threads
*
* File:: young_will_HW4_main.c
*
* Description:: This is the main file that makes the program 
*   funciton.
*
**************************************************************/
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <stdio.h>
#include <stdlib.h>
#include <time.h>
#include <unistd.h>
#include <pthread.h>
#include "young_will_HW4_helper_functions.h"
#include <string.h>

aggStats globStatArray[3];



int main (int argc, char *argv[])
    {
    //***TO DO***  Look at arguments, initialize application
    int recordCount = 0;
    const char *datFileName = argv[1];
    const char *headerFileName = argv[2];
    int numThreads = atoi(argv[3]);
    char *filterFieldName = argv[4];
    char *valOne = argv[5];
    char *valTwo = argv[6];


    //array of structs
    recordData *newRec = readHeader(headerFileName, &recordCount);

    if (newRec == NULL) {

        fprintf(stderr, "Failed to read header file.");
        return -2;
    }

    int fileDesc = open(datFileName, O_RDONLY);
    if (fileDesc == -1) {

        perror("Failed to open datFile...");
        return -1;
    }

    struct stat datFileStats;
    stat(datFileName, &datFileStats);
    long totalRecs = (datFileStats.st_size) / newRec[0].totalRecordLength;

    printf("Dat File has %ld records\n", totalRecs);

    for (int i = 0; i < 3; i++) {

        pthread_mutex_init(&globStatArray[i].lock, NULL);
        initTimeData(&globStatArray[i].totStats.dispatchMinusReceived);
        initTimeData(&globStatArray[i].totStats.onsceneMinusEnroute);
        initTimeData(&globStatArray[i].totStats.onsceneMinusReceived);

        for (int j = 0; j < HASH_TABLE_SIZE; j++) {

            globStatArray[i].callTypeTable[j] = NULL;
        }
    }

    globStatArray[0].catName = "Total City";
    globStatArray[1].catName = valOne;
    globStatArray[2].catName = valTwo;


    //debug
    //printf("Record Count: %d\n", recordCount);
    //printf("The offset for %s is: %d \n", fieldName1, offset1);

    //**************************************************************
    // DO NOT CHANGE THIS BLOCK
    //Time stamp start
    struct timespec startTime;
    struct timespec endTime;

    clock_gettime(CLOCK_REALTIME, &startTime);
    //**************************************************************
    
    // *** TO DO ***  start your thread processing
    


    pthread_t threads[numThreads];
    threadData threadArgs[numThreads];
    long recordsPerThread = totalRecs / numThreads;

    printf("Launching %d number of threads, each processing roughly %ld recs.\n", numThreads, recordsPerThread);

    for(int j = 0; j < numThreads; j++) {

        threadArgs[j].threadId = j;
        threadArgs[j].fileDescriptor = fileDesc;
        threadArgs[j].recordInfo = newRec;
        threadArgs[j].recordInfoCount = recordCount;
        threadArgs[j].startRec = j * recordsPerThread;

        if (j == numThreads -1) {

            threadArgs[j].endRec = totalRecs;
        }

        else {

            threadArgs[j].endRec = (j + 1) * recordsPerThread;
        }

        threadArgs[j].fieldName = filterFieldName;
        threadArgs[j].firstVal = valOne;
        threadArgs[j].secondVal = valTwo;


        pthread_create(&threads[j], NULL, processRecordChunk, &threadArgs[j]);
    }

    //  wait for the threads to finish

    for (int k =0; k < numThreads; k++) {

        pthread_join(threads[k], NULL);
    }


    // ***TO DO *** Display Data

    analyzePrintResults();


    //**************************************************************
    // DO NOT CHANGE THIS BLOCK
    //Clock output
    clock_gettime(CLOCK_REALTIME, &endTime);
    time_t sec = endTime.tv_sec - startTime.tv_sec;
    long n_sec = endTime.tv_nsec - startTime.tv_nsec;
    if (endTime.tv_nsec < startTime.tv_nsec)
        {
        --sec;
        n_sec = n_sec + 1000000000L;
        }

    printf("Total Time was %ld.%09ld seconds for %s threads.\n", sec, n_sec, argv[3]);
    //**************************************************************


    // ***TO DO *** cleanup

    close(fileDesc);

    for (int l = 0; l < recordCount; l++) {

        free(newRec[l].fieldName);
    }

    free(newRec);
    newRec = NULL;

    for (int m = 0; m < 3; m++) {

        pthread_mutex_destroy(&globStatArray[m].lock);
    }

    return 0;
}

