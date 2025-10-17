/**************************************************************
* Class::  CSC-415-01 Fall 2025
* Name::Will Young
* Student ID::924230057
* GitHub-Name::Woyoung21
* Project:: Assignment 4 – Processing FLR Data with Threads
*
* File:: young_will_HW4_helper_functions.c
*
* Description:: This file contains functions to assist with 
*   this assignment. The goal is to keep main() as decluttered
*   as possible, and apply functional decomposition to that
*   each funciton focuses on it's main task.
*
**************************************************************/
#include "young_will_HW4_helper_functions.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h> // for open, read, close
#include <sys/stat.h> // for stat
#include <fcntl.h> // for O_RDONLY
#include <ctype.h> //debug print buff
#include <math.h>


pthread_mutex_t read_lock;

//-----------------------------------------------------------------------------------------
//parses the headter.txt file
recordData* readHeader(const char *headerFileName, int *recordCount) {

    //Step 1: open header file
    struct stat fileData;
    stat(headerFileName, &fileData);
    int headerFileSize = fileData.st_size;
    int retVal = open(headerFileName, O_RDONLY);

    //Step 2: Read in file to buffer
    char *buffer = malloc(headerFileSize +1);
    read(retVal, buffer, headerFileSize);
    buffer[headerFileSize] = '\0';
    close(retVal);

    //debug print statements
    //printf("Checking buffer: %s\n", buffer);

    //Step 3: Count "lines" in header file
    *recordCount = 0;
    for (int i = 0; i < headerFileSize; i++) {

        if (buffer[i] == '\n') {

            (*recordCount)++;
        }

    }

    recordData * newRecs = malloc(*recordCount * sizeof(recordData));

    //Step 4.) Parse the buffer, assign struct vals
    int totalLength = 0;
    int recordIndex = 0;
    int currentOffset = 0;
    char *headerLine = strtok(buffer, "\n");

    while (headerLine != NULL) {

        //debug print statements
        //printf("At record index: %d, strtok produced: %s\n", recordIndex, headerLine);

        char *lineDelim = strchr(headerLine, ':');
        *lineDelim = '\0';
        char *begFieldName = lineDelim + 1;

        //debugging print statements
        //printf("Field width (before conversion to int): %s and Field Name: %s\n", headerLine, begFieldName);

        newRecs[recordIndex].indexOffset = currentOffset;
        newRecs[recordIndex].fieldWidth = atoi(headerLine);
        totalLength += newRecs[recordIndex].fieldWidth;

        while (*begFieldName == ' '|| *begFieldName == '\t') {
            begFieldName++;

        }

        currentOffset += newRecs[recordIndex].fieldWidth;
        newRecs[recordIndex].fieldName = strdup(begFieldName);
        recordIndex++;
        headerLine = strtok(NULL, "\n");
    }

    for (int j = 0; j < *recordCount; j++) {

        newRecs[j].totalRecordLength = totalLength;
    }


    //safe memory practices
    free(buffer);
    buffer = NULL;

    return newRecs;

}


//-----------------------------------------------------------------------------------------
//searches the array of structs to find the offset of the field being searched for
int findFieldOffset(const char *fieldName, recordData *newRecs, int recordCount) {
    for (int i = 0; i < recordCount; i++) {

        if(strcmp(fieldName, newRecs[i].fieldName) == 0) {

            return i;
        }
    }

    return -1;
}

//-----------------------------------------------------------------------------------------
//converts date string to timestamp
time_t convertDateTime(const char *datetimeStr) {

    struct tm timeVar = {0};
    if (strptime(datetimeStr, "%m/%d/%Y %I:%M:%S %p", &timeVar) == NULL) {

        return -1;
    }

    return mktime(&timeVar);
}

//-----------------------------------------------------------------------------------------
//"Constructor" for timeData struct
void initTimeData(timeData *stat) {
    stat->count = 0;
    stat->capacity = 75000;

    stat->times = malloc(stat->capacity * sizeof(long));
    if(stat->times == NULL) {

        perror("Malloc() did not work properly...");
        exit(-1);
    }

    pthread_mutex_init(&stat->lock, NULL);

    stat->min = 0;
    stat->max = 0;
    stat->quartile1 = 0;
    stat->quartile3 = 0;
    stat->interquartileRange = 0;
    stat->lowerBound = 0;
    stat->upperBound = 0;
    stat->mean = 0;
    stat->stdDev = 0;

}

//-----------------------------------------------------------------------------------------
//critical section adds time to array
void addTimeToData(timeData *stat, long timeVal) {

    pthread_mutex_lock(&stat->lock);

    if(stat->count >= stat->capacity) {

        int newCap = stat->capacity * 2;
        long *newTimes = malloc(newCap * sizeof(long));
        if(newTimes == NULL) {

            pthread_mutex_unlock(&stat->lock);
            perror("Malloc() did not work properly...");
            exit(-1);
        }

        for (int i = 0; i < stat->count; i++) {

            newTimes[i] = stat->times[i];
        }

        free(stat->times);

        stat->times = newTimes;
        stat->capacity = newCap;
    }

    stat->times[stat->count] = timeVal;
    stat->count++;

    pthread_mutex_unlock(&stat->lock);
}

//-----------------------------------------------------------------------------------------
//sorts times and calculates statistics
void calcStats(timeData *stat) {


    if (stat->count == 0) {

        stat->min = 0;
        stat->max = 0;
        stat->quartile1 = 0;
        stat->quartile3 = 0;
        stat->interquartileRange = 0;
        stat->lowerBound = 0;
        stat->upperBound = 0;
        stat->mean = 0;
        stat->stdDev = 0;

        return;
    }

    
    insertionSort(stat->times, stat->count);

    stat->min = stat->times[0];
    stat->max = stat->times[stat->count -1];
    stat->median = stat->times[stat->count/2];
    stat->quartile1 = stat->times[stat->count/4];
    stat->quartile3 = stat->times[(stat->count*3)/4];
    stat->interquartileRange = stat->quartile3 - stat->quartile1;

    double sum = 0.0;
    for (int i = 0; i < stat->count; i++) {

        sum += stat->times[i];
    }
    stat->mean = sum/stat->count;

    double sumSquares = 0.0;
    for (int j = 0; j < stat->count; j++) {

        sumSquares += (stat->times[j] - stat->mean) * (stat->times[j] - stat->mean);
    }

    stat->stdDev = sqrt(sumSquares/stat->count);

    double lower = stat->quartile1 - (1.5 * stat->interquartileRange);
    double upper = stat->quartile3 + (1.5 * stat->interquartileRange);

    if (lower > stat->min) {

        stat->lowerBound = (long)lower;
    }

    else {

        stat->lowerBound = stat->min;
    }

    if (upper < stat->max) {

        stat->upperBound = (long)upper;
    }

    else {

        stat->upperBound = stat->max;
    }

}

//-----------------------------------------------------------------------------------------
//calculates and prints stats
void analyzePrintResults() {

    for (int i = 0; i < 3; i++) {

        aggStats *category = &globStatArray[i];

        calcStats(&category->totStats.dispatchMinusReceived);
        calcStats(&category->totStats.onsceneMinusEnroute);
        calcStats(&category->totStats.onsceneMinusReceived);

       
       for (int j = 0; j < HASH_TABLE_SIZE; j++) {

            callTypeNode *currentNode = category->callTypeTable[j];
            while (currentNode != NULL) {

                callType *currentCallType = &currentNode->data;
                calcStats(&currentCallType->dispatchMinusReceived);
                calcStats(&currentCallType->onsceneMinusEnroute);
                calcStats(&currentCallType->onsceneMinusReceived);

                currentNode = currentNode->next;
            }
       }
    }

    for (int k = 0; k < 3; k++) {

        aggStats *category = &globStatArray[k];

        printStats(category, "Dispatch Time - Received Time", 0);
        printStats(category, "Onscene Time - Enroute Time", 1);
        printStats(category, "Onscene Time - Received Time", 2);
    }  
}

//-----------------------------------------------------------------------------------------
//prints and formats final stats table
void printStats(aggStats *category, const char *stat, int reportIndex) {

    printf("\n%s - %s\n\n", category->catName, stat);
    printf("%-25s | %-7s | %-5s | %-5s | %-5s | %-5s | %-8s | %-5s | %-5s | %-6s | %-5s | %-8s\n",
    "Call Type", "count", "min", "LB", "Q1", "med", "mean", "Q3", "UB", "max", "IQR", "stddev");

    timeData *totalStat;

    if (reportIndex == 0) {

        totalStat = &category->totStats.dispatchMinusReceived;
    }

    else if (reportIndex == 1) {

        totalStat = &category->totStats.onsceneMinusEnroute;
    }

    else {

        totalStat = &category->totStats.onsceneMinusReceived;
    }

    printf("%-25s | %-7d | %-5ld | %-5ld | %-5ld | %-5ld | %-8.2f | %-5ld | %-5ld | %-6ld | %-5ld | %-8.2f\n",
    "TOTAL", totalStat->count, totalStat->min, totalStat->lowerBound, totalStat->quartile1, totalStat->median,
    totalStat->mean,totalStat->quartile3, totalStat->upperBound, totalStat->max, totalStat->interquartileRange,
    totalStat->stdDev);


    for (int i = 0; i < HASH_TABLE_SIZE; i++) {

        
        callTypeNode *currentNode = category->callTypeTable[i];
        while (currentNode != NULL) {
            callType *newCallType = &currentNode->data;
            timeData *newStat;

            if (reportIndex == 0) {

                newStat = &newCallType->dispatchMinusReceived;
            }

            else if (reportIndex == 1) {

                newStat = &newCallType->onsceneMinusEnroute;
            }

            else {

                newStat = &newCallType->onsceneMinusReceived;
            }

            printf("%-25s | %-7d | %-5ld | %-5ld | %-5ld | %-5ld | %-8.2f | %-5ld | %-5ld | %-6ld | %-5ld | %-8.2f\n",
                newCallType->callTypeName, newStat->count, newStat->min, newStat->lowerBound, newStat->quartile1, newStat->median,
                newStat->mean, newStat->quartile3, newStat->upperBound, newStat->max, newStat->interquartileRange,
                newStat->stdDev);
                currentNode = currentNode->next;

        }
    }
}

//-----------------------------------------------------------------------------------------
//parse records and updates global stats
void parseAggrRecord(char *dataBuffer, threadData *threadData) {

    int receivedIndex = findFieldOffset("received_datetime", threadData->recordInfo, threadData->recordInfoCount);
    int dispatchIndex = findFieldOffset("dispatch_datetime", threadData->recordInfo, threadData->recordInfoCount);
    int enrouteIndex = findFieldOffset("enroute_datetime", threadData->recordInfo, threadData->recordInfoCount);
    int onsceneIndex = findFieldOffset("onscene_datetime", threadData->recordInfo, threadData->recordInfoCount);
    int finalDescIndex = findFieldOffset("call_type_final_desc", threadData->recordInfo, threadData->recordInfoCount);
    int origDescIndex = findFieldOffset("call_type_original_desc", threadData->recordInfo, threadData->recordInfoCount);
    int filterFieldIndex = findFieldOffset(threadData->fieldName, threadData->recordInfo, threadData->recordInfoCount);

    char receivedStr[threadData->recordInfo[receivedIndex].fieldWidth + 1];
    strncpy(receivedStr, dataBuffer + threadData->recordInfo[receivedIndex].indexOffset, threadData->recordInfo[receivedIndex].fieldWidth);
    receivedStr[threadData->recordInfo[receivedIndex].fieldWidth] = '\0';

    char dispatchStr[threadData->recordInfo[dispatchIndex].fieldWidth +1];
    strncpy(dispatchStr, dataBuffer + threadData->recordInfo[dispatchIndex].indexOffset, threadData->recordInfo[dispatchIndex].fieldWidth);
    dispatchStr[threadData->recordInfo[dispatchIndex].fieldWidth] = '\0';

    char enrouteStr[threadData->recordInfo[enrouteIndex].fieldWidth + 1];
    strncpy(enrouteStr, dataBuffer + threadData->recordInfo[enrouteIndex].indexOffset, threadData->recordInfo[enrouteIndex].fieldWidth);
    enrouteStr[threadData->recordInfo[enrouteIndex].fieldWidth] = '\0';

    char onsceneStr[threadData->recordInfo[onsceneIndex].fieldWidth +1];
    strncpy(onsceneStr, dataBuffer + threadData->recordInfo[onsceneIndex].indexOffset, threadData->recordInfo[onsceneIndex].fieldWidth);
    onsceneStr[threadData->recordInfo[onsceneIndex].fieldWidth] = '\0';

    char finalDescStr[threadData->recordInfo[finalDescIndex].fieldWidth +1];
    strncpy(finalDescStr, dataBuffer + threadData->recordInfo[finalDescIndex].indexOffset, threadData->recordInfo[finalDescIndex].fieldWidth);
    finalDescStr[threadData->recordInfo[finalDescIndex].fieldWidth] = '\0';
    trimWhiteSpace(finalDescStr);

    char origDescStr[threadData->recordInfo[origDescIndex].fieldWidth +1];
    strncpy(origDescStr, dataBuffer +threadData->recordInfo[origDescIndex].indexOffset,threadData->recordInfo[origDescIndex].fieldWidth);
    origDescStr[threadData->recordInfo[origDescIndex].fieldWidth] = '\0';
    trimWhiteSpace(origDescStr);

    char filterValFromRec[threadData->recordInfo[filterFieldIndex].fieldWidth + 1];
    strncpy(filterValFromRec, dataBuffer + threadData->recordInfo[filterFieldIndex].indexOffset, threadData->recordInfo[filterFieldIndex].fieldWidth);
    filterValFromRec[threadData->recordInfo[filterFieldIndex].fieldWidth] = '\0';
    trimWhiteSpace(filterValFromRec);


    //if no finalDesc field
    char *finalFinalDesc = finalDescStr;
    if (strlen(finalDescStr) == 0 || finalDescStr[0] == ' ') {

        finalFinalDesc = origDescStr;
    }

    time_t receivedTime = convertDateTime(receivedStr);
    time_t dispatchTime = convertDateTime(dispatchStr);
    time_t enrouteTime = convertDateTime(enrouteStr);
    time_t onsceneTime = convertDateTime(onsceneStr);

    callType *totalCityStats = clarifyCallTypeH(&globStatArray[0], finalFinalDesc);

    callType *filterStats = NULL;
    int filterIndex = -1;

    if (strcmp(filterValFromRec, threadData->firstVal) == 0) {

        filterStats = clarifyCallTypeH(&globStatArray[1], finalFinalDesc);
        filterIndex = 1;
    }

    else if (strcmp(filterValFromRec, threadData->secondVal) == 0) {

        filterStats = clarifyCallTypeH(&globStatArray[2], finalFinalDesc);
        filterIndex = 2;
    }

    

    //dispatch minus received
    if (dispatchTime != (time_t)-1 && receivedTime != (time_t)-1) {

        long difference = dispatchTime - receivedTime;
        if (difference >= 0) {
            
            addTimeToData(&totalCityStats->dispatchMinusReceived, difference);
            addTimeToData(&globStatArray[0].totStats.dispatchMinusReceived, difference);

            if(filterStats != NULL) {

                addTimeToData(&filterStats->dispatchMinusReceived,difference);
                addTimeToData(&globStatArray[filterIndex].totStats.dispatchMinusReceived, difference);
            }            
        }      
    }

    //onscene minus enroute
    if (onsceneTime != (time_t)-1 && enrouteTime != (time_t)-1) {

        long difference = onsceneTime - enrouteTime;
        if (difference >= 0) {
            
            addTimeToData(&totalCityStats->onsceneMinusEnroute, difference);
            addTimeToData(&globStatArray[0].totStats.onsceneMinusEnroute, difference);

            if(filterStats != NULL) {
                
                addTimeToData(&filterStats->onsceneMinusEnroute,difference);
                addTimeToData(&globStatArray[filterIndex].totStats.onsceneMinusEnroute, difference);
            }
        }
    }

    //onscene minue received
    if (onsceneTime != (time_t)-1 && receivedTime != (time_t)-1) {

        long difference = onsceneTime - receivedTime;
        if (difference >= 0) {
            
            addTimeToData(&totalCityStats->onsceneMinusReceived, difference);
            addTimeToData(&globStatArray[0].totStats.onsceneMinusReceived, difference);

            if(filterStats != NULL) {

                addTimeToData(&filterStats->onsceneMinusReceived,difference);
                addTimeToData(&globStatArray[filterIndex].totStats.onsceneMinusReceived, difference);
            }
        }   
    } 
}

//-----------------------------------------------------------------------------------------
//worker that processes records
void* processRecordChunk(void* args) {

    threadData *ThreadData = (threadData*)args;
    int totalLineLength = ThreadData->recordInfo[0].totalRecordLength;

    char *datBuffer = malloc(totalLineLength +1);

    for (long i = ThreadData->startRec; i < ThreadData->endRec; i++) {
        
        long fileOffset = i * totalLineLength;

        pread(ThreadData->fileDescriptor, datBuffer, totalLineLength, fileOffset);
        datBuffer[totalLineLength] = '\0';

        parseAggrRecord(datBuffer, ThreadData);
    }

    free(datBuffer);

    return NULL;
}

//-----------------------------------------------------------------------------------------
//removes whitespaces for category comparison 
void trimWhiteSpace(char *strn) {

    if (strn == NULL || strn[0] == '\0') {
        return;
    }

    int length = strlen(strn);
    char *last = strn + length -1;
    while (last >= strn && isspace((unsigned char)*last)) {

        last--;
    }

    *(last + 1) = '\0';

    char *first = strn;
    while (*first && isspace((unsigned char)*first)) {

        first++;
    }

    memmove(strn, first, strlen(first) +1);

}

//-----------------------------------------------------------------------------------------
//sort algorithm
void insertionSort(long time[], int count) {

    long currentElement = 0;
    for (int i = 1; i < count; i++) {

        currentElement = time[i];
        int j = i-1;

        while (j >= 0 && time[j] > currentElement) {

            time[j+1] = time[j];
            j = j- 1;
        }
        time[j+1] = currentElement;
    }

}

//-----------------------------------------------------------------------------------------
//hash algorithm
int hash(const char *strn) {

        unsigned int hashVal = 0;
        while(*strn) {
             hashVal = (hashVal << 5) + *strn++;
        }
        return hashVal % HASH_TABLE_SIZE;
}

//-----------------------------------------------------------------------------------------
//Finds or creates call type in hash table
callType *clarifyCallTypeH(aggStats *category, const char *callTypeName) {

    int index = hash(callTypeName);

    pthread_mutex_lock(&category->lock);

    callTypeNode *currentNode = category->callTypeTable[index];
    while (currentNode != NULL) {


        if (strcmp(currentNode->data.callTypeName, callTypeName) == 0) {

            pthread_mutex_unlock(&category->lock);
            return &currentNode->data;
        }

        currentNode = currentNode->next;
    }

    callTypeNode *newNode = malloc(sizeof(callTypeNode));
    initTimeData(&newNode->data.dispatchMinusReceived);
    initTimeData(&newNode->data.onsceneMinusEnroute);
    initTimeData(&newNode->data.onsceneMinusReceived);
    newNode->data.callTypeName = strdup(callTypeName);

    newNode->next = category->callTypeTable[index];
    category->callTypeTable[index] = newNode;
    category->calltypeCount++;

    pthread_mutex_unlock(&category->lock);
    return &newNode->data;

}

















