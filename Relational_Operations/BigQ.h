#ifndef BIGQ_H
#define BIGQ_H
#include <pthread.h>
#include <iostream>
#include <vector>
#include <algorithm>
#include <queue>
#include <sstream>
#include "Pipe.h"
#include "File.h"
#include "Record.h"

using namespace std;

//This structure holds all arguments of BigQ constructor
typedef struct sortingArgs {
	Pipe* inPipe;
	Pipe* outPipe;
	OrderMaker* sortOrder;
	int runLen;
}sSortingArgs;

//Info required to sort 1 run
typedef struct infoToSortRun {
	int numRecordsInRun;
	int pageIndex;
	int runLength;
	Page* pageArr;
	File* pFile;
	OrderMaker* sortOrder;
}sInfoToSortRun;

class BigQ {

private:

	//Need to create a structure to pass to worker thread
	sSortingArgs args;
	//Worker thread
	pthread_t workerThread;

public:
	BigQ (Pipe &in, Pipe &out, OrderMaker &sortorder, int runlen);
	~BigQ ();
	static void *sortInput (void* ptr);
	static void sortAndDumpRun(sInfoToSortRun& sInfoRun, int& numberOfRuns, vector<Record>& vRecRemain,int& recRemainingIndex);
	static void mergeRuns(const int numberOfRuns, File *pFile, const sSortingArgs *args);
};


class RecordWrapper{

public:
	int pageIndex;
	Record* record;
	RecordWrapper(int i, Record *r){
		pageIndex=i;
		record = r;
	}
	RecordWrapper(){
		pageIndex = -1;
		record = NULL;
	}

	RecordWrapper(const RecordWrapper& copyMe) {
		pageIndex = copyMe.pageIndex;
		record = copyMe.record;
	}

	~RecordWrapper(){
	}
};

class RecordComparator{

private:
	OrderMaker *sortOrder;
	ComparisonEngine compEngine;
	bool isLesser;

public:
	RecordComparator(OrderMaker *order,bool _isLesser){
		this->sortOrder = order;
		isLesser=_isLesser;
	}

	bool operator()(RecordWrapper *left,RecordWrapper *right){
		int retValue = compEngine.Compare(left->record, right->record, sortOrder);
		if(isLesser)
			return retValue == -1 ? true:false;
		return retValue == -1 ? false:true;
	}
};

class Comparator{

private:
	OrderMaker *sortOrder;
	ComparisonEngine compEngine;
	bool isLesser;

public:
	Comparator(OrderMaker *order,bool _isLesser){
		this->sortOrder = order;
		isLesser=_isLesser;
	}

	bool operator()(Record *left,Record *right){
		int retValue = compEngine.Compare(left, right, sortOrder);
		if(isLesser)
			return retValue == -1 ? true:false;
		return retValue == -1 ? false:true;
	}
};



#endif
