
#ifndef SORTEDDBFILE_H_
#define SORTEDDBFILE_H_


#include "TwoWayList.h"
#include "Record.h"
#include "Schema.h"
#include "File.h"
#include "Comparison.h"
#include "ComparisonEngine.h"
#include "GenericDBFile.h"
#include "Pipe.h"
#include "BigQ.h"
#include "DBFile.h"
#include "HeapDBFile.h"

struct SortInfo {
	OrderMaker *sortOrder;
	int runLength;
};

class SortedDBFile:public GenericDBFile {

private:
	Page *pageBufRead;
	Page *pageBufWrite;
	File *filePtr;
	//used for GetNext
	off_t pageOffsetRead;
	off_t pageOffsetWrite;

	//Following is needed for BigQ
	BigQ *bigq;
	Pipe *inputPipe;
	Pipe *outputPipe;

	//sortInfo struct
	struct SortInfo sSortInfo;

	//differential merge req
	bool mergeRequired;

	//file name
	string fileName;

	//comparison engine object required getnext
	ComparisonEngine compEngine;

	//Query OrderMaker
	OrderMaker queryOm;
	OrderMaker literalOm;

	bool bBinSearchCalled;
	bool bBinSearchPossible;

	Page bsPage;
	off_t bsOffset;
	off_t bsMaxOffset;
public:
	//DBFile constructor
	SortedDBFile ();
	//DBFile destructor
	~SortedDBFile ();

	//fPath is file path physically located on disk
	//startup - dummy parameter not used for heap db file
	//Returns: a 1 on success and a zero on failure
	int Create (char *fpath, void *startup);

	//This function assumes that the DBFile already exists and has previously been created and then closed.
	//The one parameter to this function is simply the physical location of the file.
	//If your DBFile needs to know anything else about itself, it
	//should have written this to an auxiliary text file that it will also open at startup.
	//The return value is a 1 on success and a zero on failure
	int Open (char *fpath);

	//Simple closes the file. returns 1 on success and 0 on failure
	int Close ();

	//Load function bulk loads the DBFile instance from a text file, appending
	//new data to it using the SuckNextRecord function from Record.h. The character
	//string passed to Load is the name of the data file to bulk load
	void Load (Schema &myschema, char *loadpath);

	//forces the pointer to correspond to the first record in the file
	void MoveFirst ();

	//simply adds the new record to the end of the file
	void Add (Record &addme);

	//gets the next record from the	file and returns it to the user,
	//where “next” is defined to be relative to the current location of the pointer.
	int GetNext (Record &fetchme);

	//GetNext accepts a selection predicate returns the next record in the file that is accepted by the
	//selection predicate. The literal record is used to check the selection predicate, and is
	//created when the parse tree for the CNF is processed.
	int GetNext (Record &fetchme, CNF &cnf, Record &literal);

	void initBigQ();
	void MergeDifferential();

	//Checks if binary search is possible for this sorted file
	bool binarySearchPossible(CNF& cnf, Record& literal);

	//Do binary search and find the page that might contain the record
	void binarySearch(Record& literal);

};

#endif /* SORTEDDBFILE_H_ */
