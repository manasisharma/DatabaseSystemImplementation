/*
 * HeapDBFile.h
 *
 *  Created on: Mar 7, 2014
 *      Author: sagar
 */

#ifndef HEAPDBFILE_H_
#define HEAPDBFILE_H_

#include "TwoWayList.h"
#include "Record.h"
#include "Schema.h"
#include "File.h"
#include "Comparison.h"
#include "ComparisonEngine.h"
#include "GenericDBFile.h"
#include "DBFile.h"

class HeapDBFile : public GenericDBFile {

private:
	Page *pgBufrRead;
	Page *pgBufrWrite;
	File *filePointer;
	//used for GetNext
	off_t pgOffstRead;
	off_t pgOffstWrite;
public:
	//DBFile constructor
	HeapDBFile ();
	//DBFile destructor
	~HeapDBFile ();

	//fPath is the physical location of file on disk
	//startup - dummy parameter not used for heap db file
	//Returns: a 1 upon success and a zero upon failure
	int Create (char *fpath, void *startup);

	//This function assumes that DBFile is present and has been created previously and then closed.
	//The one parameter to this function is simply the physical location of the file.
	//The return value is a 1 on success and a zero on failure
	int Open (char *fpath);


	int Close ();

	//Load function bulk loads the DBFile instance from a text file, appending
	//new data to it using the SuckNextRecord function from Record.h. The character
	//string passed to Load is the name of the data file to bulk load
	void Load (Schema &myschema, char *loadpath);

	//Moves the pointer to point to the first record in the file
	void MoveFirst ();

	//This adds a new record at the end of file
	void Add (Record &addme);

	//gets the next record from the	file and returns it to the user,
	//where “next” is defined to be relative to the current location of the pointer.
	int GetNext (Record &fetchme);

	//GetNext accepts a selection predicate returns the next record in the file that is accepted by the
	//selection predicate. The literal record is used to check the selection predicate, and is
	//created when the parse tree for the CNF is processed.
	int GetNext (Record &fetchme, CNF &cnf, Record &literal);
};

#endif /* HEAPDBFILE_H_ */
