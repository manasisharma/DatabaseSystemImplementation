/*
 * HeapDBFile.cc
 *
 *  Created on: Mar 7, 2015
 */

#include "HeapDBFile.h"

#include <iostream>
#include <stdlib.h>
#include <fstream>

HeapDBFile::HeapDBFile () {
	filePointer = new File();

	pgBufrRead = new Page();
	pgBufrWrite = new Page();

	pgOffstRead = 0;
	pgOffstWrite = 0;

	if (filePointer == NULL || pgBufrRead == NULL || pgBufrWrite == NULL)
	{
		cout << "ERROR : Insufficient memory. EXIT !!!\n";
		exit(1);
	}
}

HeapDBFile::~HeapDBFile () {
	if(pgBufrWrite->GetNumRecs() != 0) {
		//Need to write pgBufrWrite if any records are left
		filePointer->AddPage(pgBufrWrite,pgOffstWrite++);
		pgBufrWrite->EmptyItOut();
	}
	delete filePointer;
	if(pgBufrRead != NULL)
		delete pgBufrRead;
	if(pgBufrWrite != NULL)
		delete pgBufrWrite;
}

int HeapDBFile::Create (char *f_path, void *startup) {
	filePointer->Open(0,f_path);
	pgOffstRead = 0;
	pgOffstWrite = 0;

	//writing in meta data file
	string metaFName;
	metaFName.append(f_path);
	metaFName.append(".metadata");

	ofstream ofMetaFile;
	ofMetaFile.open(metaFName.c_str());
	ofMetaFile << heap << endl;
	ofMetaFile.close();

	return 1;
}

//Function assumes file already exist
int HeapDBFile::Open (char *f_path) {

	// READ from meta file starts
	string metaFName;
	metaFName.append(f_path);
	metaFName.append(".metadata");

	int type;
	ifstream ifMetaFile;
	ifMetaFile.open(metaFName.c_str());
	if(!ifMetaFile) return 0;

	//Read type,runlen and myorder
	ifMetaFile >> type;
	if(type != heap) {
		cout << "Type = " << type << " - You are trying to open a file that is not a heap" << endl;
		exit(-1);
	}

	//Any value except 0 will just open the file
	filePointer->Open(1,f_path);
	MoveFirst();
	if(filePointer->GetLength() != 0) {
		// pgOffstWrite is gonna be incremented by 1 in first stmt
		// and offset should less than length. Hence '-2'
		pgOffstWrite = filePointer->GetLength() - 2 ;
	}
	else {
		pgOffstWrite = 0;
	}
	return 1;
}

int HeapDBFile::Close () {
	if(pgBufrWrite->GetNumRecs() != 0) {
		//Need to write pgBufrWrite if any records are left
		filePointer->AddPage(pgBufrWrite,pgOffstWrite++);
		pgBufrWrite->EmptyItOut();
	}
	filePointer->Close();
	return 1;
}

void HeapDBFile::Load (Schema &f_schema, char *loadpath) {
	FILE * pFile;
	pFile = fopen (loadpath,"r");

	int pageOffset = 0;

	Record *recordPtr = new Record();
	Page *pagePtr = new Page();

	while(recordPtr->SuckNextRecord(&f_schema,pFile)) {
		if(!pagePtr->Append(recordPtr)) {
			//Came here because can't append more.
			//Have to add the current records to file now
#ifdef verbose
			cout << "adding page = " << pgOffstRead << endl;
#endif
			filePointer->AddPage(pagePtr,pageOffset++);
			//Emptying the page as content is already written
			pagePtr->EmptyItOut();
			pagePtr->Append(recordPtr);
		}
	}
#ifdef verbose
	cout << "adding page last = " << pgOffstRead << endl;
#endif
	filePointer->AddPage(pagePtr,pageOffset);

	delete recordPtr;
	delete pagePtr;
}


void HeapDBFile::MoveFirst () {
	filePointer->GetPage(pgBufrRead,0);
	pgOffstRead = 0;
#ifdef verbose
	cout << "file length = " << filePointer->GetLength() << endl << flush;
#endif
}

//Limitation: This function will buffer the data until buffer is full (then it writes it)
// This means that you have to call Close() before reading from this db file
void HeapDBFile::Add (Record &rec) {
	// cout << "getting page " << f.GetLength() << endl;
	if (0 != filePointer->GetLength()) {
		//if pgBufrWrite is blank we need to get the pageBuffer from the end of the file
		if(pgBufrWrite->GetNumRecs() == 0)
			filePointer->GetPage(pgBufrWrite, pgOffstWrite);

		if (!pgBufrWrite->Append(&rec)) {
			//filePointer->AddPage(pgBufrWrite,filePointer->GetLength()-2); // same final page
			//Could not append as page is full. Need to empty and create new page to add
#ifdef LOG_ON
			cout << "writing at page offsset = " << pgOffstWrite << endl;
#endif
			filePointer->AddPage(pgBufrWrite,pgOffstWrite++);

			//pgBufrWrite is written - empty it out
			pgBufrWrite->EmptyItOut();
			pgBufrWrite->Append(&rec);
		}
	}
	else {
		//New file
		if (pgBufrWrite->Append(&rec)) {
			filePointer->AddPage(pgBufrWrite,pgOffstWrite); // new final page
			//pgBufrWrite is written - empty it out
			pgBufrWrite->EmptyItOut();
		}
		else {
			//pageBuffer has to be empty
			exit(-1);
		}
	}
}

int HeapDBFile::GetNext (Record &fetchme) {
	if(pgBufrRead->GetFirst(&fetchme))
		return 1;

	// Case where pageBuffer is empty so need to do offset++
	// Check if length is in range of total number of pages.
	if(pgOffstRead+1 < (filePointer->GetLength()-1))
		pgOffstRead++;
	else
		return 0;

#ifdef verbose
	cout << "reading page offset = " << pgOffstRead << endl;
#endif
	//Get the new page and return first record
	filePointer->GetPage(pgBufrRead,pgOffstRead);
	return pgBufrRead->GetFirst(&fetchme);
}

int HeapDBFile::GetNext (Record &fetchme, CNF &cnf, Record &literal) {
	ComparisonEngine compEngine;

	while( GetNext(fetchme) ){
		if(compEngine.Compare(&fetchme,&literal,&cnf)) {
			return 1;
		}
	}
	return 0;
}
