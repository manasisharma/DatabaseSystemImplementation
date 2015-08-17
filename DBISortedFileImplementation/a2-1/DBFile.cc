
#include "DBFile.h"

#include <iostream>
#include <stdlib.h>

DBFile::DBFile () {
	filePtr = new File();

	pageBufRead = new Page();
	pageBufWrite = new Page();

	pageOffsetRead = 0;
	pageOffsetWrite = 0;

	if (filePtr == NULL || pageBufRead == NULL || pageBufWrite == NULL)
	{
		cout << "ERROR : Not enough memory. EXIT !!!\n";
		exit(1);
	}
}

DBFile::~DBFile () {
	if(pageBufWrite->GetNumRecs() != 0) {
		//Need to write pageBufWrite if any records are left
		filePtr->AddPage(pageBufWrite,pageOffsetWrite++);
		pageBufWrite->EmptyItOut();
	}
	delete filePtr;
	if(pageBufRead != NULL)
		delete pageBufRead;
	if(pageBufWrite != NULL)
		delete pageBufWrite;
}

int DBFile::Create (char *f_path, fType f_type, void *startup) {
	if(f_type == heap) {
		//create the file using file class
		// '0' in first argument means open
		filePtr->Open(0,f_path);
		/* Heap specific code here */
		pageOffsetRead = 0;
		pageOffsetWrite = 0;
		return 1;
	}
	else if(f_type == sorted) {
		/* sorted specific code here */
	}
	else if(f_type == tree) {
		/* tree specific code here */
	}
	else {
		cout << "Don't know the file type. EXIT !!!\n";
		return 0;
	}
	return 1;
}

//Function assumes file already exist
int DBFile::Open (char *f_path) {
	//Any value except 0 will just open the file
	filePtr->Open(1,f_path);
	if(filePtr->GetLength() != 0) {
		// pageOffsetWrite is gonna be incremented by 1 in first stmt
		// and offset should less than length. Hence '-2'
		pageOffsetWrite = filePtr->GetLength() - 2 ;
	}
	else {
		pageOffsetWrite = 0;
	}
	return 1;
}

int DBFile::Close () {
	if(pageBufWrite->GetNumRecs() != 0) {
		//Need to write pageBufWrite if any records are left
		filePtr->AddPage(pageBufWrite,pageOffsetWrite++);
		pageBufWrite->EmptyItOut();
	}
	filePtr->Close();
	return 1;
}

void DBFile::Load (Schema &f_schema, char *loadpath) {
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
			cout << "adding page = " << pageOffsetRead << endl;
#endif
			filePtr->AddPage(pagePtr,pageOffset++);
			//Page is written. don't need content it anymore
			pagePtr->EmptyItOut();
			pagePtr->Append(recordPtr);
		}
	}
#ifdef verbose
	cout << "adding page last = " << pageOffsetRead << endl;
#endif
	filePtr->AddPage(pagePtr,pageOffset);

	delete recordPtr;
	delete pagePtr;
}


void DBFile::MoveFirst () {
	filePtr->GetPage(pageBufRead,0);
	pageOffsetRead = 0;
#ifdef verbose
	cout << "file length = " << filePtr->GetLength() << endl << flush;
#endif
}

void DBFile::Add (Record &rec) {
	// cout << "getting page " << f.GetLength() << endl;
	if (0 != filePtr->GetLength()) {
		//if pageBufWrite is blank we need to get the pageBuffer from the end of the file
		if(pageBufWrite->GetNumRecs() == 0)
			filePtr->GetPage(pageBufWrite, pageOffsetWrite);

		if (!pageBufWrite->Append(&rec)) {
			//filePtr->AddPage(pageBufWrite,filePtr->GetLength()-2); // same final page
			//Append didn't succeed. page is full. need to empty and create new page to add
			cout << "writing at page offsset = " << pageOffsetWrite << endl;
			filePtr->AddPage(pageBufWrite,pageOffsetWrite++);

			//PageBufWrite is written - empty it out
			pageBufWrite->EmptyItOut();
			pageBufWrite->Append(&rec);
		}
	}
	else {
		//New file
		if (pageBufWrite->Append(&rec)) {
			filePtr->AddPage(pageBufWrite,pageOffsetWrite); // new final page
			//PageBufWrite is written - empty it out
			pageBufWrite->EmptyItOut();
		}
		else {
			//pageBuffer has to be empty
			exit(-1);
		}
	}
}

int DBFile::GetNext (Record &fetchme) {
	if(pageBufRead->GetFirst(&fetchme))
		return 1;

	// Case where pageBuffer is empty so need to do offset++
	// Check if length is in range of total number of pages.
	if(pageOffsetRead+1 < (filePtr->GetLength()-1))
		pageOffsetRead++;
	else
		return 0;

#ifdef verbose
	cout << "reading page offset = " << pageOffsetRead << endl;
#endif
	//Get the new page and return first record
	filePtr->GetPage(pageBufRead,pageOffsetRead);
	return pageBufRead->GetFirst(&fetchme);
}

int DBFile::GetNext (Record &fetchme, CNF &cnf, Record &literal) {
	ComparisonEngine compEngine;

	while( GetNext(fetchme) ){
		if(compEngine.Compare(&fetchme,&literal,&cnf)) {
			return 1;
		}
	}
	return 0;
}
