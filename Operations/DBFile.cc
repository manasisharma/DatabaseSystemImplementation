#include "DBFile.h"
#include "Defs.h"

#include <iostream>
#include <stdlib.h>

DBFile::DBFile () {
	gendf = NULL;
}

DBFile::~DBFile () {
}

int DBFile::Create (char *f_path, fType f_type, void *startup) {

	if(f_type == heap) {
		gendf = new HeapDBFile();
		gendf->Create(f_path,startup);
	}
	else if(f_type == sorted) {
		/* sorted specific code here */
		gendf = new SortedDBFile();
		gendf->Create(f_path,startup);
	}
	else if(f_type == tree) {
		/* tree specific code here */
	}
	else {
		cout << "file type unknown, EXIT !!!\n";
		return 0;
	}
	return 1;
}

//Function assumes file already exist
int DBFile::Open (char *f_path) {
	gendf = new SortedDBFile();
	return gendf->Open(f_path);
}

int DBFile::Close () {
	return gendf->Close();
}

void DBFile::Load (Schema &f_schema, char *loadpath) {
	gendf->Load(f_schema,loadpath);
}

void DBFile::MoveFirst () {
	gendf->MoveFirst();
}

void DBFile::Add (Record &rec) {
	gendf->Add(rec);
}

int DBFile::GetNext (Record &fetchme) {
	return gendf->GetNext(fetchme);
}

int DBFile::GetNext (Record &fetchme, CNF &cnf, Record &literal) {
	return gendf->GetNext(fetchme,cnf,literal);
}
