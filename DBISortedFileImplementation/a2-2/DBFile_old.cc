#include "DBFile.h"
#include "Defs.h"

#include <iostream>
#include <stdlib.h>

DBFile::DBFile () {
	gdf = NULL;
}

DBFile::~DBFile () {
}

int DBFile::Create (char *f_path, fType f_type, void *startup) {

	if(f_type == heap) {
		gdf = new HeapDBFile();
		gdf->Create(f_path,startup);
	}
	else if(f_type == sorted) {
		/* sorted specific code here */
		gdf = new SortedDBFile();
		gdf->Create(f_path,startup);
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
	gdf = new SortedDBFile();
	return gdf->Open(f_path);
}

int DBFile::Close () {
	return gdf->Close();
}

void DBFile::Load (Schema &f_schema, char *loadpath) {
	gdf->Load(f_schema,loadpath);
}

void DBFile::MoveFirst () {
	gdf->MoveFirst();
}

void DBFile::Add (Record &rec) {
	gdf->Add(rec);
}

int DBFile::GetNext (Record &fetchme) {
	return gdf->GetNext(fetchme);
}

int DBFile::GetNext (Record &fetchme, CNF &cnf, Record &literal) {
	return gdf->GetNext(fetchme,cnf,literal);
}
