#include "DBFile.h"
#include <iostream>
#include <stdlib.h>
#include "Defs.h"

DBFile::DBFile () {
    gdf = NULL;
}

DBFile::~DBFile () {
}

int DBFile::Create (char *fpath, fType ftype, void *start_up) {
    
    if(ftype == heap) {
        gdf = new HeapDBFile();
        gdf->Create(fpath,start_up);
    }
    else if(ftype == sorted) {
        
        gdf = new SortedDBFile();
        gdf->Create(fpath,start_up);
    }
    else if(ftype == tree) {
        
    }
    else {
        cout << "File Type Unkonwn !!!\n";
        return 0;
    }
    return 1;
}

void DBFile::Load (Schema &f_schema_1, char *load_path) {
    gdf->Load(f_schema_1,load_path);
}

int DBFile::Close () {
    return gdf->Close();
}

int DBFile::Open (char *fpath) {
    gdf = new SortedDBFile();
    return gdf->Open(fpath);
}


void DBFile::MoveFirst () {
    gdf->MoveFirst();
}



void DBFile::Add (Record &recrd) {
    gdf->Add(recrd);
}

int DBFile::GetNext (Record &fetch_me) {
    return gdf->GetNext(fetch_me);
}

int DBFile::GetNext (Record &fetch_me, CNF &cnf, Record &literal) {
    return gdf->GetNext(fetch_me,cnf,literal);
}
