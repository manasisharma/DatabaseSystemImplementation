#include <stdlib.h>
#include <fstream>
#include <iostream>
#include "SortedDBFile.h"
SortedDBFile::SortedDBFile () {
	filePtr = new File();
    pageBufWrite = new Page();
	pageBufRead = new Page();
	pageOffsetRead = 0;
    bsMaxOffset = 0;
	bigq = NULL;
    mergeRequired = false;
    outputPipe = NULL;
    bBinSearchCalled = false;
    inputPipe = NULL;
    pageOffsetWrite = 0;
	bBinSearchPossible =  false;
	bsOffset = 0;
	if (filePtr == NULL || pageBufWrite == NULL || pageBufRead == NULL)
	{
		cout << "Memory Insufficient ! \n"; exit(1);
	}
}

SortedDBFile::~SortedDBFile () {
	delete filePtr;
	if(sSortInfo.sortOrder != NULL) delete sSortInfo.sortOrder;
	if(pageBufRead != NULL)  delete pageBufRead;
	if(pageBufWrite != NULL)  delete pageBufWrite;
}

void SortedDBFile::initBigQ() {
	int bufferz = 100;
	inputPipe = new Pipe(bufferz);
	outputPipe = new Pipe(bufferz);
	bigq = new BigQ (*inputPipe, *outputPipe, *sSortInfo.sortOrder, sSortInfo.runLength);
}

int SortedDBFile::Create (char *f_path, void *start_up) {
	fileName.empty();
	fileName.append(f_path);
	filePtr->Open(0,f_path);
	pageOffsetWrite = 0;
    pageOffsetRead = 0;
	struct SortInfo* sInfo = (struct SortInfo* )start_up;
	sSortInfo.runLength = sInfo->runLength;
	sSortInfo.sortOrder = sInfo->sortOrder;
	string metaFName;
	metaFName.append(f_path);
	metaFName.append(".metadata");
	ofstream ofMetaFile;
	ofMetaFile.open(metaFName.c_str());
	ofMetaFile << sorted << endl;
	ofMetaFile << sInfo->runLength << endl;
	ofMetaFile << sInfo->sortOrder->getNumAtts() << endl;
	int* attrs = sInfo->sortOrder->getWhichAtts();
	Type* whichTypes = sInfo->sortOrder->getWhichTypes();
	for(int s=0; s < sInfo->sortOrder->getNumAtts() ; s++) {
		ofMetaFile << attrs[s] << endl;
		ofMetaFile << whichTypes[s] << endl;
	}
	ofMetaFile.close();
	return 1;
}
int SortedDBFile::Open (char *f_path) {
	int filetype;
	fileName.empty();
	fileName.append(f_path);
	string metaFName;
	metaFName.append(f_path);
	metaFName.append(".metadata");
	ifstream ifMetaFile;
	ifMetaFile.open(metaFName.c_str());
	if(!ifMetaFile) return 0;
	ifMetaFile >> filetype;
	if(filetype != sorted) {
		cout << "Type = " << filetype << " Opening  a file that is not sorted " << endl; exit(-1);
	}
	ifMetaFile >> sSortInfo.runLength;
	int numAtts = 0;
	ifMetaFile >> numAtts;
	int *whichAttr = new int[numAtts];
	int *whichTypes = new int[numAtts];
	for(int i =0; i < numAtts;i++) {
		ifMetaFile >> whichAttr[i];
		ifMetaFile >> whichTypes[i];
	}
	ifMetaFile.close();
	sSortInfo.sortOrder = new OrderMaker;
	sSortInfo.sortOrder->setNumAtts(numAtts);
	sSortInfo.sortOrder->setWhichAtts(whichAttr);
	sSortInfo.sortOrder->setWhichType(whichTypes);
	delete whichTypes;
    delete whichAttr;
#ifdef LOG_ON
	cout << __FILE__ << __LINE__ << ":";
	cout << "  Type = " << filetype << endl;
	cout << "  Run length = " << sSortInfo.runLength << endl;
	sSortInfo.sortOrder->Print();
#endif
	filePtr->Open(1,f_path);
	if(filePtr->GetLength() != 0) {
		MoveFirst();
		pageOffsetWrite = filePtr->GetLength() - 2 ;
	}
	else {
        pageOffsetWrite = 0;
		pageOffsetRead = 0;
	}
	return 1;
}
int SortedDBFile::Close () {
	if(mergeRequired)
		MergeDifferential();
	filePtr->Close();
	return 1;
}

void SortedDBFile::Load (Schema &f_schema, char *load_path) {
	if(bigq == NULL)
		initBigQ();
	FILE * pFile;
	pFile = fopen (load_path,"r");
	int pageOffset = 0;
    Record *record_output = new Record();
    Record *record_input = new Record();
    Page *pagePtr = new Page();
	while(record_input->SuckNextRecord(&f_schema,pFile)) {
		inputPipe->Insert(record_input);
	}
#ifdef LOG_ON
	cout<< "  Sorted DB file" << endl;
	cout << "  Records inserted " << endl;
#endif
	inputPipe->ShutDown ();
	while(outputPipe->Remove(record_output)) {
		if(!pagePtr->Append(record_output)) {
#ifdef LOG_ON
			cout << "  Adding new page = " << pageOffsetRead << endl;
#endif
			filePtr->AddPage(pagePtr,pageOffset++);
			pagePtr->EmptyItOut();
			pagePtr->Append(record_output);
		}
	}
#ifdef LOG_ON
	cout << "  Records removed from outpipe" << endl;
	cout << "  Adding last page = " << pageOffsetRead << endl;
#endif
	filePtr->AddPage(pagePtr,pageOffset);
	bBinSearchCalled = false;
	delete record_input;
	delete record_output;
	delete pagePtr;
}


void SortedDBFile::MoveFirst () {
	if(mergeRequired)
		MergeDifferential();
	filePtr->GetPage(pageBufRead,0);
	pageOffsetRead = 0;
#ifdef LOG_ON
	cout << __FILE__ << __LINE__ << " Length of File = " << filePtr->GetLength() << endl << flush;
#endif
}
void SortedDBFile::Add (Record &rec) {
	if(bigq == NULL)
		initBigQ();
	bBinSearchCalled = false;
	inputPipe->Insert(&rec);
	mergeRequired = true;
}

bool SortedDBFile::binarySearchPossible(CNF &cnf, Record &lit_eral) {
    OrderMaker cnf_Order_Maker,tempr;
    cnf.GetSortOrders(cnf_Order_Maker,tempr);
    int fNumAttr = sSortInfo.sortOrder->getNumAtts();
    int* fWhichAttr = sSortInfo.sortOrder->getWhichAtts();
    Type* fWhichType = sSortInfo.sortOrder->getWhichTypes();
    int cnfNumAttr = cnf_Order_Maker.getNumAtts();
    int* cnfWhichAttr = cnf_Order_Maker.getWhichAtts();
    Type* cnfWhichType = cnf_Order_Maker.getWhichTypes();
    int qNumAttr = 0;
    int* qWhichType = new int[fNumAttr];
    int* qWhichAttr = new int[fNumAttr];
    int  lNumAttr = 0;
    int* lWhichType = new int[fNumAttr];
    int* lWhichAttr = new int[fNumAttr];
    bool attrFound = false;
    for(int j=0; j<fNumAttr; j++) {
        for(int i=0; i<cnfNumAttr; i++) {
            if(fWhichAttr[j] == cnfWhichAttr[i]) {
                attrFound = true;
                qWhichAttr[qNumAttr] = fWhichAttr[j];
                qWhichType[qNumAttr++] = (int)fWhichType[j];
                lWhichAttr[lNumAttr] = j;
                lWhichType[lNumAttr++] = (int)fWhichType[j];
            }
        }
        if(!attrFound) break;
        else attrFound = false;
    }
    if(qNumAttr == 0) return false;
    queryOm.setNumAtts(qNumAttr);
    queryOm.setWhichAtts(qWhichAttr);
    queryOm.setWhichType(qWhichType);
    literalOm.setNumAtts(lNumAttr);
    literalOm.setWhichAtts(lWhichAttr);
    literalOm.setWhichType(lWhichType);
    
    cout << "  Printing Query Order Maker" << endl;
    queryOm.Print();
    
    cout << "  Done Printing Query Order Maker" << endl;
    
    cout << "  Printing literal Order Maker" << endl;
    literalOm.Print();
    
    cout << "  Done literal Query Order Maker" << endl;
    
    delete qWhichAttr;
    delete lWhichAttr;
    delete qWhichType;
    delete lWhichType;
    return true;
}
int SortedDBFile::GetNext (Record &fetch_me) {
	if(mergeRequired)
		MergeDifferential();
	if(pageBufRead->GetFirst(&fetch_me)) return 1;
	if(pageOffsetRead+1 < (filePtr->GetLength()-1)) pageOffsetRead++;
	else return 0;
#ifdef LOG_ON
	cout << " Reading page offset = " << pageOffsetRead << endl;
#endif
	filePtr->GetPage(pageBufRead,pageOffsetRead);
	return pageBufRead->GetFirst(&fetch_me);
}
int SortedDBFile::GetNext (Record &fetch_me, CNF &cnf, Record &lit_eral) {
	if(!bBinSearchCalled){
		bBinSearchPossible = binarySearchPossible(cnf,lit_eral);
		if(bBinSearchPossible) binarySearch(lit_eral);
		bBinSearchCalled = true;
	}
	if(bBinSearchPossible == true) {
#ifdef LOG_ON
		cout << " Bin search: reading page offset " << bsOffset << endl;
#endif
		while(bsOffset < bsMaxOffset) {
			while(bsPage.GetFirst(&fetch_me) == 1) {
				if(compEngine.Compare(&fetch_me,&lit_eral,&cnf) == 1) return 1;
			}
			bsOffset++;
			if(bsOffset >= bsMaxOffset)
				break;			filePtr->GetPage(&bsPage,bsOffset);
			if(bsPage.GetFirst(&fetch_me) == 1) {
				if(compEngine.Compare(&fetch_me,&lit_eral,&cnf) == 1) return 1;
			}
		}
		return 0;
	}
	else {
		while( GetNext(fetch_me) ){
			if(compEngine.Compare(&fetch_me,&lit_eral,&cnf))
				return 1;
		}
	}
	return 0;
}

void SortedDBFile::binarySearch(Record &lit_eral) {
	off_t lowOffset = pageOffsetRead;
	off_t highOffset = filePtr->GetLength() >= 2 ? filePtr->GetLength() - 2:0;
    off_t midOffset = 0;
    off_t initMidOffset = 0;

#ifdef LOG_ON
    cout << "  High offset = " << highOffset << endl;
	cout << "  Low offset = " << lowOffset << endl;
#endif
	Record temp_record;
    Page tempPage;
	int retValue;
	while(lowOffset <= highOffset) {
		midOffset = lowOffset  + (highOffset - lowOffset)/2;
#ifdef LOG_ON
		cout << "  Inside bin search - mid offset = " << midOffset << endl;
#endif
		filePtr->GetPage(&tempPage,midOffset);
		if(tempPage.GetFirst(&temp_record) == 0) {
			cout << "  !! Error bin search : record is empty" << endl  ;
			exit(-1);
		}
		retValue = compEngine.Compare(&lit_eral,&literalOm,&temp_record,&queryOm);
#ifdef LOG_ON
		cout << "   Retvalue  = " << retValue << endl;
#endif
		if(retValue == -1) {
			highOffset = midOffset - 1;
		}
		else if(retValue == 1)
			lowOffset = midOffset + 1;
		else
			break;
	}
#ifdef LOG_ON
	cout << " Inside loop over and mid offset = " << midOffset << endl;
#endif
	initMidOffset = midOffset;
	filePtr->GetPage(&tempPage,midOffset);
	if(tempPage.GetFirst(&temp_record) == 0) {
		cout << "  Error bin search : record is empty" << endl  ;
		exit(-1);
	}

	retValue = compEngine.Compare(&lit_eral,&literalOm,&temp_record,&queryOm);
#ifdef LOG_ON
	cout << "  After bin search : retvalue  = " << retValue << endl;
#endif
	if(retValue == -1 || retValue == 0) {
		midOffset--;
		if(midOffset >= 0) {
			filePtr->GetPage(&tempPage,midOffset);
			if(tempPage.GetFirst(&temp_record) != 0) {
				retValue = compEngine.Compare(&lit_eral,&literalOm,&temp_record,&queryOm);
				while(retValue <= 0 && midOffset > 0) {
					filePtr->GetPage(&tempPage,--midOffset);
					if(tempPage.GetFirst(&temp_record) == 1) {
						retValue = compEngine.Compare(&lit_eral,&literalOm,&temp_record,&queryOm);
					}
					else break;
				}
			}
		}
		bsOffset = max(midOffset,bsOffset);
	}
	else{
		bsOffset = midOffset;
	}
	initMidOffset++;
	if(initMidOffset <= filePtr->GetLength()-2) {
		filePtr->GetPage(&tempPage,initMidOffset);
		if(tempPage.GetFirst(&temp_record) != 0) {
			retValue = compEngine.Compare(&lit_eral,&literalOm,&temp_record,&queryOm);
			while(retValue >= 0 && initMidOffset < filePtr->GetLength()-2) {
				filePtr->GetPage(&tempPage,++initMidOffset);
				if(tempPage.GetFirst(&temp_record) == 1) {
					retValue = compEngine.Compare(&lit_eral,&literalOm,&temp_record,&queryOm);
				}
				else break;
			}
		}
	}
	bsMaxOffset = min(initMidOffset,filePtr->GetLength()-2);
	if(bsMaxOffset == filePtr->GetLength()-2)
		bsMaxOffset++;

#ifdef LOG_ON
	cout << "  bs offset = " << bsOffset << endl;
	cout << "  bsMaxOffset is set to : = " << bsMaxOffset << endl;
#endif

	filePtr->GetPage(&bsPage,bsOffset);
}
void SortedDBFile::MergeDifferential() {
	inputPipe->ShutDown ();
	mergeRequired = false;
#ifdef LOG_ON
	cout << __FILE__ << ":" << __LINE__ << " Merging with existing records ===" << endl;
#endif
	Record curFileRec, bigQRec;
	bool fileNotEmpty = filePtr->GetLength() > 0 ? true:false;
	bool pipeNotEmpty = outputPipe->Remove(&bigQRec) == 1 ? true:false;
	HeapDBFile tmpHeapFile;
	string mergedFname(fileName);
	mergedFname.append(".tmp");
	tmpHeapFile.Create(const_cast<char*>(mergedFname.c_str()), NULL);
	if (fileNotEmpty) {
		MoveFirst();
		fileNotEmpty = GetNext(curFileRec);
	}
	while (pipeNotEmpty || fileNotEmpty) {
		if ((pipeNotEmpty && compEngine.Compare(&curFileRec, &bigQRec, sSortInfo.sortOrder) > 0) || !fileNotEmpty) {
			tmpHeapFile.Add(bigQRec);
			pipeNotEmpty = outputPipe->Remove(&bigQRec);
		}
		else if ((fileNotEmpty && compEngine.Compare(&curFileRec, &bigQRec, sSortInfo.sortOrder) <= 0) || !pipeNotEmpty) {
			tmpHeapFile.Add(curFileRec);
			fileNotEmpty = GetNext(curFileRec);
		}
		else {
			cout << __FILE__ << ":" << "Two-way merge FAILURE" << endl; exit(-1);
		}
	}
	tmpHeapFile.Close();
	if(remove(fileName.c_str()) != 0) {
		cout << "Error Can not remove original File" << endl;
		exit(-1);
	}
	if(rename(mergedFname.c_str(),fileName.c_str()) == 0) {
#ifdef LOG_ON
		cout << "  File Renamed to--> " << fileName << endl;
#endif
	}
	else {
		cout << "  Error renaming the file from" << mergedFname << "to " << fileName << endl;
		exit(-1);
	}
	remove(mergedFname.append(".metadata").c_str());
}