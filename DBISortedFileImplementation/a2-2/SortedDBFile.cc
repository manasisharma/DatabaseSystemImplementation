

#include <iostream>
#include <stdlib.h>
#include <fstream>

#include "SortedDBFile.h"

SortedDBFile::SortedDBFile () {
	filePtr = new File();

	pageBufRead = new Page();
	pageBufWrite = new Page();

	pageOffsetRead = 0;
	pageOffsetWrite = 0;

	bigq = NULL;
	inputPipe = NULL;
	outputPipe = NULL;

	mergeRequired = false;

	bBinSearchCalled = false;
	bBinSearchPossible =  false;
	bsOffset = 0;
	bsMaxOffset = 0;
	if (filePtr == NULL || pageBufRead == NULL || pageBufWrite == NULL)
	{
		cout << "ERROR : Not enough memory. EXIT !!!\n";
		exit(1);
	}
}

SortedDBFile::~SortedDBFile () {
	delete filePtr;
	if(sSortInfo.sortOrder != NULL)
		delete sSortInfo.sortOrder;
	if(pageBufRead != NULL)
		delete pageBufRead;
	if(pageBufWrite != NULL)
		delete pageBufWrite;
}

void SortedDBFile::initBigQ() {
	int buffsz = 100; // pipe cache size
	inputPipe = new Pipe(buffsz);
	outputPipe = new Pipe(buffsz);
	bigq = new BigQ (*inputPipe, *outputPipe, *sSortInfo.sortOrder, sSortInfo.runLength);
}

int SortedDBFile::Create (char *f_path, void *startup) {
	//save file name for later use
	fileName.empty();
	fileName.append(f_path);

	filePtr->Open(0,f_path);
	pageOffsetRead = 0;
	pageOffsetWrite = 0;

	struct SortInfo* sInfo = (struct SortInfo* )startup;
	
	sSortInfo.runLength = sInfo->runLength;
	sSortInfo.sortOrder = sInfo->sortOrder;

	//writing in meta data file
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
	for(int i=0; i < sInfo->sortOrder->getNumAtts() ; i++) {
		ofMetaFile << attrs[i] << endl;
		ofMetaFile << whichTypes[i] << endl;
	}
	ofMetaFile.close();

	return 1;
}

//Function assumes file already exist
int SortedDBFile::Open (char *f_path) {
	//Any value except 0 will just open the file
	int type;

	//save file name for later use
	fileName.empty();
	fileName.append(f_path);

	// READ from meta file starts
	string metaFName;
	metaFName.append(f_path);
	metaFName.append(".metadata");

	ifstream ifMetaFile;
	ifMetaFile.open(metaFName.c_str());
	if(!ifMetaFile) return 0;

	//Read type,runlen and myorder
	ifMetaFile >> type;
	if(type != sorted) {
		cout << "Type = " << type << " - Trying to open a file that not sorted" << endl;
		exit(-1);
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

	//populate sort order
	sSortInfo.sortOrder = new OrderMaker;
	sSortInfo.sortOrder->setNumAtts(numAtts);
	sSortInfo.sortOrder->setWhichAtts(whichAttr);
	sSortInfo.sortOrder->setWhichType(whichTypes);

	delete whichAttr;
	delete whichTypes;
#ifdef LOG_ON
	cout << __FILE__ << __LINE__ << ":";
	cout << "type = " << type << endl;
	cout << "runlen = " << sSortInfo.runLength << endl;
	sSortInfo.sortOrder->Print();
#endif
	// READ from meta file ends

	filePtr->Open(1,f_path);

	if(filePtr->GetLength() != 0) {
		// pageOffsetWrite is gonna be incremented by 1 in first stmt
		// and offset should less than length. Hence '-2'
		MoveFirst();
		pageOffsetWrite = filePtr->GetLength() - 2 ;
	}
	else {
		pageOffsetRead = 0;
		pageOffsetWrite = 0;
	}
	return 1;
}

int SortedDBFile::Close () {
	//if this function called after Add
	if(mergeRequired)
		MergeDifferential();

	filePtr->Close();
	return 1;
}

void SortedDBFile::Load (Schema &f_schema, char *loadpath) {
	if(bigq == NULL)
		initBigQ();
	FILE * pFile;
	pFile = fopen (loadpath,"r");

	int pageOffset = 0;

	Record *recInput = new Record();
	Record *recOutput = new Record();
	Page *pagePtr = new Page();

	while(recInput->SuckNextRecord(&f_schema,pFile)) {
		inputPipe->Insert(recInput);
	}
#ifdef LOG_ON
	cout<< "==== SORTEDBFILE ===== " << endl;
	cout << "all records inserted... " << endl;
#endif
	inputPipe->ShutDown ();

	while(outputPipe->Remove(recOutput)) {
		if(!pagePtr->Append(recOutput)) {
			//Came here because can't append more.
			//Have to add the current records to file now
#ifdef LOG_ON
			cout << "adding page = " << pageOffsetRead << endl;
#endif
			filePtr->AddPage(pagePtr,pageOffset++);
			//Page is written. don't need content it anymore
			pagePtr->EmptyItOut();
			pagePtr->Append(recOutput);
		}
	}
#ifdef LOG_ON
	cout << "all records removed from OUTPIPE.." << endl;
	cout << "adding page last = " << pageOffsetRead << endl;
#endif
	filePtr->AddPage(pagePtr,pageOffset);

	//Need to call binary search again because records are added
	bBinSearchCalled = false;

	delete recInput;
	delete recOutput;
	delete pagePtr;
}


void SortedDBFile::MoveFirst () {
	//if this function called after Add
	if(mergeRequired)
		MergeDifferential();

	filePtr->GetPage(pageBufRead,0);
	pageOffsetRead = 0;
#ifdef LOG_ON
	cout << __FILE__ << __LINE__ << "file length = " << filePtr->GetLength() << endl << flush;
#endif
}

//Limitation: This function will buffer the data until buffer is full (then it writes it)
// This means that you have to call Close() before reading from this db file
void SortedDBFile::Add (Record &rec) {
	if(bigq == NULL)
		initBigQ();
	//Need to call binary search again because records are added
	bBinSearchCalled = false;
	inputPipe->Insert(&rec);
	mergeRequired = true;
}

int SortedDBFile::GetNext (Record &fetchme) {
	//if this function called after Add
	if(mergeRequired)
		MergeDifferential();

	if(pageBufRead->GetFirst(&fetchme))
		return 1;

	// Case where pageBuffer is empty so need to do offset++
	// Check if length is in range of total number of pages.
	if(pageOffsetRead+1 < (filePtr->GetLength()-1))
		pageOffsetRead++;
	else
		return 0;

#ifdef LOG_ON
	cout << "reading page offset = " << pageOffsetRead << endl;
#endif
	//Get the new page and return first record
	filePtr->GetPage(pageBufRead,pageOffsetRead);
	return pageBufRead->GetFirst(&fetchme);
}

int SortedDBFile::GetNext (Record &fetchme, CNF &cnf, Record &literal) {
	//The following if block is needed just to make sure that
	//the binary search function is called only once
	if(!bBinSearchCalled){
		bBinSearchPossible = binarySearchPossible(cnf,literal);
		if(bBinSearchPossible)
			binarySearch(literal);
		bBinSearchCalled = true;
	}
	if(bBinSearchPossible == true) {
#ifdef LOG_ON
		cout << "===== bin search reading page offset ==== " << bsOffset << endl;
#endif
		//Now start reading from the page
		while(bsOffset < bsMaxOffset) {
			while(bsPage.GetFirst(&fetchme) == 1) {
				if(compEngine.Compare(&fetchme,&literal,&cnf) == 1)
					return 1;
			}
			bsOffset++;
			if(bsOffset >= bsMaxOffset)
				break;
			//Get the new page and check first record
			filePtr->GetPage(&bsPage,bsOffset);
			if(bsPage.GetFirst(&fetchme) == 1) {
				if(compEngine.Compare(&fetchme,&literal,&cnf) == 1)
					return 1;
			}
		}
		return 0;
	}
	else {
		//Do sequential search
		while( GetNext(fetchme) ){
			if(compEngine.Compare(&fetchme,&literal,&cnf)) {
				return 1;
			}
		}
	}
	return 0;
}

bool SortedDBFile::binarySearchPossible(CNF &cnf, Record &literal) {
	//This function should check whether "query" OrderMaker constructed
	//from CNF and literal matches with file's sorted order
	OrderMaker cnfOrderMaker,dummy;
	cnf.GetSortOrders(cnfOrderMaker,dummy);
	//cnfOrderMaker.Print();

	int fNumAttr = sSortInfo.sortOrder->getNumAtts();
	int* fWhichAttr = sSortInfo.sortOrder->getWhichAtts();
	Type* fWhichType = sSortInfo.sortOrder->getWhichTypes();

	int cnfNumAttr = cnfOrderMaker.getNumAtts();
	int* cnfWhichAttr = cnfOrderMaker.getWhichAtts();
	Type* cnfWhichType = cnfOrderMaker.getWhichTypes();

	int qNumAttr = 0;
	int* qWhichAttr = new int[fNumAttr];
	int* qWhichType = new int[fNumAttr];

	int  lNumAttr = 0;
	int* lWhichAttr = new int[fNumAttr];
	int* lWhichType = new int[fNumAttr];
	bool attrFound = false;
	for(int i=0; i<fNumAttr; i++) {
		for(int j=0; j<cnfNumAttr; j++) {
			if(fWhichAttr[i] == cnfWhichAttr[j]) {
				attrFound = true;
				qWhichAttr[qNumAttr] = fWhichAttr[i];
				qWhichType[qNumAttr++] = (int)fWhichType[i];
				lWhichAttr[lNumAttr] = i;
				lWhichType[lNumAttr++] = (int)fWhichType[i];
			}
		}
		if(!attrFound)
			break;
		else
			attrFound = false;
	}

	//Didn't find any matching attributes, return false
	if(qNumAttr == 0)
		return false;

	//Found something - copy in qOrderMaker
	queryOm.setNumAtts(qNumAttr);
	queryOm.setWhichAtts(qWhichAttr);
	queryOm.setWhichType(qWhichType);

	literalOm.setNumAtts(lNumAttr);
	literalOm.setWhichAtts(lWhichAttr);
	literalOm.setWhichType(lWhichType);

	cout << "== Printing Query Order Maker" << endl;
	queryOm.Print();
	cout << "== Done Printing Query Order Maker" << endl;

	cout << "== Printing literal Order Maker" << endl;
	literalOm.Print();
	cout << "== Done literal Query Order Maker" << endl;

	delete qWhichAttr;
	delete qWhichType;
	delete lWhichAttr;
	delete lWhichType;
	return true;
}

void SortedDBFile::binarySearch(Record &literal) {
	//Following binary search just gives you the pageIndex that might have the record
	off_t lowOffset = pageOffsetRead;
	off_t highOffset = filePtr->GetLength() >= 2 ? filePtr->GetLength() - 2:0;
	off_t midOffset = 0;
	off_t initMidOffset = 0;
#ifdef LOG_ON
	cout << "low offset = " << lowOffset << endl;
	cout << "high offset = " << highOffset << endl;
#endif
	Record tempRec;
	Page tempPage;

	int retValue;

	while(lowOffset <= highOffset) {
		midOffset = lowOffset  + (highOffset - lowOffset)/2;
#ifdef LOG_ON
		cout << "in bin search - mid offset = " << midOffset << endl;
#endif
		//Get mid offset's first record
		filePtr->GetPage(&tempPage,midOffset);
		if(tempPage.GetFirst(&tempRec) == 0) {
			cout << "error bin search , record empty" << endl  ;
			exit(-1);
		}

		retValue = compEngine.Compare(&literal,&literalOm,&tempRec,&queryOm);

#ifdef LOG_ON
		cout << "->retvalue  = " << retValue << endl;
#endif
		//literal is less
		if(retValue == -1) {
			highOffset = midOffset - 1;
		}
		//literal is greater
		else if(retValue == 1)
			lowOffset = midOffset + 1;
		//equal
		else
			break;
	}
#ifdef LOG_ON
	cout << "==== bin loop over and mid offset = " << midOffset << endl;
#endif
	initMidOffset = midOffset;
	//Need to make sure that literal is in this page
	//otherwise search in neighboring pages
	filePtr->GetPage(&tempPage,midOffset);
	if(tempPage.GetFirst(&tempRec) == 0) {
		cout << "error bin search , record empty" << endl  ;
		exit(-1);
	}

	retValue = compEngine.Compare(&literal,&literalOm,&tempRec,&queryOm);
#ifdef LOG_ON
	cout << "after bin search ->retvalue  = " << retValue << endl;
#endif
	if(retValue == -1 || retValue == 0) {
		//While you find the first record which is less than the literal
		midOffset--;
		if(midOffset >= 0) {
			filePtr->GetPage(&tempPage,midOffset);
			if(tempPage.GetFirst(&tempRec) != 0) {
				retValue = compEngine.Compare(&literal,&literalOm,&tempRec,&queryOm);
				//While you find the first record which is less than the literal
				//if tempRec < literal record then retValue = 1 and will stop there
				//i.e. we want literal to be greater
				while(retValue <= 0 && midOffset > 0) {
					filePtr->GetPage(&tempPage,--midOffset);
					if(tempPage.GetFirst(&tempRec) == 1) {
						retValue = compEngine.Compare(&literal,&literalOm,&tempRec,&queryOm);
					}
					else
						break;
				}
			}
		}
		bsOffset = max(midOffset,bsOffset);
	}
	//retvalue possible is 1 and in that case midOffset is the correct page
	else{
		bsOffset = midOffset;
	}

	//Need to calculate bsMaxOffset
	initMidOffset++;
	if(initMidOffset <= filePtr->GetLength()-2) {
		filePtr->GetPage(&tempPage,initMidOffset);
		if(tempPage.GetFirst(&tempRec) != 0) {
			retValue = compEngine.Compare(&literal,&literalOm,&tempRec,&queryOm);
			//While you find the first record which is greater than the literal
			//if tempRec > literal then retValue = -1 and we will stop there
			while(retValue >= 0 && initMidOffset < filePtr->GetLength()-2) {
				filePtr->GetPage(&tempPage,++initMidOffset);
				if(tempPage.GetFirst(&tempRec) == 1) {
					retValue = compEngine.Compare(&literal,&literalOm,&tempRec,&queryOm);
				}
				else
					break;
			}
		}
	}
	bsMaxOffset = min(initMidOffset,filePtr->GetLength()-2);
	//Make sure that bsOffset just sits outside of the data boundary
	if(bsMaxOffset == filePtr->GetLength()-2)
		bsMaxOffset++;

#ifdef LOG_ON
	cout << "==== bs offset = " << bsOffset << endl;
	cout << "==== bsMaxOffset set to : = " << bsMaxOffset << endl;
#endif

	filePtr->GetPage(&bsPage,bsOffset);
}

void SortedDBFile::MergeDifferential() {
	inputPipe->ShutDown ();
	mergeRequired = false;
#ifdef LOG_ON
	cout << __FILE__ << ":" << __LINE__ << " ===going to with merge existing records ===" << endl;
#endif

	Record curFileRec, bigQRec;
	bool fileNotEmpty = filePtr->GetLength() > 0 ? true:false;
	bool pipeNotEmpty = outputPipe->Remove(&bigQRec) == 1 ? true:false;

	//Temporary file for the merge
	//will be renamed to original file in the end
	HeapDBFile tmpHeapFile;
	string mergedFname(fileName);
	mergedFname.append(".tmp");
	tmpHeapFile.Create(const_cast<char*>(mergedFname.c_str()), NULL);

	// initialize
	if (fileNotEmpty) {
		MoveFirst();
		fileNotEmpty = GetNext(curFileRec);
	}

	// two-way merge
	while (fileNotEmpty || pipeNotEmpty) {
		if (!fileNotEmpty || (pipeNotEmpty && compEngine.Compare(&curFileRec, &bigQRec, sSortInfo.sortOrder) > 0)) {
			tmpHeapFile.Add(bigQRec);
			pipeNotEmpty = outputPipe->Remove(&bigQRec);
		}
		else if (!pipeNotEmpty
				|| (fileNotEmpty && compEngine.Compare(&curFileRec, &bigQRec, sSortInfo.sortOrder) <= 0)) {
			tmpHeapFile.Add(curFileRec);
			fileNotEmpty = GetNext(curFileRec);
		}
		else {
			cout << __FILE__ << ":" << "two-way merge failed" << endl;
			exit(-1);
		}
	}
	// Make sure you close the Heap file
	tmpHeapFile.Close();

	if(remove(fileName.c_str()) != 0) {
		cout << "error removing original file" << endl;
		exit(-1);
	}
	if(rename(mergedFname.c_str(),fileName.c_str()) == 0) {
#ifdef LOG_ON
		cout << "file renamed to " << fileName << endl;
#endif
	}
	else {
		cout << "error renaming file from " << mergedFname << "to " << fileName << endl;
		exit(-1);
	}
	//remove temp heap file's meta data file
	remove(mergedFname.append(".metadata").c_str());
}
