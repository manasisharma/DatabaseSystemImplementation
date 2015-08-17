#include "BigQ.h"

using namespace std;


BigQ :: BigQ (Pipe &in, Pipe &out, OrderMaker &sortorder, int runlen) {

	//Copy constructor args in sSortingArgs structure
	args.inPipe = &in;
	args.outPipe = &out;
	args.sortOrder = &sortorder;
	args.runLen = runlen;

	//spawn a worker thread
	pthread_create( &workerThread, NULL, &BigQ::sortInput, (void*)&args);

	//Wait for thread join
	pthread_join(workerThread, NULL);
}

BigQ::~BigQ () {
}

void* BigQ::sortInput (void* ptr) {

	sSortingArgs* args = (sSortingArgs*) ptr;

	File* pFile = new File();
	//Creating unique runfile name based on threadId
	string strRunsFile = "bigqthread" + static_cast<ostringstream*>( &(ostringstream() << pthread_self()) )->str()
				+ ".bin";
	//have to const_cast because File class open doenst take const char*
	char* psRunsFile = const_cast<char*> (strRunsFile.c_str());
	pFile->Open(0,psRunsFile);

	Page *pageArr = new Page[args->runLen];

	sInfoToSortRun sInfoRun;
	sInfoRun.pageIndex = 0;
	sInfoRun.numRecordsInRun = 0;
	sInfoRun.runLength = args->runLen;
	sInfoRun.pageArr = pageArr;
	sInfoRun.pFile = pFile;
	sInfoRun.sortOrder = args->sortOrder;

	//int pageIndex = 0,numberOfRuns = 0, numRecordsInRun = 0;

	int numberOfRuns = 0;

	//This is required because there is a bug in alignment after sorting
	//So we keep track of remaining records in each run
	int recRemaining = 0;
	vector<Record> vRecRemain;

	Record in;

	// read data from in pipe sort them into runlen pages
	while(args->inPipe->Remove(&in)){

		if(pageArr[sInfoRun.pageIndex].Append(&in) == 1) {
			sInfoRun.numRecordsInRun++;
		}	//if retValue = 0 that means page is full
		else {
			//now that page if full we need to create a new page
			//Also we need to check that page size is not equal to runLen
			if(sInfoRun.pageIndex + 1 != args->runLen) {
				sInfoRun.pageIndex++;
#ifdef LOG_ON
				cout << "creating new page = " << sInfoRun.pageIndex <<endl;
#endif
				pageArr[sInfoRun.pageIndex].Append(&in);
				sInfoRun.numRecordsInRun++;
			}
			else {
				//New RUN - sort old one first

				numberOfRuns++;
#ifdef LOG_ON
				cout << "ELSE run num = " << numberOfRuns << endl;
				cout<< "numRecordsInRun = " << sInfoRun.numRecordsInRun << endl;
				cout<< "pageIndex = " << sInfoRun.pageIndex << endl;

#endif
				BigQ::sortAndDumpRun(sInfoRun, numberOfRuns,vRecRemain,recRemaining);

				//empty pagearray
				for(int j=0;j < args->runLen;j++){
					pageArr[j].EmptyItOut();
				}

				sInfoRun.pageIndex = 0;
				sInfoRun.numRecordsInRun = 0;
				pageArr[sInfoRun.pageIndex].Append(&in);
				//This is a new run so setting it to 1
				sInfoRun.numRecordsInRun++;
			}
		}
	}

	if(sInfoRun.numRecordsInRun > 0 || vRecRemain.size()>0) {
#ifdef LOG_ON
		cout << "Rec remaining = " << recRemaining << endl ;
		cout << " LAST run : pageIndex = " << sInfoRun.pageIndex << " and runlen =" << sInfoRun.runLength << endl;
#endif
		numberOfRuns++;
		for(int i=0;i<recRemaining;i++){
			//last page.. for example, if its 19th page, then add the remaining terms to
			//pageArr[18] here index = 19-1
			if(pageArr[sInfoRun.pageIndex].Append(&vRecRemain[i]) == 0) {
				sInfoRun.pageIndex++;
				if(sInfoRun.pageIndex == args->runLen) {
					cout << "This should never happen .. EXITING!" << endl;
					exit(-1);
				}
#ifdef LOG_ON
				cout << "pageIndex = " << sInfoRun.pageIndex << " and runlen =" << sInfoRun.runLength << endl;
#endif
				pageArr[sInfoRun.pageIndex].Append(&vRecRemain[i]);
			}
			else {
				sInfoRun.numRecordsInRun++;
			}
		}
#ifdef LOG_ON
		cout << "run num = " << numberOfRuns << endl;
		cout<< "numRecordsInRun = " << sInfoRun.numRecordsInRun << endl;
#endif
		BigQ::sortAndDumpRun(sInfoRun, numberOfRuns,vRecRemain,recRemaining);
	}

    //Read all records hence close inpipe
	args->inPipe->ShutDown();

	//Cleanup page array
	delete[] pageArr;

	//Now merge using Priority Queue
	BigQ::mergeRuns(numberOfRuns, pFile, args);

	//Close the file
	pFile->Close();
	delete pFile;

	// finally shut down the out pipe
	args->outPipe->ShutDown();

	//Setting pointers to null
	sInfoRun.pageArr = NULL;
	sInfoRun.pFile = NULL;

	return 0;
}

void BigQ::sortAndDumpRun(sInfoToSortRun& sInfoRun, int& numberOfRuns, vector<Record>& vRecRemain,int& recRemainingIndex) {

	uint iRecord = 0,j = 0;
	Record temp;
	Record** recordBuf = new Record*[sInfoRun.numRecordsInRun];
#ifdef LOG_ON
	cout << "------------- START of run# " << numberOfRuns <<" -----------" << endl;
#endif
	int givenRec = 0;
	//actual records in given page arr
	for(j = 0; j<= sInfoRun.pageIndex;j++) {
		givenRec += sInfoRun.pageArr[j].GetNumRecs();
	}
#ifdef LOG_ON
	cout << "incoming actual records = " << givenRec << endl;
#endif

	for(j= 0;j <= sInfoRun.pageIndex && iRecord < sInfoRun.numRecordsInRun; j++) {
		while(sInfoRun.pageArr[j].GetFirst(&temp)) {
			recordBuf[iRecord] = new Record();
			recordBuf[iRecord]->Consume(&temp);
			iRecord++;
		}
	}

#ifdef LOG_ON
	cout << "iRecord = " << iRecord << endl;
	cout << "numRecordsinRun = " << sInfoRun.numRecordsInRun<< endl;
	cout << "j = " << j << endl;
	cout << "pageIndex = " << sInfoRun.pageIndex << endl;
#endif


	//Now sort
	sort(recordBuf, recordBuf + sInfoRun.numRecordsInRun, Comparator(sInfoRun.sortOrder,true));

#ifdef LOG_ON
	ComparisonEngine cEng;
	int k = 0,err = 0,succ = 0;
	Record *last = NULL, *prev = NULL;
	ComparisonEngine ceng;
	while (k < sInfoRun.numRecordsInRun) {
		prev = last;
		last = recordBuf[k];
		if (prev && last) {
			if (ceng.Compare (prev, last, sInfoRun.sortOrder) == 1) {
				err++;
			}
			else {
				succ++;
			}
		}
		k++;
	}
	cout << "ERROR in sorting this run = " << err << endl;
	cout << "SUCC in sorting this run = " << succ << endl;
#endif

	int startPageOffset = (numberOfRuns-1) * sInfoRun.runLength;
	int endPageOffset = numberOfRuns * sInfoRun.runLength - 1;
	int recordsActualsInPage = 0;

	//empty pagearray
	for(j=0; j<= sInfoRun.pageIndex;j++){
		sInfoRun.pageArr[j].EmptyItOut();
	}

	iRecord=0; 
	int iPage=0, val;

	//all the pages in page array
	 while(iRecord < sInfoRun.numRecordsInRun){
		 val = sInfoRun.pageArr[iPage].Append(recordBuf[iRecord]);
		 if(val==1){
			 iRecord++;
		 }
		 else{
			 iPage++;

			 if(iPage > sInfoRun.pageIndex){
				 //Comes here when it misaligns after sorting
#ifdef LOG_ON
				 cout<< "    ======> START : shouldnt have come here" << endl;
				 cout << "    Record index= " << iRecord << endl;
				 cout << "    pageIndex = " << iPage << endl;
				 cout<< "    ======> END : shouldnt have come here" << endl;
#endif
				 while(iRecord < sInfoRun.numRecordsInRun){
					 vRecRemain.push_back(*recordBuf[iRecord]);
					 recRemainingIndex++;
					 iRecord++;
				 }
			 }
		 }
	 }

	 iPage = 0;
	 //Add sorted run to file back so that we can merge later
	 for(iPage=0; iPage<=sInfoRun.pageIndex; iPage++){
		 sInfoRun.pFile->AddPage(&sInfoRun.pageArr[iPage],startPageOffset + iPage);
#ifdef LOG_ON
		 cout << "records in this page = " << sInfoRun.pageArr[iPage].GetNumRecs() << endl;
		 cout << "added page at = " << startPageOffset + iPage <<endl;
#endif
	 }

#ifdef LOG_ON
	cout << "pFile length: " << sInfoRun.pFile->GetLength()-1 << endl;
	cout << "Records added = " << iRecord << endl;
	cout << "Records given = " << sInfoRun.numRecordsInRun << endl;
	cout << "Records in Page = " << recordsActualsInPage << endl;
	cout << "last page offset = " << startPageOffset + j << endl;
	cout << "end offset should be = " << endPageOffset << endl;
	cout << "------------- END of run# " << numberOfRuns <<" -----------" << endl;
#endif

	for(iRecord = 0 ; iRecord < sInfoRun.numRecordsInRun;iRecord++)
		delete recordBuf[iRecord];
	delete[] recordBuf;
}

void BigQ::mergeRuns(const int numberOfRuns, File *pFile, const sSortingArgs *args){

    //if only 1 run then no need to create priority queue
	if(numberOfRuns == 1) {
		Page* p = new Page();
		off_t pageOffset = 0;
		Record r;
        
		for(off_t pageOffset = 0; pageOffset < pFile->GetLength()-1; pageOffset++) {
			pFile->GetPage(p,pageOffset);
			while(p->GetFirst(&r)) {
				args->outPipe->Insert(&r);
			}
		}
		//Close the file
		pFile->Close();
		delete p;
		// finally shut down the out pipe
		args->outPipe->ShutDown();
		return;
	}

#ifdef LOG_ON
		cout << endl << "-----------------------------" << endl;
		cout << "Merging of runs STARTS ..." << endl;
#endif
	Page* pCurPages = new Page[numberOfRuns];
	RecordWrapper* recordWrapper = new RecordWrapper[numberOfRuns];
	// Array that maintains actual page index in physical file for every run
	int runningPageIndex[numberOfRuns];
    
	//STL Priority Queue
	priority_queue<RecordWrapper*,vector<RecordWrapper*>,RecordComparator>  pqRecords(RecordComparator(args->sortOrder,false));
    
	int inserted = 0;
    
	// Get 1st record from every run to create a initial PQ
	for(int i=0; i<numberOfRuns;i++){
		runningPageIndex[i]=i*(args->runLen);
		//Get all first pages
		pFile->GetPage(&pCurPages[i],runningPageIndex[i]);
		//Get records from first pages
		recordWrapper[i].pageIndex = i;
		recordWrapper[i].record = new Record();
		pCurPages[i].GetFirst(recordWrapper[i].record);
		inserted++;
		pqRecords.push(&recordWrapper[i]);
	}
    
	int counter=0;
	RecordWrapper* next;

	// Start removing records from the top of the PQ.
	// Dump sorted data into the out pipe
	// Add next record from corresponding page

	while(pqRecords.size()!=0){

		next = pqRecords.top();
#ifdef LOG_ON
		cout << "Record removed counter = " << counter << endl;
#endif
		args->outPipe->Insert(next->record);

		pqRecords.pop();
		counter++;

		if(pCurPages[next->pageIndex].GetFirst(next->record)!=0){
			inserted++;
			pqRecords.push(next);
		}
		else{
#ifdef LOG_ON
			cout<<"counter number remaining = " << counter << endl;
#endif
			runningPageIndex[next->pageIndex]++;

			if(runningPageIndex[next->pageIndex] < ((next->pageIndex)+1)*(args->runLen) &&
               runningPageIndex[next->pageIndex] < (pFile->GetLength()-1)) {
#ifdef LOG_ON
				cout<<"Getting new page: " << runningPageIndex[next->pageIndex] << " Total length: " << pFile->GetLength() << endl;
#endif
                
				pFile->GetPage(&pCurPages[next->pageIndex],runningPageIndex[next->pageIndex]);
				if(pCurPages[next->pageIndex].GetFirst(next->record)!=0){
					pqRecords.push(next);
					inserted++;
#ifdef LOG_ON
					cout<<"Getting new page: "<<runningPageIndex[next->pageIndex] << " counter = " << counter <<endl;
#endif
				}else{
#ifdef LOG_ON
					cout<<"in else"<<" page no.: "<<runningPageIndex[next->pageIndex]<<endl;
#endif
				}
			}
			else{
#ifdef LOG_ON
				cout<<" page# already done: "<< runningPageIndex[next->pageIndex] << endl;
#endif
                
			}
		}
	}

	delete[] pCurPages;

	for(int k = 0; k < numberOfRuns; k++)
		delete recordWrapper[k].record;
	delete[] recordWrapper;

#ifdef LOG_ON
	cout<< endl << "Priority Queue: inserted records = " << inserted <<endl;
#endif

#ifdef LOG_ON
	cout << "Priority Queue: removed records = " << counter << endl;
	cout << "Merging of runs ENDS ..." << endl;
	cout << "-----------------------------" << endl << endl;
#endif
}
