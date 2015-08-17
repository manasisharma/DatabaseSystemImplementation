#include "BigQ.h"
using namespace std;
BigQ :: BigQ (Pipe &in_Pipe, Pipe &out_Pipe, OrderMaker &sort_order, int run_len) {
    //Arguments for the Copy constructor args
    args.outPipe = &out_Pipe;
    args.inPipe = &in_Pipe;
    args.runLen = run_len;
    args.sortOrder = &sort_order;
    pthread_create( &workerThread, NULL, &BigQ::sortInput, (void*)&args);
}

BigQ::~BigQ () {
}

void* BigQ::sortInput (void* ptr) {
    sSortingArgs* args = (sSortingArgs*) ptr;
    File* pFile = new File();
    string strRunsFile = "bigqthread" + static_cast<ostringstream*>( &(ostringstream() << pthread_self()) )->str()
				+ ".bin";
    char* psRunsFile = const_cast<char*> (strRunsFile.c_str());
    pFile->Open(0,psRunsFile);
    Page *pageArr = new Page[args->runLen];
    sInfoToSortRun sInfoRun;
    sInfoRun.numRecordsInRun = 0;
    sInfoRun.pageIndex = 0;
    sInfoRun.runLength = args->runLen;
    sInfoRun.pFile = pFile;
    sInfoRun.pageArr = pageArr;
    sInfoRun.sortOrder = args->sortOrder;
    int recRemaining = 0;
    int numberOfRuns = 0;
    vector<Record> vRecRemain;
    Record in;
    while(args->inPipe->Remove(&in)){
        if(pageArr[sInfoRun.pageIndex].Append(&in) == 1) {
            sInfoRun.numRecordsInRun++;
        }
        else {
            if(sInfoRun.pageIndex + 1 != args->runLen) {
                sInfoRun.pageIndex++;
#ifdef LOG_ON
                cout << "Creating a new page = " << sInfoRun.pageIndex <<endl;
#endif
                pageArr[sInfoRun.pageIndex].Append(&in);
                sInfoRun.numRecordsInRun++;
            }
            else {
                
                numberOfRuns++;
#ifdef LOG_ON
                cout << "Else number of runs = " << numberOfRuns << endl;
                cout<< "Number of records in run = " << sInfoRun.numRecordsInRun << endl;
                cout<< "Index of Page = " << sInfoRun.pageIndex << endl;
                
#endif
                BigQ::sortAndDumpRun(sInfoRun,numberOfRuns,vRecRemain,recRemaining);
                for(int m=0;m < args->runLen;m++){
                    pageArr[m].EmptyItOut();
                }
                sInfoRun.numRecordsInRun = 0;
                sInfoRun.pageIndex = 0;
                pageArr[sInfoRun.pageIndex].Append(&in);
                sInfoRun.numRecordsInRun++;
            }
        }
    }
    
    if(sInfoRun.numRecordsInRun > 0 || vRecRemain.size()>0) {
#ifdef LOG_ON
        cout << "Records remaining = " << recRemaining << endl ;
        cout << " Last run : Page Index = " << sInfoRun.pageIndex << " and runlen =" << sInfoRun.runLength << endl;
#endif
        numberOfRuns++;
        for(int i=0;i<recRemaining;i++){
            if(pageArr[sInfoRun.pageIndex].Append(&vRecRemain[i]) == 0) {
                sInfoRun.pageIndex++;
                if(sInfoRun.pageIndex == args->runLen) {
                    cout << "Exiting!" << endl;
                    exit(-1);
                }
#ifdef LOG_ON
                cout << "Index of Page = " << sInfoRun.pageIndex << " and runlen =" << sInfoRun.runLength << endl;
#endif
                pageArr[sInfoRun.pageIndex].Append(&vRecRemain[i]);
            }
            else {
                sInfoRun.numRecordsInRun++;
            }
        }
#ifdef LOG_ON
        cout << "Number of runs = " << numberOfRuns << endl;
        cout<< "Number of records in runs = " << sInfoRun.numRecordsInRun << endl;
#endif
        BigQ::sortAndDumpRun(sInfoRun, numberOfRuns,vRecRemain,recRemaining);
    }
    args->inPipe->ShutDown();
    delete[] pageArr;
    BigQ::mergeRuns(numberOfRuns, pFile, args);
    // using priority queue to merge
    pFile->Close();
    delete pFile;
    args->outPipe->ShutDown();
    sInfoRun.pageArr = NULL;
    sInfoRun.pFile = NULL;
    remove(strRunsFile.c_str());
    pthread_exit(NULL);
}
void BigQ::sortAndDumpRun(sInfoToSortRun& sInfoRun, int& numberOfRuns, vector<Record>& vRecRemain,int& recRemainingIndex) {
    uint i_Record = 0,p = 0;
    Record temp;
    Record** recordBuf = new Record*[sInfoRun.numRecordsInRun];
#ifdef LOG_ON
    cout << "Start of run number " << numberOfRuns << endl;
#endif
    int given_Rec = 0;
    for(p = 0; p<= sInfoRun.pageIndex;p++) {
        given_Rec += sInfoRun.pageArr[p].GetNumRecs();
    }
#ifdef LOG_ON
    cout << "incoming actual records = " << given_Rec << endl;
#endif
    
    for(p= 0;p <= sInfoRun.pageIndex && i_Record < sInfoRun.numRecordsInRun; p++) {
        while(sInfoRun.pageArr[p].GetFirst(&temp)) {
            recordBuf[i_Record] = new Record();
            recordBuf[i_Record]->Consume(&temp);
            i_Record++;
        }
    }
    
#ifdef LOG_ON
    cout << "i_Record = " << i_Record << endl;
    cout << "numRecordsinRun = " << sInfoRun.numRecordsInRun<< endl;
    cout << "p = " << p << endl;
    cout << "pageIndex = " << sInfoRun.pageIndex << endl;
#endif
    sort(recordBuf, recordBuf + sInfoRun.numRecordsInRun, Comparator(sInfoRun.sortOrder,true));
#ifdef LOG_ON
    ComparisonEngine cEng;
    int k = 0,error = 0,success = 0;
    Record *last = NULL, *prev = NULL;
    ComparisonEngine ceng;
    while (k < sInfoRun.numRecordsInRun) {
        prev = last;
        last = recordBuf[k];
        if (prev && last) {
            if (ceng.Compare (prev, last, sInfoRun.sortOrder) == 1) {
                error++;
            }
            else {
                success++;
            }
        }
        k++;
    }
    cout << "Error in sorting this run = " << error << endl;
    cout << "Successful in sorting this run = " << success << endl;
#endif
    int startPageOffset = (numberOfRuns-1) * sInfoRun.runLength;
    int endPageOffset = numberOfRuns * sInfoRun.runLength - 1;
    int recordsActualsInPage = 0;
    for(p=0; p<= sInfoRun.pageIndex;p++){
        sInfoRun.pageArr[p].EmptyItOut();
    }
    i_Record=0;
    int iPage=0, val;
    while(i_Record < sInfoRun.numRecordsInRun){
        val = sInfoRun.pageArr[iPage].Append(recordBuf[i_Record]);
        if(val==1){
            i_Record++;
        }
        else{
            iPage++;
            if(iPage > sInfoRun.pageIndex){
#ifdef LOG_ON
                cout<< "Shouldn't have come here --> Start" << endl;
                cout<< "Index record= " << i_Record << endl;
                cout<< "Page Index = " << iPage << endl;
                cout<< "End" << endl;
#endif
                while(i_Record < sInfoRun.numRecordsInRun){
                    vRecRemain.push_back(*recordBuf[i_Record]);
                    recRemainingIndex++;
                    i_Record++;
                }
            }
        }
    }
    iPage = 0;
    for(iPage=0; iPage<=sInfoRun.pageIndex; iPage++){
        sInfoRun.pFile->AddPage(&sInfoRun.pageArr[iPage],startPageOffset + iPage);
#ifdef LOG_ON
        cout << "Records present in this page = " << sInfoRun.pageArr[iPage].GetNumRecs() << endl;
        cout << "Added the page at = " << startPageOffset + iPage <<endl;
#endif
    }
#ifdef LOG_ON
    cout << "Length of pFile: " << sInfoRun.pFile->GetLength()-1 << endl;
    cout << "Number of records added = " << i_Record << endl;
    cout << "Number of records given = " << sInfoRun.numRecordsInRun << endl;
    cout << "Number of records in Page = " << recordsActualsInPage << endl;
    cout << "last page offset = " << startPageOffset + j << endl;
    cout << "End offset should be = " << endPageOffset << endl;
    cout << "End of run Num " << numberOfRuns << endl;
#endif
    for(i_Record = 0 ; i_Record < sInfoRun.numRecordsInRun;i_Record++)
        delete recordBuf[i_Record];
    delete[] recordBuf;
}
void BigQ::mergeRuns(const int numberOfRuns, File *pFile, const sSortingArgs *args){
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
        pFile->Close();
        delete p;
        args->outPipe->ShutDown();
        return;
    }
#ifdef LOG_ON
    cout << "Starting to merge runs" << endl;
#endif
    Page* pCurPages = new Page[numberOfRuns];
    RecordWrapper* recordWrapper = new RecordWrapper[numberOfRuns];
    int runningPageIndex[numberOfRuns];
    priority_queue<RecordWrapper*,vector<RecordWrapper*>,RecordComparator>  pqRecords(RecordComparator(args->sortOrder,false));
    int inserted = 0;
    for(int m=0; m<numberOfRuns;m++){
        runningPageIndex[m]=m*(args->runLen);
        pFile->GetPage(&pCurPages[m],runningPageIndex[m]);
        recordWrapper[m].pageIndex = m;
        recordWrapper[m].record = new Record();
        pCurPages[m].GetFirst(recordWrapper[m].record);
        inserted++;
        pqRecords.push(&recordWrapper[m]);
    }
    RecordWrapper* next;
    int counter=0;
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
            cout<<"Counter number remaining = " << counter << endl;
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
                    cout<<"Getting a new page: "<<runningPageIndex[next->pageIndex] << " Counter = " << counter <<endl;
#endif
                }else{
#ifdef LOG_ON
                    cout<<" Inside else"<<" Page number :"<<runningPageIndex[next->pageIndex]<<endl;
#endif
                }
            }
            else{
#ifdef LOG_ON
                cout<<"Page number already done: "<< runningPageIndex[next->pageIndex] << endl;
#endif          
            }
        }
    }
    delete[] pCurPages;
    for(int s = 0; s < numberOfRuns; s++)
        delete recordWrapper[s].record;
    delete[] recordWrapper;
#ifdef LOG_ON
    cout<< endl << "Priority Queue: Records inserted = " << inserted <<endl;
#endif
#ifdef LOG_ON
    cout << "Priority Queue: Number of removed records = " << counter << endl;
    cout << "End of Merging of runs" << endl;
#endif
}