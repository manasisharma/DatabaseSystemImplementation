#include "RelOp.h"

//SelectFile STARTS
void SelectFile::Run (DBFile &_inputFile, Pipe &_outputPipe, CNF &_selOp, Record &_literal) {
	inFile = &_inputFile;
	outPipe = &_outputPipe;
	selOp = &_selOp;
	literal = &_literal;
	//spawn a worker thread
	pthread_create( &selectFileThread, NULL, &SelectFile::work, this);
}

void* SelectFile::work(void* ptr) {
	SelectFile *theObj = (SelectFile*) ptr;
	DBFile &inFile = *(theObj->inFile);
	Pipe &outPipe = *(theObj->outPipe);
	CNF &selOp = *(theObj->selOp);
	Record &literal = *(theObj->literal);

	int counter = 0;
	Record tempRec;
	inFile.MoveFirst();
	while (inFile.GetNext (tempRec,selOp,literal) == 1) {
		counter += 1;
		outPipe.Insert(&tempRec);
	}
	cout << " selected " << counter << " recs " << endl;

	outPipe.ShutDown();
	// Destroy thread
	pthread_exit(NULL);
}

void SelectFile::WaitUntilDone () {
	pthread_join (selectFileThread, NULL);
}

void SelectFile::Use_n_Pages (int runlen) {
	numPages = runlen;
}
//SelectFile ENDS

//SelectPipe STARTS
void SelectPipe::Run (Pipe &_inputPipe, Pipe &_outputPipe, CNF &_selOp, Record &_literal) {
	inPipe = &_inputPipe;
	outPipe = &_outputPipe;
	selOp = &_selOp;
	literal = &_literal;
	//spawn a worker thread
	pthread_create( &selectPipeThread, NULL, &SelectPipe::work, this);
}

void* SelectPipe::work(void* ptr) {
	SelectPipe *theObj = (SelectPipe*) ptr;
	Pipe &inPipe = *(theObj->inPipe);
	Pipe &outPipe = *(theObj->outPipe);
	CNF &selOp = *(theObj->selOp);
	Record &literal = *(theObj->literal);

	int counter = 0;
	Record tempRec;
	ComparisonEngine cEng;

	while (inPipe.Remove(&tempRec) == 1) {
		if(cEng.Compare(&tempRec,&literal,&selOp) == 1) {
			counter += 1;
			outPipe.Insert(&tempRec);
		}
	}
	cout << " selected " << counter << " recs " << endl;

	inPipe.ShutDown();
	outPipe.ShutDown();
	// Destroy thread
	pthread_exit(NULL);
}

void SelectPipe::WaitUntilDone () {
	pthread_join (selectPipeThread, NULL);
}

void SelectPipe::Use_n_Pages (int runlen) {
	numPages = runlen;
}
//SelectPipe ENDS


//Project STARTS
void Project::Run(Pipe &_inputPipe, Pipe &_outputPipe, int *_keepMe, int _numAttsInput, int _numAttsOutput) {
	inPipe = &_inputPipe;
	outPipe = &_outputPipe;
	keepMe = _keepMe;
	numAttsInput = _numAttsInput;
	numAttsOutput = _numAttsOutput;
	//spawn a worker thread
	pthread_create( &projectThread, NULL, &Project::work, this);
}

void* Project::work(void* ptr) {
	Project *theObj = (Project*) ptr;
	Pipe &inPipe = *(theObj->inPipe);
	Pipe &outPipe = *(theObj->outPipe);
	int *keepMe = theObj->keepMe;
	int numAttsInput = (theObj->numAttsInput);
	int numAttsOutput = (theObj->numAttsOutput);

	Record tempRec;
	int counter = 0;
	while (inPipe.Remove(&tempRec) == 1) {
		//numAttsOutput = count of records required to be kept in output
		//numAttsInput = Number of records currently present in the input
		tempRec.Project(keepMe,numAttsOutput,numAttsInput);
		counter += 1;
		outPipe.Insert(&tempRec);
	}
	cout << " Projected " << counter << " recs " << endl;

	inPipe.ShutDown();
	outPipe.ShutDown();
	// Destroy thread
	pthread_exit(NULL);
}

void Project::WaitUntilDone () {
	pthread_join (projectThread, NULL);
}

void Project::Use_n_Pages (int runlen) {
	numPages = runlen;
}
//Project ENDS

//Duplicate removal STARTS
void DuplicateRemoval::Run(Pipe &_inputPipe, Pipe &_outputPipe, Schema &_schema) {
	inPipe = &_inputPipe;
	outPipe = &_outputPipe;
	pSchema = &_schema;
	//spawn a worker thread
	pthread_create( &drThread, NULL, &DuplicateRemoval::work, this);
}

void* DuplicateRemoval::work(void* ptr) {
	DuplicateRemoval *theObj = (DuplicateRemoval*) ptr;
	Pipe &inPipe = *(theObj->inPipe);
	Pipe &outPipe = *(theObj->outPipe);
	Schema &schemaVar = *(theObj->pSchema);

	OrderMaker sortOrder(&schemaVar);
#ifdef LOG_ON
	cout << "the sort order is:" << endl;
	sortOrder.Print();
#endif
	Pipe sortedPipe(100);

	BigQ biqq(inPipe, sortedPipe, sortOrder, theObj->numPages);

	Record cur, next;
	ComparisonEngine cmp;

	//Cur points to the current record
	if(sortedPipe.Remove(&cur)) {
		while(sortedPipe.Remove(&next)) {
			//because the records are sorted.
			//Compare returns 0 when equal or -1 when less than.
			if(cmp.Compare(&cur, &next, &sortOrder) == -1) {
				outPipe.Insert(&cur);
				cur.Consume(&next);
			}
			//continue till the next distinct record is found.
		}
		//Insert the latest distinct record
		outPipe.Insert(&cur);
	}
	outPipe.ShutDown();
	// Destroy thread
	pthread_exit(NULL);
}

void DuplicateRemoval::WaitUntilDone() {
	pthread_join (drThread, NULL);
}

void DuplicateRemoval::Use_n_Pages (int n) {
	numPages = n;
}



void WriteOut::Run(Pipe &_inputPipe, FILE *_outFile, Schema &_schema) {
	inPipe = &_inputPipe;
	pSchema = &_schema;
	outFile = _outFile;
	//create a worker thread
	pthread_create( &writeOutThread, NULL, &WriteOut::work, this);
}

void* WriteOut::work(void* ptr) {
	WriteOut *theObj = (WriteOut*) ptr;
	Pipe &inPipe = *(theObj->inPipe);
	FILE *outFile = theObj->outFile;
	Schema &schemaVar = *(theObj->pSchema);

	Record tempRec;
	ostringstream os;
	int counter = 0;
	while(inPipe.Remove(&tempRec) == 1) {
		counter+=1;
		tempRec.Print(&schemaVar,os);
		fputs(os.str().c_str(),outFile);
		os.str("");
		os.clear();
	}

#ifdef LOG_ON
	cout << "WriteOut ==> removed records are = " << counter << endl;
#endif
	inPipe.ShutDown();
	// Kill the thread
	pthread_exit(NULL);
}

void WriteOut::WaitUntilDone() {
	pthread_join (writeOutThread, NULL);
}

void WriteOut::Use_n_Pages (int n) {
	numPages = n;
}



void Sum::Run(Pipe &_inputPipe, Pipe &_outputPipe, Function &_computeMe) {
	inPipe = &_inputPipe;
	outPipe = &_outputPipe;
	func = &_computeMe;
	//create a worker thread
	pthread_create( &sumThread, NULL, &Sum::work, this);
}

void* Sum::work(void* ptr) {
	Sum *theObj = (Sum*) ptr;
	Pipe &inPipe = *(theObj->inPipe);
	Pipe &outPipe = *(theObj->outPipe);
	Function &func = *(theObj->func);

	int int_reslt = 0;
	double dbl_reslt = 0;

	Record rec;
	stringstream ss;
	Attribute attr;
	if(func.resultType() == Int) {
		int_reslt = Sum::getIntSum(&inPipe,&func);
		attr.name = "int";
		attr.myType = Int;
		ss << int_reslt << "|";
	}
	else if(func.resultType() == Double) {
		dbl_reslt = Sum::getDblSum(&inPipe,&func);
		attr.name = "double";
		attr.myType = Double;
		cout << "result in format Double is =" << dbl_reslt << endl;
		ss << dbl_reslt << "|";
	}
	else {
		cout << " unknown return type for the function..EXITING" << endl;
		exit(-1);
	}

	// sum complete, extract value from function and put into outpipe.
	Schema recSchema ("out_schema",1,&attr);
	rec.ComposeRecord(&recSchema, ss.str().c_str());
	outPipe.Insert(&rec);

//#ifdef LOG_ON
	cout << "SUM:: SUM = " << int_reslt << "," << dbl_reslt<< endl;
//#endif
	inPipe.ShutDown();
	outPipe.ShutDown();
	// Destroy thread
	pthread_exit(NULL);
}

void Sum::WaitUntilDone() {
	pthread_join (sumThread, NULL);
}

void Sum::Use_n_Pages (int n) {
	numPages = n;
}

int Sum::getIntSum(Pipe* pIn, Function* pFunc) {
	int int_reslt = 0;
	int tempInt = 0;
	double tempDbl = 0.0;

	Record tempRec;
	while(pIn->Remove(&tempRec) == 1) {
		pFunc->Apply(tempRec,tempInt,tempDbl);
		int_reslt += tempInt;
	}
	return int_reslt;
}

double Sum::getDblSum(Pipe* pIn, Function* pFunc) {
	double dbl_reslt = 0;
	int tempInt = 0;
	double tempDbl = 0.0;

	Record tempRec;
	while(pIn->Remove(&tempRec) == 1) {
		pFunc->Apply(tempRec,tempInt,tempDbl);

		dbl_reslt += tempDbl;
	}
	return dbl_reslt;
}



void GroupBy::Run(Pipe &_inputPipe, Pipe &_outputPipe, OrderMaker &_groupAtts, Function &_computeMe) {
	inPipe = &_inputPipe;
	outPipe = &_outputPipe;
	pOrderMaker = &_groupAtts;
	func = &_computeMe;
	//create a worker thread
	pthread_create( &groupThread, NULL, &GroupBy::work, this);
}

void* GroupBy::work(void* ptr) {
	GroupBy *theObj = (GroupBy*) ptr;
	Pipe &inPipe = *(theObj->inPipe);
	Pipe &outPipe = *(theObj->outPipe);
	Function &func = *(theObj->func);
	OrderMaker &om = *(theObj->pOrderMaker);


	Pipe sortedPipe(100);

	BigQ biqq(inPipe, sortedPipe, om, theObj->numPages);

	Record cur, next;
	ComparisonEngine cmp;
	int tempInt = 0;
	double tempDbl = 0.0;

	Type resultType = func.resultType();
	//Current has the current record
	if(sortedPipe.Remove(&cur) == 1) {
		func.Apply(cur,tempInt,tempDbl);
		int sumInt = tempInt;
		double sumDbl = tempDbl;
		while(sortedPipe.Remove(&next) == 1) {
//			cout << "groupby:: removing records from sorted file" << endl;
//			next.Print(&grp_sch);
			//Since the records are sorted
			//Compare will return 0 (if equal) or -1 (less than)
			if(cmp.Compare(&cur, &next, &om) == -1) {
				GroupBy::writeRecordOut(cur,&om,resultType ,sumInt,sumDbl,&outPipe);
				cur.Consume(&next);
				func.Apply(cur,tempInt,tempDbl);
				sumInt = tempInt;
				sumDbl = tempDbl;
			}
			else {
				//If repeating record found - keep adding
				func.Apply(next,tempInt,tempDbl);
				sumInt += tempInt;
				sumDbl += tempDbl;
			}
		}
		//Insert the last distinct record
		GroupBy::writeRecordOut(cur,&om,resultType,sumInt,sumDbl,&outPipe);
	}

	inPipe.ShutDown();
	outPipe.ShutDown();
	// Destroy thread
	pthread_exit(NULL);
}

void GroupBy::WaitUntilDone(){
	pthread_join (groupThread, NULL);
}

void GroupBy::Use_n_Pages(int n){
	numPages = n;
}

void GroupBy::writeRecordOut(Record &cur, OrderMaker* order, Type const retType, int &int_reslt, double &dbl_reslt, Pipe* out) {
	Record sumRec;
	stringstream ss;
	Attribute attr;
	attr.name = "sum";
	if(retType == Int) {
		attr.myType = Int;
		ss << int_reslt << "|";
	}
	else if (retType == Double) {
		attr.myType = Double;
		ss << dbl_reslt << "|";
	}
	Schema sumSchema ("out_schema",1,&attr);
	sumRec.ComposeRecord(&sumSchema, ss.str().c_str());

	//Need to prepend that with cur record, since Sum Record is ready
	int numAttsToKeep = 1 + order->getNumAtts();

	int *attsToKeep = new int[numAttsToKeep];
	int attIdx = 0;
	//for sum
	attsToKeep[0] = 0;
	attIdx++;

	int *orderAtts= order->getWhichAtts();
	for (int i = 0; i < order->getNumAtts(); i++) {
		attsToKeep[attIdx++] = orderAtts[i];
	}

#ifdef LOG_ON
	cout << "printing attrs to keep" << endl;
	for(int i = 0; i < numAttsToKeep;i++) {
		cout << attsToKeep[i] << ",";
	}
	cout << endl;
#endif

	Record newret;
	newret.MergeRecords (&sumRec, &cur, 1, cur.getNumAtts(), attsToKeep, numAttsToKeep, 1);

	delete[] attsToKeep;
	out->Insert(&newret);

}


//Join STARTS
void Join::Run (Pipe &_inputPipeL, Pipe &_inputPipeR, Pipe &_outputPipe, CNF &_selOp, Record &_literal) {
	inPipeL = &_inputPipeL;
	inPipeR = &_inputPipeR;
	outPipe = &_outputPipe;
	pCnf = &_selOp;
	pLit= &_literal;
	//create a worker thread
	pthread_create( &joinThread, NULL, &Join::work, this);
}

void* Join::work(void* ptr){
	Join *theObj = (Join*) ptr;
	Pipe &inPipeL = *(theObj->inPipeL);
	Pipe &inPipeR = *(theObj->inPipeR);
	Pipe &outPipe = *(theObj->outPipe);
	CNF &cnf = *(theObj->pCnf);
	Record &lit = *(theObj->pLit);

	OrderMaker orderLeft, orderRight;

	if (cnf.GetSortOrders(orderLeft, orderRight) != 0)
		//sort merge join is possible
		Join::sortMergeJoin(&inPipeL, &orderLeft, &inPipeR, &orderRight, &outPipe, &cnf, &lit, theObj->numPages);
	else


	inPipeL.ShutDown();
	inPipeR.ShutDown();
	outPipe.ShutDown();
	// kill thread
	pthread_exit(NULL);
}

void Join::sortMergeJoin(Pipe *inPipeL, OrderMaker *orderLeft, Pipe *inPipeR, OrderMaker *orderRight, Pipe *outPipe,
        CNF *sel, Record *lit, int runLen) {
	ComparisonEngine cEng;
	Pipe sortedLeft(100), sortedRight(100);
	BigQ qLeft(*inPipeL, sortedLeft, *orderLeft, runLen), qRight(*inPipeR, sortedRight, *orderRight, runLen);

	Record leftRec,rightRec,tempRec,mergedRec;
	bool leftExist = sortedLeft.Remove(&leftRec);
	bool rightExist = sortedRight.Remove(&rightRec);

	const int leftNumAtts = leftRec.getNumAtts();
	const int rightNumAtts = rightRec.getNumAtts();
	const int numAttsTotal = leftNumAtts + rightNumAtts;

	int *attsToKeep = new int[numAttsTotal];
	int attIdx = 0;
	for (int i = 0; i < leftNumAtts; i++)
		attsToKeep[attIdx++] = i;
	for (int i = 0; i < rightNumAtts; i++)
		attsToKeep[attIdx++] = i;

	vector<Record> vBuffer; //for holding left records
	int result = 0;
	int counter = 0;

	while(leftExist && rightExist) {
		result = cEng.Compare(&leftRec,orderLeft,&rightRec,orderRight);
		//continue to discard left and right records until they are equal
		if (result < 0)
			leftExist = sortedLeft.Remove(&leftRec);
		else if (result>0)
			rightExist = sortedRight.Remove(&rightRec);
		else {
			// since attributes are equal -  can proceed to do the Join
			vBuffer.clear();
			tempRec.Copy(&rightRec);
			uint bufCnt = 0;
			while(leftExist && cEng.Compare(&leftRec,orderLeft,&tempRec,orderRight) == 0) {

				vBuffer.push_back(leftRec);
				bufCnt++;
				leftExist = sortedLeft.Remove(&leftRec);
			}
			while(rightExist && cEng.Compare(&tempRec,&rightRec,orderRight) == 0) {
				for (vector<Record>::iterator it = vBuffer.begin() ; it != vBuffer.end(); it++) {
					if (cEng.Compare(&(*it), &rightRec, lit, sel)) {




						mergedRec.MergeRecords(&(*it),&rightRec,leftNumAtts,rightNumAtts,attsToKeep,numAttsTotal,leftNumAtts);
						outPipe->Insert(&mergedRec);
						counter++;
					}
				}
				rightExist = sortedRight.Remove(&rightRec);
			}
		}
	}
}

void Join::WaitUntilDone () {
	pthread_join (joinThread, NULL);
}

void Join::Use_n_Pages (int n) {
	numPages = n;
}

