#ifndef REL_OP_H
#define REL_OP_H

#include <pthread.h>
#include "Pipe.h"
#include "DBFile.h"
#include "Record.h"
#include "Function.h"

class RelationalOp {
	public:
	// blocks the caller until the particular relational operator 
	// has run to completion
	virtual void WaitUntilDone () = 0;

	// tell us how much internal memory the operation can use
	virtual void Use_n_Pages (int n) = 0;
};

class SelectFile : public RelationalOp { 

	private:
		int numPages;

		DBFile *inFile;
		Pipe *outPipe;
		CNF *selOp;
		Record *literal;

		pthread_t selectFileThread;

		static void* work(void* ptr);

	public:
		SelectFile() : numPages(3),inFile(NULL),outPipe(NULL),selOp(NULL),literal(NULL),selectFileThread(0){}
		void Run (DBFile &inFile, Pipe &outPipe, CNF &selOp, Record &literal);
		void WaitUntilDone ();
		void Use_n_Pages (int n);
};

class SelectPipe : public RelationalOp {
	private:
		int numPages;

		Pipe *inPipe;
		Pipe *outPipe;
		CNF *selOp;
		Record *literal;

		pthread_t selectPipeThread;
		static void* work(void* ptr);

	public:
		SelectPipe() : numPages(3),inPipe(NULL),outPipe(NULL),selOp(NULL),literal(NULL),selectPipeThread(0){}
		void Run (Pipe &inPipe, Pipe &outPipe, CNF &selOp, Record &literal);
		void WaitUntilDone ();
		void Use_n_Pages (int n);
};

class Project : public RelationalOp { 
	private:
		int numPages;

		Pipe *inPipe;
		Pipe *outPipe;
		int* keepMe;
		int numAttsInput;
		int numAttsOutput;

		pthread_t projectThread;
		static void* work(void* ptr);
	public:
		Project() : numPages(3),inPipe(NULL),outPipe(NULL),keepMe(NULL),numAttsInput(0),numAttsOutput(0),projectThread(0) {}
		void Run (Pipe &inPipe, Pipe &outPipe, int *keepMe, int numAttsInput, int numAttsOutput);
		void WaitUntilDone ();
		void Use_n_Pages (int n);
};

class Join : public RelationalOp {
	private:
		int numPages;

		Pipe *inPipeL;
		Pipe *inPipeR;
		Pipe *outPipe;
		CNF *pCnf;
		Record* pLit;

		pthread_t joinThread;
		static void* work(void* ptr);
		static void sortMergeJoin(Pipe*, OrderMaker*, Pipe*, OrderMaker*, Pipe*,CNF*, Record*, int);
	public:
		Join():numPages(3),inPipeL(NULL),inPipeR(NULL),outPipe(NULL),pCnf(NULL),pLit(0),joinThread(0) {}
		void Run (Pipe &inPipeL, Pipe &inPipeR, Pipe &outPipe, CNF &selOp, Record &literal);
		void WaitUntilDone ();
		void Use_n_Pages (int n);
};

class DuplicateRemoval : public RelationalOp {
	private:
		int numPages;

		Pipe *inPipe;
		Pipe *outPipe;
		Schema *pSchema;
		pthread_t drThread;
		static void* work(void* ptr);
	public:
		DuplicateRemoval():numPages(3),inPipe(NULL),outPipe(NULL),pSchema(NULL),drThread(0) {}
		void Run (Pipe &inPipe, Pipe &outPipe, Schema &mySchema);
		void WaitUntilDone ();
		void Use_n_Pages (int n);
};

class Sum : public RelationalOp {
	private:
		int numPages;
		Pipe *inPipe;
		Pipe *outPipe;
		Function *func;
		pthread_t sumThread;
		static void* work(void* ptr);
	public:
		Sum():numPages(3),inPipe(NULL),outPipe(NULL),func(NULL),sumThread(0) {};
		static int getIntSum(Pipe* pIn, Function* pFunc);
		static double getDblSum(Pipe* pIn, Function* pFunc);
		void Run (Pipe &inPipe, Pipe &outPipe, Function &computeMe);
		void WaitUntilDone ();
		void Use_n_Pages (int n);
};

class GroupBy : public RelationalOp {
	private:
		int numPages;
		Pipe *inPipe;
		Pipe *outPipe;
		OrderMaker *pOrderMaker;
		Function *func;
		pthread_t groupThread;
		static void* work(void* ptr);
		static void writeRecordOut(Record &rec, OrderMaker* order, Type const retType, int &resultInt, double &resultDbl,Pipe* out);
	public:
		GroupBy():numPages(3),inPipe(NULL),outPipe(NULL),func(NULL),pOrderMaker(NULL),groupThread(0) {};
		void Run (Pipe &inPipe, Pipe &outPipe, OrderMaker &groupAtts, Function &computeMe);
		void WaitUntilDone ();
		void Use_n_Pages (int n);
};

class WriteOut : public RelationalOp {
	private:
		int numPages;
		Pipe *inPipe;
		Schema *pSchema;
		FILE *outFile;
		pthread_t writeOutThread;
		static void* work(void* ptr);
	public:
		WriteOut() : numPages(3),inPipe(NULL), pSchema(NULL),outFile(NULL),writeOutThread(0) {};
		void Run (Pipe &inPipe, FILE *outFile, Schema &mySchema);
		void WaitUntilDone ();
		void Use_n_Pages (int n);
};

#endif
