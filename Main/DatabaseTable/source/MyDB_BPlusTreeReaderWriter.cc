
#ifndef BPLUS_C
#define BPLUS_C

#include "MyDB_INRecord.h"
#include "MyDB_BPlusTreeReaderWriter.h"
#include "MyDB_PageReaderWriter.h"
#include "MyDB_PageListIteratorSelfSortingAlt.h"
#include "RecordComparator.h"
#include "MyDB_PageListIteratorAlt.h"
#include <queue>

MyDB_BPlusTreeReaderWriter :: MyDB_BPlusTreeReaderWriter (string orderOnAttName, MyDB_TablePtr forMe,
	MyDB_BufferManagerPtr myBuffer) : MyDB_TableReaderWriter (forMe, myBuffer) {

	// find the ordering attribute
	auto res = forMe->getSchema ()->getAttByName (orderOnAttName);

	// remember information about the ordering attribute
	orderingAttType = res.second;
	whichAttIsOrdering = res.first;

	// and the root location
	rootLocation = getTable ()->getRootLocation ();
}

MyDB_RecordIteratorAltPtr MyDB_BPlusTreeReaderWriter :: getSortedRangeIteratorAlt (MyDB_AttValPtr lowerBound, MyDB_AttValPtr upperBound) {
    vector<MyDB_PageReaderWriter> mList;
    discoverPages(rootLocation, mList, lowerBound, upperBound);

    //Build comparator
    MyDB_RecordPtr lhs = getEmptyRecord();
    MyDB_RecordPtr rhs = getEmptyRecord();
    function <bool ()> myComp = buildComparator(lhs,rhs);

    MyDB_RecordPtr myRecIn = getEmptyRecord();

    //the record of lowerBound and upperBound
    MyDB_INRecordPtr mLBRec = make_shared<MyDB_INRecord>(lowerBound);
    MyDB_INRecordPtr mUPRec = make_shared<MyDB_INRecord>(upperBound);

    function <bool()> lowComparatorIn = buildComparator(mLBRec,lhs);
    function <bool()> highComparatorIn = buildComparator(rhs, mUPRec);

	return make_shared <MyDB_PageListIteratorSelfSortingAlt> (mList, lhs, rhs, myComp, myRecIn, lowComparatorIn, highComparatorIn, true);
}

MyDB_RecordIteratorAltPtr MyDB_BPlusTreeReaderWriter :: getRangeIteratorAlt (MyDB_AttValPtr lowerBound, MyDB_AttValPtr upperBound) {
    vector<MyDB_PageReaderWriter> mList;
    discoverPages(rootLocation, mList, lowerBound, upperBound);

    //Build comparator
    MyDB_RecordPtr lhs = getEmptyRecord();
    MyDB_RecordPtr rhs = getEmptyRecord();
    function <bool ()> myComp = buildComparator(lhs,rhs);

    MyDB_RecordPtr myRecIn = getEmptyRecord();

    //the record of lowerBound and upperBound
    MyDB_INRecordPtr mLBRec = make_shared<MyDB_INRecord>(lowerBound);
    MyDB_INRecordPtr mUPRec = make_shared<MyDB_INRecord>(upperBound);

    function <bool()> lowComparatorIn = buildComparator(mLBRec,lhs);
    function <bool()> highComparatorIn = buildComparator(rhs, mUPRec);

    return make_shared <MyDB_PageListIteratorSelfSortingAlt> (mList, lhs, rhs, myComp, myRecIn, lowComparatorIn, highComparatorIn, false);
}


bool MyDB_BPlusTreeReaderWriter :: discoverPages (int curNode, vector <MyDB_PageReaderWriter> & mList, MyDB_AttValPtr lowerBound, MyDB_AttValPtr upperBound) {

    MyDB_PageReaderWriter mPageRW = (*this)[curNode];//current page

    if(mPageRW.getType() == MyDB_PageType::RegularPage){//data file
        mList.push_back(mPageRW);
        return true;
    }else{//node
        //comparator
        MyDB_INRecordPtr mLBRec = make_shared<MyDB_INRecord>(lowerBound);
        MyDB_INRecordPtr mUPRec = make_shared<MyDB_INRecord>(upperBound);
        MyDB_INRecordPtr mInRec = getINRecord();
        function <bool()> lowComparatorIn = buildComparator(mInRec,mLBRec);
        function <bool()> highComparatorIn = buildComparator(mUPRec, mInRec);

        MyDB_RecordIteratorAltPtr mRec = mPageRW.getIteratorAlt();
        bool isLeaves = false;
        //iterate the page
        while(mRec->advance()){
            //current (key, pointer) pair in current page
            mRec->getCurrent(mInRec);
            bool high = !highComparatorIn();//mUPRec >= mInRec
            bool low = !lowComparatorIn();//mInRec >= mLBRec

            if(!high) return false;//out of bound
            if(low){//not out of bound
                if(isLeaves){
                    mList.push_back((*this)[mInRec->getPtr()]);
                }else{
                    isLeaves = discoverPages(mInRec->getPtr(),mList,lowerBound, upperBound);
                }
            }

        }
    }
	return false;
}

void MyDB_BPlusTreeReaderWriter :: append (MyDB_RecordPtr appendMe) {
}

MyDB_RecordPtr MyDB_BPlusTreeReaderWriter :: split (MyDB_PageReaderWriter, MyDB_RecordPtr) {
	return nullptr;
}

MyDB_RecordPtr MyDB_BPlusTreeReaderWriter :: append (int, MyDB_RecordPtr) {
	return nullptr;
}

MyDB_INRecordPtr MyDB_BPlusTreeReaderWriter :: getINRecord () {
	return make_shared <MyDB_INRecord> (orderingAttType->createAttMax ());
}

void MyDB_BPlusTreeReaderWriter :: printTree () {
    //BFS
    queue<int> whichPagesNum;
    queue<MyDB_PageReaderWriter> whichPagesRW;

    whichPagesNum.push(rootLocation);
    whichPagesRW.push((*this)[rootLocation]);

    int layer = 0;
    while(!whichPagesNum.empty()){
        cout<<"Layer "<<layer++<<": ";
        for(int i = whichPagesNum.size(); i>0; i--){

            int num = whichPagesNum.front(); whichPagesNum.pop();
            MyDB_PageReaderWriter RW = whichPagesRW.front(); whichPagesRW.pop();

            if(RW.getType() == MyDB_PageType::DirectoryPage){ //internal node
                cout<<"Internal node(page number: "<<num<<"): ";
                MyDB_RecordIteratorAltPtr it = RW.getIteratorAlt();
                MyDB_INRecordPtr mRec= getINRecord();

                while(it->advance()){
                    it->getCurrent(mRec);
                    cout<< mRec->getKey()->toString() << " | ";
                    whichPagesNum.push(mRec->getPtr());
                    whichPagesRW.push((*this)[mRec->getPtr()]);

                }
                cout<<"      ";//End of this page;
            }else{//leave node
                cout<<"Leaf node(page number: "<<num<<"): ";
                MyDB_RecordIteratorAltPtr it = RW.getIteratorAlt();
                MyDB_RecordPtr mRec = getEmptyRecord();
                while(it->advance()){
                    it->getCurrent(mRec);
                    cout<<mRec<<" | ";
                }
                cout<<"      ";//End of this page;
            }
        }

    }
}

MyDB_AttValPtr MyDB_BPlusTreeReaderWriter :: getKey (MyDB_RecordPtr fromMe) {

	// in this case, got an IN record
	if (fromMe->getSchema () == nullptr) 
		return fromMe->getAtt (0)->getCopy ();

	// in this case, got a data record
	else 
		return fromMe->getAtt (whichAttIsOrdering)->getCopy ();
}

function <bool ()>  MyDB_BPlusTreeReaderWriter :: buildComparator (MyDB_RecordPtr lhs, MyDB_RecordPtr rhs) {

	MyDB_AttValPtr lhAtt, rhAtt;

	// in this case, the LHS is an IN record
	if (lhs->getSchema () == nullptr) {
		lhAtt = lhs->getAtt (0);	

	// here, it is a regular data record
	} else {
		lhAtt = lhs->getAtt (whichAttIsOrdering);
	}

	// in this case, the LHS is an IN record
	if (rhs->getSchema () == nullptr) {
		rhAtt = rhs->getAtt (0);	

	// here, it is a regular data record
	} else {
		rhAtt = rhs->getAtt (whichAttIsOrdering);
	}
	
	// now, build the comparison lambda and return
	if (orderingAttType->promotableToInt ()) {
		return [lhAtt, rhAtt] {return lhAtt->toInt () < rhAtt->toInt ();};
	} else if (orderingAttType->promotableToDouble ()) {
		return [lhAtt, rhAtt] {return lhAtt->toDouble () < rhAtt->toDouble ();};
	} else if (orderingAttType->promotableToString ()) {
		return [lhAtt, rhAtt] {return lhAtt->toString () < rhAtt->toString ();};
	} else {
		cout << "This is bad... cannot do anything with the >.\n";
		exit (1);
	}
}


#endif
