
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
    MyDB_INRecordPtr mLBRec = getINRecord();
    mLBRec->setKey (lowerBound);
    MyDB_INRecordPtr mUPRec = getINRecord();
    mUPRec->setKey (upperBound);

    function <bool()> lowComparator = buildComparator(mLBRec,lhs);
    function <bool()> highComparator = buildComparator(rhs, mUPRec);

	return make_shared <MyDB_PageListIteratorSelfSortingAlt> (mList, lhs, rhs, myComp, myRecIn, lowComparator, highComparator, true);
}

MyDB_RecordIteratorAltPtr MyDB_BPlusTreeReaderWriter :: getRangeIteratorAlt (MyDB_AttValPtr lowerBound, MyDB_AttValPtr upperBound) {
    vector<MyDB_PageReaderWriter> mList;
    cout<<"11"<<endl;
    discoverPages(rootLocation, mList, lowerBound, upperBound);
    cout<<"22"<<endl;
    cout<<mList.size()<<endl;
    //Build comparator
    MyDB_RecordPtr lhs = getEmptyRecord();
    MyDB_RecordPtr rhs = getEmptyRecord();
    function <bool ()> myComp = buildComparator(lhs,rhs);

    MyDB_RecordPtr myRecIn = getEmptyRecord();

    //the record of lowerBound and upperBound
    MyDB_INRecordPtr mLBRec = getINRecord();
    mLBRec->setKey (lowerBound);
    MyDB_INRecordPtr mUPRec = getINRecord();
    mUPRec->setKey (upperBound);

    function <bool()> lowComparator = buildComparator(myRecIn,mLBRec);
    function <bool()> highComparator = buildComparator(mUPRec, myRecIn);

    return make_shared <MyDB_PageListIteratorSelfSortingAlt> (mList, lhs, rhs, myComp, myRecIn, lowComparator, highComparator, false);
}


bool MyDB_BPlusTreeReaderWriter :: discoverPages (int curNode, vector <MyDB_PageReaderWriter> & mList, MyDB_AttValPtr lowerBound, MyDB_AttValPtr upperBound) {

    MyDB_PageReaderWriter mPageRW = (*this)[curNode];//current page

    if(mPageRW.getType() == MyDB_PageType::RegularPage){//data file
        mList.push_back(mPageRW);
        return true;
    }else{//node
        //comparator
        MyDB_INRecordPtr mLBRec = getINRecord ();
        mLBRec->setKey(lowerBound);
        MyDB_INRecordPtr mUPRec = getINRecord ();
        mUPRec->setKey(upperBound);
        MyDB_INRecordPtr mInRec = getINRecord();

        function <bool()> lowComparator = buildComparator(mInRec,mLBRec);
        function <bool()> highComparator = buildComparator(mUPRec, mInRec);

        MyDB_RecordIteratorAltPtr mRec = mPageRW.getIteratorAlt();
        bool isLeaves = false;
        //iterate the page
        while(mRec->advance()){
            //current (key, pointer) pair in current page
            mRec->getCurrent(mInRec);
            bool high = !highComparator();//mUPRec >= mInRec
            bool low = !lowComparator();//mInRec >= mLBRec

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

    if(rootLocation==-1){//empty table
        //root page
        MyDB_PageReaderWriter mPageRW = (*this)[0]; //page zero already exist. Just use it.
        mPageRW.clear();
        mPageRW.setType(MyDB_PageType::DirectoryPage);
        MyDB_INRecordPtr newRec= getINRecord();
        newRec->setPtr(1);
        mPageRW.append(newRec);
        forMe->setRootLocation(0);
        rootLocation = forMe->getRootLocation();//set rootLocation

        //leaf page
        forMe->setLastPage(1);
        MyDB_PageReaderWriter correspLeave = (*this)[1];
        correspLeave.clear();
        correspLeave.setType(MyDB_PageType::RegularPage);
        correspLeave.append(appendMe);
        //cout<<"empty node"<<endl;
        return;
    }
    //otherwise
    //cout<<"not empty node"<<endl;
    auto newRoot = append(rootLocation, appendMe);
    if(newRoot){//old root split, get new Root
        //new root
        MyDB_PageReaderWriter newRootPage = (*this)[forMe->lastPage() + 1];
        newRootPage.setType(MyDB_PageType::DirectoryPage);
        newRootPage.append(newRoot);

        //inf root point to old root(larger one);
        MyDB_INRecordPtr newInf = getINRecord();
        newInf->setPtr(rootLocation);//old root;
        newRootPage.append(newInf);

        rootLocation = forMe->lastPage();
        forMe->setRootLocation(rootLocation);
    }
}

MyDB_RecordPtr MyDB_BPlusTreeReaderWriter :: append (int whichPage, MyDB_RecordPtr mRec) {
    //if interal node page
    if((*this)[whichPage].getType() == MyDB_PageType::DirectoryPage){
        //get the page
        MyDB_PageReaderWriter mPageRW = (*this)[whichPage];
        MyDB_RecordIteratorAltPtr it = mPageRW.getIteratorAlt();
        MyDB_INRecordPtr tempRec = getINRecord();
        auto comparator = buildComparator(tempRec, mRec);//return true if tempRec < mRec, otherwise return false
        while(it->advance()){
            it->getCurrent(tempRec);//iterate the internal record
            //if mRec <= tempRec
            if(!comparator()) break;
        }

        MyDB_RecordPtr newRec = append(tempRec->getPtr(), mRec);
        if(!newRec) return nullptr;//no split in the page it points to.
        else{
            bool isAppend = mPageRW.append(newRec);
            if(!isAppend){
                return split(mPageRW, newRec);
            }else{//successfully append
                MyDB_INRecordPtr lhs = getINRecord();
                MyDB_INRecordPtr rhs = getINRecord();
                auto tempComparator = buildComparator(lhs, rhs);
                mPageRW.sortInPlace(comparator,lhs,rhs);
                return nullptr;
            }
        }

    }else{//leave page
        MyDB_PageReaderWriter mPageRW = (*this)[whichPage];
        bool isAppend = mPageRW.append(mRec);
        if(isAppend) return nullptr;
        else{//not appended
            return split(mPageRW, mRec);
        }
    }

    // call append(root, rec), traverse until find a leaf node using page type
    //if the helper function indicates that a split has happened 
    // then this method needs to handle this by creating a new root
    // that contains pointers to both the old root and the result of the split.
}

MyDB_RecordPtr MyDB_BPlusTreeReaderWriter :: split (MyDB_PageReaderWriter splitMe, MyDB_RecordPtr addMe) {
    //cout<<"split"<<endl;
    MyDB_RecordIteratorAltPtr itr = splitMe.getIteratorAlt();
    MyDB_INRecordPtr middleRec = getINRecord();
    // create a new page
    int newPage =  this->getTable()->lastPage() + 1;
    forMe->setLastPage(newPage);


    if (splitMe.getType() == RegularPage) {
        // Leaf Page
        // find number of records in this page

        MyDB_RecordPtr lhs = getEmptyRecord();
        MyDB_RecordPtr rhs = getEmptyRecord();
        auto comp = buildComparator(lhs,rhs);
        splitMe.sortInPlace(comp, lhs,rhs);

        MyDB_RecordPtr curr = this->getEmptyRecord();
        // construct comparator
        function<bool()> comparatorLeaf = buildComparator(addMe, curr); //return true is addMe < curr
        // store every record in the old page
        vector<MyDB_RecordPtr> oldRecList;
        bool found = false;
        while (itr->advance()) {
            itr->getCurrent(curr);
            //cout<<curr<<endl;
            if (!found && comparatorLeaf()) {
                // we find andMe's pos
                oldRecList.push_back(addMe);
                found = true;
            }
            oldRecList.push_back(curr);
        }
        if(!found){
            oldRecList.push_back(addMe);
        }
        // [ r1, r2 .... andMe .... rn] n+1 records

        splitMe.clear();
        splitMe.setType(MyDB_PageType::RegularPage);
        (*this)[newPage].clear();
        (*this)[newPage].setType(MyDB_PageType::RegularPage);

        // append first half of records in current page to the new page, second half to the old page
        int midPos = oldRecList.size() / 2;
        for (int i = 1; i <= oldRecList.size(); i++) {
            // now check myPos is in the first half or second half
            if (i <= midPos) {
                (*this)[newPage].append(oldRecList[i-1]);
                if (i == midPos) {
                    middleRec->setKey(getKey(oldRecList[i-1])); //getAtt不知道对不对
                }
            } else {
                splitMe.append(oldRecList[i-1]);
            }
        }
    } else if (splitMe.getType() == DirectoryPage) {
        // Inner Page
        // new page append an empty inner node
        MyDB_INRecordPtr curr = getINRecord();
        //construct a comparatpor
        function<bool()> comparatorIN = buildComparator(addMe, curr);//return true is addMe < curr
        // store every record in the old page
        vector<MyDB_RecordPtr> innerNodes;
        bool found = false;
        while (itr->advance()) {
            itr->getCurrent(curr);
            if(!found && comparatorIN()){
                innerNodes.push_back(addMe);
                found = true;
            }
            innerNodes.push_back(curr);
        }
        if(!found){
            innerNodes.push_back(addMe);
        }
        splitMe.clear();
        splitMe.setType(MyDB_PageType::DirectoryPage);
        (*this)[newPage].clear();
        (*this)[newPage].setType(MyDB_PageType::DirectoryPage);
        // kick the middle one to the parent inner page
        int mid = innerNodes.size() / 2;

        for (int i = 1; i <= innerNodes.size(); i++) {
            if (i < mid) {
                (*this)[newPage].append(innerNodes[i-1]);
            } else if (i == mid) {
                middleRec = static_pointer_cast<MyDB_INRecord>(innerNodes[i-1]);
            } else {
                splitMe.append(innerNodes[i-1]);
            }
        }
        MyDB_INRecordPtr infinity = getINRecord();
        // middle record's child
        infinity->setPtr(middleRec->getPtr());
        //cout<<middleRec->getPtr()<<endl;
        (*this)[newPage].append(infinity);
        // 疑问：正无穷应该不算是一个record
        // done.
    }
    // modify pointers
    middleRec->setPtr(newPage);
    //cout<<"finish split"<<endl;
    // return the IN Record of the new page
	return middleRec;
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
                cout<<endl;
                cout<<endl;//End of this page;
            }else{//leave node
                cout<<"Leaf node(page number: "<<num<<"): ";
                MyDB_RecordIteratorAltPtr it = RW.getIteratorAlt();
                MyDB_RecordPtr mRec = getEmptyRecord();
                while(it->advance()){
                    it->getCurrent(mRec);
                    cout<<mRec<<" - ";
                }
                cout<<endl;
                cout<<endl;//End of this page;
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
