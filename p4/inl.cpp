#include "catalog.h"
#include "query.h"
#include "sort.h"
#include "index.h"
#include <map>

/* 
 * Indexed nested loop evaluates joins with an index on the 
 * inner/right relation (attrDesc2)
 */

Status Operators::INL(const string& result,           // Name of the output relation
                      const int projCnt,              // Number of attributes in the projection
                      const AttrDesc attrDescArray[], // The projection list (as AttrDesc)
                      const AttrDesc& attrDesc1,      // The left attribute in the join predicate
                      const Operator op,              // Predicate operator
                      const AttrDesc& attrDesc2,      // The left attribute in the join predicate
                      const int reclen)               // Length of a tuple in the output relation
{
  cout << "Algorithm: Indexed NL Join" << endl;

    /* Your solution goes here */

    Status s;

    Index *index;
    HeapFileScan *heapFile;
    HeapFileScan *indexHeapFile;

    bool indexedOnOne;

    // choose correct indexed attribute
    if (attrDesc1.indexed)
    {
        index = new Index(attrDesc1.relName, attrDesc1.attrOffset, attrDesc1.attrLen, static_cast<Datatype>(attrDesc1.attrType), 0, s);
        if (s != OK) return s;
        indexHeapFile = new HeapFileScan(attrDesc1.relName, s);
        if (s != OK) return s;
        heapFile = new HeapFileScan(attrDesc2.relName, s);
        if (s != OK) return s;
        indexedOnOne = true;
    } else
    {
        index = new Index(attrDesc2.relName, attrDesc2.attrOffset, attrDesc2.attrLen, static_cast<Datatype>(attrDesc2.attrType), 0, s);
        if (s != OK) return s;
        indexHeapFile = new HeapFileScan(attrDesc2.relName, s);
        if (s != OK) return s;
        heapFile = new HeapFileScan(attrDesc1.relName, s);
        if (s != OK) return s;
        indexedOnOne = false;
    }

    int resultAttrCount;
    AttrDesc *resultAttrDesc;

    map<char*, int, Operators::strCmpFunctor> attrMap;

    int size = 0;

    s = parseRelation(result, resultAttrCount, resultAttrDesc, attrMap, size);
    if (s != OK) return s;

    RID firstRID;
    Record firstRecord;

    RID secondRID;
    Record secondRecord;

    s = heapFile->startScan(0, 0, INTEGER, NULL, NOTSET);
    if (s != OK) return s;
    // begin unfiltered scan

    HeapFile resultFile(result, s);
    if (s != OK) return s;

    while ((s = heapFile->scanNext(firstRID, firstRecord)) != FILEEOF)
    {
        if (s != OK) {
            return s;
        }

        s = index->startScan((char*)firstRecord.data + (indexedOnOne ? attrDesc2.attrOffset : attrDesc1.attrOffset));
        if (s != OK) return s;
        // begin index scan filtered by the current outer record

        while ((s = index->scanNext(secondRID)) != NOMORERECS)
        {
            if (s != OK) {
                return s;
            }

            s = indexHeapFile->getRandomRecord(secondRID, secondRecord);
            if (s != OK) {
                return s;
            }

            // insert matching record

            char *data = new char[size];

            for (int i = 0; i < projCnt; i++)
            {
                AttrDesc currAttr = resultAttrDesc[i];
                if (strcmp(attrDescArray[i].relName, attrDesc1.relName))
                {
                    memcpy(data + currAttr.attrOffset, static_cast<char*>(indexedOnOne ? secondRecord.data : firstRecord.data) + attrDescArray[i].attrOffset, currAttr.attrLen);
                } else
                {
                    memcpy(data + currAttr.attrOffset, static_cast<char*>(indexedOnOne ? firstRecord.data : secondRecord.data) + attrDescArray[i].attrOffset, currAttr.attrLen);
                }
            }

            Record record;
            record.data = data;
            record.length = size;
            RID rid;
            s = resultFile.insertRecord(record, rid);
            if (s != OK) return s;
        }

        s = index->endScan();
        if (s != OK) return s;
    }

    s = heapFile->endScan();
    if (s != OK) return s;

    delete index;
    delete indexHeapFile;
    delete heapFile;

    return OK;
}

