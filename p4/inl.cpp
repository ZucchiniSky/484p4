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

    if (attrDesc1.indexed)
    {
        index = new Index(attrDesc1.relName, attrDesc1.attrOffset, attrDesc1.attrLen, attrDesc1.attrType, 0, s);
        if (s != OK) return s;
        indexHeapFile = new HeapFileScan(attrDesc1.attrName, s);
        if (s != OK) return s;
        heapFile = new HeapFileScan(attrDesc2.attrName, s);
        if (s != OK) return s;
    } else
    {
        index = new Index(attrDesc2.relName, attrDesc2.attrOffset, attrDesc2.attrLen, attrDesc2.attrType, 0, s);
        if (s != OK) return s;
        indexHeapFile = new HeapFileScan(attrDesc2.attrName, s);
        if (s != OK) return s;
        heapFile = new HeapFileScan(attrDesc1.attrName, s);
        if (s != OK) return s;
    }

    int resultAttrCount;
    AttrDesc *resultAttrDesc;

    map<char*, int> attrMap;

    int size = 0;

    s = parseRelation(result, resultAttrCount, resultAttrDesc, attrMap, size);
    if (s != OK) return s;

    RID firstRID;
    Record firstRecord;

    RID secondRID;
    Record secondRecord;

    s = heapFile->startScan(0, 0, INTEGER, NULL, NOTSET);
    if (s != OK) return s;

    while ((s = heapFile->scanNext(firstRID, firstRecord)) != FILEEOF)
    {
        if (s != OK) {
            return s;
        }

        char *filter;
        s = parseFilter(attrDesc1.indexed ? attrDesc1 : attrDesc2, filter, firstRecord);
        if (s != OK) return s;

        s = index->startScan(filter);
        if (s != OK) return s;

        while ((s = index->scanNext(secondRID)) != FILEEOF)
        {
            if (s != OK) {
                return s;
            }

            s = indexHeapFile->getRandomRecord(secondRID, secondRecord);
            if (s != OK) {
                return s;
            }

            char *data = new char[size];

            for (int i = 0; i < projCnt; i++)
            {
                AttrDesc currAttr = resultAttrDesc[attrMap[const_cast<char*>(attrDescArray[i].attrName)]];
                if (strcmp(attrDescArray[i].relName, attrDesc1.relName))
                {
                    memcpy(data + currAttr.attrOffset, static_cast<char*>(firstRecord.data) + attrDescArray[i].attrOffset, currAttr.attrLen);
                } else
                {
                    memcpy(data + currAttr.attrOffset, static_cast<char*>(secondRecord.data) + attrDescArray[i].attrOffset, currAttr.attrLen);
                }
            }

            Record record;
            record.data = data;
            record.length = size;

            HeapFile resultFile(result, s);
            if (s != OK) return s;
            RID rid;
            s = resultFile.insertRecord(record, rid);
            if (s != OK) return s;

            delete data;
        }

        s = index->endScan();
        if (s != OK) return s;

        delete filter;
    }

    s = heapFile->endScan();
    if (s != OK) return s;

    delete index;
    delete indexHeapFile;
    delete heapFile;

    return OK;
}

