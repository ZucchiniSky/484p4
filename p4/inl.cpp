#include "catalog.h"
#include "query.h"
#include "sort.h"
#include "index.h"

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

    Index index;
    HeapFileScan heapFile;
    HeapFileScan indexHeapFile;

    if (attrDesc1.indexed)
    {
        index(attrDesc1->relName, attrDesc1->attrOffset, attrDesc1->attrLen, attrDesc1->attrType, 0, s);
        if (s != OK) return s;
        indexHeapFile(attrDesc1.attrName, s);
        if (s != OK) return s;
        heapFile(attrDesc2.attrName, s);
        if (s != OK) return s;
    } else
    {
        index(attrDesc2->relName, attrDesc2->attrOffset, attrDesc2->attrLen, attrDesc2->attrType, 0, s);
        if (s != OK) return s;
        indexHeapFile(attrDesc2.attrName, s);
        if (s != OK) return s;
        heapFile(attrDesc1.attrName, s);
        if (s != OK) return s;
    }

    int resultAttrCount;
    AttrDesc *resultAttrDesc;

    unordered_map<char*, int> attrMap;

    int size = 0;

    s = parseRelation(result, resultAttrCount, resultAttrDesc, attrMap, size);
    if (s != OK) return s;

    RID firstRID;
    Record firstRecord;

    RID secondRID;
    Record secondRecord;

    s = heapFile.startScan(0, 0, INTEGER, nullptr, NOTSET);
    if (s != OK) return s;

    while ((s = heapFile.scanNext(firstRID, firstRecord)) != FILEEOF)
    {
        if (s != OK) {
            return s;
        }

        char *filter;

        switch(attrDesc1.attrType)
        {
            case INTEGER:
                filter = new char*(sizeof(int));
                memcpy(&filter, (char *) firstRecord.data + attrDesc1.attrOffset, sizeof(int));
                break;
            case DOUBLE:
                filter = new char*(sizeof(double));
                memcpy(&filter, (char *) firstRecord.data + attrDesc1.attrOffset, sizeof(double));
                break;
            case STRING:
                filter = new char*(sizeof(attrDesc1.attrLen));
                memcpy(&filter, (char *) firstRecord.data + attrDesc1.attrOffset, sizeof(attrDesc1.attrLen));
                break;
        }

        s = index.startScan(filter);
        if (s != OK) return s;

        while ((s = index.scanNext(secondRID, secondRecord)) != FILEEOF)
        {
            if (s != OK) {
                return s;
            }

            s = indexHeapFile.getRandomRecord(secondRID, secondRecord);
            if (s != OK) {
                return s;
            }
            void *data = new(size);

            for (int i = 0; i < projCnt; i++)
            {
                AttrDesc currAttr = resultAttrDesc[attrMap[projNames[i].attrName]];
                memcpy(data + currAttr.attrOffset, nextRecord.data, currAttr.attrLen);
            }

            Record record;
            record.data = data;
            record.length = size;

            HeapFile resultFile(result, s);
            if (s != OK) return s;
            RID rid;
            s = resultFile.insertRecord(record, rid);
            if (s != OK) return s;

            /*int comp = matchRec(firstRecord, secondRecord, attrDesc1, attrDesc2);

            bool match = false;

            if (comp == -1)
            {
                match = (op == LT || op == LTE || op == NE);
            } else if (comp == 0)
            {
                match = (op == LTE || op == EQ || op == GTE);
            } else
            {
                match = (op == GT || op == GTE || op == NE);
            }

            if (match)
            {
            }*/
        }

        index.endScan();

        delete filter;
    }

    heapFile.endScan();

    return OK;
}

