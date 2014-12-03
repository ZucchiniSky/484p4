#include "catalog.h"
#include "query.h"
#include "index.h"
#include <map>

/* 
 * A simple scan select using a heap file scan
 */

Status Operators::ScanSelect(const string& result,       // Name of the output relation
                             const int projCnt,          // Number of attributes in the projection
                             const AttrDesc projNames[], // Projection list (as AttrDesc)
                             const AttrDesc* attrDesc,   // Attribute in the selection predicate
                             const Operator op,          // Predicate operator
                             const void* attrValue,      // Pointer to the literal value in the predicate
                             const int reclen)           // Length of a tuple in the result relation
{
    cout << "Algorithm: File Scan" << endl;

    /* Your solution goes here */

    Status s;

    HeapFileScan heapFile(attrDesc->relName, attrDesc->attrOffset, attrDesc->attrLen, attrDesc->attrType, attrValue, op, s);
    if (s != OK) return s;

    int resultAttrCount;
    AttrDesc *resultAttrDesc;

    s = attrCat->getRelInfo(result, resultAttrCount, resultAttrDesc);
    if (s != OK) return s;

    map<char*, int> attrMap;

    int size = 0;

    s = parseRelation(result, resultAttrCount, resultAttrDesc, attrMap, size);
    if (s != OK) return s;

    RID nextRID;
    Record nextRecord;

    s = heapFile.startScan(0, 0, INTEGER, nullptr, NOTSET);
    if (s != OK) return s;

    while ((s = heapFile.scanNext(nextRID, nextRecord)) != FILEEOF)
    {
        if (s != OK)
        {
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

        delete data;
    }

    heapFile.endScan();

    return OK;
}
