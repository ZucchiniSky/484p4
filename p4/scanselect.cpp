#include "catalog.h"
#include "query.h"
#include "index.h"

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

    vector<int> indexAttrs = new vector<int>();
    unordered_map<char*, int> attrMap = new unordered_map<char*, int>();

    int size = 0;

    for (int i = 0; i < resultAttrCount; i++)
    {
        AttrDesc *currAttr = resultAttrDesc + i;
        if (currAttr->indexed)
        {
            indexAttrs.push_back(i);
        }
        int currSize = currAttr->.attrOffset + currAttr->attrLen;
        if (currSize > size)
        {
            size = currSize;
        }
        attrMap[currAttr->attrName] = i;
    }

    RID nextRID;
    Record nextRecord;

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
    }

    return OK;
}
