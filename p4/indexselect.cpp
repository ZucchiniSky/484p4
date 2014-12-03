#include "catalog.h"
#include "query.h"
#include "index.h"
#include <map>
#include <string.h>

Status Operators::IndexSelect(const string& result,       // Name of the output relation
                              const int projCnt,          // Number of attributes in the projection
                              const AttrDesc projNames[], // Projection list (as AttrDesc)
                              const AttrDesc* attrDesc,   // Attribute in the selection predicate
                              const Operator op,          // Predicate operator
                              const void* attrValue,      // Pointer to the literal value in the predicate
                              const int reclen)           // Length of a tuple in the output relation
{
    cout << "Algorithm: File Scan" << endl;

    /* Your solution goes here */

    Status s;

    Index index(attrDesc->relName, attrDesc->attrOffset, attrDesc->attrLen, static_cast<Datatype>(attrDesc->attrType), 0, s);
    if (s != OK) return s;

    int resultAttrCount;
    AttrDesc *resultAttrDesc;

    map<char*, int> attrMap;

    int size = 0;

    s = parseRelation(result, resultAttrCount, resultAttrDesc, attrMap, size);
    if (s != OK) return s;

    RID nextRID;
    Record nextRecord;

    HeapFileScan fromFile(attrDesc->relName, s);
    if (s != OK) return s;

    s = index.startScan(attrValue);
    if (s != OK) return s;

    while ((s = index.scanNext(nextRID)) != FILEEOF)
    {
        if (s != OK) return s;

        fromFile.getRandomRecord(nextRID, nextRecord);

        char *data = new char[size];

        for (int i = 0; i < projCnt; i++)
        {
            AttrDesc currAttr = resultAttrDesc[attrMap[const_cast<char*>(projNames[i].attrName)]];
            memcpy(data + currAttr.attrOffset, static_cast<char*>(nextRecord.data) + projNames[i].attrOffset, currAttr.attrLen);
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

    index.endScan();

    return OK;
}

