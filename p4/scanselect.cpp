#include "catalog.h"
#include "query.h"
#include "index.h"
#include <map>
#include <string.h>

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

    cout << "scan selecting" << endl;

    Status s;

    HeapFileScan *heapFile;

    if (attrDesc != NULL && attrValue != NULL)
    {
        heapFile = new HeapFileScan(attrDesc->relName, attrDesc->attrOffset, attrDesc->attrLen, static_cast<Datatype>(attrDesc->attrType), static_cast<char*>(const_cast<void*>(attrValue)), op, s);
    } else
    {
        heapFile = new HeapFileScan(projNames[0].relName, s);
    }
    if (s != OK) return s;

    cout << "after declaring heapfilescan" << endl;

    int resultAttrCount;
    AttrDesc *resultAttrDesc;

    map<char*, int, Operators::strCmpFunctor> attrMap;

    int size = 0;

    s = parseRelation(result, resultAttrCount, resultAttrDesc, attrMap, size);
    if (s != OK) return s;

    cout << "after parseRelation" << endl;

    RID nextRID;
    Record nextRecord;

    if (attrDesc != NULL)
    {
        char *filter;
        if (attrValue == NULL)
        {
            filter = NULL;
        } else
        {
            filter = (char*) attrValue;
        }
        s = heapFile->startScan(attrDesc->attrOffset, attrDesc->attrLen, (Datatype) attrDesc->attrType, (char*) attrValue, op);
    } else
    {
        s = heapFile->startScan(0, 0, INTEGER, NULL, op);
    }
    if (s != OK) return s;

    cout << "after starting scan" << endl;

    while ((s = heapFile->scanNext(nextRID, nextRecord)) != FILEEOF)
    {
        if (s != OK)
        {
            return s;
        }

        cout << "found a match" << endl;

        char *data = new char[size];

        for (int i = 0; i < projCnt; i++)
        {
            AttrDesc currAttr = resultAttrDesc[attrMap[const_cast<char*>(projNames[i].attrName)]];
            memcpy(data + currAttr.attrOffset, nextRecord.data, currAttr.attrLen);
        }
        cout << "after memcpy" << endl;

        Record record;
        record.data = data;
        record.length = size;

        HeapFile resultFile(result, s);
        if (s != OK) return s;
        RID rid;
        s = resultFile.insertRecord(record, rid);
        if (s != OK) return s;

        delete data;

        cout << "after insert" << endl;
    }

    s = heapFile->endScan();
    if (s != OK) return s;

    cout << "after endscan" << endl;

    return OK;
}
