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

    Status s;

    HeapFileScan *heapFile;

    // choose whether to start an unfiltered scan or a filtered scan
    if (attrDesc != NULL && attrValue != NULL)
    {
        heapFile = new HeapFileScan(attrDesc->relName, attrDesc->attrOffset, attrDesc->attrLen, static_cast<Datatype>(attrDesc->attrType), static_cast<char*>(const_cast<void*>(attrValue)), op, s);
    } else
    {
        heapFile = new HeapFileScan(projNames[0].relName, s);
    }
    if (s != OK) return s;

    int resultAttrCount;
    AttrDesc *resultAttrDesc;

    map<char*, int, Operators::strCmpFunctor> attrMap;

    int size = 0;

    s = parseRelation(result, resultAttrCount, resultAttrDesc, attrMap, size);
    if (s != OK) return s;

    RID nextRID;
    Record nextRecord;

    // start a filtered scan if applicable
    if (attrDesc != NULL)
    {
        s = heapFile->startScan(attrDesc->attrOffset, attrDesc->attrLen, (Datatype) attrDesc->attrType, (char*) attrValue, op);
    }
    if (s != OK) return s;

    HeapFile resultFile(result, s);
    if (s != OK) return s;

    while ((s = heapFile->scanNext(nextRID, nextRecord)) != FILEEOF)
    {
        if (s != OK)
        {
            return s;
        }

        // insert matching tuple

        char *data = new char[size];

        for (int i = 0; i < projCnt; i++)
        {
            AttrDesc currAttr = resultAttrDesc[i];
            memcpy(data + currAttr.attrOffset, (char*) nextRecord.data + projNames[i].attrOffset, currAttr.attrLen);
        }

        Record record;
        record.data = data;
        record.length = size;
        RID rid;
        s = resultFile.insertRecord(record, rid);
        if (s != OK) return s;
    }

    s = heapFile->endScan();
    if (s != OK) return s;

    delete heapFile;

    return OK;
}
