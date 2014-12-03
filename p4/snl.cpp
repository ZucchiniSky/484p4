#include "catalog.h"
#include "query.h"
#include "sort.h"
#include "index.h"
#include <map>
#include <string.h>

Status Operators::SNL(const string& result,           // Output relation name
                      const int projCnt,              // Number of attributes in the projection
                      const AttrDesc attrDescArray[], // Projection list (as AttrDesc)
                      const AttrDesc& attrDesc1,      // The left attribute in the join predicate
                      const Operator op,              // Predicate operator
                      const AttrDesc& attrDesc2,      // The left attribute in the join predicate
                      const int reclen)               // The length of a tuple in the result relation
{
    cout << "Algorithm: Simple NL Join" << endl;

    /* Your solution goes here */

    Status s;

    HeapFileScan rel1(attrDesc1.attrName, s);
    if (s != OK) return s;
    HeapFileScan rel2(attrDesc2.attrName, s);
    if (s != OK) return s;

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

    s = rel1.startScan(0, 0, INTEGER, NULL, NOTSET);
    if (s != OK) return s;

    while ((s = rel1.scanNext(firstRID, firstRecord)) != FILEEOF)
    {
        if (s != OK) {
            return s;
        }

        char *filter;

        switch(attrDesc1.attrType)
        {
            case INTEGER:
                filter = new char[sizeof(int)];
                memcpy(&filter, (char *) firstRecord.data + attrDesc1.attrOffset, sizeof(int));
                break;
            case DOUBLE:
                filter = new char[sizeof(double)];
                memcpy(&filter, (char *) firstRecord.data + attrDesc1.attrOffset, sizeof(double));
                break;
            case STRING:
                filter = new char[attrDesc1.attrLen];
                memcpy(&filter, (char *) firstRecord.data + attrDesc1.attrOffset, sizeof(attrDesc1.attrLen));
                break;
        }

        s = rel2.startScan(attrDesc2.attrOffset, attrDesc2.attrLen, static_cast<Datatype>(attrDesc2.attrType), filter, op);
        if (s != OK) return s;

        while ((s = rel2.scanNext(secondRID, secondRecord)) != FILEEOF)
        {
            if (s != OK) {
                return s;
            }

            char *data = new char[size];

            for (int i = 0; i < projCnt; i++)
            {
                AttrDesc currAttr = resultAttrDesc[attrMap[static_cast<char*>(projNames[i].attrName)]];
                memcpy(data + currAttr.attrOffset, static_cast<char*>(nextRecord.data), currAttr.attrLen);
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

        rel2.endScan();

        delete filter;

    }

    rel1.endScan();

    return OK;
}

