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

    HeapFileScan rel1(attrDesc2.relName, s);
    if (s != OK) return s;
    HeapFileScan rel2(attrDesc1.relName, s);
    if (s != OK) return s;
    // outer relation is attrDesc2, inner relation is attrDesc1

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

    HeapFile resultFile(result, s);
    if (s != OK) return s;


    // begin outer scan
    while ((s = rel1.scanNext(firstRID, firstRecord)) != FILEEOF)
    {
        if (s != OK) {
            return s;
        }

        s = rel2.startScan(attrDesc2.attrOffset, attrDesc2.attrLen, static_cast<Datatype>(attrDesc2.attrType), (char*)firstRecord.data + attrDesc1.attrOffset, op);
        if (s != OK) return s;
        // begin inner scan

        while ((s = rel2.scanNext(secondRID, secondRecord)) != FILEEOF)
        {
            if (s != OK) {
                return s;
            }

            // insert matching tuple

            char *data = new char[size];

            for (int i = 0; i < projCnt; i++)
            {
                AttrDesc currAttr = resultAttrDesc[i];
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
            RID rid;
            s = resultFile.insertRecord(record, rid);
            if (s != OK) return s;
        }

        s = rel2.endScan();
        if (s != OK) return s;

    }

    s = rel1.endScan();
    if (s != OK) return s;

    return OK;
}

