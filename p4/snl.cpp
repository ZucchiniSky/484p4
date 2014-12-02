#include "catalog.h"
#include "query.h"
#include "sort.h"
#include "index.h"

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

    unordered_map<char*, int> attrMap;

    int size = 0;

    s = parseRelation(result, resultAttrCount, resultAttrDesc, attrMap, size);
    if (s != OK) return s;

    RID firstRID;
    Record firstRecord;

    RID secondRID;
    Record secondRecord;

    s = rel1.startScan(0, 0, INTEGER, nullptr, NOTSET);
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

        s = rel2.startScan(attrDesc2.attrOffset, attrDesc2.attrLen, attrDesc2.attrType, filter, op);
        if (s != OK) return s;

        while ((s = rel2.scanNext(secondRID, secondRecord)) != FILEEOF)
        {
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

        rel2.endScan();

        delete filter;

    }

    rel1.endScan();

    return OK;
}

