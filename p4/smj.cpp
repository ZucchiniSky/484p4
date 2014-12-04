#include "catalog.h"
#include "query.h"
#include "sort.h"
#include "index.h"
#include <map>
#include <string.h>

/* Consider using Operators::matchRec() defined in join.cpp
 * to compare records when joining the relations */
  
Status Operators::SMJ(const string& result,           // Output relation name
                      const int projCnt,              // Number of attributes in the projection
                      const AttrDesc attrDescArray[], // Projection list (as AttrDesc)
                      const AttrDesc& attrDesc1,      // The left attribute in the join predicate
                      const Operator op,              // Predicate operator
                      const AttrDesc& attrDesc2,      // The left attribute in the join predicate
                      const int reclen)               // The length of a tuple in the result relation
{
    cout << "Algorithm: SM Join" << endl;

    /* Your solution goes here */

    Status s;

    int maxPages = bufMgr->numUnpinnedPages() * 4 / 5;

    int availableMemory = maxPages * PAGESIZE;

    int attr1Count, attr2Count;
    AttrDesc *attrs1, *attrs2;

    s = attrCat->getRelInfo(attrDesc1.attrName, attr1Count, attrs1);
    if (s != OK) return s;
    s = attrCat->getRelInfo(attrDesc2.attrName, attr2Count, attrs2);
    if (s != OK) return s;

    int size1 = 0, size2 = 0;

    for (int i = 0; i < attr1Count; i++)
    {
        AttrDesc *currAttr = attrs1 + i;
        int currSize = currAttr->attrOffset + currAttr->attrLen;
        if (currSize > size1)
        {
            size1 = currSize;
        }
    }

    for (int i = 0; i < attr2Count; i++)
    {
        AttrDesc *currAttr = attrs2 + i;
        int currSize = currAttr->attrOffset + currAttr->attrLen;
        if (currSize > size2)
        {
            size2 = currSize;
        }
    }

    int resultAttrCount;
    AttrDesc *resultAttrDesc;

    map<char*, int, Operators::strCmpFunctor> attrMap;

    int size = 0;

    s = parseRelation(result, resultAttrCount, resultAttrDesc, attrMap, size);
    if (s != OK) return s;

    SortedFile rel1(attrDesc1.attrName, attrDesc1.attrOffset, attrDesc1.attrLen, static_cast<Datatype>(attrDesc1.attrType), availableMemory / size1, s);
    if (s != OK) return s;
    SortedFile rel2(attrDesc2.attrName, attrDesc2.attrOffset, attrDesc2.attrLen, static_cast<Datatype>(attrDesc2.attrType), availableMemory / size2, s);
    if (s != OK) return s;

    Record firstRecord, secondRecord;

    HeapFile resultFile(result, s);
    if (s != OK) return s;

    while ((s = rel1.next(firstRecord)) != FILEEOF)
    {
        if (s != OK) {
            return s;
        }

        s = rel2.gotoMark();
        if (s != OK) return s;
        s = rel2.next(secondRecord);
        if (s == FILEEOF) break;
        else if (s != OK) return s;

        bool found = false;
        int match;

        while ((match = matchRec(firstRecord, secondRecord, attrDesc1, attrDesc2)) <= 0)
        {
            if (!found && match == 0)
            {
                found = true;
                s = rel2.setMark();
                if (s != OK) return s;
            }

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
            s = rel2.next(secondRecord);
            if (s == FILEEOF)
            {
                break;
            } else if (s != OK) return s;

            delete data;
        }

        if (!found)
        {
            s = rel2.setMark();
            if (s != OK) return s;
        }

    }

    return OK;
}

