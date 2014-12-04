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

    int availableMemory = bufMgr->numUnpinnedPages() * 4 * PAGESIZE / 5;

    int size1 = 0, size2 = 0;

    s = grabRelationSize(attrDesc1.relName, size1);
    s = grabRelationSize(attrDesc2.relName, size2);

    int resultAttrCount;
    AttrDesc *resultAttrDesc;

    map<char*, int, Operators::strCmpFunctor> attrMap;

    int size = 0;

    s = parseRelation(result, resultAttrCount, resultAttrDesc, attrMap, size);
    if (s != OK) return s;

    SortedFile rel1(attrDesc1.relName, attrDesc1.attrOffset, attrDesc1.attrLen, static_cast<Datatype>(attrDesc1.attrType), availableMemory / size1, s);
    if (s != OK) return s;
    SortedFile rel2(attrDesc2.relName, attrDesc2.attrOffset, attrDesc2.attrLen, static_cast<Datatype>(attrDesc2.attrType), availableMemory / size2, s);
    if (s != OK) return s;

    Record firstRecord, secondRecord;

    bool first = true;

    HeapFile resultFile(result, s);
    if (s != OK) return s;

    while ((s = rel1.next(firstRecord)) != FILEEOF)
    {
        if (s != OK) {
            return s;
        }

        if (!first)
        {
            s = rel2.gotoMark();
        }
        if (s != OK) return s;
        s = rel2.next(secondRecord);
        if (s == FILEEOF) continue;
        else if (s != OK) return s;

        bool found = false;
        int match;

        while ((match = matchRec(firstRecord, secondRecord, attrDesc1, attrDesc2)) >= 0)
        {
            if (match == 0)
            {
                if (!found)
                {
                    found = true;
                    s = rel2.setMark();
                    if (s != OK) return s;
                }

                char *data = new char[size];

                for (int i = 0; i < projCnt; i++)
                {
                    AttrDesc currAttr = resultAttrDesc[i];
                    if (strcmp(currAttr.relName, attrDesc1.relName))
                    {
                        memcpy(data + currAttr.attrOffset, static_cast<char*>(firstRecord.data) + currAttr.attrOffset, currAttr.attrLen);
                    } else
                    {
                        memcpy(data + currAttr.attrOffset, static_cast<char*>(secondRecord.data) + currAttr.attrOffset, currAttr.attrLen);
                    }
                }

                Record record;
                record.data = data;
                record.length = size;
                RID rid;
                s = resultFile.insertRecord(record, rid);
                if (s != OK) return s;
            }
            s = rel2.next(secondRecord);
            if (s == FILEEOF)
            {
                break;
            } else if (s != OK) return s;
        }

        if (!found)
        {
            s = rel2.setMark();
            if (s != OK) return s;
        }

        first = false;

    }

    return OK;
}

