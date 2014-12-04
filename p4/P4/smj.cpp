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
    // memory available for sorting without thrashing

    int size1 = 0, size2 = 0;

    s = grabTupleSize(attrDesc1.relName, size1);
    s = grabTupleSize(attrDesc2.relName, size2);
    // need to know tuple size

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
    // sort the relations

    Record firstRecord, secondRecord;

    bool first = true;

    HeapFile resultFile(result, s);
    if (s != OK) return s;

    // begin scanning
    while ((s = rel1.next(firstRecord)) != FILEEOF)
    {
        if (s != OK) {
            return s;
        }

        if (!first)
        {
            // if mark is not set yet, it defaults to the end of the file which causes an instant fileeof
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
            // keep scanning until we've passed all the records that could possibly match
            if (match == 0)
            {
                if (!found)
                {
                    found = true;
                    s = rel2.setMark();
                    if (s != OK) return s;
                    // set mark to the first match so that duplicates are handled correctly
                }

                // add tuple
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
            s = rel2.next(secondRecord);
            if (s == FILEEOF)
            {
                break;
            } else if (s != OK) return s;
            // scan next
        }

        if (!found)
        {
            s = rel2.setMark();
            if (s != OK) return s;
            // advance mark if no matches are found
        }

        first = false;

    }

    return OK;
}

