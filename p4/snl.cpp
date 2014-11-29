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

    vector<int> indexAttrs;
    unordered_map<char*, int> attrMap;

    int size = 0;

    s = parseRelation(result, resultAttrCount, resultAttrDesc, attrMap, indexAttrs, size);
    if (s != OK) return s;

    RID firstRID;
    Record firstRecord;

    RID secondRID;
    Record secondRecord;

    while ((s = rel1.scanNext(firstRID, firstRecord)) != FILEEOF)
    {
        if (s != OK) {
            return s;
        }

        while ((s = rel2.scanNext(secondRID, secondRecord)) != FILEEOF)
        {
            if (s != OK) {
                return s;
            }

            int comp = matchRec(firstRecord, secondRecord, attrDesc1, attrDesc2);

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

            }
        }
    }

    return OK;
}

