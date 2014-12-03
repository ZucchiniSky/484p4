#include "catalog.h"
#include "query.h"
#include "index.h"
#include <map>

/*
 * Selects records from the specified relation.
 *
 * Returns:
 * 	OK on success
 * 	an error code otherwise
 */
Status Operators::Select(const string & result,      // name of the output relation
	                 const int projCnt,          // number of attributes in the projection
		         const attrInfo projNames[], // the list of projection attributes
		         const attrInfo *attr,       // attribute used in the selection predicate
		         const Operator op,         // predicate operation
		         const void *attrValue)     // literal value in the predicate
{
    /* Your solution goes here */
    Status s;

    cout << "selecting" << endl;

    int reclen = 0;
    for (int i = 0; i < projCnt; i++)
    {
        reclen += projNames[i].attrLen;
    }

    cout << "after reclen" << endl;

    int relAttrCount;
    AttrDesc *attrs;

    map<char*, int, Operators::strCmpFunctor> attrMap;

    int size;

    cout << "before parseRelation" << endl;

    s = Operators::parseRelation(projNames[0].relName, relAttrCount, attrs, attrMap, size);
    if (s != OK) return s;

    AttrDesc proj[projCnt];
    AttrDesc *targetAttr;

    cout << "after parseRelation" << endl;

    for (int i = 0; i < projCnt; i++)
    {
        if (attrMap.find(const_cast<char*>(projNames[i].attrName)) == attrMap.end())
        {
            cout << "couldn't find attr " << projNames[i].attrName << endl;
            return ATTRNOTFOUND;
        }
        proj[i] = attrs[attrMap[const_cast<char*>(projNames[i].attrName)]];
    }

    cout << "after populating proj" << endl;

    if (attr != NULL)
    {
        if (attrMap.find(const_cast<char*>(attr->attrName)) == attrMap.end())
        {
            return ATTRNOTFOUND;
        }
        targetAttr = &attrs[attrMap[const_cast<char*>(attr->attrName)]];
    }

    cout << "after populating targetAttr" << endl;

    if (attr != NULL && targetAttr->indexed && op == EQ)
    {
        return IndexSelect(result, projCnt, proj, targetAttr, op, attrValue, reclen);
    } else
    {
        return ScanSelect(result, projCnt, proj, targetAttr, op, attrValue, reclen);
    }
}

