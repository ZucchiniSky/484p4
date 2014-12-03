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

    int reclen = 0;
    for (int i = 0; i < projCnt; i++)
    {
        reclen += projNames[i].attrLen;
    }

    int relAttrCount;
    AttrDesc *attrs;

    map<char*, int> attrMap;

    int size;

    s = Operators::parseRelation(projNames[0].relName, relAttrCount, attrs, attrMap, size);
    if (s != OK) return s;

    AttrDesc proj[projCnt];
    AttrDesc *targetAttr;

    for (int i = 0; i < projCnt; i++)
    {
        if (attrMap.find(const_cast<char*>(projNames[i].attrName)) == attrMap.end())
        {
            return ATTRNOTFOUND;
        }
        proj[i] = attrs[attrMap[const_cast<char*>(projNames[i].attrName)]];
    }

    if (attr != NULL)
    {
        if (attrMap.find(const_cast<char*>(attr->attrName)) == attrMap.end())
        {
            return ATTRNOTFOUND;
        }
        targetAttr = &attrs[attrMap[const_cast<char*>(attr->attrName)]];
    }

    if (attr != NULL && targetAttr->indexed && op == EQ)
    {
        return IndexSelect(result, projCnt, proj, targetAttr, op, attrValue, reclen);
    } else
    {
        return ScanSelect(result, projCnt, proj, targetAttr, op, attrValue, reclen);
    }
}

