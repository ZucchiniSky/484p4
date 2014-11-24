#include "catalog.h"
#include "query.h"
#include "index.h"
#include <unordered_map>

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
    Status s;

    int reclen = 0;
    for (int i = 0; i < projCnt; i++)
    {
        reclen += projNames[i].attrLen;
    }

    int relAttrCount;
    AttrDesc *attrs;
    s = attrCat->getRelInfo(attr->relName, relAttrCount, attrs);

    unordered_map<char*, int> attrMap = new unordered_map<char*, int>();

    for (int i = 0; i < relAttrCount; i++)
    {
        AttrDesc *currAttr = attrs + i;
        int currSize = currAttr->.attrOffset + currAttr->attrLen;
        if (currSize > size)
        {
            size = currSize;
        }
        attrMap[currAttr->attrName] = i;
    }

    AttrDesc proj[projCnt];
    AttrDesc *targetAttr;

    for (int i = 0; i < projCnt; i++)
    {
        proj[i] = attrs[attrMap[projNames[i].attrName]];
        if (proj[i] == nullptr)
        {
            return ATTRNOTFOUND;
        }
    }

    if (attr != nullptr)
    {
        targetAttr = attrs[attrMap[attr->attrName]];
        if (targetAttr == nullptr)
        {
            return ATTRNOTFOUND;
        }
    }

    if (attr != nullptr && targetAttr.indexed && op == EQ)
    {
        return IndexSelect(result, projCnt, proj, targetAttr, op, attrValue, reclen);
    } else
    {
        return ScanSelect(result, projCnt, proj, targetAttr, op, attrValue, reclen);
    }
}

