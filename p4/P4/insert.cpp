#include "catalog.h"
#include "query.h"
#include "index.h"
#include <vector>
#include <map>
#include <string.h>

/*
 * Inserts a record into the specified relation
 *
 * Returns:
 * 	OK on success
 * 	an error code otherwise
 */

Status Updates::Insert(const string& relation,      // Name of the relation
                       const int attrCnt,           // Number of attributes specified in INSERT statement
                       const attrInfo attrList[])   // Value of attributes specified in INSERT statement
{
    /* Your solution goes here */

    Status s;

    int relAttrCount;
    AttrDesc *attrDesc;

    vector<int> indexAttrs;
    map<char*, int, Operators::strCmpFunctor> attrMap;

    int size = 0;

    s = Operators::parseRelation(relation, relAttrCount, attrDesc, attrMap, indexAttrs, size);
    if (s != OK) return s;

    if (relAttrCount != attrCnt)
    {
        return ATTRNOTFOUND;
        // this insert does not include every attribute, so it is invalid
    }

    // insert tuple

    char *data = new char[size];

    bool *accessed = new bool[attrCnt];
    for (int i = 0; i < attrCnt; i++)
    {
        accessed[i] = 0;
    }

    for (int i = 0; i < attrCnt; i++)
    {
        int index = attrMap[const_cast<char*>(attrList[i].attrName)];
        if (accessed[index])
        {
            return DUPLATTR;
            // the same attribute is set twice, this is an invalid insert
        }
        accessed[index] = true;
        AttrDesc currAttr = attrDesc[index];
        if ((Datatype) attrList[i].attrType == STRING && strlen((char*) attrList[i].attrValue) > (unsigned int) attrList[i].attrLen)
        {
            return ATTRTOOLONG;
            // an inserted string is longer than the attrLen of the attribute
        }
        if (attrList[i].attrValue == NULL)
        {
            return ATTRNOTFOUND;
            // an attribute inserted is null
        }
        memcpy(data + currAttr.attrOffset, attrList[i].attrValue, currAttr.attrLen);
    }

    Record record;
    record.data = data;
    record.length = size;

    HeapFile heapFile(relation, s);
    if (s != OK) return s;
    RID rid;
    s = heapFile.insertRecord(record, rid);
    if (s != OK) return s;
    for (unsigned int i = 0; i < indexAttrs.size(); i++)
    {
        AttrDesc currAttr = attrDesc[indexAttrs.at(i)];
        Index index(relation, currAttr.attrOffset, currAttr.attrLen, static_cast<Datatype>(currAttr.attrType), 0, s);
        if (s != OK) return s;
        s = index.insertEntry(static_cast<char*>(record.data) + currAttr.attrOffset, rid);
        if (s != OK) return s;
        s = index.endScan();
        if (s != OK) return s;
    }

    return OK;
}
