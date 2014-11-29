#include "catalog.h"
#include "query.h"
#include "index.h"
#include <vector>
#include <unordered_map>

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

    Status status;

    int relAttrCount;
    AttrDesc *attrDesc;

    vector<int> indexAttrs;
    unordered_map<char*, int> attrMap;

    int size = 0;

    status = Operators::parseRelation(relation, relAttrCount, attrDesc, attrMap, indexAttrs, size);
    if (status != OK) return status;

    void *data = new(size);

    for (int i = 0; i < attrCnt; i++)
    {
        AttrDesc currAttr = attrDesc[attrMap[attrList[i].attrName]];
        memcpy(data + currAttr.attrOffset, attrList[i].attrValue, currAttr.attrLen);
    }

    Record record;
    record.data = data;
    record.length = size;

    HeapFile heapFile(relation, status);
    if (status != OK) return status;
    RID rid;
    status = heapFile.insertRecord(record, rid);
    if (status != OK) return status;
    for (int i = 0; i < indexAttrs.size; i++)
    {
        AttrDesc currAttr = attrDesc[indexAttrs.at(i)];
        Index index(relation, currAttr.attrOffset, currAttr.attrLen, currAttr.attrType, 0, status);
        if (status != OK) return status;
        s = index.insertEntry(record.data + currAttr.attrOffset, rid);
        if (status != OK) return status;
    }

    return OK;
}
