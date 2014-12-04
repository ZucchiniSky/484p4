#ifndef QUERY_H
#define QUERY_H

#include "heapfile.h"
#include "index.h"
#include "catalog.h"
#include <map>
#include <vector>
#include <string.h>

//
// Prototypes for query layer functions
//

//
// The class for encapsulating the query operators: selects and joins
// Projections are folded into the selects and joins
//
class Operators
{

public:
  // The select operator
  static Status Select(const string & result,      // Name of the output relation
            	         const int projCnt,          // Number of attributes in the projection
            		       const attrInfo projNames[], // List of projection attributes
            		       const attrInfo *attr,       // Attribute used in the selection predicate 
#ifdef BTREE_INDEX
		       const Operator op, 
		       const void *attrValue,
		       const Operator op2 = NOTSET, 
		       const void *attrValue2 = 0);
#else // hash index only
		       const Operator op,                     // Predicate operator
		       const void *attrValue);                // Literal value in the predicate
#endif // BTREE_INDEX

  // The join operator
  static Status Join(const string& result,           // Name of the output relation 
                     const int projCnt,              // Number of attributes in the projection
                     const attrInfo projNames[],     // List of projection attributes
                     const attrInfo* attr1,          // Left attr in the join predicate
                     const Operator op,              // Predicate operator
                     const attrInfo* attr2);         // Right attr in the join predicate

    struct strCmpFunctor
    {
        bool operator()(char const *first, char const *second)
        {
            return strcmp(first, second) < 0;
        }
    };

    // Fetch the AttrDesc list, attribute count, a map from AttrNames to index in AttrDesc,
    // a list of the attributes that are indexed, and the size of a tuple for a specified relation
    static Status parseRelation(const string& relname, // target relation
            int& attrCount, // number of attributes in relation
            AttrDesc *&attrs, // attr desc list for relation
            map<char*, int, strCmpFunctor> &attrMap, // map from attribute name to index in attrs
            vector<int> &indexedAttrs, // list of attrs that are indexed
            int &size) // size of tuple
    {
        Status status = attrCat->getRelInfo(relname, attrCount, attrs);
        if (status != OK) return status;

        size = 0;

        for (int i = 0; i < attrCount; i++)
        {
            AttrDesc *currAttr = attrs + i;
            if (currAttr->indexed)
            {
                indexedAttrs.push_back(i);
            }
            int currSize = currAttr->attrOffset + currAttr->attrLen;
            if (currSize > size)
            {
                size = currSize;
            }
            attrMap[currAttr->attrName] = i;
        }

        return OK;
    }

    // Fetch the AttrDesc list, attribute count, a map from AttrNames to index in AttrDesc,
    // and the size of a tuple for a specified relation
    static Status parseRelation(const string& relname, // target relation
            int& attrCount, // number of attributes in relation
            AttrDesc *&attrs, // attr desc list for relation
            map<char*, int, strCmpFunctor> &attrMap, // map from attribute name to index in attrs
            int &size) // size of tuple
    {
        Status status = attrCat->getRelInfo(relname, attrCount, attrs);
        if (status != OK) return status;

        size = 0;

        for (int i = 0; i < attrCount; i++)
        {
            AttrDesc *currAttr = attrs + i;
            int currSize = currAttr->attrOffset + currAttr->attrLen;
            if (currSize > size)
            {
                size = currSize;
            }
            attrMap[currAttr->attrName] = i;
        }

        return OK;
    }

    // Finds the size of a tuple in the specified relation
    static Status grabRelationSize(const string& relname, // target relation
            int &size) // size of tuple
    {
        AttrDesc *attrs;
        int attrCount;
        
        Status status = attrCat->getRelInfo(relname, attrCount, attrs);
        if (status != OK) return status;

        size = 0;

        for (int i = 0; i < attrCount; i++)
        {
            AttrDesc *currAttr = attrs + i;
            int currSize = currAttr->attrOffset + currAttr->attrLen;
            if (currSize > size)
            {
                size = currSize;
            }
        }

        return OK;
    }


private: 

   // A simple scan select using a heap file scan
   static Status ScanSelect(const string & result,      // Name of the output relation
	                          const int projCnt,          // Number of attributes in the projection
                            const AttrDesc projNames[], // Projection list (as AttrDesc)
                            const AttrDesc *attrDesc,   // Attribute in the selection predicate
                            const Operator op,          // Predicate operator
                            const void *attrValue,      // Pointer to the literal value in the predicate
                            const int reclen);          // Length of a tuple in the result relation

   // Select using an index
   static Status IndexSelect(const string& result,       // Name of the output relation
  	                         const int projCnt,          // Number of attributes in the projection
                             const AttrDesc projNames[], // Projection list (as AttrDesc)
                             const AttrDesc* attrDesc,   // Attribute in the selection predicate
                             const Operator op,          // Predicate operator
                             const void* attrValue,      // Pointer to the literal value in the predicate
#ifdef BTREE_INDEX
                             const Operator op2,
                             const void *attrValue2,
#endif // BTREE_INDEX
                             const int reclen);          // Length of a tuple in the result relation

   // Function to match two record based on the predicate. Returns 0 if the two attributes 
   // are equal, a negative number if the left (attrDesc1) attribute is less that the right 
   // attribute, otherwise this function returns a positive number.
   static int matchRec(const Record & outerRec,     // Left record
                       const Record & innerRec,     // Right record
                       const AttrDesc & attrDesc1,  // Left attribute in the predicate
                       const AttrDesc & attrDesc2); // Right attribute in the predicate

   // The various join algorithms are declared below.
   // Simple nested loops
   static Status SNL(const string& result,           // Output relation name
	                   const int projCnt,              // Number of attributes in the projection
                     const AttrDesc attrDescArray[], // Projection list (as AttrDesc)
                     const AttrDesc& attrDesc1,      // The left attribute in the join predicate
                     const Operator op,              // Predicate operator
                     const AttrDesc& attrDesc2,      // The left attribute in the join predicate
                     const int reclen);              // The length of a tuple in the result relation

   // indexed nested loops
   static Status INL(const string& result,           // Output relation name
	                   const int projCnt,              // Number of attributes in the projection
                     const AttrDesc attrDescArray[], // Projection list (as AttrDesc)
                     const AttrDesc& attrDesc1,      // The left attribute in the join predicate
                     const Operator op,              // Predicate operator
                     const AttrDesc& attrDesc2,      // The left attribute in the join predicate
                     const int reclen);              // The length of a tuple in the result relation

   // Sort-merge join algorithm
   static Status SMJ(const string& result,           // Output relation name
	                   const int projCnt,              // Number of attributes in the projection
                     const AttrDesc attrDescArray[], // Projection list (as AttrDesc)
                     const AttrDesc& attrDesc1,      // The left attribute in the join predicate
                     const Operator op,              // Predicate operator
                     const AttrDesc& attrDesc2,      // The left attribute in the join predicate
                     const int reclen);              // The length of a tuple in the result relation
};


// The class encapsulating the insert and delete operators
class Updates
{
public:
   // The insert operator
   static Status Insert(const string& relation,     // The relation into which to insert the tuple
                        const int attrCnt,          // Number of attributes in the attrList
		                    const attrInfo attrList[]); // The attribute list (with the attribute name and value)

   // The delete operator
   static Status Delete(const string& relation,     // The relation into which to insert the tuple
            		        const string& attrName,     // The attribute in the predicate
            		        const Operator op,          // The predicate operation
            		        const Datatype type,        // The predicate type
            		        const void* attrValue);     // The literal used in the predicate
}; 



#endif
