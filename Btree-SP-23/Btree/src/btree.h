/**
 * @author See Contributors.txt for code contributors and overview of BadgerDB.
 *
 * @section LICENSE
 * Copyright (c) 2012 Database Group, Computer Sciences Department, University
 * of Wisconsin-Madison.
 */

#pragma once

#include <limits.h>

#include <algorithm>
#include <iostream>
#include <sstream>
#include <string>
#include <vector>

#include "buffer.h"
#include "file.h"
#include "page.h"
#include "string.h"
#include "types.h"

namespace badgerdb {

const int IDEAL_OCCUPANCY = 1;
const int INVALID_KEY = INT_MIN;
const int INVALID_PAGE = INT_MIN;
const int INVALID_KEY_INDEX = INT_MIN;

/**
 * @brief Datatype enumeration type.
 */
enum Datatype { INTEGER = 0, DOUBLE = 1, STRING = 2 };

/**
 * @brief Scan operations enumeration. Passed to BTreeIndex::startScan() method.
 */
enum Operator {
  LT,  /* Less Than */
  LTE, /* Less Than or Equal to */
  GTE, /* Greater Than or Equal to */
  GT   /* Greater Than */
};

/**
 * @brief Size of String key.
 */
const int STRINGSIZE = 10;

/**
 * @brief Number of key slots in B+Tree leaf for INTEGER key.
 */
//                                                  sibling ptr   len          key
//                                                  rid
const int INTARRAYLEAFSIZE =
    (Page::SIZE - sizeof(PageId) - sizeof(int)) / (sizeof(int) + sizeof(RecordId));

/**
 * @brief Number of key slots in B+Tree leaf for DOUBLE key.
 */
//                                                     sibling ptr len key rid
const int DOUBLEARRAYLEAFSIZE =
    (Page::SIZE - sizeof(PageId) - sizeof(int)) / (sizeof(double) + sizeof(RecordId));

/**
 * @brief Number of key slots in B+Tree leaf for STRING key.
 */
//                                                    sibling ptr len          key
//                                                    rid
const int STRINGARRAYLEAFSIZE =
    (Page::SIZE - sizeof(PageId) - sizeof(int)) / (10 * sizeof(char) + sizeof(RecordId));

/**
 * @brief Number of key slots in B+Tree non-leaf for INTEGER key.
 */
//                                                     level     extra pageNo
//                                                     key       pageNo
const int INTARRAYNONLEAFSIZE = (Page::SIZE - 2*sizeof(int) - sizeof(PageId)) /
                                (sizeof(int) + sizeof(PageId));

/**
 * @brief Number of key slots in B+Tree leaf for DOUBLE key.
 */
//                                                        level        extra
//                                                        pageNo key pageNo   -1
//                                                        due to structure
//                                                        padding
const int DOUBLEARRAYNONLEAFSIZE =
    ((Page::SIZE - 2*sizeof(int) - sizeof(PageId)) /
     (sizeof(double) + sizeof(PageId))) -
    1;

/**
 * @brief Number of key slots in B+Tree leaf for STRING key.
 */
//                                                        level        extra
//                                                        pageNo             key
//                                                        pageNo
const int STRINGARRAYNONLEAFSIZE = (Page::SIZE - 2*sizeof(int) - sizeof(PageId)) /
                                   (10 * sizeof(char) + sizeof(PageId));

/**
 * @brief Structure to store a key-rid pair. It is used to pass the pair to
 * functions that add to or make changes to the leaf node pages of the tree. Is
 * templated for the key member.
 */
template <class T>
class RIDKeyPair {
 public:
  RecordId rid;
  T key;
  void set(RecordId r, T k) {
    rid = r;
    key = k;
  }
};

/**
 * @brief Structure to store a key page pair which is used to pass the key and
 * page to functions that make any modifications to the non leaf pages of the
 * tree.
 */
template <class T>
class PageKeyPair {
 public:
  PageId pageNo;
  T key;
  void set(int p, T k) {
    pageNo = p;
    key = k;
  }
};

/**
 * @brief Overloaded operator to compare the key values of two rid-key pairs
 * and if they are the same compares to see if the first pair has
 * a smaller rid.pageNo value.
 */
template <class T>
bool operator<(const RIDKeyPair<T> &r1, const RIDKeyPair<T> &r2) {
  if (r1.key != r2.key)
    return r1.key < r2.key;
  else
    return r1.rid.page_number < r2.rid.page_number;
}


/**
 * @brief The meta page, which holds metadata for Index file, is always first
 * page of the btree index file and is cast to the following structure to store
 * or retrieve information from it. Contains the relation name for which the
 * index is created, the byte offset of the key value on which the index is
 * made, the type of the key and the page no of the root page. Root page starts
 * as page 2 but since a split can occur at the root the root page may get moved
 * up and get a new page no.
 */
struct IndexMetaInfo {
  /**
   * Name of base relation.
   */
  char relationName[20];

  /**
   * Offset of attribute, over which index is built, inside the record stored in
   * pages.
   */
  int attrByteOffset;

  /**
   * Type of the attribute over which index is built.
   */
  Datatype attrType;

  /**
   * Page number of root page of the B+ Tree inside the file index file.
   */
  PageId rootPageNo;
};

/*
Each node is a page, so once we read the page in we just cast the pointer to the
page to this struct and use it to access the parts These structures basically
are the format in which the information is stored in the pages for the index
file depending on what kind of node they are. The level memeber of each non leaf
structure seen below is set to 1 if the nodes at this level are just above the
leaf nodes. Otherwise set to 0.
*/

/**
 * @brief Structure for all non-leaf nodes when the key is of INTEGER type.
 */
struct NonLeafNodeInt {
  /**
   * Level of the node in the tree.
   */
  int level;

  /**
   * Stores keys.
   */
  int keyArray[INTARRAYNONLEAFSIZE];

  /**
   * Stores page numbers of child pages which themselves are other non-leaf/leaf
   * nodes in the tree.
   */
  PageId pageNoArray[INTARRAYNONLEAFSIZE + 1];

  /**
   * Length of the node (number of keys in the node)
  */
  int len;
};

/**
 * @brief Structure for all non-leaf nodes when the key is of DOUBLE type.
 */
struct NonLeafNodeDouble {
  /**
   * Level of the node in the tree.
   */
  int level;

  /**
   * Stores keys.
   */
  double keyArray[DOUBLEARRAYNONLEAFSIZE];

  /**
   * Stores page numbers of child pages which themselves are other non-leaf/leaf
   * nodes in the tree.
   */
  PageId pageNoArray[DOUBLEARRAYNONLEAFSIZE + 1];

  /**
   * Length of the node (number of keys in the node)
  */
  int len;
};

/**
 * @brief Structure for all non-leaf nodes when the key is of STRING type.
 */
struct NonLeafNodeString {
  /**
   * Level of the node in the tree.
   */
  int level;

  /**
   * Stores keys.
   */
  char keyArray[STRINGARRAYNONLEAFSIZE][STRINGSIZE];

  /**
   * Stores page numbers of child pages which themselves are other non-leaf/leaf
   * nodes in the tree.
   */
  PageId pageNoArray[STRINGARRAYNONLEAFSIZE + 1];

  /**
   * Length of the node (number of keys in the node)
  */
  int len;
};

/**
 * @brief Structure for all leaf nodes when the key is of INTEGER type.
 */
struct LeafNodeInt {
  /**
   * Stores keys.
   */
  int keyArray[INTARRAYLEAFSIZE];

  /**
   * Stores RecordIds.
   */
  RecordId ridArray[INTARRAYLEAFSIZE];

  /**
   * Page number of the leaf on the right side.
   * This linking of leaves allows to easily move from one leaf to the next leaf
   * during index scan.
   */
  PageId rightSibPageNo;

  /**
   * Length of the node (number of keys in the node)
  */
  int len;
};

/**
 * @brief Structure for all leaf nodes when the key is of DOUBLE type.
 */
struct LeafNodeDouble {
  /**
   * Stores keys.
   */
  double keyArray[DOUBLEARRAYLEAFSIZE];

  /**
   * Stores RecordIds.
   */
  RecordId ridArray[DOUBLEARRAYLEAFSIZE];

  /**
   * Page number of the leaf on the right side.
   * This linking of leaves allows to easily move from one leaf to the next leaf
   * during index scan.
   */
  PageId rightSibPageNo;

  /**
   * Length of the node (number of keys in the node)
  */
  int len;
};

/**
 * @brief Structure for all leaf nodes when the key is of STRING type.
 */
struct LeafNodeString {
  /**
   * Stores keys.
   */
  char keyArray[STRINGARRAYLEAFSIZE][STRINGSIZE];

  /**
   * Stores RecordIds.
   */
  RecordId ridArray[STRINGARRAYLEAFSIZE];

  /**
   * Page number of the leaf on the right side.
   * This linking of leaves allows to easily move from one leaf to the next leaf
   * during index scan.
   */
  PageId rightSibPageNo;

  /**
   * Length of the node (number of keys in the node)
  */
  int len;
};

/**
 * @brief BTreeIndex class. It implements a B+ Tree index on a single attribute
 * of a relation. This index supports only one scan at a time.
 */
class BTreeIndex {
 private:
  /**
   * File object for the index file.
   */
  File *file;

  /**
   * Buffer Manager Instance.
   */
  BufMgr *bufMgr;

  /**
   * Page number of meta page.
   */
  PageId headerPageNum;

  /**
   * page number of root page of B+ tree inside index file.
   */
  PageId rootPageNum;

  /**
   * Datatype of attribute over which index is built.
   */
  Datatype attributeType;

  /**
   * Offset of attribute, over which index is built, inside records.
   */
  int attrByteOffset;

  /**
   * Number of keys in leaf node, depending upon the type of key.
   */
  int leafOccupancy;

  /**
   * Number of keys in non-leaf node, depending upon the type of key.
   */
  int nodeOccupancy;

  // MEMBERS SPECIFIC TO SCANNING

  /**
   * True if an index scan has been started.
   */
  bool scanExecuting;

  /**
   * Index of next entry to be scanned in current leaf being scanned.
   */
  int nextEntry;

  /**
   * Page number of current page being scanned.
   */
  PageId currentPageNum;

  /**
   * Current Page being scanned.
   */
  Page *currentPageData;

  /**
   * Low INTEGER value for scan.
   */
  int lowValInt;

  /**
   * Low DOUBLE value for scan.
   */
  double lowValDouble;

  /**
   * Low STRING value for scan.
   */
  std::string lowValString;

  /**
   * High INTEGER value for scan.
   */
  int highValInt;

  /**
   * High DOUBLE value for scan.
   */
  double highValDouble;

  /**
   * High STRING value for scan.
   */
  std::string highValString;

  /**
   * Low Operator. Can only be GT(>) or GTE(>=).
   */
  Operator lowOp;

  /**
   * High Operator. Can only be LT(<) or LTE(<=).
   */
  Operator highOp;

  bool isRootLeaf;

  /**
   * Sets up the leaf node occupancy data member of the class based on the data type
   * @param dataType        Data Type of the attribute
   * */
  void setLeafOccupancy(const Datatype dataType);

  /**
   * Sets up the non leaf node occupancy data member of the class based on the data type
   * @param dataType        Data Type of the attribute
   * */
  void setNodeOccupancy(const Datatype dataType);

 public:
  /**
   * BTreeIndex Constructor.
   * Check to see if the corresponding index file exists. If so, open the file.
   * If not, create it and insert entries for every tuple in the base relation
   * using FileScan class.
   *
   * @param relationName        Name of file.
   * @param outIndexName        Return the name of index file.
   * @param bufMgrIn						Buffer Manager
   * Instance
   * @param attrByteOffset			Offset of attribute, over which
   * index is to be built, in the record
   * @param attrType						Datatype of
   * attribute over which index is built
   * @throws  BadIndexInfoException     If the index file already exists for the
   * corresponding attribute, but values in metapage(relationName, attribute
   * byte offset, attribute type etc.) do not match with values received through
   * constructor parameters.
   */
  BTreeIndex(const std::string &relationName, std::string &outIndexName,
             BufMgr *bufMgrIn, const int attrByteOffset,
             const Datatype attrType);

  /**
   * BTreeIndex Destructor.
   * End any initialized scan, flush index file, after unpinning any pinned
   * pages, from the buffer manager and delete file instance thereby closing the
   * index file. Destructor should not throw any exceptions. All exceptions
   * should be caught in here itself.
   * */
  ~BTreeIndex();

  /**
   * Insert a new entry using the pair <value,rid>.
   * Start from root to recursively find out the leaf to insert the entry in.
   *The insertion may cause splitting of leaf node. This splitting will require
   *addition of new leaf page number entry into the parent non-leaf, which may
   *in-turn get split. This may continue all the way upto the root causing the
   *root to get split. If root gets split, metapage needs to be changed
   *accordingly. Make sure to unpin pages as soon as you can.
   * @param key			Key to insert, pointer to integer/double/char
   *string
   * @param rid			Record ID of a record whose entry is getting
   *inserted into the index.
   **/
  const void insertEntry(const void *key, const RecordId rid);

 void insertRecursive(PageId nodePageNumber, const void *key,
                       const RecordId rid, bool &isSplit, void *splitKey,
                       PageId &splitRightNodePageId);

  void insertNonLeaf(PageId nodePageNumber, int nextPageIndex, void* middleKey,
                     bool &isSplit, void* splitKey, PageId &splitRightNodePageId) {
    // std::cout << "Non leaf insert case" << std::endl;
    // Read current page
    Page *curPage;
    this->bufMgr->readPage(this->file, nodePageNumber, curPage);
    if (this->attributeType == Datatype::INTEGER) {
          // Cast to non leaf node
      NonLeafNodeInt *curNonLeafNode = (NonLeafNodeInt *)curPage;
      // Check if space exists
      if (hasSpaceInNonLeafNode(curNonLeafNode)) {
        // Insert the key and rightPageId
        int keyCopy = *(int*)middleKey;
        insertKeyPageIdToKeyPageIdArray<int>(
            curNonLeafNode->keyArray, curNonLeafNode->pageNoArray,
            curNonLeafNode->len, keyCopy, splitRightNodePageId);
        // Set isSplit to false
        isSplit = false;
        curNonLeafNode->len += 1;
        this->bufMgr->unPinPage(this->file, nodePageNumber, true);
      } else {
        std::cout<<"Non leaf split case"<<std::endl;
        // Split and move up the middleKey
        // nextPageIndex is the index in the pageNoArray whose page was selected
        // while recursively inserting the key Insert the splitRightNodePageId
        // after the nextPageIndex in the pageNoArray
        PageId tempPageNoArray[curNonLeafNode->len + 2];
        int tempKeyArray[curNonLeafNode->len + 1];
        for (int i = curNonLeafNode->len; i >= nextPageIndex + 1; i--) {
          tempPageNoArray[i + 1] = curNonLeafNode->pageNoArray[i];
          tempKeyArray[i] = curNonLeafNode->keyArray[i - 1];
        }
        // Insert splitRightNodePageId at nextPageIndex + 1
        tempPageNoArray[nextPageIndex + 1] = splitRightNodePageId;
        // Insert middleKey at nextPageIndex
        tempKeyArray[nextPageIndex] = *(int*)middleKey;

        // Create new page for the split
        Page *newPage;
        PageId newPageNum;
        bufMgr->allocPage(this->file, newPageNum, newPage);
        // Cast new page to non leaf node int
        NonLeafNodeInt *newNonLeafNodeInt = (NonLeafNodeInt *)newPage;
        newNonLeafNodeInt->len = 0;
        newNonLeafNodeInt->level = curNonLeafNode->level;
        // New key array length - curNonLeafNode->len + 1
        int splitKeyIndex = (curNonLeafNode->len) / 2;
        int newSplitKey = tempKeyArray[splitKeyIndex];
        // Ignore key at splitKeyIndex and move all the keys after that to new
        // node
        for (int i = 0; i < splitKeyIndex; i++) {
          curNonLeafNode->keyArray[i] = tempKeyArray[i];
        }
        curNonLeafNode->len = splitKeyIndex;
        std::cout<<"cur non leaf node with page id "<<nodePageNumber<<" has length "<<curNonLeafNode->len<<std::endl;

        curNonLeafNode->pageNoArray[splitKeyIndex] = splitRightNodePageId;
        // Need to move every page number after index splitIndex+1 to new page
        // node
        for (int i = splitKeyIndex + 1; i < curNonLeafNode->len + 1; i++) {
          newNonLeafNodeInt->keyArray[i - splitKeyIndex - 1] = tempKeyArray[i];
          newNonLeafNodeInt->pageNoArray[i - splitKeyIndex - 1] =
              tempPageNoArray[i];
          newNonLeafNodeInt->len += 1;
        }
        std::cout<<"new non leaf node with page id "<<newPageNum<<" has length "<<newNonLeafNodeInt->len<<std::endl;

        newNonLeafNodeInt->pageNoArray[curNonLeafNode->len - splitKeyIndex] =
            tempPageNoArray[curNonLeafNode->len + 1];

        // Set the splitKey and splitRightNodePageId
        *static_cast<int*>(splitKey) = newSplitKey;
        splitRightNodePageId = newPageNum;

        this->bufMgr->unPinPage(this->file, nodePageNumber, true);
        this->bufMgr->unPinPage(this->file, newPageNum, true);
      }
    } else if (this->attributeType == Datatype::DOUBLE) {
      NonLeafNodeDouble *curNonLeafNode = (NonLeafNodeDouble *)curPage;
      // Check if space exists
      if (hasSpaceInNonLeafNode(curNonLeafNode)) {
        // Insert the key and rightPageId
        double keyCopy = *(double*)middleKey;
        insertKeyPageIdToKeyPageIdArray<double>(
            curNonLeafNode->keyArray, curNonLeafNode->pageNoArray,
            curNonLeafNode->len, keyCopy, splitRightNodePageId);
        // Set isSplit to false
        isSplit = false;
        curNonLeafNode->len += 1;
        this->bufMgr->unPinPage(this->file, nodePageNumber, true);
      } else {
        std::cout<<"Non leaf split case"<<std::endl;
        // Split and move up the middleKey
        // nextPageIndex is the index in the pageNoArray whose page was selected
        // while recursively inserting the key Insert the splitRightNodePageId
        // after the nextPageIndex in the pageNoArray
        PageId tempPageNoArray[curNonLeafNode->len + 2];
        double tempKeyArray[curNonLeafNode->len + 1];
        for (int i = curNonLeafNode->len; i >= nextPageIndex + 1; i--) {
          tempPageNoArray[i + 1] = curNonLeafNode->pageNoArray[i];
          tempKeyArray[i] = curNonLeafNode->keyArray[i - 1];
        }
        // Insert splitRightNodePageId at nextPageIndex + 1
        tempPageNoArray[nextPageIndex + 1] = splitRightNodePageId;
        // Insert middleKey at nextPageIndex
        tempKeyArray[nextPageIndex] = *(double*)middleKey;

        // Create new page for the split
        Page *newPage;
        PageId newPageNum;
        bufMgr->allocPage(this->file, newPageNum, newPage);
        // Cast new page to non leaf node int
        NonLeafNodeDouble *newNonLeafNodeInt = (NonLeafNodeDouble *)newPage;
        newNonLeafNodeInt->len = 0;
        newNonLeafNodeInt->level = curNonLeafNode->level;
        // New key array length - curNonLeafNode->len + 1
        int splitKeyIndex = (curNonLeafNode->len) / 2;
        double newSplitKey = tempKeyArray[splitKeyIndex];
        // Ignore key at splitKeyIndex and move all the keys after that to new
        // node
        for (int i = 0; i < splitKeyIndex; i++) {
          curNonLeafNode->keyArray[i] = tempKeyArray[i];
        }
        curNonLeafNode->len = splitKeyIndex;
        std::cout<<"cur non leaf node with page id "<<nodePageNumber<<" has length "<<curNonLeafNode->len<<std::endl;

        curNonLeafNode->pageNoArray[splitKeyIndex] = splitRightNodePageId;
        // Need to move every page number after index splitIndex+1 to new page
        // node
        for (int i = splitKeyIndex + 1; i < curNonLeafNode->len + 1; i++) {
          newNonLeafNodeInt->keyArray[i - splitKeyIndex - 1] = tempKeyArray[i];
          newNonLeafNodeInt->pageNoArray[i - splitKeyIndex - 1] =
              tempPageNoArray[i];
          newNonLeafNodeInt->len += 1;
        }
        std::cout<<"new non leaf node with page id "<<newPageNum<<" has length "<<newNonLeafNodeInt->len<<std::endl;

        newNonLeafNodeInt->pageNoArray[curNonLeafNode->len - splitKeyIndex] =
            tempPageNoArray[curNonLeafNode->len + 1];

        // Set the splitKey and splitRightNodePageId
        *static_cast<double*>(splitKey) = newSplitKey;
        splitRightNodePageId = newPageNum;

        this->bufMgr->unPinPage(this->file, nodePageNumber, true);
        this->bufMgr->unPinPage(this->file, newPageNum, true);
      }
    } else if (this->attributeType == Datatype::STRING) {
      NonLeafNodeString *curNonLeafNode = (NonLeafNodeString *)curPage;
      // Check if space exists
      if (hasSpaceInNonLeafNode(curNonLeafNode)) {
        // Insert the key and rightPageId
        std::string keyCopy = *(std::string*)middleKey;
        insertKeyPageIdToKeyPageIdArrayForString(
            curNonLeafNode->keyArray, curNonLeafNode->pageNoArray,
            curNonLeafNode->len, keyCopy, splitRightNodePageId);
        // Set isSplit to false
        isSplit = false;
        curNonLeafNode->len += 1;
        this->bufMgr->unPinPage(this->file, nodePageNumber, true);
      } else {
        std::cout<<"Non leaf split case"<<std::endl;
        // Split and move up the middleKey
        // nextPageIndex is the index in the pageNoArray whose page was selected
        // while recursively inserting the key Insert the splitRightNodePageId
        // after the nextPageIndex in the pageNoArray
        PageId tempPageNoArray[curNonLeafNode->len + 2];
        std::string tempKeyArray[curNonLeafNode->len + 1];
        for (int i = curNonLeafNode->len; i >= nextPageIndex + 1; i--) {
          tempPageNoArray[i + 1] = curNonLeafNode->pageNoArray[i];
          tempKeyArray[i] = curNonLeafNode->keyArray[i - 1];
        }
        // Insert splitRightNodePageId at nextPageIndex + 1
        tempPageNoArray[nextPageIndex + 1] = splitRightNodePageId;
        // Insert middleKey at nextPageIndex
        tempKeyArray[nextPageIndex] = *(std::string*)middleKey;

        // Create new page for the split
        Page *newPage;
        PageId newPageNum;
        bufMgr->allocPage(this->file, newPageNum, newPage);
        // Cast new page to non leaf node int
        NonLeafNodeString *newNonLeafNodeString = (NonLeafNodeString *)newPage;
        newNonLeafNodeString->len = 0;
        newNonLeafNodeString->level = curNonLeafNode->level;
        // New key array length - curNonLeafNode->len + 1
        int splitKeyIndex = (curNonLeafNode->len) / 2;
        std::string newSplitKey(tempKeyArray[splitKeyIndex], STRINGSIZE);
        // Ignore key at splitKeyIndex and move all the keys after that to new
        // node
        for (int i = 0; i < splitKeyIndex; i++) {
          strncpy(curNonLeafNode->keyArray[i], tempKeyArray[i].c_str(), STRINGSIZE);
        }
        curNonLeafNode->len = splitKeyIndex;
        std::cout<<"cur non leaf node with page id "<<nodePageNumber<<" has length "<<curNonLeafNode->len<<std::endl;

        curNonLeafNode->pageNoArray[splitKeyIndex] = splitRightNodePageId;
        // Need to move every page number after index splitIndex+1 to new page
        // node
        for (int i = splitKeyIndex + 1; i < curNonLeafNode->len + 1; i++) {
          strncpy(newNonLeafNodeString->keyArray[i - splitKeyIndex - 1] , tempKeyArray[i].c_str(), STRINGSIZE);
          newNonLeafNodeString->pageNoArray[i - splitKeyIndex - 1] =
              tempPageNoArray[i];
          newNonLeafNodeString->len += 1;
        }
        std::cout<<"new non leaf node with page id "<<newPageNum<<" has length "<<newNonLeafNodeString->len<<std::endl;

        newNonLeafNodeString->pageNoArray[curNonLeafNode->len - splitKeyIndex] =
            tempPageNoArray[curNonLeafNode->len + 1];

        // Set the splitKey and splitRightNodePageId
        *static_cast<std::string*>(splitKey) = newSplitKey;
        splitRightNodePageId = newPageNum;

        this->bufMgr->unPinPage(this->file, nodePageNumber, true);
        this->bufMgr->unPinPage(this->file, newPageNum, true);
      }
    }
  }

  void insertLeaf(PageId pageNum, void* key, const RecordId rid,
                  bool &isSplit, void* splitKey, PageId &splitRightNodePageId) {
    // std::cout << "Inserting leaf case" << std::endl;
    // Read current page
    Page *curPage;
    this->bufMgr->readPage(this->file, pageNum, curPage);
    if (this->attributeType == Datatype::INTEGER) {
      // Cast to LeafNode
      LeafNodeInt *curLeafNode = (LeafNodeInt *)curPage;
      // Check the occupancy of the leaf node
      if (hasSpaceInLeafNode(curLeafNode)) {
        // SubCase 1: Non-Split
        // Insert the (key, record)
        insertKeyRidToKeyRidArray<int>(curLeafNode->keyArray,
                                       curLeafNode->ridArray, curLeafNode->len,
                                       *(int*)key, rid);
        curLeafNode->len += 1;
        this->bufMgr->unPinPage(this->file, pageNum, true);
        isSplit = false;
      } else {
        std::cout << "Leaf split case" << std::endl;
        // SubCase 2: Split the leaf-node
        std::vector<RIDKeyPair<int>> ridKeyPairVec;
        // Insert all the key, rid pairs including current key, rid to be
        // inserted
        for (int i = 0; i < curLeafNode->len; i++) {
          RIDKeyPair<int> ridKeyPair;
          const RecordId rid_ = curLeafNode->ridArray[i];
          const int key_ = curLeafNode->keyArray[i];
          ridKeyPair.set(rid_, key_);
          ridKeyPairVec.push_back(ridKeyPair);
        }
        // Insert current key, rid
        RIDKeyPair<int> ridKeyPair;
        ridKeyPair.set(rid, *(int*)key);
        ridKeyPairVec.push_back(ridKeyPair);
        // Sort the vector
        sort(ridKeyPairVec.begin(), ridKeyPairVec.end());
        int middleKeyIndex = ridKeyPairVec.size() / 2;
        int middleKey = ridKeyPairVec[middleKeyIndex].key;

        // Create another page and move half the (key, recordID) to that node
        Page *newPage;
        PageId newPageNum;
        bufMgr->allocPage(this->file, newPageNum, newPage);
        // Move half the (key, recordID) to the new node
        // Cast the page to leaf node
        LeafNodeInt *newPageLeafNode = (LeafNodeInt *)newPage;
        newPageLeafNode->len = 0;
        for (int i = middleKeyIndex; i < ridKeyPairVec.size(); i++) {
          int key_ = ridKeyPairVec[i].key;
          RecordId rid_ = ridKeyPairVec[i].rid;
          insertKeyRidToKeyRidArray<int>(newPageLeafNode->keyArray,
                                         newPageLeafNode->ridArray,
                                         newPageLeafNode->len, key_, rid_);
          newPageLeafNode->len += 1;
        }
        std::cout<<"new leaf node with page id "<<newPageNum<<" has length "<<newPageLeafNode->len<<std::endl;

        curLeafNode->len = 0;
        for (int i = 0; i < middleKeyIndex; i++) {
          int key_ = ridKeyPairVec[i].key;
          RecordId rid_ = ridKeyPairVec[i].rid;
          insertKeyRidToKeyRidArray<int>(curLeafNode->keyArray,
                                         curLeafNode->ridArray,
                                         curLeafNode->len, key_, rid_);
          curLeafNode->len += 1;
        }
        std::cout<<"cur leaf node with page id "<<pageNum<<" has length "<<curLeafNode->len<<std::endl;

        // Set next page id of left leaf node
        newPageLeafNode->rightSibPageNo = curLeafNode->rightSibPageNo;
        curLeafNode->rightSibPageNo = newPageNum;

        // For leaf node, middle key is inserted in the leaf as well as copied
        // to non-leaf
        *static_cast<int*>(splitKey) = middleKey;
        isSplit = true;
        splitRightNodePageId = newPageNum;
        this->bufMgr->unPinPage(this->file, newPageNum, true);
        this->bufMgr->unPinPage(this->file, pageNum, true);
      }
    } else if (this->attributeType == Datatype::DOUBLE) {
      // Cast to LeafNode
      LeafNodeDouble *curLeafNode = (LeafNodeDouble *)curPage;
      // Check the occupancy of the leaf node
      if (hasSpaceInLeafNode(curLeafNode)) {
        // SubCase 1: Non-Split
        // Insert the (key, record)
        insertKeyRidToKeyRidArray<double>(curLeafNode->keyArray,
                                       curLeafNode->ridArray, curLeafNode->len,
                                       *(double*)key, rid);
        curLeafNode->len += 1;
        this->bufMgr->unPinPage(this->file, pageNum, true);
        isSplit = false;
      } else {
        std::cout << "Leaf split case" << std::endl;
        // SubCase 2: Split the leaf-node
        std::vector<RIDKeyPair<double>> ridKeyPairVec;
        // Insert all the key, rid pairs including current key, rid to be
        // inserted
        for (int i = 0; i < curLeafNode->len; i++) {
          RIDKeyPair<double> ridKeyPair;
          const RecordId rid_ = curLeafNode->ridArray[i];
          const double key_ = curLeafNode->keyArray[i];
          ridKeyPair.set(rid_, key_);
          ridKeyPairVec.push_back(ridKeyPair);
        }
        // Insert current key, rid
        RIDKeyPair<double> ridKeyPair;
        ridKeyPair.set(rid, *(double*)key);
        ridKeyPairVec.push_back(ridKeyPair);
        // Sort the vector
        sort(ridKeyPairVec.begin(), ridKeyPairVec.end());
        int middleKeyIndex = ridKeyPairVec.size() / 2;
        double middleKey = ridKeyPairVec[middleKeyIndex].key;

        // Create another page and move half the (key, recordID) to that node
        Page *newPage;
        PageId newPageNum;
        bufMgr->allocPage(this->file, newPageNum, newPage);
        // Move half the (key, recordID) to the new node
        // Cast the page to leaf node
        LeafNodeDouble *newPageLeafNode = (LeafNodeDouble *)newPage;
        newPageLeafNode->len = 0;
        for (int i = middleKeyIndex; i < ridKeyPairVec.size(); i++) {
          double key_ = ridKeyPairVec[i].key;
          RecordId rid_ = ridKeyPairVec[i].rid;
          insertKeyRidToKeyRidArray<double>(newPageLeafNode->keyArray,
                                         newPageLeafNode->ridArray,
                                         newPageLeafNode->len, key_, rid_);
          newPageLeafNode->len += 1;
        }
        std::cout<<"new leaf node with page id "<<newPageNum<<" has length "<<newPageLeafNode->len<<std::endl;

        curLeafNode->len = middleKeyIndex;
        std::cout<<"cur leaf node with page id "<<pageNum<<" has length "<<curLeafNode->len<<std::endl;

        // Set next page id of left leaf node
        newPageLeafNode->rightSibPageNo = curLeafNode->rightSibPageNo;
        curLeafNode->rightSibPageNo = newPageNum;

        // For leaf node, middle key is inserted in the leaf as well as copied
        // to non-leaf
        *static_cast<double*>(splitKey) = middleKey;
        isSplit = true;
        splitRightNodePageId = newPageNum;
        this->bufMgr->unPinPage(this->file, newPageNum, true);
        this->bufMgr->unPinPage(this->file, pageNum, true);
      }
    } else if (this->attributeType == Datatype::STRING) {
      // Cast to LeafNode
      LeafNodeString *curLeafNode = (LeafNodeString *)curPage;
      // Check the occupancy of the leaf node
      if (hasSpaceInLeafNode(curLeafNode)) {
        // SubCase 1: Non-Split
        // Insert the (key, record)
        insertKeyRidToKeyRidArrayForString(curLeafNode->keyArray,
                                       curLeafNode->ridArray, curLeafNode->len,
                                       *(std::string*)key, rid);
        curLeafNode->len += 1;
        this->bufMgr->unPinPage(this->file, pageNum, true);
        isSplit = false;
      } else {
        std::cout << "Leaf split case" << std::endl;
        // SubCase 2: Split the leaf-node
        std::vector<RIDKeyPair<std::string>> ridKeyPairVec;
        // Insert all the key, rid pairs including current key, rid to be
        // inserted
        for (int i = 0; i < curLeafNode->len; i++) {
          RIDKeyPair<std::string> ridKeyPair;
          const RecordId rid_ = curLeafNode->ridArray[i];
          std::string key_(curLeafNode->keyArray[i], STRINGSIZE);
          ridKeyPair.set(rid_, key_);
          ridKeyPairVec.push_back(ridKeyPair);
        }
        // Insert current key, rid
        RIDKeyPair<std::string> ridKeyPair;
        ridKeyPair.set(rid, *(std::string*)key);
        ridKeyPairVec.push_back(ridKeyPair);
        // Sort the vector
        sort(ridKeyPairVec.begin(), ridKeyPairVec.end());
        int middleKeyIndex = ridKeyPairVec.size() / 2;
        std::string middleKey = ridKeyPairVec[middleKeyIndex].key;

        // Create another page and move half the (key, recordID) to that node
        Page *newPage;
        PageId newPageNum;
        bufMgr->allocPage(this->file, newPageNum, newPage);
        // Move half the (key, recordID) to the new node
        // Cast the page to leaf node
        LeafNodeString *newPageLeafNode = (LeafNodeString *)newPage;
        newPageLeafNode->len = 0;
        for (int i = middleKeyIndex; i < ridKeyPairVec.size(); i++) {
          std::string key_ = ridKeyPairVec[i].key;
          RecordId rid_ = ridKeyPairVec[i].rid;
          insertKeyRidToKeyRidArrayForString(newPageLeafNode->keyArray,
                                         newPageLeafNode->ridArray,
                                         newPageLeafNode->len, key_, rid_);
          newPageLeafNode->len += 1;
        }
        std::cout<<"new leaf node with page id "<<newPageNum<<" has length "<<newPageLeafNode->len<<std::endl;

        curLeafNode->len = middleKeyIndex;
        std::cout<<"cur leaf node with page id "<<pageNum<<" has length "<<curLeafNode->len<<std::endl;

        // Set next page id of left leaf node
        newPageLeafNode->rightSibPageNo = curLeafNode->rightSibPageNo;
        curLeafNode->rightSibPageNo = newPageNum;

        // For leaf node, middle key is inserted in the leaf as well as copied
        // to non-leaf
        *static_cast<std::string*>(splitKey) = middleKey;
        isSplit = true;
        splitRightNodePageId = newPageNum;
        this->bufMgr->unPinPage(this->file, newPageNum, true);
        this->bufMgr->unPinPage(this->file, pageNum, true);
    }
  }
  }

  template <class T>
  void insertKeyRidToKeyRidArray(T keyArray[], RecordId ridArray[], int len,
                                 T key, const RecordId rid) {
    if (len == 0) {
      keyArray[0] = key;
      ridArray[0] = rid;
      return;
    }
    bool foundKeyIndex = false;
    int keyIndex =
        -1;  // keyIndex is the index just before which to insert the given key
    for (int i = 0; i < len; i++) {
      if (keyArray[i] >= key) {
        keyIndex = i;
        foundKeyIndex = true;
        break;
      }
    }
    if (foundKeyIndex) {
      T tempKeyArray[len + 1];
      RecordId tempRidArray[len + 1];
      for (int i = 0; i < len; i++) {
        tempKeyArray[i] = keyArray[i];
        tempRidArray[i] = ridArray[i];
      }
      // Insert key before index keyIndex
      keyArray[keyIndex] = key;
      ridArray[keyIndex] = rid;
      for (int i = keyIndex; i < len; i++) {
        keyArray[i + 1] = tempKeyArray[i];
        ridArray[i + 1] = tempRidArray[i];
      }
    } else {
      // it means key needs to be inserted at the last
      keyArray[len] = key;
      ridArray[len] = rid;
    }
  }

  void insertKeyRidToKeyRidArrayForString(char keyArray[][10], RecordId ridArray[], int len,
                                 std::string key, const RecordId rid) {
    if (len == 0) {
      strncpy(keyArray[0], key.c_str(), STRINGSIZE);
      ridArray[0] = rid;
      return;
    }
    bool foundKeyIndex = false;
    int keyIndex =
        -1;  // keyIndex is the index just before which to insert the given key
    for (int i = 0; i < len; i++) {
      if (strncmp(keyArray[i], key.c_str(), STRINGSIZE) >= 0) {
        keyIndex = i;
        foundKeyIndex = true;
        break;
      }
    }
    if (foundKeyIndex) {
      char tempKeyArray[len + 1][10];
      RecordId tempRidArray[len + 1];
      for (int i = 0; i < len; i++) {
        strncpy(tempKeyArray[i], keyArray[i], STRINGSIZE);
        tempRidArray[i] = ridArray[i];
      }
      // Insert key before index keyIndex
      strncpy(keyArray[keyIndex], key.c_str(), STRINGSIZE);
      ridArray[keyIndex] = rid;
      for (int i = keyIndex; i < len; i++) {
        strncpy(keyArray[i+1], tempKeyArray[i], STRINGSIZE);
        ridArray[i + 1] = tempRidArray[i];
      }
    } else {
      // It means key needs to be inserted at the last
      strncpy(keyArray[len], key.c_str(), 10);
      ridArray[len] = rid;
    }
  }

  template <typename T>
  bool hasSpaceInLeafNode(T *leafNode) {
    return leafNode->len < IDEAL_OCCUPANCY * this->leafOccupancy;
  }

  template <typename T>
  bool hasSpaceInNonLeafNode(T *nonLeafNode) {
    return nonLeafNode->len < IDEAL_OCCUPANCY * this->nodeOccupancy;
  }

  template <class T>
  void insertKeyPageIdToKeyPageIdArray(T keyArray[], PageId pageNoArray[],
                                       int len, T key, PageId pageNo) {
    bool foundKeyIndex = false;
    int keyIndex =
        -1;  // keyIndex is the index just before which to insert the given key
    for (int i = 0; i < len; i++) {
      if (keyArray[i] >= key) {
        keyIndex = i;
        foundKeyIndex = true;
        break;
      }
    }
    if (foundKeyIndex) {
      T tempKeyArray[len + 1];
      PageId tempPageNoArray[len + 2];
      for (int i = 0; i < len; i++) {
        tempKeyArray[i] = keyArray[i];
        tempPageNoArray[i] = pageNoArray[i];
      }
      tempPageNoArray[len] = pageNoArray[len];
      // Insert key before index keyIndex
      keyArray[keyIndex] = key;
      pageNoArray[keyIndex + 1] = pageNo;
      for (int i = keyIndex+1; i <= len; i++) {
        keyArray[i] = tempKeyArray[i-1];
        pageNoArray[i + 1] = tempPageNoArray[i];
      }
    } else {
      // It means key needs to be inserted at the last
      keyArray[len] = key;
      pageNoArray[len + 1] = pageNo;
    }
  }

  void insertKeyPageIdToKeyPageIdArrayForString(char keyArray[][10], PageId pageNoArray[],
                                       int len, std::string key, PageId pageNo) {
    bool foundKeyIndex = false;
    int keyIndex =
        -1;  // keyIndex is the index just before which to insert the given key
    for (int i = 0; i < len; i++) {
      if (strncmp(keyArray[i], key.c_str(), STRINGSIZE) >= 0) {
        keyIndex = i;
        foundKeyIndex = true;
        break;
      }
    }
    if (foundKeyIndex) {
      char tempKeyArray[len + 1][10];
      PageId tempPageNoArray[len + 2];
      for (int i = 0; i < len; i++) {
        strncpy(tempKeyArray[i], keyArray[i], STRINGSIZE);
        tempPageNoArray[i] = pageNoArray[i];
      }
      tempPageNoArray[len] = pageNoArray[len];
      // Insert key before index keyIndex
      strncpy(keyArray[keyIndex], key.c_str(), STRINGSIZE);
      pageNoArray[keyIndex + 1] = pageNo;
      for (int i = keyIndex+1; i <= len; i++) {
        strncpy(keyArray[i], tempKeyArray[i-1], STRINGSIZE);
        pageNoArray[i + 1] = tempPageNoArray[i];
      }
    } else {
      // It means key needs to be inserted at the last
      strncpy(keyArray[len], key.c_str(), STRINGSIZE);
      pageNoArray[len + 1] = pageNo;
    }
  }

  void printBTree();

  /**
   * Begin a filtered scan of the index.  For instance, if the method is called
   * using ("a",GT,"d",LTE) then we should seek all entries with a value
   * greater than "a" and less than or equal to "d".
   * If another scan is already executing, that needs to be ended here.
   * Set up all the variables for scan. Start from root to find out the leaf
   *page that contains the first RecordID that satisfies the scan parameters.
   *Keep that page pinned in the buffer pool.
   * @param lowVal	Low value of range, pointer to integer / double / char
   *string
   * @param lowOp		Low operator (GT/GTE)
   * @param highVal	High value of range, pointer to integer / double / char
   *string
   * @param highOp	High operator (LT/LTE)
   * @throws  BadOpcodesException If lowOp and highOp do not contain one of
   *their their expected values
   * @throws  BadScanrangeException If lowVal > highval
   * @throws  NoSuchKeyFoundException If there is no key in the B+ tree that
   *satisfies the scan criteria.
   **/
  const void startScan(const void *lowVal, const Operator lowOp,
                       const void *highVal, const Operator highOp);

  /**
   * Fetch the record id of the next index entry that matches the scan.
   * Return the next record from current page being scanned. If current page has
   *been scanned to its entirety, move on to the right sibling of current page,
   *if any exists, to start scanning that page. Make sure to unpin any pages
   *that are no longer required.
   * @param outRid	RecordId of next record found that satisfies the scan
   *criteria returned in this
   * @throws ScanNotInitializedException If no scan has been initialized.
   * @throws IndexScanCompletedException If no more records, satisfying the scan
   *criteria, are left to be scanned.
   **/
  const void scanNext(RecordId &outRid);  // returned record id

  /**
   * Terminate the current scan. Unpin any pinned pages. Reset scan specific
   *variables.
   * @throws ScanNotInitializedException If no scan has been initialized.
   **/
  const void endScan();
};  // namespace badgerdb
}