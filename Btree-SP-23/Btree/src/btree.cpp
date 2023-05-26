/**
 * @author See Contributors.txt for code contributors and overview of BadgerDB.
 *
 * @section LICENSE
 * Copyright (c) 2012 Database Group, Computer Sciences Department, University
 * of Wisconsin-Madison.
 */

#include "btree.h"

#include <limits.h>

#include <algorithm>
#include <deque>
#include <vector>

#include "exceptions/bad_index_info_exception.h"
#include "exceptions/bad_opcodes_exception.h"
#include "exceptions/bad_scanrange_exception.h"
#include "exceptions/end_of_file_exception.h"
#include "exceptions/file_not_found_exception.h"
#include "exceptions/index_scan_completed_exception.h"
#include "exceptions/no_such_key_found_exception.h"
#include "exceptions/scan_not_initialized_exception.h"
#include "filescan.h"

// #define DEBUG

namespace badgerdb {
// -----------------------------------------------------------------------------
// BTreeIndex::BTreeIndex -- Constructor
// -----------------------------------------------------------------------------

const bool fileExists(const std::string &fileName) {
  return File::exists(fileName);
}

/**
 * Sets up the leaf node occupancy data member of the class based on the data type
 * @param attrType        Data Type of the attribute
 * */
void BTreeIndex::setLeafOccupancy(const Datatype attrType) {
  if (attrType == Datatype::INTEGER) {
    this->leafOccupancy = INTARRAYLEAFSIZE;
  } else if (attrType == Datatype::DOUBLE) {
    this->leafOccupancy = DOUBLEARRAYLEAFSIZE;
  } else if (attrType == Datatype::STRING) {
    this->leafOccupancy = STRINGARRAYLEAFSIZE;
  }
}

/**
 * Sets up the non leaf node occupancy data member of the class based on the data type
 * @param attrType        Data Type of the attribute
 * */
void BTreeIndex::setNodeOccupancy(const Datatype attrType) {
  if (attrType == Datatype::INTEGER) {
    this->nodeOccupancy = INTARRAYNONLEAFSIZE;
  } else if (attrType == Datatype::DOUBLE) {
    this->nodeOccupancy = DOUBLEARRAYNONLEAFSIZE;
  } else if (attrType == Datatype::STRING) {
    this->nodeOccupancy = STRINGARRAYNONLEAFSIZE;
  }
}

BTreeIndex::BTreeIndex(const std::string &relationName,
                       std::string &outIndexName, BufMgr *bufMgrIn,
                       const int attrByteOffset, const Datatype attrType) {
  // Create index file name
  std::ostringstream idxStr;
  idxStr << relationName << "." << attrByteOffset;
  outIndexName = idxStr.str();

  // Initialize member variables
  this->attributeType = attrType;
  this->attrByteOffset = attrByteOffset;
  this->setLeafOccupancy(attrType);
  this->setNodeOccupancy(attrType);
  // Meta page is always the first page of the index file
  this->headerPageNum = 1;
  this->isRootLeaf = true;
  this->bufMgr = bufMgrIn;
  this->scanExecuting = false;
  this->nextEntry = INVALID_KEY_INDEX;

  try {
    this->file = new BlobFile(outIndexName, false);
    std::cout<<"Index already exists"<<std::endl;
    PageId metaPageId = this->headerPageNum;
    Page *metaPage;
    // Read the meta page
    this->bufMgr->readPage(this->file, metaPageId, metaPage);
    // Unpin the page since the page won't be used for writing here after
    this->bufMgr->unPinPage(this->file, metaPageId, false);
    // Cast meta page to IndexMetaInfo
    IndexMetaInfo *indexMetaInfo = (IndexMetaInfo *)(metaPage);
    // Since, this is the case where index file already exists
    // read the isRootLeaf attribute value from the meta page and set it
    this->isRootLeaf = indexMetaInfo->isRootLeaf;
    // Set the root page id
    this->rootPageNum = indexMetaInfo->rootPageNo;
    // Values in metapage (relationName, attribute byte offset, attribute type
    // etc.) must match
    // Compare both the length and the characters comparison
    bool relationNameMatch =
        (strlen(indexMetaInfo->relationName) == relationName.size()) &&
        !strcmp(indexMetaInfo->relationName, relationName.c_str());
    bool attributeByteOffsetMatch =
        indexMetaInfo->attrByteOffset == attrByteOffset;
    bool attrTypeMatch = indexMetaInfo->attrType == attrType;
    if (!(relationNameMatch && attributeByteOffsetMatch && attrTypeMatch)) {
      throw BadIndexInfoException(
          "Parameters passed while creating the index don't match");
    }
  } catch (FileNotFoundException e) {
    // Create the blob file for the index
    this->file = new BlobFile(outIndexName, true);
    // Create pages for metadata and root (page 1 and 2 repectively)
    PageId metaPageNo;
    Page *metaPage;
    PageId rootPageNo;
    Page *rootPage;
    // Allocate the page for metapage
    this->bufMgr->allocPage(this->file, metaPageNo, metaPage);
    this->headerPageNum = metaPageNo;
    // Cast the metaPage into the IndexMetaInfo
    IndexMetaInfo *indexMetaInfo = (IndexMetaInfo *)metaPage;
    // Allocate the page for root node
    this->bufMgr->allocPage(this->file, rootPageNo, rootPage);
    this->rootPageNum = rootPageNo;
    // Write meta data to the meta page
    indexMetaInfo->isRootLeaf = this->isRootLeaf;
    indexMetaInfo->attrType = attributeType;
    indexMetaInfo->attrByteOffset = attrByteOffset;
    strcpy((char *)(&indexMetaInfo->relationName), relationName.c_str());
    indexMetaInfo->relationName[relationName.size()] = '\0';
    indexMetaInfo->rootPageNo = rootPageNo;
    // Meta page is modified now, we can unpin this page which will result in
    // disk flushing
    this->bufMgr->unPinPage(this->file, metaPageNo, true);
    // Dont unpin root page since its useful for insert, scans operations.
    // Unpin it in the destructor or when the root node changes
    this->bufMgr->unPinPage(this->file, rootPageNo, true);

    // Set root node members
    // Root page is initially leaf node
    // Cast root page to leaf node
    if (this->attributeType == Datatype::INTEGER) {
      LeafNodeInt *rootLeafNode = (LeafNodeInt *)rootPage;
      rootLeafNode->len = 0;
      rootLeafNode->rightSibPageNo = INVALID_PAGE;
    } else if (this->attributeType == Datatype::DOUBLE) {
      LeafNodeDouble *rootLeafNode = (LeafNodeDouble *)rootPage;
      rootLeafNode->len = 0;
      rootLeafNode->rightSibPageNo = INVALID_PAGE;
    } else if (this->attributeType == Datatype::STRING) {
      LeafNodeString *rootLeafNode = (LeafNodeString *)rootPage;
      rootLeafNode->len = 0;
      rootLeafNode->rightSibPageNo = INVALID_PAGE;
    }

    // Scan the file and insert the records into the index
    FileScan fscan(relationName, bufMgr);
    try {
      RecordId scanRid;
      while (true) {
        fscan.scanNext(scanRid);
        std::string recordStr = fscan.getRecord();
        const char *record = recordStr.c_str();
        if (this->attributeType == Datatype::INTEGER) {
          int *key = ((int *)(record + attrByteOffset));
          this->insertEntry((void *)key, scanRid);
        } else if (this->attributeType == Datatype::DOUBLE) {
          double *key = ((double *)(record + attrByteOffset));
          this->insertEntry((void *)key, scanRid);
        } else if (this->attributeType == Datatype::STRING) {
          const char *key = ((char *)(record + attrByteOffset));
          std::string key_str(key);
          // Key is only first 10 characters of the record's string value
          std::string actualIndexKeyValue = key_str.substr(0, STRINGSIZE);
          this->insertEntry((void *)&actualIndexKeyValue, scanRid);
        }
      }
    } catch (EndOfFileException e) {
      std::cout << "Read all records" << std::endl;
    }
  }
}

// -----------------------------------------------------------------------------
// BTreeIndex::~BTreeIndex -- destructor
// -----------------------------------------------------------------------------

BTreeIndex::~BTreeIndex() {
  // Clear up state variables
  Page* metaPage;
  // Read meta page
  this->bufMgr->readPage(this->file, this->headerPageNum, metaPage);
  // Set the metadata in the metapage
  IndexMetaInfo *indexMetaInfo = (IndexMetaInfo *)metaPage;
  indexMetaInfo->rootPageNo = this->rootPageNum;
  indexMetaInfo->isRootLeaf = this->isRootLeaf;
  this->bufMgr->unPinPage(this->file, this->headerPageNum, true);
  this->scanExecuting = false;
  try {
    // Unpin the root page if not already unpinned
    this->bufMgr->unPinPage(this->file, this->rootPageNum, true);
  } catch (...) {
  }
  try {
    // Unpin the current page if not already unpinned
    this->bufMgr->unPinPage(this->file, this->currentPageNum, true);
  } catch (...) {
  }
  // Call endScan to stop the scan function
  try {
    this->endScan();
  } catch (...) {
  }
  this->bufMgr->flushFile(this->file);
  // Delete blobfile used for the index
  delete this->file;
  this->file = nullptr;
}

// -----------------------------------------------------------------------------
// BTreeIndex::insertEntry
// -----------------------------------------------------------------------------

const void BTreeIndex::insertEntry(const void *key, const RecordId rid) {
  PageId rootPageId = this->rootPageNum;
  Page *rootPage;
  // Read the root page
  this->bufMgr->readPage(this->file, rootPageId, rootPage);
  // First identify the leaf node
  if (this->isRootLeaf) {
    // std::cout << "Inserting when root is leaf" << std::endl;
    if (this->attributeType == Datatype::INTEGER) {
      // Cast the rootPage to the leaf node
      LeafNodeInt *rootLeafNode = (LeafNodeInt *)rootPage;
        // Case 1: Root is leaf and ideal occupancy is not attained (Non-split)
      if (hasSpaceInLeafNode<LeafNodeInt>(rootLeafNode)) {
        // Non-split, insert the (key, record)
        int keyCopy = *(int *)key;
        insertKeyRidToKeyRidArray<int>(rootLeafNode->keyArray,
                                       rootLeafNode->ridArray,
                                       rootLeafNode->len, keyCopy, rid);
        rootLeafNode->len += 1;
        this->bufMgr->unPinPage(this->file, rootPageId, true);
      } else {
        std::cout << "Leaf Root split case" << std::endl;
        // Split the root node
        this->isRootLeaf = false;
        std::vector<RIDKeyPair<int>> ridKeyPairVec;
        // Insert all the key, rid pairs including current key, rid to be
        // inserted
        for (int i = 0; i < rootLeafNode->len; i++) {
          RIDKeyPair<int> ridKeyPair;
          const RecordId rid_ = rootLeafNode->ridArray[i];
          const int key_ = rootLeafNode->keyArray[i];
          ridKeyPair.set(rid_, key_);
          ridKeyPairVec.push_back(ridKeyPair);
        }
        // Insert current key, rid
        RIDKeyPair<int> ridKeyPair;
        ridKeyPair.set(rid, *(int *)key);
        ridKeyPairVec.push_back(ridKeyPair);

        // Sort the vector
        sort(ridKeyPairVec.begin(), ridKeyPairVec.end());
        int middleKeyIndex = ridKeyPairVec.size() / 2;
        int middleKey = ridKeyPairVec[middleKeyIndex].key;
        // Create new page
        Page *newPage;
        PageId newPageNum;
        bufMgr->allocPage(this->file, newPageNum, newPage);
        // Move half the (key, recordID) to the new node
        // Cast the page to leaf node
        LeafNodeInt *newPageLeafNode = (LeafNodeInt *)newPage;
        newPageLeafNode->rightSibPageNo = INVALID_PAGE;
        newPageLeafNode->len = 0;
        for (int i = middleKeyIndex; i < (int)ridKeyPairVec.size(); i++) {
          int key_ = ridKeyPairVec[i].key;
          RecordId rid_ = ridKeyPairVec[i].rid;
          insertKeyRidToKeyRidArray<int>(newPageLeafNode->keyArray,
                                         newPageLeafNode->ridArray,
                                         newPageLeafNode->len, key_, rid_);
          newPageLeafNode->len += 1;
        }
        std::cout << "new leaf node with page id " << newPageNum
                  << " has length " << newPageLeafNode->len << std::endl;

        // Move the first half to the root leaf node
        rootLeafNode->len = 0;
        for (int i = 0; i < middleKeyIndex; i++) {
          int key_ = ridKeyPairVec[i].key;
          RecordId rid_ = ridKeyPairVec[i].rid;
          insertKeyRidToKeyRidArray<int>(rootLeafNode->keyArray,
                                         rootLeafNode->ridArray,
                                         rootLeafNode->len, key_, rid_);
          rootLeafNode->len += 1;
        }
        std::cout << "old root leaf node with page id " << rootPageId
                  << " has length " << middleKeyIndex << std::endl;

        // Set next page id of left leaf node
        rootLeafNode->rightSibPageNo = newPageNum;

        // Create root node non leaf page
        Page *newRootPage;
        PageId newRootPageNum;
        bufMgr->allocPage(this->file, newRootPageNum, newRootPage);
        std::cout << "new root non leaf node with page id " << newRootPageNum << std::endl;
        this->rootPageNum = newRootPageNum;
        NonLeafNodeInt *rootNonLeafNode = (NonLeafNodeInt *)newRootPage;
        rootNonLeafNode->level = 1;  // Since this is the node above the leaf
        // Copy up the middle key to root;
        rootNonLeafNode->keyArray[0] = middleKey;
        rootNonLeafNode->len = 1;
        rootNonLeafNode->pageNoArray[0] = rootPageId;
        rootNonLeafNode->pageNoArray[1] = newPageNum;

        // Unpin old root page, new leaf page, new root page
        this->bufMgr->unPinPage(this->file, rootPageId, true);
        this->bufMgr->unPinPage(this->file, newPageNum, true);
        this->bufMgr->unPinPage(this->file, newRootPageNum, true);
      }
    } else if (this->attributeType == Datatype::DOUBLE) {
      // std::cout << "Inserting when root is leaf" << std::endl;
      // Cast the rootPage to the leaf node
      LeafNodeDouble *rootLeafNode = (LeafNodeDouble *)rootPage;
      if (hasSpaceInLeafNode<LeafNodeDouble>(rootLeafNode)) {
        // Non-split, insert the (key, record)
        double keyCopy = *(double *)key;
        insertKeyRidToKeyRidArray<double>(rootLeafNode->keyArray,
                                          rootLeafNode->ridArray,
                                          rootLeafNode->len, keyCopy, rid);
        rootLeafNode->len += 1;
        this->bufMgr->unPinPage(this->file, rootPageId, true);
      } else {
        std::cout << "Leaf Root split case" << std::endl;
        // Split the root node
        this->isRootLeaf = false;
        std::vector<RIDKeyPair<double>> ridKeyPairVec;
        // Insert all the key, rid pairs including current key, rid to be
        // inserted
        for (int i = 0; i < rootLeafNode->len; i++) {
          RIDKeyPair<double> ridKeyPair;
          const RecordId rid_ = rootLeafNode->ridArray[i];
          const double key_ = rootLeafNode->keyArray[i];
          ridKeyPair.set(rid_, key_);
          ridKeyPairVec.push_back(ridKeyPair);
        }
        // Insert current key, rid
        RIDKeyPair<double> ridKeyPair;
        ridKeyPair.set(rid, *(double *)key);
        ridKeyPairVec.push_back(ridKeyPair);

        // Sort the vector
        sort(ridKeyPairVec.begin(), ridKeyPairVec.end());
        int middleKeyIndex = ridKeyPairVec.size() / 2;
        double middleKey = ridKeyPairVec[middleKeyIndex].key;
        // Create new page
        Page *newPage;
        PageId newPageNum;
        bufMgr->allocPage(this->file, newPageNum, newPage);
        // Move half the (key, recordID) to the new node
        // Cast the page to leaf node
        LeafNodeDouble *newPageLeafNode = (LeafNodeDouble *)newPage;
        newPageLeafNode->rightSibPageNo = INVALID_PAGE;
        newPageLeafNode->len = 0;
        for (int i = middleKeyIndex; i < (int)ridKeyPairVec.size(); i++) {
          double key_ = ridKeyPairVec[i].key;
          RecordId rid_ = ridKeyPairVec[i].rid;
          insertKeyRidToKeyRidArray<double>(newPageLeafNode->keyArray,
                                            newPageLeafNode->ridArray,
                                            newPageLeafNode->len, key_, rid_);
          newPageLeafNode->len += 1;
        }
        std::cout << "new leaf node with page id " << newPageNum
                  << " has length " << newPageLeafNode->len << std::endl;

        // Move the first half to the root leaf node
        rootLeafNode->len = 0;
        for (int i = 0; i < middleKeyIndex; i++) {
          double key_ = ridKeyPairVec[i].key;
          RecordId rid_ = ridKeyPairVec[i].rid;
          insertKeyRidToKeyRidArray<double>(rootLeafNode->keyArray,
                                         rootLeafNode->ridArray,
                                         rootLeafNode->len, key_, rid_);
          rootLeafNode->len += 1;
        }
        std::cout << "old root leaf node with page id " << rootPageId
                  << " has length " << middleKeyIndex << std::endl;

        // Set next page id of left leaf node
        rootLeafNode->rightSibPageNo = newPageNum;

        // Create root node non leaf page
        Page *newRootPage;
        PageId newRootPageNum;
        bufMgr->allocPage(this->file, newRootPageNum, newRootPage);
        this->rootPageNum = newRootPageNum;
        NonLeafNodeDouble *rootNonLeafNode = (NonLeafNodeDouble *)newRootPage;
        rootNonLeafNode->level = 1;  // Since this is the node above the leaf
        // Copy up the middle key to root;
        rootNonLeafNode->keyArray[0] = middleKey;
        rootNonLeafNode->len = 1;
        rootNonLeafNode->pageNoArray[0] = rootPageId;
        rootNonLeafNode->pageNoArray[1] = newPageNum;

        // Unpin old root page and new leaf page
        this->bufMgr->unPinPage(this->file, rootPageId, true);
        this->bufMgr->unPinPage(this->file, newPageNum, true);
        this->bufMgr->unPinPage(this->file, newRootPageNum, true);
      }
    } else if (this->attributeType == Datatype::STRING) {
      // std::cout << "Inserting when root is leaf" << std::endl;
      // Cast the rootPage to the leaf node
      LeafNodeString *rootLeafNode = (LeafNodeString *)rootPage;
      if (hasSpaceInLeafNode<LeafNodeString>(rootLeafNode)) {
        // Non-split, insert the (key, record)
        std::string* keyCopy = (std::string *)key;
        insertKeyRidToKeyRidArrayForString(rootLeafNode->keyArray,
                                          rootLeafNode->ridArray,
                                          rootLeafNode->len, *keyCopy, rid);
        rootLeafNode->len += 1;
        this->bufMgr->unPinPage(this->file, rootPageId, true);
      } else {
        std::cout << "Leaf Root split case" << std::endl;
        // Split the root node
        this->isRootLeaf = false;
        std::vector<RIDKeyPair<std::string>> ridKeyPairVec;
        // Insert all the key, rid pairs including current key, rid to be
        // inserted
        for (int i = 0; i < rootLeafNode->len; i++) {
          RIDKeyPair<std::string> ridKeyPair;
          const RecordId rid_ = rootLeafNode->ridArray[i];
          std::string key_(rootLeafNode->keyArray[i], STRINGSIZE);
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
        // Create new page
        Page *newPage;
        PageId newPageNum;
        bufMgr->allocPage(this->file, newPageNum, newPage);
        // Move half the (key, recordID) to the new node
        // Cast the page to leaf node
        LeafNodeString *newPageLeafNode = (LeafNodeString *)newPage;
        newPageLeafNode->rightSibPageNo = INVALID_PAGE;
        newPageLeafNode->len = 0;
        for (int i = middleKeyIndex; i < (int)ridKeyPairVec.size(); i++) {
          std::string key_ = ridKeyPairVec[i].key;
          RecordId rid_ = ridKeyPairVec[i].rid;
          insertKeyRidToKeyRidArrayForString(newPageLeafNode->keyArray,
                                            newPageLeafNode->ridArray,
                                            newPageLeafNode->len, key_, rid_);
          newPageLeafNode->len += 1;
        }
        std::cout << "new leaf node with page id " << newPageNum
                  << " has length " << newPageLeafNode->len << std::endl;

        // Move the first half to the root leaf node
        rootLeafNode->len = 0;
        for (int i = 0; i < middleKeyIndex; i++) {
          std::string key_ = ridKeyPairVec[i].key;
          RecordId rid_ = ridKeyPairVec[i].rid;
          insertKeyRidToKeyRidArrayForString(rootLeafNode->keyArray,
                                         rootLeafNode->ridArray,
                                         rootLeafNode->len, key_, rid_);
          rootLeafNode->len += 1;
        }
        std::cout << "old root leaf node with page id " << rootPageId
                  << " has length " << middleKeyIndex << std::endl;

        // Set next page id of left leaf node
        rootLeafNode->rightSibPageNo = newPageNum;

        // Create root node non leaf page
        Page *newRootPage;
        PageId newRootPageNum;
        bufMgr->allocPage(this->file, newRootPageNum, newRootPage);
        this->rootPageNum = newRootPageNum;
        NonLeafNodeString *rootNonLeafNode = (NonLeafNodeString *)newRootPage;
        rootNonLeafNode->level = 1;  // Since this is the node above the leaf
        // Copy up the middle key to root;
        strncpy(rootNonLeafNode->keyArray[0], middleKey.c_str(), STRINGSIZE);
        rootNonLeafNode->len = 1;
        rootNonLeafNode->pageNoArray[0] = rootPageId;
        rootNonLeafNode->pageNoArray[1] = newPageNum;

        // Unpin old root page and new leaf page
        this->bufMgr->unPinPage(this->file, rootPageId, true);
        this->bufMgr->unPinPage(this->file, newPageNum, true);
        this->bufMgr->unPinPage(this->file, newRootPageNum, true);
      }
    }
  } else {
    // std::cout << "Inserting when root is non-leaf" << std::endl;
    // Case 2: Root is non-leaf
    // isSplit will be passed by reference and will by set 
    // by children if split happens at children 
    bool isSplit = false;
    // splitKey is the key which needs to be moved up in the current node
    // when the children node split
    void *splitKey = nullptr;
    PageId splitRightNodePageId;
    insertRecursive(rootPageId, key, rid, isSplit, splitKey,
                    splitRightNodePageId);
    this->bufMgr->unPinPage(this->file, rootPageId, true);
  }
}

void BTreeIndex::insertRecursive(PageId nodePageNumber, const void *key,
                                 const RecordId rid, bool &isSplit,
                                 void *splitKey, PageId &splitRightNodePageId) {
  // Read current page
  Page *curPage;
  bufMgr->readPage(this->file, nodePageNumber, curPage);
  if (this->attributeType == Datatype::INTEGER) {
    // Cast it to non leaf
    NonLeafNodeInt *curNode = (NonLeafNodeInt *)curPage;
    int keyCopy = *(int *)key;
    PageId nextPage = -1;
    int nextPageIndex = -1;
    bool foundKey = false;
    // Find the index of the next page to traverse
    for (int i = 0; i < curNode->len; i++) {
      int curKey = curNode->keyArray[i];
      int nextKey = -1;
      if (i == curNode->len - 1) {
        nextKey = INT_MAX;
      } else {
        nextKey = curNode->keyArray[i + 1];
      }
      if (i == 0 && keyCopy < curKey) {
        // Insert in the left of the first key
        nextPageIndex = 0;
        nextPage = curNode->pageNoArray[0];
        foundKey = true;
        break;
      } else {
        if (keyCopy >= curKey && keyCopy < nextKey) {
          nextPageIndex = i + 1;
          nextPage = curNode->pageNoArray[i + 1];
          foundKey = true;
          break;
        }
      }
    }
    // Split key passed by reference to children nodes which is set when children nodes
    // split and the split key is the key which needs to be inserted in the current node
    int splitKey;
    if (foundKey) {
      // If the current non-leaf node is just above the leaf nodes, then call
      // insertAtLeaf else call insertRecursive
      if (curNode->level) {
        // The split key to be copied up the non-leaf nodes will be in the
        // splitKey variable
        insertLeaf(nextPage, &keyCopy, rid, isSplit, &splitKey,
                        splitRightNodePageId);
      } else {
        std::cout << "Insert recursive" << std::endl;
        // The split key will contain the value which has to be moved up to
        // above non-leaf nodes
        insertRecursive(nextPage, key, rid, isSplit, (void *)&splitKey,
                        splitRightNodePageId);
      }
      if (isSplit) {
        // Check the current node is the root node
        // If current node is root node, then new root will be created
        int middleKey = splitKey;
        insertNonLeaf(nodePageNumber, nextPageIndex, &middleKey, isSplit,
                           &splitKey, splitRightNodePageId);
        if (isSplit && nodePageNumber == this->rootPageNum) {
          std::cout << "Non leaf Root split case" << std::endl;
          // Create new page for the root
          Page *newRootPage;
          PageId newRootPageNum;
          // Unpin current root page
          this->bufMgr->unPinPage(this->file, this->rootPageNum, true);
          this->bufMgr->allocPage(this->file, newRootPageNum, newRootPage);
          // Cast to non leaf node int
          NonLeafNodeInt *nonLeafRootNodeInt = (NonLeafNodeInt *)newRootPage;
          nonLeafRootNodeInt->keyArray[0] = splitKey;
          nonLeafRootNodeInt->pageNoArray[0] = nodePageNumber;
          nonLeafRootNodeInt->pageNoArray[1] = splitRightNodePageId;
          nonLeafRootNodeInt->len = 1;
          nonLeafRootNodeInt->level = 0;
          this->rootPageNum = newRootPageNum;
          this->bufMgr->unPinPage(this->file, this->rootPageNum, true);
        }
      }
    }
    this->bufMgr->unPinPage(this->file, nodePageNumber, true);
  } else if (this->attributeType == Datatype::DOUBLE) {
    // Cast it to non leaf
    NonLeafNodeDouble *curNode = (NonLeafNodeDouble *)curPage;
    double keyCopy = *(double *)key;
    PageId nextPage = -1;
    int nextPageIndex = -1;
    bool foundKey = false;
    // Find the index of the next page to traverse
    for (int i = 0; i < curNode->len; i++) {
      double curKey = curNode->keyArray[i];
      double nextKey = -1;
      if (i == curNode->len - 1) {
        nextKey = INT_MAX;
      } else {
        nextKey = curNode->keyArray[i + 1];
      }
      if (i == 0 && keyCopy < curKey) {
        // Insert in the left of the first key
        nextPageIndex = 0;
        nextPage = curNode->pageNoArray[0];
        foundKey = true;
        break;
      } else {
        if (keyCopy >= curKey && keyCopy < nextKey) {
          nextPageIndex = i + 1;
          nextPage = curNode->pageNoArray[i + 1];
          foundKey = true;
          break;
        }
      }
    }
    // Split key passed by reference to children nodes which is set when children nodes
    // split and the split key is the key which needs to be inserted in the current node
    double splitKey;
    if (foundKey) {
      // If the current non-leaf node is just above the leaf nodes, then call
      // insertAtLeaf else call insertRecursive
      if (curNode->level) {
        // The split key to be copied up the non-leaf nodes will be in the
        // splitKey variable
        insertLeaf(nextPage, &keyCopy, rid, isSplit, &splitKey,
                           splitRightNodePageId);
      } else {
        std::cout << "Insert recursive" << std::endl;
        // The split key will contain the value which has to be moved up to
        // above non-leaf nodes
        insertRecursive(nextPage, key, rid, isSplit, (void *)&splitKey,
                        splitRightNodePageId);
      }
      if (isSplit) {
        // Check the current node is the root node
        // If current node is root node, then new root will be created
        double middleKey = splitKey;
        insertNonLeaf(nodePageNumber, nextPageIndex, &middleKey, isSplit,
                              &splitKey, splitRightNodePageId);
        if (isSplit && nodePageNumber == this->rootPageNum) {
          std::cout << "Non leaf Root split case" << std::endl;
          // Create new page for the root
          Page *newRootPage;
          PageId newRootPageNum;
          // Unpin current root page
          this->bufMgr->unPinPage(this->file, this->rootPageNum, true);
          this->bufMgr->allocPage(this->file, newRootPageNum, newRootPage);
          // Cast to non leaf node double
          NonLeafNodeDouble *nonLeafRootNodeDouble = (NonLeafNodeDouble *)newRootPage;
          nonLeafRootNodeDouble->keyArray[0] = splitKey;
          nonLeafRootNodeDouble->pageNoArray[0] = nodePageNumber;
          nonLeafRootNodeDouble->pageNoArray[1] = splitRightNodePageId;
          nonLeafRootNodeDouble->len = 1;
          nonLeafRootNodeDouble->level = 0;
          this->rootPageNum = newRootPageNum;
          this->bufMgr->unPinPage(this->file, this->rootPageNum, true);
        }
      }
    }
    this->bufMgr->unPinPage(this->file, nodePageNumber, true);
  } else if (this->attributeType == Datatype::STRING) {
    // Cast it to non leaf
    NonLeafNodeString *curNode = (NonLeafNodeString *)curPage;
    std::string keyCopy = *(std::string *)key;
    PageId nextPage = -1;
    int nextPageIndex = -1;
    bool foundKey = false;
    // Find the index of the next page to traverse
    for (int i = 0; i < curNode->len; i++) {
      std::string curKey = curNode->keyArray[i];
      std::string nextKey = "";
      if (i == curNode->len - 1) {
        nextKey = INT_MAX; // last key plus extra character to make it larger than last key
      } else {
        nextKey = curNode->keyArray[i + 1];
      }
      if (i == 0 && keyCopy < curKey) {
        // Insert in the left of the first key
        nextPageIndex = 0;
        nextPage = curNode->pageNoArray[0];
        foundKey = true;
        break;
      } else {
        if ((i == curNode->len - 1 && keyCopy >= curKey) || (keyCopy >= curKey && keyCopy < nextKey)) {
          nextPageIndex = i + 1;
          nextPage = curNode->pageNoArray[i + 1];
          foundKey = true;
          break;
        }
      }
    }
    // Split key passed by reference to children nodes which is set when children nodes
    // split and the split key is the key which needs to be inserted in the current node
    std::string splitKey;
    if (foundKey) {
      // If the current non-leaf node is just above the leaf nodes, then call
      // insertAtLeaf else call insertRecursive
      if (curNode->level) {
        // The split key to be copied up the non-leaf nodes will be in the
        // splitKey variable
        insertLeaf(nextPage, &keyCopy, rid, isSplit, &splitKey,
                           splitRightNodePageId);
      } else {
        std::cout << "Insert recursive" << std::endl;
        // The split key will contain the value which has to be moved up to
        // above non-leaf nodes
        insertRecursive(nextPage, key, rid, isSplit, (void *)&splitKey,
                        splitRightNodePageId);
      }
      if (isSplit) {
        // Check the current node is the root node
        // If current node is root node, then new root will be created
        std::string middleKey = splitKey;
        insertNonLeaf(nodePageNumber, nextPageIndex, &middleKey, isSplit,
                              &splitKey, splitRightNodePageId);
        if (isSplit && nodePageNumber == this->rootPageNum) {
          std::cout << "Non leaf Root split case" << std::endl;
          // Create new page for the root
          Page *newRootPage;
          PageId newRootPageNum;
          // Unpin current root page
          this->bufMgr->unPinPage(this->file, this->rootPageNum, true);
          this->bufMgr->allocPage(this->file, newRootPageNum, newRootPage);
          // Cast to non leaf node double
          NonLeafNodeString *nonLeafRootNodeString = (NonLeafNodeString *)newRootPage;
          strncpy(nonLeafRootNodeString->keyArray[0], splitKey.c_str(), STRINGSIZE);
          nonLeafRootNodeString->pageNoArray[0] = nodePageNumber;
          nonLeafRootNodeString->pageNoArray[1] = splitRightNodePageId;
          nonLeafRootNodeString->len = 1;
          nonLeafRootNodeString->level = 0;
          this->rootPageNum = newRootPageNum;
          this->bufMgr->unPinPage(this->file, this->rootPageNum, true);
        }
      }
    }
    this->bufMgr->unPinPage(this->file, nodePageNumber, true);
  }

  // insertRecursive(childPageNum, key, rid, isSplit, splitKey);
  // Else recurse
}

// -----------------------------------------------------------------------------
// BTreeIndex::startScan
// -----------------------------------------------------------------------------

const void BTreeIndex::startScan(const void *lowValParm,
                                 const Operator lowOpParm,
                                 const void *highValParm,
                                 const Operator highOpParm) {
  // Check if another scan is executing
  // If another scan is executing, end that scan
  if (this->scanExecuting) {
    this->endScan();
  }
  if (lowOpParm == Operator::LT || lowOpParm == Operator::LTE) {
    throw BadOpcodesException();
  }
  if (highOpParm == Operator::GT || highOpParm == Operator::GTE) {
    throw BadOpcodesException();
  }
  // Set up scan variables
  this->scanExecuting = true;
  this->lowOp = lowOpParm;
  this->highOp = highOpParm;
  if (this->attributeType == Datatype::INTEGER) {
    this->lowValInt = *(int *)lowValParm;
    this->highValInt = *(int *)highValParm;
  } else if (this->attributeType == Datatype::DOUBLE) {
    this->lowValDouble = *(double *)lowValParm;
    this->highValDouble = *(double *)highValParm;
  } else if (this->attributeType == Datatype::STRING) {
    const char* lowStrKey = static_cast<const char*>(lowValParm);
    const char* highStrKey = static_cast<const char*>(highValParm);
    std::string lowStrKey_(lowStrKey);
    std::string highStrKey_(highStrKey);
    lowStrKey_ = lowStrKey_.substr(0, STRINGSIZE);
    highStrKey_ = highStrKey_.substr(0, STRINGSIZE);
    this->lowValString = lowStrKey_;
    this->highValString = highStrKey_;
  }

  if (this->attributeType == Datatype::INTEGER) {
    int lowIntValue = *(int *)lowValParm;
    int highIntValue = *(int *)highValParm;
    if (lowIntValue > highIntValue) {
      throw BadScanrangeException();
    }
    PageId rootPageId = this->rootPageNum;
    this->currentPageNum = rootPageId;
    Page *rootPage;
    if (isRootLeaf) {
      this->bufMgr->readPage(this->file, rootPageId, rootPage);
      // Cast root page to leaf node
      LeafNodeInt *rootLeafNode = (LeafNodeInt *)rootPage;
      this->currentPageData = rootPage;
      for (int i = 0; i < rootLeafNode->len; i++) {
        if (lowOpParm == Operator::GT &&
            rootLeafNode->keyArray[i] > lowIntValue) {
          this->nextEntry = i;
          break;
        }
        if (lowOpParm == Operator::GTE &&
            rootLeafNode->keyArray[i] >= lowIntValue) {
          this->nextEntry = i;
          break;
        }
      }
      // If the nextEntry is still not set, it means no keys match the scan
      // criteria
      if (this->nextEntry == INVALID_KEY_INDEX) {
        this->bufMgr->unPinPage(this->file, rootPageId, false);
        throw NoSuchKeyFoundException();
      }
      this->bufMgr->unPinPage(this->file, rootPageId, false);
    } else {
      PageId curPageNum = this->rootPageNum;
      Page *curPage;
      // Navigate till the node which is just above the leaf node
      while (true) {
        bufMgr->readPage(this->file, curPageNum, curPage);
        this->currentPageNum = curPageNum;
        this->currentPageData = curPage;
        // Cast to non leaf node
        NonLeafNodeInt *curNonLeafNode = (NonLeafNodeInt *)curPage;
        // Unpin each page except the leaf page
        bufMgr->unPinPage(this->file, curPageNum, false);
        bool nextKeyFound = false;
        for (int i = 0; i < curNonLeafNode->len; i++) {
          // For both GT and GTE operator, need to find first key index
          // which is greater than lowIntValue (convention: left child node
          // contains keys strictly less than key of parent node and right
          // child node contains keys greater than or equal to parent node
          // key)
          if (curNonLeafNode->keyArray[i] > lowIntValue) {
            curPageNum = curNonLeafNode->pageNoArray[i];
            nextKeyFound = true;
            break;
          }
        }
        if (!nextKeyFound) {
          // It means the next page should be the last index of pageNoArray
          curPageNum = curNonLeafNode->pageNoArray[curNonLeafNode->len];
        }
        // Break the loop, when we are at one level above the leaf node level
        // curPageNum now points to a leaf node
        if (curNonLeafNode->level == 1) {
          break;
        }
      }
      bufMgr->readPage(this->file, curPageNum, curPage);
      this->currentPageData = curPage;
      // Reached the leaf node
      LeafNodeInt *curLeafNode = (LeafNodeInt *)curPage;
      // Unpin each page except the leaf page
      bufMgr->unPinPage(this->file, curPageNum, false);
      // Iterate over the leaf nodes and its siblings until a key is found
      // satisfying the criteria or the end of index is reached
      while (true) {
        // Scan current page
        // Its possible that current page doesn't contain any satisfying
        // key, in which case move to next sibling (eg 10, 11, 12 are the
        // keys in current page and lowIntValue - 14)
        bool satisfyingKeyFound = false;
        for (int i = 0; i < curLeafNode->len; i++) {
          if (this->lowOp == GTE && curLeafNode->keyArray[i] >= lowIntValue) {
            this->nextEntry = i;
            this->currentPageData = curPage;
            satisfyingKeyFound = true;
            break;
          }
          if (this->lowOp == GT && curLeafNode->keyArray[i] > lowIntValue) {
            this->nextEntry = i;
            this->currentPageData = curPage;
            satisfyingKeyFound = true;
            break;
          }
        }
        if (!satisfyingKeyFound) {
          PageId nextPageNo = curLeafNode->rightSibPageNo;
          if (nextPageNo == INVALID_PAGE) {
            this->nextEntry = INVALID_KEY_INDEX;
            return;
          }
          curPageNum = nextPageNo;
          this->nextEntry = 0;
          bufMgr->readPage(this->file, curPageNum, curPage);
          this->currentPageData = curPage;
          this->currentPageNum = curPageNum;
          bufMgr->unPinPage(this->file, curPageNum, false);
        } else {
          break;
        }
      }
    }
  } else if (this->attributeType == Datatype::DOUBLE) {
    // Check if another scan is executing
    // If another scan is executing, end that scan
      double lowDoubleValue = *(double *)lowValParm;
      double highDoubleValue = *(double *)highValParm;
      if (lowDoubleValue > highDoubleValue) {
        throw BadScanrangeException();
      }
      PageId rootPageId = this->rootPageNum;
      this->currentPageNum = rootPageId;
      Page *rootPage;
      if (isRootLeaf) {
        this->bufMgr->readPage(this->file, rootPageId, rootPage);
        // Cast root page to leaf node
        LeafNodeDouble *rootLeafNode = (LeafNodeDouble *)rootPage;
        this->currentPageData = rootPage;
        for (int i = 0; i < rootLeafNode->len; i++) {
          if (lowOpParm == Operator::GT &&
              rootLeafNode->keyArray[i] > lowDoubleValue) {
            this->nextEntry = i;
            break;
          }
          if (lowOpParm == Operator::GTE &&
              rootLeafNode->keyArray[i] >= lowDoubleValue) {
            this->nextEntry = i;
            break;
          }
        }
        // If the nextEntry is still not set, it means no keys match the scan
        // criteria
        if (this->nextEntry == INVALID_KEY_INDEX) {
          this->bufMgr->unPinPage(this->file, rootPageId, false);
          throw NoSuchKeyFoundException();
        }
        this->bufMgr->unPinPage(this->file, rootPageId, false);
      } else {
        PageId curPageNum = this->rootPageNum;
        Page *curPage;
        // Navigate till the node which is just above the leaf node
        while (true) {
          bufMgr->readPage(this->file, curPageNum, curPage);
          this->currentPageNum = curPageNum;
          this->currentPageData = curPage;
          // Cast to non leaf node
          NonLeafNodeDouble *curNonLeafNode = (NonLeafNodeDouble *)curPage;
          // Unpin each page except the leaf page
          bufMgr->unPinPage(this->file, curPageNum, false);
          bool nextKeyFound = false;
          for (int i = 0; i < curNonLeafNode->len; i++) {
            // For both GT and GTE operator, need to find first key index
            // which is greater than lowIntValue (convention: left child node
            // contains keys strictly less than key of parent node and right
            // child node contains keys greater than or equal to parent node
            // key)
            if (curNonLeafNode->keyArray[i] > lowDoubleValue) {
              curPageNum = curNonLeafNode->pageNoArray[i];
              nextKeyFound = true;
              break;
            }
          }
          if (!nextKeyFound) {
            // It means the next page should be the last index of pageNoArray
            curPageNum = curNonLeafNode->pageNoArray[curNonLeafNode->len];
          }
          // Break the loop, when we are at one level above the leaf node level
          // curPageNum now points to a leaf node
          if (curNonLeafNode->level == 1) {
            break;
          }
        }
        bufMgr->readPage(this->file, curPageNum, curPage);
        this->currentPageData = curPage;
        // Reached the leaf node
        LeafNodeDouble *curLeafNode = (LeafNodeDouble *)curPage;
        // Unpin each page except the leaf page
        bufMgr->unPinPage(this->file, curPageNum, false);
        // Iterate over the leaf nodes and its siblings until a key is found
        // satisfying the criteria or the end of index is reached
        while (true) {
          // Scan current page
          // Its possible that current page doesn't contain any satisfying
          // key, in which case move to next sibling (eg 10, 11, 12 are the
          // keys in current page and lowIntValue - 14)
          bool satisfyingKeyFound = false;
          for (int i = 0; i < curLeafNode->len; i++) {
            if (this->lowOp == GTE && curLeafNode->keyArray[i] >= lowDoubleValue) {
              this->nextEntry = i;
              this->currentPageData = curPage;
              satisfyingKeyFound = true;
              break;
            }
            if (this->lowOp == GT && curLeafNode->keyArray[i] > lowDoubleValue) {
              this->nextEntry = i;
              this->currentPageData = curPage;
              satisfyingKeyFound = true;
              break;
            }
          }
          if (!satisfyingKeyFound) {
            PageId nextPageNo = curLeafNode->rightSibPageNo;
            if (nextPageNo == INVALID_PAGE) {
              this->nextEntry = INVALID_KEY_INDEX;
              return;
            }
            curPageNum = nextPageNo;
            this->nextEntry = 0;
            bufMgr->readPage(this->file, curPageNum, curPage);
            this->currentPageData = curPage;
            this->currentPageNum = curPageNum;
            bufMgr->unPinPage(this->file, curPageNum, false);
          } else {
            break;
          }
        }
      }
    } else if (this->attributeType == Datatype::STRING) {
      // Check if another scan is executing
      // If another scan is executing, end that scan
      std::string lowStringValue = this->lowValString;
      std::string highStringValue = this->highValString;
      std::cout<<lowStringValue<<" "<<highStringValue<<std::endl;

      if (lowStringValue > highStringValue) {
        throw BadScanrangeException();
      }
      PageId rootPageId = this->rootPageNum;
      this->currentPageNum = rootPageId;
      Page *rootPage;
      if (isRootLeaf) {
        this->bufMgr->readPage(this->file, rootPageId, rootPage);
        // Cast root page to leaf node
        LeafNodeString *rootLeafNode = (LeafNodeString *)rootPage;
        this->currentPageData = rootPage;
        for (int i = 0; i < rootLeafNode->len; i++) {
          if (lowOpParm == Operator::GT &&
              (strncmp(rootLeafNode->keyArray[i], lowStringValue.c_str(), STRINGSIZE) > 0)) {
            this->nextEntry = i;
            break;
          }
          if (lowOpParm == Operator::GTE &&
          (strncmp(rootLeafNode->keyArray[i], lowStringValue.c_str(), STRINGSIZE) >= 0)) {
            this->nextEntry = i;
            break;
          }
        }
        // If the nextEntry is still not set, it means no keys match the scan
        // criteria
        if (this->nextEntry == INVALID_KEY_INDEX) {
          this->bufMgr->unPinPage(this->file, rootPageId, false);
          throw NoSuchKeyFoundException();
        }
        this->bufMgr->unPinPage(this->file, rootPageId, false);
      } else {
        PageId curPageNum = this->rootPageNum;
        Page *curPage;
        // Navigate till the node which is just above the leaf node
        while (true) {
          bufMgr->readPage(this->file, curPageNum, curPage);
          this->currentPageNum = curPageNum;
          this->currentPageData = curPage;
          // Cast to non leaf node
          NonLeafNodeString *curNonLeafNode = (NonLeafNodeString *)curPage;
          // Unpin each page except the leaf page
          bufMgr->unPinPage(this->file, curPageNum, false);
          bool nextKeyFound = false;
          for (int i = 0; i < curNonLeafNode->len; i++) {
            // For both GT and GTE operator, need to find first key index
            // which is greater than lowIntValue (convention: left child node
            // contains keys strictly less than key of parent node and right
            // child node contains keys greater than or equal to parent node
            // key)
            if (strncmp(curNonLeafNode->keyArray[i], lowStringValue.c_str(), STRINGSIZE) > 0) {
              curPageNum = curNonLeafNode->pageNoArray[i];
              nextKeyFound = true;
              break;
            }
          }
          if (!nextKeyFound) {
            // It means the next page should be the last index of pageNoArray
            curPageNum = curNonLeafNode->pageNoArray[curNonLeafNode->len];
          }
          // Break the loop, when we are at one level above the leaf node level
          // curPageNum now points to a leaf node
          if (curNonLeafNode->level == 1) {
            break;
          }
        }
        bufMgr->readPage(this->file, curPageNum, curPage);
        this->currentPageData = curPage;
        // Reached the leaf node
        LeafNodeString *curLeafNode = (LeafNodeString *)curPage;
        // Unpin each page except the leaf page
        bufMgr->unPinPage(this->file, curPageNum, false);
        // Iterate over the leaf nodes and its siblings until a key is found
        // satisfying the criteria or the end of index is reached
        while (true) {
          // Scan current page
          // Its possible that current page doesn't contain any satisfying
          // key, in which case move to next sibling (eg 10, 11, 12 are the
          // keys in current page and lowIntValue - 14)
          bool satisfyingKeyFound = false;
          for (int i = 0; i < curLeafNode->len; i++) {
            
            if (this->lowOp == GTE && (strncmp(curLeafNode->keyArray[i], lowStringValue.c_str(), STRINGSIZE) >= 0)) {
              this->nextEntry = i;
              this->currentPageData = curPage;
              satisfyingKeyFound = true;
              break;
            }
            if (this->lowOp == GT && (strncmp(curLeafNode->keyArray[i], lowStringValue.c_str(), STRINGSIZE) > 0)) {
              this->nextEntry = i;
              this->currentPageData = curPage;
              satisfyingKeyFound = true;
              break;
            }
          }
          if (!satisfyingKeyFound) {
            PageId nextPageNo = curLeafNode->rightSibPageNo;
            if (nextPageNo == INVALID_PAGE) {
              this->nextEntry = INVALID_KEY_INDEX;
              return;
            }
            curPageNum = nextPageNo;
            this->nextEntry = 0;
            bufMgr->readPage(this->file, curPageNum, curPage);
            this->currentPageData = curPage;
            this->currentPageNum = curPageNum;
            bufMgr->unPinPage(this->file, curPageNum, false);
          } else {
            break;
          }
        }
      }
    }
    // Unpin the current page if not already unpinned
    try {
      bufMgr->unPinPage(this->file, this->currentPageNum, false);
    } catch (...) {
    }
  }

  // -----------------------------------------------------------------------------
  // BTreeIndex::scanNext
  // -----------------------------------------------------------------------------

  const void BTreeIndex::scanNext(RecordId & outRid) {
    if (!scanExecuting) {
      try{
      this->bufMgr->unPinPage(this->file, this->currentPageNum, false);
      }catch(...){
      }
      throw ScanNotInitializedException();
    }
    // Check if nextEntry is valid or not (points to valid entry in the page or
    // not)
    if (this->nextEntry == INVALID_KEY_INDEX) {
      try {
        this->bufMgr->unPinPage(this->file, this->currentPageNum, false);
      } catch(...) {

      }
      throw IndexScanCompletedException();
    }
    if (this->attributeType == Datatype::INTEGER) {
      // Cast the curPage to leaf page node
      LeafNodeInt *curLeafNode = (LeafNodeInt *)this->currentPageData;
      // Before setting the record id, check if it matches the criteria
      if (this->highOp == LT &&
          curLeafNode->keyArray[this->nextEntry] >= this->highValInt) {
          try {
            this->bufMgr->unPinPage(this->file, this->currentPageNum, false);
          } catch(...) {
          }
        throw IndexScanCompletedException();
      }
      if (this->highOp == LTE &&
          curLeafNode->keyArray[this->nextEntry] > this->highValInt) {
          try {
            this->bufMgr->unPinPage(this->file, this->currentPageNum, false);
          } catch(...) {
          }
        throw IndexScanCompletedException();
      }
      // Read the nextEntry, its valid now since we are validating it in the
      // function start
      RecordId entryRid = curLeafNode->ridArray[this->nextEntry];
      outRid = entryRid;
      // Update the nextEntry member
      this->nextEntry += 1;
      if (this->nextEntry < curLeafNode->len) {
        // Fetch the record from this page
        // Check if the nextEntry matches the scan criteria
        if (this->highOp == LT) {
          if (curLeafNode->keyArray[this->nextEntry] >= this->highValInt) {
            // Reached the end of the scan
            this->nextEntry = INVALID_KEY_INDEX;
          }
        } else if (this->highOp == LTE) {
          if (curLeafNode->keyArray[this->nextEntry] > this->highValInt) {
            // Reached the end of the scan
            this->nextEntry = INVALID_KEY_INDEX;
          }
        }
      } else {
        // Reached the end of the current page, need to read the sibling page
        if (curLeafNode->rightSibPageNo == INVALID_PAGE) {
          this->nextEntry = INVALID_KEY_INDEX;
          // Reached the end
          try {
            this->bufMgr->unPinPage(this->file, this->currentPageNum, false);
          } catch(...) {
          }
          return;
        }
        PageId siblingPageNo = curLeafNode->rightSibPageNo;
        // Unpin the current page
        try {
          this->bufMgr->unPinPage(this->file, this->currentPageNum, false);
        } catch(...) {
        }
        this->currentPageNum = siblingPageNo;
        // Read the sibling page and keep it pinned
        this->bufMgr->readPage(this->file, this->currentPageNum,
                               this->currentPageData);
        // Cast the page to leaf node
        curLeafNode = (LeafNodeInt *)this->currentPageData;
        // Check if the first entry of the new sibling page is valid or not as
        // per scan critiera
        if (this->highOp == LT) {
          if (curLeafNode->keyArray[0] >= this->highValInt) {
            // Reached the end of the scan
            this->nextEntry = INVALID_KEY_INDEX;
          } else {
            this->nextEntry = 0;
          }
        } else if (this->highOp == LTE) {
          if (curLeafNode->keyArray[0] > this->highValInt) {
            // Reached the end of the scan
            this->nextEntry = INVALID_KEY_INDEX;
          } else {
            this->nextEntry = 0;
          }
        }
        this->bufMgr->unPinPage(this->file, this->currentPageNum, false);
      }
    } else if (this->attributeType == Datatype::DOUBLE) {
      // Cast the curPage to leaf page node
      LeafNodeDouble *curLeafNode = (LeafNodeDouble *)this->currentPageData;
      // Before setting the record id, check if it matches the criteria
      if (this->highOp == LT &&
          curLeafNode->keyArray[this->nextEntry] >= this->highValDouble) {
          try {
            this->bufMgr->unPinPage(this->file, this->currentPageNum, false);
          } catch(...) {
          }
        throw IndexScanCompletedException();
      }
      if (this->highOp == LTE &&
          curLeafNode->keyArray[this->nextEntry] > this->highValDouble) {
             try {
            this->bufMgr->unPinPage(this->file, this->currentPageNum, false);
          } catch(...) {
          }
        throw IndexScanCompletedException();
      }
      // Read the nextEntry, its valid now since we are validating it in the
      // function start
      RecordId entryRid = curLeafNode->ridArray[this->nextEntry];
      outRid = entryRid;
      // Update the nextEntry member
      this->nextEntry += 1;
      if (this->nextEntry < curLeafNode->len) {
        // Fetch the record from this page
        // Check if the nextEntry matches the scan criteria
        if (this->highOp == LT) {
          if (curLeafNode->keyArray[this->nextEntry] >= this->highValDouble) {
            // Reached the end of the scan
            this->nextEntry = INVALID_KEY_INDEX;
          }
        } else if (this->highOp == LTE) {
          if (curLeafNode->keyArray[this->nextEntry] > this->highValDouble) {
            // Reached the end of the scan
            this->nextEntry = INVALID_KEY_INDEX;
          }
        }
      } else {
        // Reached the end of the current page, need to read the sibling page
        if (curLeafNode->rightSibPageNo == INVALID_PAGE) {
          this->nextEntry = INVALID_KEY_INDEX;
          // Reached the end
          try {
            this->bufMgr->unPinPage(this->file, this->currentPageNum, false);
          } catch(...) {
          }
          return;
        }
        PageId siblingPageNo = curLeafNode->rightSibPageNo;
        // Unpin the current page
        try {
          this->bufMgr->unPinPage(this->file, this->currentPageNum, false);
        } catch(...) {
        }
        this->currentPageNum = siblingPageNo;
        // Read the sibling page and keep it pinned
        this->bufMgr->readPage(this->file, this->currentPageNum,
                               this->currentPageData);
        // Cast the page to leaf node
        curLeafNode = (LeafNodeDouble *)this->currentPageData;
        // Check if the first entry of the new sibling page is valid or not as
        // per scan critiera
        if (this->highOp == LT) {
          if (curLeafNode->keyArray[0] >= this->highValDouble) {
            // Reached the end of the scan
            this->nextEntry = INVALID_KEY_INDEX;
          } else {
            this->nextEntry = 0;
          }
        } else if (this->highOp == LTE) {
          if (curLeafNode->keyArray[0] > this->highValDouble) {
            // Reached the end of the scan
            this->nextEntry = INVALID_KEY_INDEX;
          } else {
            this->nextEntry = 0;
          }
        }
        this->bufMgr->unPinPage(this->file, this->currentPageNum, false);
      }
    } else if (this->attributeType == Datatype::STRING) {
      // Cast the curPage to leaf page node
      LeafNodeString *curLeafNode = (LeafNodeString *)this->currentPageData;
      // Before setting the record id, check if it matches the criteria
      if (this->highOp == LT &&
          (strncmp(curLeafNode->keyArray[this->nextEntry], this->highValString.c_str(), STRINGSIZE) >= 0)) {
             try {
            this->bufMgr->unPinPage(this->file, this->currentPageNum, false);
          } catch(...) {
          }
        throw IndexScanCompletedException();
      }
      if (this->highOp == LTE &&
          (strncmp(curLeafNode->keyArray[this->nextEntry], this->highValString.c_str(), STRINGSIZE) > 0)) {
             try {
            this->bufMgr->unPinPage(this->file, this->currentPageNum, false);
          } catch(...) {
          }
        throw IndexScanCompletedException();
      }
      // Read the nextEntry, its valid now since we are validating it in the
      // function start
      RecordId entryRid = curLeafNode->ridArray[this->nextEntry];
      // std::cout<<curLeafNode->keyArray[this->nextEntry]<<std::endl;
      // std::cout<<"&&&&&&&&&&&&&&&&&"<<std::endl;
      outRid = entryRid;
      std::string s(curLeafNode->keyArray[this->nextEntry], 10);
      // std::cout<<s<<std::endl;
      // Update the nextEntry member
      this->nextEntry += 1;
      if (this->nextEntry < curLeafNode->len) {
        // Fetch the record from this page
        // Check if the nextEntry matches the scan criteria
        if (this->highOp == LT) {
          if (strncmp(curLeafNode->keyArray[this->nextEntry], this->highValString.c_str(), STRINGSIZE) >= 0) {
            // Reached the end of the scan
            this->nextEntry = INVALID_KEY_INDEX;
          }
        } else if (this->highOp == LTE) {
          if (strncmp(curLeafNode->keyArray[this->nextEntry], this->highValString.c_str(), STRINGSIZE) > 0) {
            // Reached the end of the scan
            this->nextEntry = INVALID_KEY_INDEX;
          }
        }
      } else {
        // Reached the end of the current page, need to read the sibling page
        if (curLeafNode->rightSibPageNo == INVALID_PAGE) {
          this->nextEntry = INVALID_KEY_INDEX;
          // Reached the end
          try {
            this->bufMgr->unPinPage(this->file, this->currentPageNum, false);
          } catch(...) {
          }
          return;
        }
        PageId siblingPageNo = curLeafNode->rightSibPageNo;
        this->currentPageNum = siblingPageNo;
        // Read the sibling page and keep it pinned
        this->bufMgr->readPage(this->file, this->currentPageNum,
                               this->currentPageData);
        // Cast the page to leaf node
        curLeafNode = (LeafNodeString *)this->currentPageData;
        // Check if the first entry of the new sibling page is valid or not as
        // per scan critiera
        if (this->highOp == LT) {        
          if (strncmp(curLeafNode->keyArray[0], this->highValString.c_str(), STRINGSIZE) >= 0) {
            // Reached the end of the scan
            this->nextEntry = INVALID_KEY_INDEX;
          } else {
            this->nextEntry = 0;
          }
        } else if (this->highOp == LTE) {
          if (strncmp(curLeafNode->keyArray[0], this->highValString.c_str(), STRINGSIZE) > 0) {
            // Reached the end of the scan
            this->nextEntry = INVALID_KEY_INDEX;
          } else {
            this->nextEntry = 0;
          }
        }
        this->bufMgr->unPinPage(this->file, this->currentPageNum, false);
      }
    }
  }

  // -----------------------------------------------------------------------------
  // BTreeIndex::endScan
  // -----------------------------------------------------------------------------
  //
  const void BTreeIndex::endScan() {
    if (!this->scanExecuting) {
      throw ScanNotInitializedException();
    }
    this->scanExecuting = false;
    this->nextEntry = INVALID_KEY_INDEX;
    try {
      // Unpin all the pages that were pinned
      this->bufMgr->unPinPage(this->file, this->currentPageNum, true);
    } catch (...) {
    }
  }
}  // namespace badgerdb