/**
 * @author See Contributors.txt for code contributors and overview of BadgerDB.
 *
 * @section LICENSE
 * Copyright (c) 2012 Database Group, Computer Sciences Department, University
 * of Wisconsin-Madison.
 */

#include "btree.h"

#include <algorithm>
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

const int IDEAL_OCCUPANCY = 1;

const int INVALID_KEY = INT_MIN;
const int INVALID_PAGE = INT_MIN;
const int INVALID_KEY_INDEX = INT_MIN;

// -----------------------------------------------------------------------------
// BTreeIndex::BTreeIndex -- Constructor
// -----------------------------------------------------------------------------

const bool fileExists(const std::string &fileName) { File::exists(fileName); }

void BTreeIndex::setLeafOccupancy(const Datatype attrType) {
  if (attrType == Datatype::INTEGER) {
    this->leafOccupancy = INTARRAYLEAFSIZE;
  } else if (attrType == Datatype::DOUBLE) {
    this->leafOccupancy = DOUBLEARRAYLEAFSIZE;
  } else if (attrType == Datatype::STRING) {
    this->leafOccupancy = STRINGARRAYLEAFSIZE;
  }
}

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
  std::string indexName = idxStr.str();
  bool indexFileExists = fileExists(indexName);

  // Initialize member variables
  this->attributeType = attrType;
  this->attrByteOffset = attrByteOffset;
  this->setLeafOccupancy(attrType);
  this->setNodeOccupancy(attrType);
  this->headerPageNum = 1;
  this->isRootLeaf = true;
  this->treeLevel = 0;
  this->bufMgr = bufMgrIn;
  this->scanExecuting = false;
  this->nextEntry = INVALID_KEY_INDEX;

  // Assign buffer manager
  if (indexFileExists) {
    // Validate the parameters of the method with the index file
    // Find out the meta page of the index file
    // Meta page is always the first page of the index file
    BlobFile indexFile = BlobFile::open(indexName);
    this->file = (File *)&indexFile;
    PageId metaPageId = this->headerPageNum;
    Page *metaPage;
    this->bufMgr->readPage(this->file, metaPageId, metaPage);
    // Unpin the page since the page won't be used for writing here after
    this->bufMgr->unPinPage(this->file, metaPageId, metaPage);
    // Cast meta page to IndexMetaInfo
    IndexMetaInfo *indexMetaInfo = (IndexMetaInfo *)(metaPage);
    // Set the root page id
    this->rootPageNum = indexMetaInfo->rootPageNo;
    // Values in metapage (relationName, attribute byte offset, attribute type
    // etc.) must match
    bool relationNameMatch =
        std::string(indexMetaInfo->relationName) == relationName;
    bool attributeByteOffsetMatch =
        indexMetaInfo->attrByteOffset == attrByteOffset;
    bool attrTypeMatch = indexMetaInfo->attrType == attrType;
    if (!(relationNameMatch && attributeByteOffsetMatch && attrTypeMatch)) {
      throw BadIndexInfoException(
          "Parameters passed while creating the index don't match");
    }
  } else {
    // Create the blob file for the index
    this->file = &BlobFile::create(indexName);
    // Create pages for metadata and root (page 1 and 2 repectively)
    PageId metaPageNo;
    Page *metaPage;
    PageId rootPageNo;
    Page *rootPage;
    this->bufMgr->allocPage(this->file, metaPageNo, metaPage);
    this->headerPageNum = metaPageNo;
    // Cast the metaPage into the IndexMetaInfo
    IndexMetaInfo *indexMetaInfo = (IndexMetaInfo *)metaPage;
    this->bufMgr->allocPage(this->file, rootPageNo, rootPage);
    // Write meta data to the meta page
    indexMetaInfo->attrType = attributeType;
    indexMetaInfo->attrByteOffset = attrByteOffset;
    strcpy(indexMetaInfo->relationName, relationName.c_str());
    indexMetaInfo->rootPageNo = rootPageNo;
    // Meta page is modified now, we can unpin this page which will result in
    // disk flushing
    this->bufMgr->unPinPage(this->file, metaPageNo, true);
    // Dont unpin root page since its useful for insert, scans operations.
    // Unpin it in the destructor or when the root node changes

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
  this->isRootLeaf = true;
  this->treeLevel = 0;
  this->scanExecuting = false;
  // Unpin the root page
  this->bufMgr->unPinPage(this->file, this->rootPageNum, true);
  // Call endScan to stop the function
  this->endScan();
  this->bufMgr->flushFile(this->file);
  // Delete blobfile used for the index
  delete this->file;
  File::remove(this->file->filename());
}

template <typename T>
bool BTreeIndex::hasSpaceInLeafNode(T *leafNode) {
  return leafNode->len != IDEAL_OCCUPANCY * this->leafOccupancy;
}

template <typename T>
bool BTreeIndex::hasSpaceInNonLeafNode(T *nonLeafNode) {
  return nonLeafNode->len != IDEAL_OCCUPANCY * this->nodeOccupancy;
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
    }
  }
  if (foundKeyIndex) {
    T tempKeyArray[len + 1];
    PageId tempPageNoArray[len + 1];
    for (int i = 0; i < len; i++) {
      tempKeyArray[i] = keyArray[i];
      tempPageNoArray[i] = pageNoArray[i];
    }
    // Insert key before index keyIndex
    tempKeyArray[keyIndex] = key;
    tempPageNoArray[keyIndex] = pageNo;
    for (int i = keyIndex; i < len; i++) {
      keyArray[i + 1] = tempKeyArray[i];
      pageNoArray[i + 1] = tempPageNoArray[i];
    }
  } else {
    // it means key needs to be inserted at the last
    keyArray[len] = key;
    pageNoArray[len] = pageNo;
  }
}

// -----------------------------------------------------------------------------
// BTreeIndex::insertEntry
// -----------------------------------------------------------------------------

const void BTreeIndex::insertEntry(const void *key, const RecordId rid) {
  // Read the root page (Root page must be cached in the buffer pool)
  PageId rootPageId = this->rootPageNum;
  Page *rootPage;
  bufMgr->readPage(this->file, rootPageId, rootPage);
  // First identify the leaf node
  // Case 1: Root is leaf and ideal occupancy is not attained (Non-split)
  if (this->isRootLeaf) {
    if (this->attributeType == Datatype::INTEGER) {
      // Cast the rootPage to the leaf node
      LeafNodeInt *rootLeafNode = (LeafNodeInt *)rootPage;
      if (hasSpaceInLeafNode<LeafNodeInt>(rootLeafNode)) {
        // Non-split, insert the (key, record)
        // RIDKeyPair<int> ridKeyPair;
        int keyCopy = *(int *)key;
        // ridKeyPair.set(rid, keyCopy);
        // std::string new_data(reinterpret_cast<char *>(&ridKeyPair),
        // sizeof(ridKeyPair));
        insertKeyRidToKeyRidArray<int>(rootLeafNode->keyArray,
                                       rootLeafNode->ridArray,
                                       rootLeafNode->len, keyCopy, rid);
        rootLeafNode->len += 1;
      } else {
        // Split the root node
        this->isRootLeaf = false;
        // Increase the tree level
        this->treeLevel += 1;

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
        newPageLeafNode->level = 1;
        rootLeafNode->level = 1;
        for (int i = middleKeyIndex; i < ridKeyPairVec.size(); i++) {
          int key_ = ridKeyPairVec[i].key;
          RecordId rid_ = ridKeyPairVec[i].rid;
          insertKeyRidToKeyRidArray<int>(newPageLeafNode->keyArray,
                                         newPageLeafNode->ridArray,
                                         newPageLeafNode->len, key_, rid_);
          newPageLeafNode->len += 1;
        }

        rootLeafNode->len = middleKeyIndex;

        // Set next page id of left leaf node
        rootLeafNode->rightSibPageNo = newPageNum;

        // Create root node non leaf page
        Page *rootPage;
        PageId rootPageNum;
        bufMgr->allocPage(this->file, rootPageNum, rootPage);
        NonLeafNodeInt *rootNonLeafNode = (NonLeafNodeInt *)rootPage;
        rootNonLeafNode->level = 0;
        // Copy up the middle key to root;
        rootNonLeafNode->keyArray[0] = middleKey;
        rootNonLeafNode->len = 1;
        rootNonLeafNode->pageNoArray[0] = rootPageId;
        rootNonLeafNode->pageNoArray[1] = newPageNum;

        // Unpin old root page and new leaf page
        bufMgr->unPinPage(this->file, rootPageId, true);
        bufMgr->unPinPage(this->file, newPageNum, true);
      }
    } else if (this->attributeType == Datatype::DOUBLE) {
      // TODO
    } else if (this->attributeType == Datatype::STRING) {
      // TODO
    }
  } else {
    // Case 2: Root is non-leaf
    bool isSplit = false;
    void *splitKey = nullptr;
    PageId splitRightNodePageId;
    insertRecursive(0, rootPageId, key, rid, isSplit, splitKey,
                    splitRightNodePageId);
  }
}

const void BTreeIndex::insertRecursive(int level, PageId nodePageNumber,
                                       const void *key, const RecordId rid,
                                       bool &isSplit, void *splitKey,
                                       PageId &splitRightNodePageId) {
  // Stop when the leaf node is reached
  if (level == this->treeLevel) {
    insertLeaf(nodePageNumber, key, rid, isSplit, splitKey,
               splitRightNodePageId);
  } else {
    // Read current page
    Page *curPage;
    bufMgr->readPage(this->file, nodePageNumber, curPage);
    bufMgr->unPinPage(this->file, nodePageNumber, false);
    if (this->attributeType == Datatype::INTEGER) {
      // Cast it to non leaf
      NonLeafNodeInt *curNode = (NonLeafNodeInt *)curPage;
      int keyCopy = *(int *)key;
      PageId nextPage = -1;
      bool foundKey = false;
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
          nextPage = curNode->pageNoArray[0];
          foundKey = true;
          break;
        } else {
          if (keyCopy > curKey && keyCopy <= nextKey) {
            nextPage = curNode->pageNoArray[i + 1];
            foundKey = true;
            break;
          }
        }
      }
      if (foundKey) {
        insertRecursive(level + 1, nextPage, key, rid, isSplit, splitKey,
                        splitRightNodePageId);
        if (isSplit) {
          int middleKey = *(int *)key;
          insertNonLeaf(nodePageNumber, &middleKey, isSplit, splitKey,
                        splitRightNodePageId);
          return;
        }
      }
    } else if (this->attributeType == Datatype::DOUBLE) {
    } else if (this->attributeType == Datatype::STRING) {
    }

    // insertRecursive(childPageNum, key, rid, isSplit, splitKey);
    // Else recurse
  }
}

const void BTreeIndex::insertLeaf(PageId pageNum, const void *key,
                                  const RecordId rid, bool &isSplit,
                                  void *splitKey,
                                  PageId &splitRightNodePageId) {
  // Read current page
  Page *curPage;
  this->bufMgr->readPage(this->file, pageNum, curPage);
  this->bufMgr->unPinPage(this->file, pageNum, false);
  // Cast to LeafNode
  LeafNodeInt *curLeafNode = (LeafNodeInt *)curPage;
  if (this->attributeType == Datatype::INTEGER) {
    // Check the occupancy of the leaf node
    if (hasSpaceInLeafNode(curLeafNode)) {
      // SubCase 1: Non-Split
      // Insert the (key, record)
      int keyCopy = *(int *)key;
      RecordId ridCopy = rid;
      insertKeyRidToKeyRidArray<int>(curLeafNode->keyArray,
                                     curLeafNode->ridArray, curLeafNode->len,
                                     keyCopy, ridCopy);
      isSplit = false;
    } else {
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
      ridKeyPair.set(rid, *(int *)key);
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
      newPageLeafNode->level = 1;
      curLeafNode->level = 1;
      for (int i = middleKeyIndex; i < ridKeyPairVec.size(); i++) {
        int key_ = ridKeyPairVec[i].key;
        RecordId rid_ = ridKeyPairVec[i].rid;
        insertKeyRidToKeyRidArray<int>(newPageLeafNode->keyArray,
                                       newPageLeafNode->ridArray,
                                       newPageLeafNode->len, key_, rid_);
        newPageLeafNode->len += 1;
      }

      curLeafNode->len = middleKeyIndex;

      // Set next page id of left leaf node
      curLeafNode->rightSibPageNo = newPageNum;

      splitKey = &middleKey;
      isSplit = true;
      splitRightNodePageId = newPageNum;
    }
  } else if (this->attributeType == Datatype::DOUBLE) {
    // TODO
  } else if (this->attributeType == Datatype::STRING) {
    // TODO
  }
}

const void BTreeIndex::insertNonLeaf(PageId nodePageNumber,
                                     const void *middleKey, bool &isSplit,
                                     void *splitKey,
                                     PageId &splitRightNodePageId) {
  // Read current page
  Page *curPage;
  this->bufMgr->readPage(this->file, nodePageNumber, curPage);
  this->bufMgr->unPinPage(this->file, nodePageNumber, false);
  // Cast to NonleafNode
  if (this->attributeType == Datatype::INTEGER) {
    NonLeafNodeInt *curNonLeafNode = (NonLeafNodeInt *)curPage;
    // Check if space exists
    if (hasSpaceInNonLeafNode(curNonLeafNode)) {
      // Insert the key and rightPageId
      int keyCopy = *(int *)middleKey;
      insertKeyPageIdToKeyPageIdArray<int>(
          curNonLeafNode->keyArray, curNonLeafNode->pageNoArray,
          curNonLeafNode->len, keyCopy, splitRightNodePageId);
    } else {
      // Split and move up the middleKey
      Page *newPage;
      PageId newPageNum;
      bufMgr->allocPage(this->file, newPageNum, newPage);

      int halfOffset = curNonLeafNode->len / 2;
      // Cast the page to leaf node
      NonLeafNodeInt *newPageNonLeafNode = (NonLeafNodeInt *)newPage;
      for (int i = halfOffset; i < curNonLeafNode->len; i++) {
        int keyCopy = *(int *)curNonLeafNode->keyArray[i];
        PageId pageIdCopy = curNonLeafNode->pageNoArray[i];
        insertKeyPageIdToKeyPageIdArray<int>(
            newPageNonLeafNode->keyArray, newPageNonLeafNode->pageNoArray,
            newPageNonLeafNode->len, keyCopy, pageIdCopy);
        newPageNonLeafNode->len += 1;
      }

      // Insert current key
      int keyCopy = *(int *)splitKey;
      PageId pageIdCopy = splitRightNodePageId;
      insertKeyPageIdToKeyPageIdArray<int>(
          newPageNonLeafNode->keyArray, newPageNonLeafNode->pageNoArray,
          newPageNonLeafNode->len, keyCopy, pageIdCopy);
      newPageNonLeafNode->len += 1;

      curNonLeafNode->len = halfOffset;

      int middleKey = newPageNonLeafNode->keyArray[0];

      splitKey = (void *)middleKey;
      isSplit = true;
      splitRightNodePageId = newPageNum;
    }
  }
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
  this->scanExecuting = true;
  if (this->attributeType == Datatype::INTEGER) {
    int lowIntValue = *(int *)lowValParm;
    int highIntValue = *(int *)highValParm;
    if (lowIntValue > highIntValue) {
      throw BadScanrangeException();
    }
    int curLevel = 0;
    PageId rootPageId = this->rootPageNum;
    Page *rootPage;
    bufMgr->readPage(this->file, rootPageId, rootPage);
    if (isRootLeaf) {
      LeafNodeInt *rootLeafNode = (LeafNodeInt *)rootPage;
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
        throw NoSuchKeyFoundException();
      }
    } else {
      int curPageNum = this->rootPageNum;
      // Navigate till the leaf node
      while (curLevel <= this->treeLevel) {
        Page *curPage;
        bufMgr->readPage(this->file, curPageNum, curPage);
        // Unpin each page except the leaf page
        if (curLevel != this->treeLevel) {
          bufMgr->unPinPage(this->file, curPageNum, false);
        }
        if (curLevel != this->treeLevel) {
          // Cast to non leaf node
          NonLeafNodeInt *curNonLeafNode = (NonLeafNodeInt *)curPageNum;
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
        } else {
          // Reached the leaf node
          LeafNodeInt *curLeafNode = (LeafNodeInt *)curPageNum;
          // Iterate over the leaf nodes and its siblings until a key is found
          // satisfying the criteria or the end of index is reached
          while (true) {
            // Scan current page
            // Its possible that current page doesn't contain any satisfying
            // key, in which case move to next sibling (eg 10, 11, 12 are the
            // keys in current page and lowIntValue - 14)
            bool satisfyingKeyFound = false;
            for (int i = 0; i < curLeafNode->len; i++) {
              if (curLeafNode->keyArray[i] >= lowIntValue) {
                this->nextEntry = i;
                this->currentPageData = curPage;
                satisfyingKeyFound = true;
                break;
              }
            }
            if (!satisfyingKeyFound) {
              // Unpin current page
              PageId nextPageNo = curLeafNode->rightSibPageNo;
              bufMgr->unPinPage(this->file, curPageNum, false);
              curPageNum = nextPageNo;
              if (nextPageNo == INVALID_PAGE) {
                throw NoSuchKeyFoundException();
              }
              bufMgr->readPage(this->file, curPageNum, curPage);
              curLeafNode = (LeafNodeInt *)curPage;
            }
          }
        }
        curLevel += 1;
      }
    }
  } else if (this->attributeType == Datatype::DOUBLE) {
    // Complete TODO
  } else if (this->attributeType == Datatype::STRING) {
    // Complete TODO
  }
}

// -----------------------------------------------------------------------------
// BTreeIndex::scanNext
// -----------------------------------------------------------------------------

const void BTreeIndex::scanNext(RecordId &outRid) {
  if (!scanExecuting) {
    throw ScanNotInitializedException();
  }
  // Check if nextEntry is valid or not (points to valid entry in the page or
  // not)
  if (this->nextEntry == INT_MIN) {
    throw IndexScanCompletedException();
  }
  if (this->attributeType == Datatype::INTEGER) {
    // Cast the curPage to leaf page node
    LeafNodeInt *curLeafNode = (LeafNodeInt *)this->currentPageData;
    // Read the nextEntry, its valid now since we are validating it in the
    // function start
    const RecordId entryRid = curLeafNode->ridArray[nextEntry];
    outRid = entryRid;
    // Update the nextEntry member
    if (this->nextEntry < curLeafNode->len) {
      // Fetch the record from this page
      // Check if the nextEntry matches the scan criteria
      if (this->highOp == LT) {
        if (curLeafNode->keyArray[nextEntry] >= this->highValInt) {
          // Reached the end of the scan
          nextEntry = INT_MIN;
        } else {
          nextEntry += 1;
        }
      } else if (this->highOp == LTE) {
        if (curLeafNode->keyArray[nextEntry] > this->highValInt) {
          // Reached the end of the scan
          nextEntry = INT_MIN;
        } else {
          nextEntry += 1;
        }
      }
    } else {
      // Reached the end of the current page, need to read the sibling page
      if (curLeafNode->rightSibPageNo == -1) {
        // Reached the end
        throw IndexScanCompletedException();
      }
      PageId siblingPageNo = curLeafNode->rightSibPageNo;
      // Unpin the current page
      this->bufMgr->unPinPage(this->file, this->currentPageNum, false);
      this->currentPageNum = siblingPageNo;
      // Read the sibling page and keep it pinned
      this->bufMgr->readPage(this->file, this->currentPageNum,
                             this->currentPageData);
      // Cast the page to leaf node
      curLeafNode = (LeafNodeInt *)this->currentPageData;
      // Check if the first entry of the new sibling page is valid or not as per
      // scan critiera
      if (this->highOp == LT) {
        if (curLeafNode->keyArray[0] >= this->highValInt) {
          // Reached the end of the scan
          nextEntry = INT_MIN;
        } else {
          nextEntry = 0;
        }
      } else if (this->highOp == LTE) {
        if (curLeafNode->keyArray[0] > this->highValInt) {
          // Reached the end of the scan
          nextEntry = INT_MIN;
        } else {
          nextEntry = 0;
        }
      }
      outRid = curLeafNode->ridArray[0];
      // Move to the next sibling
      this->nextEntry = 1;
    }
  } else if (this->attributeType == Datatype::DOUBLE) {
  } else if (this->attributeType == Datatype::STRING) {
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
  // Unpin all the pages that were pinned
  this->bufMgr->unPinPage(this->file, this->currentPageNum, true);
}
}  // namespace badgerdb
