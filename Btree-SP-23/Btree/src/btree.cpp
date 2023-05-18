/**
 * @author See Contributors.txt for code contributors and overview of BadgerDB.
 *
 * @section LICENSE
 * Copyright (c) 2012 Database Group, Computer Sciences Department, University of Wisconsin-Madison.
 */

#include "btree.h"
#include "filescan.h"
#include "exceptions/bad_index_info_exception.h"
#include "exceptions/bad_opcodes_exception.h"
#include "exceptions/bad_scanrange_exception.h"
#include "exceptions/no_such_key_found_exception.h"
#include "exceptions/scan_not_initialized_exception.h"
#include "exceptions/index_scan_completed_exception.h"
#include "exceptions/file_not_found_exception.h"
#include "exceptions/end_of_file_exception.h"

// #define DEBUG

namespace badgerdb
{

	const int IDEAL_OCCUPANCY = 0.67;

	// -----------------------------------------------------------------------------
	// BTreeIndex::BTreeIndex -- Constructor
	// -----------------------------------------------------------------------------

	const bool fileExists(const std::string &fileName)
	{
		File::exists(fileName);
	}

	void BTreeIndex::setLeafOccupancy(const Datatype attrType)
	{
		if (attrType == Datatype::INTEGER)
		{
			this->leafOccupancy = INTARRAYLEAFSIZE;
		}
		else if (attrType == Datatype::DOUBLE)
		{
			this->leafOccupancy = DOUBLEARRAYLEAFSIZE;
		}
		else if (attrType == Datatype::STRING)
		{
			this->leafOccupancy = STRINGARRAYLEAFSIZE;
		}
	}

	void BTreeIndex::setNodeOccupancy(const Datatype attrType)
	{
		if (attrType == Datatype::INTEGER)
		{
			this->nodeOccupancy = INTARRAYNONLEAFSIZE;
		}
		else if (attrType == Datatype::DOUBLE)
		{
			this->nodeOccupancy = DOUBLEARRAYNONLEAFSIZE;
		}
		else if (attrType == Datatype::STRING)
		{
			this->nodeOccupancy = STRINGARRAYNONLEAFSIZE;
		}
	}

	BTreeIndex::BTreeIndex(const std::string &relationName,
						   std::string &outIndexName,
						   BufMgr *bufMgrIn,
						   const int attrByteOffset,
						   const Datatype attrType)
	{
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
		this->headerPageNum = 0;
		this->rootPageNum = 1;
		this->isRootLeaf = true;
		this->treeLevel = 0;
		this->bufMgr = bufMgr;

		// Assign buffer manager
		if (indexFileExists)
		{
			// Validate the parameters of the method with the index file
			// Find out the meta page of the index file
			// Meta page is always the first page of the index file
			BlobFile indexFile = BlobFile::open(indexName);
			this->file = &indexFile;
			PageId metaPageId = this->headerPageNum;
			Page metaPage = indexFile.readPage(metaPageId);
			// Cast meta page to IndexMetaInfo
			IndexMetaInfo *indexMetaInfo = (IndexMetaInfo *)(&metaPage);
			// Values in metapage (relationName, attribute byte offset, attribute type etc.) must match
			bool relationNameMatch = indexMetaInfo->relationName == relationName;
			bool attributeByteOffsetMatch = indexMetaInfo->attrByteOffset == attrByteOffset;
			bool attrTypeMatch = indexMetaInfo->attrType == attrType;
			if (!(relationNameMatch && attributeByteOffsetMatch && attrTypeMatch))
			{
				throw BadIndexInfoException("Parameters don't match");
			}
		}
		else
		{
			// Create the blob file for the index
			this->file = &BlobFile::create(indexName);
			FileScan fscan(relationName, bufMgr);
			try
			{
				RecordId scanRid;
				while (true)
				{
					fscan.scanNext(scanRid);
					std::string recordStr = fscan.getRecord();
					const char *record = recordStr.c_str();
					void *key = nullptr;
					if (this->attributeType == Datatype::INTEGER)
					{
						key = ((int *)(record + attrByteOffset));
					}
					else if (this->attributeType == Datatype::DOUBLE)
					{
						key = ((double *)(record + attrByteOffset));
					}
					else if (this->attributeType == Datatype::STRING)
					{
						key = ((char *)(record + attrByteOffset));
					}
					this->insertEntry((void *)key, scanRid);
				}
			}
			catch (EndOfFileException e)
			{
				std::cout << "Read all records" << std::endl;
			}
		}
	}

	// -----------------------------------------------------------------------------
	// BTreeIndex::~BTreeIndex -- destructor
	// -----------------------------------------------------------------------------

	BTreeIndex::~BTreeIndex()
	{
		// Clear up state variables
		this->isRootLeaf = true;
		this->treeLevel = 0;
		this->scanExecuting = false;
		// Call endScan to stop the function
		this->endScan();
		this->bufMgr->flushFile(this->file);
		// Delete blobfile used for the index
		File::remove(this->file->filename());
	}

	template <typename T>
	bool BTreeIndex::hasSpaceInLeafNode(T *leafNode)
	{
		return leafNode->len != IDEAL_OCCUPANCY * this->leafOccupancy;
	}

	template <typename T>
	bool BTreeIndex::hasSpaceInNonLeafNode(T *nonLeafNode)
	{
		return nonLeafNode->len != IDEAL_OCCUPANCY * this->nodeOccupancy;
	}

	template <class T>
	void insertKeyRidToKeyRidArray(T keyArray[], RecordId ridArray[], int len, T key, const RecordId rid)
	{
		if (len == 0)
		{
			keyArray[0] = key;
			ridArray[0] = rid;
			return;
		}
		bool foundKeyIndex = false;
		int keyIndex = -1; // keyIndex is the index just before which to insert the given key
		for (int i = 0; i < len; i++)
		{
			if (keyArray[i] >= key)
			{
				keyIndex = i;
				foundKeyIndex = true;
			}
		}
		if (foundKeyIndex)
		{
			int tempKeyArray[len + 1];
			int tempRidArray[len + 1];
			for (int i = 0; i < len; i++)
			{
				tempKeyArray[i] = keyArray[i];
				tempRidArray[i] = ridArray[i];
			}
			// Insert key before index keyIndex
			keyArray[keyIndex] = key;
			ridArray[keyIndex] = rid;
			for (int i = keyIndex; i < len; i++)
			{
				keyArray[i + 1] = tempKeyArray[i];
				ridArray[i + 1] = ridArray[i];
			}
		}
		else
		{
			// it means key needs to be inserted at the last
			keyArray[len] = key;
			ridArray[len] = rid;
		}
	}

	template <class T>
	void insertKeyPageIdToKeyPageIdArray(T keyArray[], PageId pageNoArray[], int len, T key, PageId pageId)
	{
		bool foundKeyIndex = false;
		int keyIndex = -1; // keyIndex is the index just before which to insert the given key
		for (int i = 0; i < len; i++)
		{
			if (keyArray[i] >= key)
			{
				keyIndex = i;
				foundKeyIndex = true;
			}
		}
		if (foundKeyIndex)
		{
			int tempKeyArray[len + 1];
			int tempRidArray[len + 1];
			for (int i = 0; i < len; i++)
			{
				tempKeyArray[i] = keyArray[i];
				tempRidArray[i] = ridArray[i];
			}
			// Insert key before index keyIndex
			keyArray[keyIndex] = key;
			ridArray[keyIndex] = rid;
			for (int i = keyIndex; i < len; i++)
			{
				keyArray[i + 1] = tempKeyArray[i];
				ridArray[i + 1] = ridArray[i];
			}
		}
		else
		{
			// it means key needs to be inserted at the last
			keyArray[len] = key;
			ridArray[len] = rid;
		}
	}

	// -----------------------------------------------------------------------------
	// BTreeIndex::insertEntry
	// -----------------------------------------------------------------------------

	const void BTreeIndex::insertEntry(const void *key, const RecordId rid)
	{
		// Read the root page
		PageId rootPageId = this->rootPageNum;
		Page *rootPage;
		bufMgr->readPage(this->file, rootPageId, rootPage);
		bufMgr->unPinPage(this->file, rootPageId, true);
		// First identify the leaf node
		// Case 1: Root is leaf and ideal occupancy is not attained (Non-split)
		if (this->isRootLeaf)
		{
			if (this->attributeType == Datatype::INTEGER)
			{
				// Cast the rootPage to the leaf node
				LeafNodeInt *rootLeafNode = (LeafNodeInt *)rootPage;
				if (hasSpaceInLeafNode<LeafNodeInt>(rootLeafNode))
				{
					// Non-split, insert the (key, record)
					RIDKeyPair<int> ridKeyPair;
					int keyCopy = *(int *)key;
					ridKeyPair.set(rid, keyCopy);
					// std::string new_data(reinterpret_cast<char *>(&ridKeyPair), sizeof(ridKeyPair));
					insertKeyRidToKeyRidArray<int>(rootLeafNode->keyArray, rootLeafNode->ridArray, rootLeafNode->len, keyCopy, rid);
				}
				else
				{
					// Split the root node
					this->isRootLeaf = false;
					// Increase the tree level
					this->treeLevel += 1;
					// Create new page
					Page *newPage;
					PageId newPageNum;
					bufMgr->allocPage(this->file, newPageNum, newPage);
					// Move half the (key, recordID) to the new node
					// Cast the page to leaf node
					LeafNodeInt *newPageLeafNode = (LeafNodeInt *)newPage;
					newPageLeafNode->len = 0;
					int halfOffset = rootLeafNode->len / 2;
					for (int i = halfOffset; i < rootLeafNode->len; i++)
					{
						int keyCopy = rootLeafNode->keyArray[i];
						RecordId ridCopy = rootLeafNode->ridArray[i];
						insertKeyRidToKeyRidArray<int>(newPageLeafNode->keyArray, newPageLeafNode->ridArray, newPageLeafNode->len, keyCopy, ridCopy);
						newPageLeafNode->len += 1;
					}

					// Insert current key
					int keyCopy = *(int *)key;
					RecordId ridCopy = rid;
					insertKeyRidToKeyRidArray<int>(newPageLeafNode->keyArray, newPageLeafNode->ridArray, newPageLeafNode->len, keyCopy, ridCopy);
					newPageLeafNode->len += 1;

					rootLeafNode->len = halfOffset;

					// Set next page id of left leaf node
					rootLeafNode->rightSibPageNo = newPageNum;

					int middleKey = newPageLeafNode->keyArray[0];

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
				}
			}
			else if (this->attributeType == Datatype::DOUBLE)
			{
			}
			else if (this->attributeType == Datatype::STRING)
			{
			}
		}
		else
		{
			// Case 2: Root is non-leaf
			bool isSplit = false;
			void *splitKey = nullptr;
			if (this->attributeType == Datatype::INTEGER)
			{
				insertRecursive(0, rootPageId, key, rid, isSplit, splitKey);
			}
			else if (this->attributeType == Datatype::DOUBLE)
			{
				insertRecursive(0, rootPageId, key, rid, isSplit, splitKey);
			}
			else if (this->attributeType == Datatype::STRING)
			{
				insertRecursive(0, rootPageId, key, rid, isSplit, splitKey);
			}
		}
	}

	const void BTreeIndex::insertRecursive(int level, PageId nodePageNumber, const void *key, const RecordId rid, bool &isSplit, void *splitKey, PageId &splitRightNodePageId)
	{
		// Stop when the leaf node is reached
		if (level == this->treeLevel)
		{
			insertLeaf(nodePageNumber, key, rid, isSplit, splitKey, splitRightNodePageId);
		}
		else
		{
			// Read current page
			Page *curPage;
			bufMgr->readPage(this->file, nodePageNumber, curPage);
			bufMgr->unPinPage(this->file, nodePageNumber, true);
			if (this->attributeType == Datatype::INTEGER)
			{
				// Cast it to non leaf
				NonLeafNodeInt *curNode = (NonLeafNodeInt *)curPage;
				int keyCopy = *(int *)key;
				PageId nextPage = -1;
				bool foundKey = false;
				for (int i = 0; i < curNode->len; i++)
				{
					int curKey = curNode->keyArray[i];
					int nextKey = -1;
					if (i == curNode->len - 1)
					{
						nextKey = INT_MAX;
					}
					else
					{
						nextKey = curNode->keyArray[i + 1];
					}
					if (i == 0 && keyCopy < curKey)
					{
						// Insert in the left of the first key
						nextPage = curNode->pageNoArray[0];
						foundKey = true;
						break;
					}
					else
					{
						if (keyCopy > curKey && keyCopy <= nextKey)
						{
							nextPage = curNode->pageNoArray[i + 1];
							foundKey = true;
							break;
						}
					}
				}
				if (foundKey)
				{
					insertRecursive(level + 1, nextPage, key, rid, isSplit, splitKey, splitRightNodePageId);
					if (isSplit)
					{
						int middleKey = *(int *)key;
						insertNonLeaf(nodePageNumber, &middleKey, isSplit, splitKey, splitRightNodePageId);
						return;
					}
				}
			}
			else if (this->attributeType == Datatype::DOUBLE)
			{
			}
			else if (this->attributeType == Datatype::STRING)
			{
			}

			// insertRecursive(childPageNum, key, rid, isSplit, splitKey);
			// Else recurse
		}
	}

	const void BTreeIndex::insertLeaf(PageId pageNum, const void *key, const RecordId rid, bool &isSplit, void *splitKey, PageId &splitRightNodePageId)
	{
		// Read current page
		Page *curPage;
		this->bufMgr->readPage(this->file, pageNum, curPage);
		this->bufMgr->unPinPage(this->file, pageNum, true);
		// Cast to LeafNode
		LeafNodeInt *curLeafNode = (LeafNodeInt *)curPage;
		// Check the occupancy of the leaf node
		if (hasSpaceInLeafNode(curLeafNode))
		{
			// SubCase 1: Non-Split
			// Insert the (key, record)
			int keyCopy = *(int *)key;
			RecordId ridCopy = rid;
			insertKeyRidToKeyRidArray<int>(curLeafNode->keyArray, curLeafNode->ridArray, curLeafNode->len, keyCopy, ridCopy);
			isSplit = false;
		}
		else
		{
			// SubCase 2: Split the leaf-node
			// Create another page and move half the (key, recordID) to that node
			Page *newPage;
			PageId newPageNum;
			bufMgr->allocPage(this->file, newPageNum, newPage);

			int halfOffset = curLeafNode->len / 2;
			// Cast the page to leaf node
			LeafNodeInt *newPageLeafNode = (LeafNodeInt *)newPage;
			for (int i = halfOffset; i < curLeafNode->len; i++)
			{
				int keyCopy = *(int *)curLeafNode->keyArray[i];
				RecordId ridCopy = curLeafNode->ridArray[i];
				insertKeyRidToKeyRidArray<int>(newPageLeafNode->keyArray, newPageLeafNode->ridArray, newPageLeafNode->len, keyCopy, ridCopy);
				newPageLeafNode->len += 1;
			}

			// Insert current key
			int keyCopy = *(int *)key;
			RecordId ridCopy = rid;
			insertKeyRidToKeyRidArray<int>(newPageLeafNode->keyArray, newPageLeafNode->ridArray, newPageLeafNode->len, keyCopy, ridCopy);
			newPageLeafNode->len += 1;

			curLeafNode->len = halfOffset;

			// Set next page id of left leaf node
			curLeafNode->rightSibPageNo = newPageNum;

			int middleKey = newPageLeafNode->keyArray[0];

			splitKey = (void *)middleKey;
			isSplit = true;
			splitRightNodePageId = newPageNum;
		}
	}

	const void BTreeIndex::insertNonLeaf(PageId nodePageNumber, const void *middleKey, bool &isSplit, void *splitKey, PageId &splitRightNodePageId)
	{
		// Read current page
		Page *curPage;
		this->bufMgr->readPage(this->file, nodePageNumber, curPage);
		this->bufMgr->unPinPage(this->file, nodePageNumber, true);
		// Cast to NonleafNode
		if (this->attributeType == Datatype::INTEGER)
		{
			NonLeafNodeInt *curNonLeafNode = (NonLeafNodeInt *)curPage;
			// Check if space exists
			if (hasSpaceInNonLeafNode(curNonLeafNode))
			{
				// Insert the key and rightPageId
				insertKeyPageIdToKeyPageIdArray<int>(curNonLeafNode->keyArray, curNonLeafNode->pageNoArray, middleKey, splitRightNodePageId);
			}
			else
			{
				// Split
			}
		}
	}

	// -----------------------------------------------------------------------------
	// BTreeIndex::startScan
	// -----------------------------------------------------------------------------

	const void BTreeIndex::startScan(const void *lowValParm,
									 const Operator lowOpParm,
									 const void *highValParm,
									 const Operator highOpParm)
	{
		this->scanExecuting = true;
		if (this->attributeType == Datatype::INTEGER)
		{
			int lowIntValue = *(int *)lowValParm;
			int highIntValue = *(int *)highValParm;
			if (lowIntValue > highIntValue)
			{
				throw BadScanrangeException();
			}
			int curLevel = 0;
			PageId rootPageId = this->rootPageNum;
			Page *rootPage;
			bufMgr->readPage(this->file, rootPageId, rootPage);
			bufMgr->unPinPage(this->file, rootPageId, true);
			if (isRootLeaf)
			{
				if (this->attributeType == Datatype::INTEGER)
				{
					LeafNodeInt *rootLeafNode = (LeafNodeInt *)rootPage;
					for (int i = 0; i < leafOccupancy; i++)
					{
						if (rootLeafNode->keyArray[i] >= lowIntValue)
						{
							this->nextEntry = i;
						}
					}
				}
				else if (this->attributeType == Datatype::DOUBLE)
				{
					// TODO
				}
				else if (this->attributeType == Datatype::STRING)
				{
					// TODO
				}
			}
			else
			{
				int curPageNum = this->rootPageNum;
				while (curLevel < this->treeLevel)
				{
					Page *curPage;
					bufMgr->readPage(this->file, curPageNum, curPage);
					bufMgr->unPinPage(this->file, curPageNum, true);
					// Navigate till the leaf node
					if (curLevel != this->treeLevel)
					{
						if (this->attributeType == Datatype::INTEGER)
						{
							NonLeafNodeInt *curNonLeafNode = (NonLeafNodeInt *)curPageNum;
							for (int i = 0; i < this->nodeOccupancy; i++)
							{
								if (curNonLeafNode->keyArray[i] >= lowIntValue)
								{
									curPageNum = curNonLeafNode->pageNoArray[i];
									break;
								}
							}
						}
						else if (this->attributeType == Datatype::DOUBLE)
						{
							// TODO
						}
						else if (this->attributeType == Datatype::STRING)
						{
							// TODO
						}
					}
					else
					{
						// Reached the leaf node
						LeafNodeInt *curLeafNode = (LeafNodeInt *)curPageNum;
						for (int i = 0; i < this->leafOccupancy; i++)
						{
							if (curLeafNode->keyArray[i] >= lowIntValue)
							{
								this->nextEntry = i;
								break;
							}
						}
					}
					curLevel += 1;
				}
			}
		}
	}

	// -----------------------------------------------------------------------------
	// BTreeIndex::scanNext
	// -----------------------------------------------------------------------------

	const void BTreeIndex::scanNext(RecordId &outRid)
	{
		if (this->nextEntry < this->leafOccupancy)
		{
			// Fetch the record from this entry
			// Cast currentPageData to appropriate leafNode struct
			if (this->attributeType == Datatype::INTEGER)
			{
				LeafNodeInt leafNodeInt = *(LeafNodeInt *)(this->currentPageData);
			}
		}
		else
		{
			// Move to the next sibling
			// Set the nextEntry to 1
		}
	}

	// -----------------------------------------------------------------------------
	// BTreeIndex::endScan
	// -----------------------------------------------------------------------------
	//
	const void BTreeIndex::endScan()
	{
		if (!this->scanExecuting)
		{
			throw ScanNotInitializedException();
		}
		this->scanExecuting = false;
		// Unpin all the pages that were pinned
	}
}
