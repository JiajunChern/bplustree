//===----------------------------------------------------------------------===//
//
//                         CMU-DB Project (15-445/645)
//                         ***DO NO SHARE PUBLICLY***
//
// Identification: src/page/b_plus_tree_internal_page.cpp
//
// Copyright (c) 2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <iostream>
#include <sstream>

#include "common/exception.h"
#include "storage/page/b_plus_tree_internal_page.h"

namespace bustub {
/*****************************************************************************
 * HELPER METHODS AND UTILITIES
 *****************************************************************************/
/*
 * Init method after creating a new internal page
 * Including set page type, set current size, and set max page size
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::Init(int max_size) {
  SetPageType(IndexPageType::INTERNAL_PAGE);
  SetMaxSize(max_size);
  SetSize(0);
}
/*
 * Helper method to get/set the key associated with input "index"(a.k.a
 * array offset)
 */
INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::KeyAt(int index) const -> KeyType {
  // replace with your own code
  //  BUSTUB_ASSERT(index >= 0 && index < GetSize(), "Invalid page index!");
  if (index < 0 || index >= GetSize()) {
    return KeyType{};
  }

  KeyType key = array_[index].first;
  return key;
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::SetKeyAt(int index, const KeyType &key) {
  //  BUSTUB_ASSERT(index >= 0 && index < GetSize(), "Invalid page index!");
  if (index < 0 || index >= GetSize()) {
    return;
  }
  array_[index].first = key;
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::SetValueAt(int index, const ValueType &value) {
  //  BUSTUB_ASSERT(index >= 0 && index < GetSize(), "Invalid page index!");
  if (index < 0 || index >= GetSize()) {
    return;
  }
  array_[index].second = value;
}

/*
 * Helper method to get the value associated with input "index"(a.k.a array
 * offset)
 */
INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::ValueAt(int index) const -> ValueType {
  //  BUSTUB_ASSERT(index >= 0 && index < GetSize(), "Invalid page index!");
  if (index < 0 || index >= GetSize()) {
    return ValueType{};
  }
  ValueType value = array_[index].second;
  return value;
}

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::Insert(int index, const KeyType &key, const ValueType &value) -> void {
  int size = GetSize();
  if (size != index) {
    memmove(reinterpret_cast<void *>(&array_[index + 1]), reinterpret_cast<void *>(&(array_[index])),
            (size - index) * sizeof(MappingType));
  }
  array_[index].first = key;
  array_[index].second = value;
  IncreaseSize(1);
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::Delete(int index) {
  BUSTUB_ASSERT(index >= 0 && index < GetSize(), "Invalid page index!");

  int size = GetSize();
  memmove(reinterpret_cast<void *>(&array_[index]), reinterpret_cast<void *>(&(array_[index + 1])),
          (size - index - 1) * sizeof(MappingType));
  IncreaseSize(-1);
}

// valuetype for internalNode should be page id_t
template class BPlusTreeInternalPage<GenericKey<4>, page_id_t, GenericComparator<4>>;
template class BPlusTreeInternalPage<GenericKey<8>, page_id_t, GenericComparator<8>>;
template class BPlusTreeInternalPage<GenericKey<16>, page_id_t, GenericComparator<16>>;
template class BPlusTreeInternalPage<GenericKey<32>, page_id_t, GenericComparator<32>>;
template class BPlusTreeInternalPage<GenericKey<64>, page_id_t, GenericComparator<64>>;
}  // namespace bustub
