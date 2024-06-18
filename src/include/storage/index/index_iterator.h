//===----------------------------------------------------------------------===//
//
//                         CMU-DB Project (15-445/645)
//                         ***DO NO SHARE PUBLICLY***
//
// Identification: src/include/index/index_iterator.h
//
// Copyright (c) 2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
/**
 * index_iterator.h
 * For range scan of b+ tree
 */
#pragma once
#include "storage/page/b_plus_tree_leaf_page.h"

namespace bustub {

#define INDEXITERATOR_TYPE IndexIterator<KeyType, ValueType, KeyComparator>

INDEX_TEMPLATE_ARGUMENTS
class IndexIterator {
  using LeafPage = BPlusTreeLeafPage<KeyType, ValueType, KeyComparator>;

 public:
  // you may define your own constructor based on your member variables
  IndexIterator();
  ~IndexIterator();  // NOLINT
  IndexIterator(BufferPoolManager *bpm, ReadPageGuard guard, int pos);

  auto IsEnd() -> bool;

  auto operator*() -> const MappingType &;

  auto operator++() -> IndexIterator &;

  auto operator==(const IndexIterator &itr) const -> bool {
    if (is_valid_ != itr.is_valid_) {
      return false;
    }

    if (!is_valid_) {
      return true;
    }
    return cur_leaf_page_ == itr.cur_leaf_page_ && cur_pos_ == itr.cur_pos_;
  }

  auto operator!=(const IndexIterator &itr) const -> bool { return !operator==(itr); }

 private:
  // add your own private member variables here
  BufferPoolManager *bpm_{nullptr};
  ReadPageGuard guard_;
  const LeafPage *cur_leaf_page_{nullptr};
  int cur_pos_{0};
  MappingType cur_pair_{};
  bool is_valid_{false};
};

}  // namespace bustub
