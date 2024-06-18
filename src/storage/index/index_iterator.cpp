/**
 * index_iterator.cpp
 */
#include <cassert>

#include "storage/index/index_iterator.h"

namespace bustub {

/*
 * NOTE: you can change the destructor/constructor method here
 * set your own input parameters
 */
INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE::IndexIterator() = default;

INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE::~IndexIterator() = default;  // NOLINT

INDEX_TEMPLATE_ARGUMENTS
auto INDEXITERATOR_TYPE::IsEnd() -> bool {
  // throw std::runtime_error("unimplemented");
  return !is_valid_ || cur_leaf_page_->GetSize() == 0;
}

INDEX_TEMPLATE_ARGUMENTS
auto INDEXITERATOR_TYPE::operator*() -> const MappingType & {
  // throw std::runtime_error("unimplemented");
  BUSTUB_ASSERT(!IsEnd(), "Iterator already reach the end!");

  cur_pair_.first = cur_leaf_page_->KeyAt(cur_pos_);
  cur_pair_.second = cur_leaf_page_->ValueAt(cur_pos_);
  return cur_pair_;
}

INDEX_TEMPLATE_ARGUMENTS
auto INDEXITERATOR_TYPE::operator++() -> INDEXITERATOR_TYPE & {
  // throw std::runtime_error("unimplemented");
  BUSTUB_ASSERT(!IsEnd(), "Iter already reach the end!");

  if (cur_pos_ == cur_leaf_page_->GetSize() - 1) {
    if (cur_leaf_page_->GetNextPageId() == INVALID_PAGE_ID) {
      guard_.Drop();
      is_valid_ = false;
    } else {
      guard_ = bpm_->FetchPageRead(cur_leaf_page_->GetNextPageId());
      if (guard_.IsValid()) {
        cur_leaf_page_ = guard_.template As<LeafPage>();
        cur_pos_ = 0;
        is_valid_ = true;
      } else {
        is_valid_ = false;
      }
    }
  } else {
    ++cur_pos_;
  }

  return *this;
}

INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE::IndexIterator(BufferPoolManager *bpm, ReadPageGuard guard, int pos)
    : bpm_(bpm), guard_(std::move(guard)), cur_pos_(pos) {
  if (bpm_ != nullptr) {
    if (guard_.IsValid()) {
      cur_leaf_page_ = guard_.template As<LeafPage>();
      is_valid_ = true;
    }
  }
}

template class IndexIterator<GenericKey<4>, RID, GenericComparator<4>>;

template class IndexIterator<GenericKey<8>, RID, GenericComparator<8>>;

template class IndexIterator<GenericKey<16>, RID, GenericComparator<16>>;

template class IndexIterator<GenericKey<32>, RID, GenericComparator<32>>;

template class IndexIterator<GenericKey<64>, RID, GenericComparator<64>>;

}  // namespace bustub
