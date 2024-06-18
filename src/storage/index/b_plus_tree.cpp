#include <sstream>
#include <string>

#include "common/exception.h"
#include "common/logger.h"
#include "common/rid.h"
#include "storage/index/b_plus_tree.h"

namespace bustub {

INDEX_TEMPLATE_ARGUMENTS
BPLUSTREE_TYPE::BPlusTree(std::string name, page_id_t header_page_id, BufferPoolManager *buffer_pool_manager,
                          const KeyComparator &comparator, int leaf_max_size, int internal_max_size)
    : index_name_(std::move(name)),
      bpm_(buffer_pool_manager),
      comparator_(std::move(comparator)),
      leaf_max_size_(leaf_max_size),
      internal_max_size_(internal_max_size),
      header_page_id_(header_page_id) {
  WritePageGuard guard = bpm_->FetchPageWrite(header_page_id_);
  auto header_page = guard.AsMut<BPlusTreeHeaderPage>();
  header_page->root_page_id_ = INVALID_PAGE_ID;
}

/*
 * Helper function to decide whether current b+tree is empty
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::IsEmpty() const -> bool {
  ReadPageGuard header_guard = bpm_->FetchPageRead(header_page_id_);
  auto header_page = header_guard.As<BPlusTreeHeaderPage>();
  if (header_page->root_page_id_ == INVALID_PAGE_ID) {
    return true;
  }

  ReadPageGuard root_guard = bpm_->FetchPageRead(header_page->root_page_id_);
  auto root_page = root_guard.As<BPlusTreePage>();

  return root_page->IsLeafPage() && root_page->GetSize() == 0;
}
/*****************************************************************************
 * SEARCH
 *****************************************************************************/
/*
 * Return the only value that associated with input key
 * This method is used for point query
 * @return : true means key exists
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::GetValue(const KeyType &key, std::vector<ValueType> *result, Transaction *txn) -> bool {
  if (IsEmpty()) {
    return false;
  }

  // Declaration of context instance.
  Context<KeyType> ctx;
  // (void)ctx;
  ReadPageGuard header_guard = bpm_->FetchPageRead(header_page_id_);
  auto header_page = header_guard.As<BPlusTreeHeaderPage>();

  return SearchValueOfKey(ctx, header_page->root_page_id_, key, result);
}

/*****************************************************************************
 * INSERTION
 *****************************************************************************/
/*
 * Insert constant key & value pair into b+ tree
 * if current tree is empty, start new tree, update root page id and insert
 * entry, otherwise insert into leaf page.
 * @return: since we only support unique key, if user try to insert duplicate
 * keys return false, otherwise return true.
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::Insert(const KeyType &key, const ValueType &value, Transaction *txn) -> bool {
  // Declaration of context instance.
  Context<KeyType> ctx;

  // WritePageGuard header_guard = bpm_->FetchPageWrite(header_page_id_);
  ctx.header_page_ = std::make_optional<WritePageGuard>(bpm_->FetchPageWrite(header_page_id_));
  auto header_page = ctx.header_page_.value().template AsMut<BPlusTreeHeaderPage>();
  if (header_page->root_page_id_ == INVALID_PAGE_ID) {
    BasicPageGuard root_page_guard = bpm_->NewPageGuarded(&(header_page->root_page_id_));
    auto *root_page = root_page_guard.template AsMut<LeafPage>();
    root_page->Init(leaf_max_size_);
    root_page->Insert(0, key, value);
    header_page->root_page_id_ = root_page_guard.PageId();
    return true;
  }

  ctx.root_page_id_ = header_page->root_page_id_;
  ctx.write_set_.push_back(bpm_->FetchPageWrite(ctx.root_page_id_));
  bool res = InsertImpl(ctx, key, value);
  ctx.header_page_ = std::nullopt;
  return res;
}

/*****************************************************************************
 * REMOVE
 *****************************************************************************/
/*
 * Delete key & value pair associated with input key
 * If current tree is empty, return immediately.
 * If not, User needs to first find the right leaf page as deletion target, then
 * delete entry from leaf page. Remember to deal with redistribute or merge if
 * necessary.
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::Remove(const KeyType &key, Transaction *txn) {
  if (IsEmpty()) {
    return;
  }

  // Declaration of context instance.
  Context<KeyType> ctx;
  (void)ctx;

  ctx.header_page_ = std::make_optional<WritePageGuard>(bpm_->FetchPageWrite(header_page_id_));
  auto header_page = ctx.header_page_.value().template AsMut<BPlusTreeHeaderPage>();

  ctx.root_page_id_ = header_page->root_page_id_;
  ctx.write_set_.push_back(bpm_->FetchPageWrite(ctx.root_page_id_));
  RemoveImpl(ctx, key);
  ctx.header_page_ = std::nullopt;
}

/*****************************************************************************
 * INDEX ITERATOR
 *****************************************************************************/
/*
 * Input parameter is void, find the leftmost leaf page first, then construct
 * index iterator
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::Begin() -> INDEXITERATOR_TYPE {
  auto page_guard = GetFirstLeafPageGuard();
  if (page_guard == std::nullopt) {
    return INDEXITERATOR_TYPE();
  }

  return {bpm_, std::move(page_guard.value()), 0};
}

/*
 * Input parameter is low key, find the leaf page that contains the input key
 * first, then construct index iterator
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::Begin(const KeyType &key) -> INDEXITERATOR_TYPE {
  auto page_info = GetPageGuardByKey(key);
  if (page_info == std::nullopt) {
    return INDEXITERATOR_TYPE();
  }

  return INDEXITERATOR_TYPE(bpm_, std::move(page_info.value().first), page_info.value().second);
}

/*
 * Input parameter is void, construct an index iterator representing the end
 * of the key/value pair in the leaf node
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::End() -> INDEXITERATOR_TYPE { return INDEXITERATOR_TYPE(); }

/**
 * @return Page id of the root of this tree
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::GetRootPageId() -> page_id_t {
  ReadPageGuard header_guard = bpm_->FetchPageRead(header_page_id_);
  auto header_page = header_guard.As<BPlusTreeHeaderPage>();
  return header_page->root_page_id_;
}

/*****************************************************************************
 * UTILITIES AND DEBUG
 *****************************************************************************/

/*
 * This method is used for test only
 * Read data from file and insert one by one
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::InsertFromFile(const std::string &file_name, Transaction *txn) {
  int64_t key;
  std::ifstream input(file_name);
  while (input) {
    input >> key;

    KeyType index_key;
    index_key.SetFromInteger(key);
    RID rid(key);
    Insert(index_key, rid, txn);
  }
}
/*
 * This method is used for test only
 * Read data from file and remove one by one
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::RemoveFromFile(const std::string &file_name, Transaction *txn) {
  int64_t key;
  std::ifstream input(file_name);
  while (input) {
    input >> key;
    KeyType index_key;
    index_key.SetFromInteger(key);
    Remove(index_key, txn);
  }
}

INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::Print(BufferPoolManager *bpm) {
  auto root_page_id = GetRootPageId();
  auto guard = bpm->FetchPageBasic(root_page_id);
  PrintTree(guard.PageId(), guard.template As<BPlusTreePage>());
}

INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::PrintTree(page_id_t page_id, const BPlusTreePage *page) {
  if (page->IsLeafPage()) {
    auto *leaf = reinterpret_cast<const LeafPage *>(page);
    std::cout << "Leaf Page: " << page_id << "\tNext: " << leaf->GetNextPageId() << std::endl;

    // Print the contents of the leaf page.
    std::cout << "Contents: ";
    for (int i = 0; i < leaf->GetSize(); i++) {
      std::cout << leaf->KeyAt(i);
      if ((i + 1) < leaf->GetSize()) {
        std::cout << ", ";
      }
    }
    std::cout << std::endl;
    std::cout << std::endl;

  } else {
    auto *internal = reinterpret_cast<const InternalPage *>(page);
    std::cout << "Internal Page: " << page_id << std::endl;

    // Print the contents of the internal page.
    std::cout << "Contents: ";
    for (int i = 0; i < internal->GetSize(); i++) {
      std::cout << internal->KeyAt(i) << ": " << internal->ValueAt(i);
      if ((i + 1) < internal->GetSize()) {
        std::cout << ", ";
      }
    }
    std::cout << std::endl;
    std::cout << std::endl;
    for (int i = 0; i < internal->GetSize(); i++) {
      auto guard = bpm_->FetchPageBasic(internal->ValueAt(i));
      PrintTree(guard.PageId(), guard.template As<BPlusTreePage>());
    }
  }
}

/**
 * This method is used for debug only, You don't need to modify
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::Draw(BufferPoolManager *bpm, const std::string &outf) {
  if (IsEmpty()) {
    LOG_WARN("Drawing an empty tree");
    return;
  }

  std::ofstream out(outf);
  out << "digraph G {" << std::endl;
  auto root_page_id = GetRootPageId();
  auto guard = bpm->FetchPageBasic(root_page_id);
  ToGraph(guard.PageId(), guard.template As<BPlusTreePage>(), out);
  out << "}" << std::endl;
  out.close();
}

/**
 * This method is used for debug only, You don't need to modify
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::ToGraph(page_id_t page_id, const BPlusTreePage *page, std::ofstream &out) {
  std::string leaf_prefix("LEAF_");
  std::string internal_prefix("INT_");
  if (page->IsLeafPage()) {
    auto *leaf = reinterpret_cast<const LeafPage *>(page);
    // Print node name
    out << leaf_prefix << page_id;
    // Print node properties
    out << "[shape=plain color=green ";
    // Print data of the node
    out << "label=<<TABLE BORDER=\"0\" CELLBORDER=\"1\" CELLSPACING=\"0\" CELLPADDING=\"4\">\n";
    // Print data
    out << "<TR><TD COLSPAN=\"" << leaf->GetSize() << "\">P=" << page_id << "</TD></TR>\n";
    out << "<TR><TD COLSPAN=\"" << leaf->GetSize() << "\">"
        << "max_size=" << leaf->GetMaxSize() << ",min_size=" << leaf->GetMinSize() << ",size=" << leaf->GetSize()
        << "</TD></TR>\n";
    out << "<TR>";
    for (int i = 0; i < leaf->GetSize(); i++) {
      out << "<TD>" << leaf->KeyAt(i) << "</TD>\n";
    }
    out << "</TR>";
    // Print table end
    out << "</TABLE>>];\n";
    // Print Leaf node link if there is a next page
    if (leaf->GetNextPageId() != INVALID_PAGE_ID) {
      out << leaf_prefix << page_id << " -> " << leaf_prefix << leaf->GetNextPageId() << ";\n";
      out << "{rank=same " << leaf_prefix << page_id << " " << leaf_prefix << leaf->GetNextPageId() << "};\n";
    }
  } else {
    auto *inner = reinterpret_cast<const InternalPage *>(page);
    // Print node name
    out << internal_prefix << page_id;
    // Print node properties
    out << "[shape=plain color=pink ";  // why not?
    // Print data of the node
    out << "label=<<TABLE BORDER=\"0\" CELLBORDER=\"1\" CELLSPACING=\"0\" CELLPADDING=\"4\">\n";
    // Print data
    out << "<TR><TD COLSPAN=\"" << inner->GetSize() << "\">P=" << page_id << "</TD></TR>\n";
    out << "<TR><TD COLSPAN=\"" << inner->GetSize() << "\">"
        << "max_size=" << inner->GetMaxSize() << ",min_size=" << inner->GetMinSize() << ",size=" << inner->GetSize()
        << "</TD></TR>\n";
    out << "<TR>";
    for (int i = 0; i < inner->GetSize(); i++) {
      out << "<TD PORT=\"p" << inner->ValueAt(i) << "\">";
      if (i > 0) {
        out << inner->KeyAt(i);
      } else {
        out << " ";
      }
      out << "</TD>\n";
    }
    out << "</TR>";
    // Print table end
    out << "</TABLE>>];\n";
    // Print leaves
    for (int i = 0; i < inner->GetSize(); i++) {
      auto child_guard = bpm_->FetchPageBasic(inner->ValueAt(i));
      auto child_page = child_guard.template As<BPlusTreePage>();
      ToGraph(child_guard.PageId(), child_page, out);
      if (i > 0) {
        auto sibling_guard = bpm_->FetchPageBasic(inner->ValueAt(i - 1));
        auto sibling_page = sibling_guard.template As<BPlusTreePage>();
        if (!sibling_page->IsLeafPage() && !child_page->IsLeafPage()) {
          out << "{rank=same " << internal_prefix << sibling_guard.PageId() << " " << internal_prefix
              << child_guard.PageId() << "};\n";
        }
      }
      out << internal_prefix << page_id << ":p" << child_guard.PageId() << " -> ";
      if (child_page->IsLeafPage()) {
        out << leaf_prefix << child_guard.PageId() << ";\n";
      } else {
        out << internal_prefix << child_guard.PageId() << ";\n";
      }
    }
  }
}

INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::DrawBPlusTree() -> std::string {
  if (IsEmpty()) {
    return "()";
  }

  PrintableBPlusTree p_root = ToPrintableBPlusTree(GetRootPageId());
  std::ostringstream out_buf;
  p_root.Print(out_buf);

  return out_buf.str();
}

INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::ToPrintableBPlusTree(page_id_t root_id) -> PrintableBPlusTree {
  auto root_page_guard = bpm_->FetchPageBasic(root_id);
  auto root_page = root_page_guard.template As<BPlusTreePage>();
  PrintableBPlusTree proot;

  if (root_page->IsLeafPage()) {
    auto leaf_page = root_page_guard.template As<LeafPage>();
    proot.keys_ = leaf_page->ToString();
    proot.size_ = proot.keys_.size() + 4;  // 4 more spaces for indent

    return proot;
  }

  // draw internal page
  auto internal_page = root_page_guard.template As<InternalPage>();
  proot.keys_ = internal_page->ToString();
  proot.size_ = 0;
  for (int i = 0; i < internal_page->GetSize(); i++) {
    page_id_t child_id = internal_page->ValueAt(i);
    PrintableBPlusTree child_node = ToPrintableBPlusTree(child_id);
    proot.size_ += child_node.size_;
    proot.children_.push_back(child_node);
  }

  return proot;
}

INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::SearchValueOfKey(Context<KeyType> &ctx, page_id_t page_id, const KeyType &key,
                                      std::vector<ValueType> *result) -> bool {
  auto page_read_guard = bpm_->FetchPageRead(page_id);
  auto cur_page = page_read_guard.template As<BPlusTreePage>();
  if (cur_page->IsLeafPage() && cur_page->GetSize() == 0) {
    return false;
  }
  // ctx.read_set_.push_back(std::move(page_read_guard));

  if (cur_page->IsLeafPage()) {
    const auto *leaf_page = page_read_guard.template As<LeafPage>();
    // auto leaf_page = reinterpret_cast<const LeafPage*>(cur_page);
    return BinarySearchOfLeafPage(leaf_page, key, result).first;
  }

  const auto *internal_page = page_read_guard.template As<InternalPage>();
  // auto internal_page = reinterpret_cast<const InternalPage*>(cur_page);
  auto child_page_index = BinarySearchOfInternalPage(internal_page, key);
  return SearchValueOfKey(ctx, internal_page->ValueAt(child_page_index), key, result);

  return true;
}

INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::BinarySearchOfInternalPage(const InternalPage *cur_page, const KeyType &key) -> int {
  int size = cur_page->GetSize();
  BUSTUB_ASSERT(size > 1, "Size of key must greater than one!");

  if (comparator_(key, cur_page->KeyAt(1)) < 0) {
    return 0;
  }

  if (comparator_(key, cur_page->KeyAt(size - 1)) >= 0) {
    return size - 1;
  }

  int left = 1;
  int right = size - 1;
  while (left < right) {
    int mid = left + (right - left) / 2;
    if (comparator_(key, cur_page->KeyAt(mid)) < 0) {
      right = mid - 1;
    } else if (comparator_(key, cur_page->KeyAt(mid + 1)) >= 0) {
      left = mid + 1;
    } else {
      return mid;
    }
  }

  return left;
}

INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::BinarySearchOfLeafPage(const BPlusTree::LeafPage *cur_page, const KeyType &key,
                                            std::vector<ValueType> *result) -> std::pair<bool, int> {
  auto cur_size = cur_page->GetSize();
  if (cur_size == 0) {
    return {false, 0};
  }

  int left = 0;
  int right = cur_size;
  while (left < right) {
    int mid = left + (right - left) / 2;
    auto mid_key = cur_page->KeyAt(mid);
    if (comparator_(mid_key, key) > 0) {
      right = mid;
    } else if (comparator_(mid_key, key) < 0) {
      left = mid + 1;
    } else {
      if (result) {
        result->push_back(cur_page->ValueAt(mid));
      }
      return {true, mid};
    }
  }

  if (left == cur_size) {
    return {false, cur_size};
  }

  if (comparator_(cur_page->KeyAt(left), key) == 0) {
    return {true, left};
  }

  return {false, left};
}

INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::InsertImpl(Context<KeyType> &ctx, const KeyType &key, const ValueType &value) -> bool {
  auto &guard = ctx.write_set_.back();
  auto page = guard.template AsMut<BPlusTreePage>();

  if (page->IsLeafPage()) {
    auto *leaf_page = guard.template AsMut<LeafPage>();
    auto res = BinarySearchOfLeafPage(leaf_page, key, nullptr);
    if (static_cast<bool>(res.first)) {
      return false;
    }

    int index = res.second;
    if (res.second == leaf_page->GetMaxSize() || leaf_page->GetMaxSize() == leaf_page->GetSize()) {
      page_id_t new_page_id = INVALID_PAGE_ID;
      auto new_page_guard = bpm_->NewPageGuarded(&new_page_id);
      if (new_page_id == INVALID_PAGE_ID) {
        return false;
      }

      auto *new_page = new_page_guard.template AsMut<LeafPage>();
      new_page->Init(leaf_max_size_);
      int mid_pos = leaf_page->GetMinSize();
      int end_pos = leaf_page->GetMaxSize();
      if (index >= mid_pos) {
        for (int i = mid_pos; i < end_pos; ++i) {
          new_page->Insert(i - mid_pos, leaf_page->KeyAt(i), leaf_page->ValueAt(i));
        }
        new_page->Insert(index - mid_pos, key, value);
        leaf_page->IncreaseSize(mid_pos - end_pos);
      } else {
        for (int i = mid_pos - 1; i < end_pos; ++i) {
          new_page->Insert(i - mid_pos + 1, leaf_page->KeyAt(i), leaf_page->ValueAt(i));
        }
        leaf_page->IncreaseSize(mid_pos - end_pos - 1);
        leaf_page->Insert(index, key, value);
      }
      new_page->SetNextPageId(leaf_page->GetNextPageId());
      leaf_page->SetNextPageId(new_page_id);

      if (ctx.IsRootPage(guard.PageId())) {
        page_id_t new_root_id = INVALID_PAGE_ID;
        auto new_root_guard = bpm_->NewPageGuarded(&new_root_id);
        if (new_root_id == INVALID_PAGE_ID) {
          return false;
        }

        auto *new_root_page = new_root_guard.template AsMut<InternalPage>();
        new_root_page->Init(internal_max_size_);
        new_root_page->IncreaseSize(2);
        new_root_page->SetKeyAt(1, new_page->KeyAt(0));
        new_root_page->SetValueAt(1, new_page_id);
        new_root_page->SetValueAt(0, guard.PageId());
        auto header_page = ctx.header_page_.value().template AsMut<BPlusTreeHeaderPage>();
        header_page->root_page_id_ = new_root_id;
      } else {
        ctx.is_need_change_ = true;
        ctx.up_page_id_ = new_page_id;
        ctx.up_key_ = new_page->KeyAt(0);
        // ctx.change_page_type_ = IndexPageType::LEAF_PAGE;
      }
    } else {
      leaf_page->Insert(res.second, key, value);
    }
  } else {
    auto *internal_page = guard.template AsMut<InternalPage>();
    auto page_index = BinarySearchOfInternalPage(internal_page, key);
    BUSTUB_ASSERT(page_index < internal_page->GetSize(), "Invalid page index!");
    auto internal_page_id = internal_page->ValueAt(page_index);
    BUSTUB_ASSERT(internal_page_id != INVALID_PAGE_ID, "Invalid page id!");

    ctx.write_set_.push_back(bpm_->FetchPageWrite(internal_page_id));
    auto flag = InsertImpl(ctx, key, value);
    if (!flag) {
      return false;
    }
    if (ctx.is_need_change_) {
      if (internal_page->GetSize() < internal_page->GetMaxSize()) {
        internal_page->Insert(page_index + 1, ctx.up_key_, ctx.up_page_id_);
        ctx.is_need_change_ = false;
      } else {
        page_id_t new_page_id = INVALID_PAGE_ID;
        auto new_page_guard = bpm_->NewPageGuarded(&new_page_id);
        if (new_page_id == INVALID_PAGE_ID) {
          return false;
        }

        auto *new_page = new_page_guard.template AsMut<InternalPage>();
        new_page->Init(internal_max_size_);
        KeyType new_parent_key;
        int mid_pos = internal_page->GetMinSize();
        int end_pos = internal_page->GetMaxSize();
        if (page_index >= mid_pos - 1) {
          new_parent_key = internal_page->KeyAt(mid_pos);
          for (int i = mid_pos; i < end_pos; ++i) {
            new_page->Insert(i - mid_pos, internal_page->KeyAt(i), internal_page->ValueAt(i));
          }
          new_page->Insert(page_index - mid_pos + 1, ctx.up_key_, ctx.up_page_id_);
          internal_page->IncreaseSize(mid_pos - end_pos);
        } else {
          for (int i = mid_pos - 1; i < end_pos; ++i) {
            new_page->Insert(i - mid_pos + 1, internal_page->KeyAt(i), internal_page->ValueAt(i));
          }
          internal_page->IncreaseSize(mid_pos - end_pos - 1);
          internal_page->Insert(page_index + 1, ctx.up_key_, ctx.up_page_id_);
          // internal_page->InsertKeyAndValue(std::min(page_index + 1, internal_page->GetSize()), ctx.up_key_,
          // ctx.up_page_id_);
        }

        new_parent_key = new_page->KeyAt(0);

        if (ctx.IsRootPage(guard.PageId())) {
          page_id_t new_root_id = INVALID_PAGE_ID;
          auto new_root_guard = bpm_->NewPageGuarded(&new_root_id);
          if (new_page_id == INVALID_PAGE_ID) {
            return false;
          }

          auto *new_root_page = new_root_guard.template AsMut<InternalPage>();
          new_root_page->Init(internal_max_size_);
          new_root_page->IncreaseSize(2);
          new_root_page->SetKeyAt(1, new_parent_key);
          new_root_page->SetValueAt(1, new_page_id);
          new_root_page->SetValueAt(0, guard.PageId());
          auto header_page = ctx.header_page_.value().template AsMut<BPlusTreeHeaderPage>();
          header_page->root_page_id_ = new_root_id;
        } else {
          ctx.is_need_change_ = true;
          ctx.up_page_id_ = new_page_id;
          ctx.up_key_ = new_parent_key;
          // ctx.change_page_type_ = IndexPageType::INTERNAL_PAGE;
        }
      }
    }
  }

  return true;
}

INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::RemoveImpl(Context<KeyType> &ctx, const KeyType &key) -> bool {
  auto &guard = ctx.write_set_.back();
  auto page = guard.template AsMut<BPlusTreePage>();

  if (page->IsLeafPage()) {
    auto *leaf_page = guard.template AsMut<LeafPage>();
    auto res = BinarySearchOfLeafPage(leaf_page, key, nullptr);
    if (!static_cast<bool>(res.first)) {
      return true;
    }

    int index = res.second;
    leaf_page->Delete(index);
    if (leaf_page->GetSize() >= leaf_page->GetMinSize()) {
      return true;
    }

    if (guard.PageId() == ctx.root_page_id_) {
      return true;
    }
    ctx.is_need_change_ = true;
    ctx.need_change_page_ = std::move(ctx.write_set_.back());
    ctx.write_set_.pop_back();
    ctx.change_page_type_ = IndexPageType::LEAF_PAGE;
    return false;
  }

  auto *internal_page = guard.template AsMut<InternalPage>();
  auto page_index = BinarySearchOfInternalPage(internal_page, key);
  BUSTUB_ASSERT(page_index < internal_page->GetSize(), "Invalid page index!");
  auto internal_page_id = internal_page->ValueAt(page_index);
  BUSTUB_ASSERT(internal_page_id != INVALID_PAGE_ID, "Invalid page id!");

  ctx.write_set_.push_back(bpm_->FetchPageWrite(internal_page_id));
  auto flag = RemoveImpl(ctx, key);
  if (flag) {
    return true;
  }

  if (ctx.is_need_change_) {
    if (ctx.change_page_type_ == IndexPageType::LEAF_PAGE) {
      if (page_index > 0) {
        auto left_page_guard = bpm_->FetchPageWrite(internal_page->ValueAt(page_index - 1));
        auto *right_page = ctx.need_change_page_.template AsMut<LeafPage>();
        auto *left_page = left_page_guard.template AsMut<LeafPage>();
        if (left_page->GetSize() > left_page->GetMinSize()) {
          auto new_key = left_page->KeyAt(left_page->GetSize() - 1);
          auto new_value = left_page->ValueAt(left_page->GetSize() - 1);
          internal_page->SetKeyAt(page_index, new_key);
          right_page->Insert(0, new_key, new_value);
          left_page->IncreaseSize(-1);
        } else {
          for (int i = 0; i < right_page->GetSize(); ++i) {
            left_page->Insert(left_page->GetSize(), right_page->KeyAt(i), right_page->ValueAt(i));
          }
          left_page->SetNextPageId(right_page->GetNextPageId());
          internal_page->Delete(page_index);
          bpm_->DeletePage(ctx.need_change_page_.PageId());
          if (guard.PageId() == ctx.root_page_id_ && internal_page->GetSize() == 1) {
            auto header_page = ctx.header_page_.value().template AsMut<BPlusTreeHeaderPage>();
            header_page->root_page_id_ = left_page_guard.PageId();
            bpm_->DeletePage(ctx.root_page_id_);
            return true;
          }
        }
      } else {
        auto right_page_guard = bpm_->FetchPageWrite(internal_page->ValueAt(page_index + 1));
        auto *left_page = ctx.need_change_page_.template AsMut<LeafPage>();
        auto *right_page = right_page_guard.template AsMut<LeafPage>();
        if (right_page->GetSize() > right_page->GetMinSize()) {
          auto new_key = right_page->KeyAt(0);
          auto new_value = right_page->ValueAt(0);
          internal_page->SetKeyAt(page_index + 1, right_page->KeyAt(1));
          left_page->Insert(left_page->GetSize(), new_key, new_value);
          right_page->Delete(0);
        } else {
          for (int i = 0; i < right_page->GetSize(); ++i) {
            left_page->Insert(left_page->GetSize(), right_page->KeyAt(i), right_page->ValueAt(i));
          }

          left_page->SetNextPageId(right_page->GetNextPageId());
          bpm_->DeletePage(right_page_guard.PageId());
          internal_page->Delete(page_index + 1);
          if (guard.PageId() == ctx.root_page_id_ && internal_page->GetSize() == 1) {
            auto header_page = ctx.header_page_.value().template AsMut<BPlusTreeHeaderPage>();
            header_page->root_page_id_ = ctx.need_change_page_.PageId();
            bpm_->DeletePage(ctx.root_page_id_);
            return true;
          }
        }
      }
    } else {
      if (page_index > 0) {
        auto left_page_guard = bpm_->FetchPageWrite(internal_page->ValueAt(page_index - 1));
        auto *right_page = ctx.need_change_page_.template AsMut<InternalPage>();
        auto *left_page = left_page_guard.template AsMut<InternalPage>();
        if (left_page->GetSize() > left_page->GetMinSize()) {
          auto new_key = left_page->KeyAt(left_page->GetSize() - 1);
          auto new_value = left_page->ValueAt(left_page->GetSize() - 1);
          internal_page->SetKeyAt(page_index, new_key);
          right_page->Insert(0, new_key, new_value);
          left_page->IncreaseSize(-1);
        } else {
          int old_size = left_page->GetSize();
          for (int i = 0; i < right_page->GetSize(); ++i) {
            left_page->Insert(left_page->GetSize(), right_page->KeyAt(i), right_page->ValueAt(i));
          }
          left_page->SetKeyAt(old_size, internal_page->KeyAt(page_index));
          internal_page->Delete(page_index);
          bpm_->DeletePage(ctx.need_change_page_.PageId());
          if (guard.PageId() == ctx.root_page_id_ && internal_page->GetSize() == 1) {
            auto header_page = ctx.header_page_.value().template AsMut<BPlusTreeHeaderPage>();
            header_page->root_page_id_ = left_page_guard.PageId();
            bpm_->DeletePage(ctx.root_page_id_);
            return true;
          }
        }
      } else {
        auto right_page_guard = bpm_->FetchPageWrite(internal_page->ValueAt(page_index + 1));
        auto *left_page = ctx.need_change_page_.template AsMut<InternalPage>();
        auto *right_page = right_page_guard.template AsMut<InternalPage>();
        if (right_page->GetSize() > right_page->GetMinSize()) {
          auto new_key = right_page->KeyAt(0);
          auto new_value = right_page->ValueAt(0);
          internal_page->SetKeyAt(page_index + 1, right_page->KeyAt(1));
          left_page->Insert(left_page->GetSize(), new_key, new_value);
          right_page->Delete(0);
        } else {
          int old_size = left_page->GetSize();
          for (int i = 0; i < right_page->GetSize(); ++i) {
            left_page->Insert(left_page->GetSize(), right_page->KeyAt(i), right_page->ValueAt(i));
          }

          left_page->SetKeyAt(old_size, internal_page->KeyAt(page_index + 1));
          bpm_->DeletePage(right_page_guard.PageId());
          internal_page->Delete(page_index + 1);
          if (guard.PageId() == ctx.root_page_id_ && internal_page->GetSize() == 1) {
            auto header_page = ctx.header_page_.value().template AsMut<BPlusTreeHeaderPage>();
            header_page->root_page_id_ = ctx.need_change_page_.PageId();
            bpm_->DeletePage(ctx.root_page_id_);
            return true;
          }
        }
      }
    }

    if (guard.PageId() == ctx.root_page_id_ || internal_page->GetSize() >= internal_page->GetMinSize()) {
      return true;
    }

    ctx.is_need_change_ = true;
    ctx.need_change_page_ = std::move(ctx.write_set_.back());
    ctx.write_set_.pop_back();
    ctx.change_page_type_ = IndexPageType::INTERNAL_PAGE;
    return false;
  }

  return true;
}

INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::GetFirstLeafPageGuard() -> std::optional<ReadPageGuard> {
  if (IsEmpty()) {
    return std::nullopt;
  }

  // Declaration of context instance.
  Context<KeyType> ctx;
  // (void)ctx;
  ReadPageGuard header_guard = bpm_->FetchPageRead(header_page_id_);
  auto header_page = header_guard.As<BPlusTreeHeaderPage>();
  auto cur_page_id = header_page->root_page_id_;

  while (true) {
    auto cur_page_guard = bpm_->FetchPageRead(cur_page_id);
    if (cur_page_guard.IsValid()) {
      auto *cur_page = cur_page_guard.template As<BPlusTreePage>();
      if (cur_page->IsLeafPage()) {
        return std::make_optional(std::move(cur_page_guard));
      }

      const auto *cur_internal_page = cur_page_guard.template As<InternalPage>();
      BUSTUB_ASSERT(cur_internal_page->GetSize() >= 2, "The size of internal page must greater than one");
      cur_page_id = cur_internal_page->ValueAt(0);
    } else {
      return std::nullopt;
    }
  }

  return std::nullopt;
}

INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::GetPageGuardByKey(const KeyType &key) -> std::optional<std::pair<ReadPageGuard, int>> {
  if (IsEmpty()) {
    return std::nullopt;
  }

  // Declaration of context instance.
  Context<KeyType> ctx;
  // (void)ctx;
  ReadPageGuard header_guard = bpm_->FetchPageRead(header_page_id_);
  auto header_page = header_guard.As<BPlusTreeHeaderPage>();
  auto cur_page_id = header_page->root_page_id_;

  while (true) {
    auto cur_page_guard = bpm_->FetchPageRead(cur_page_id);
    if (cur_page_guard.IsValid()) {
      auto *cur_page = cur_page_guard.template As<BPlusTreePage>();
      if (cur_page->IsLeafPage()) {
        const auto *cur_leaf_page = cur_page_guard.template As<LeafPage>();
        auto key_search = BinarySearchOfLeafPage(cur_leaf_page, key, nullptr);
        if (key_search.first) {
          return std::make_optional(std::pair<ReadPageGuard, int>{std::move(cur_page_guard), key_search.second});
        }

        return std::nullopt;
      }

      const auto *cur_internal_page = cur_page_guard.template As<InternalPage>();
      BUSTUB_ASSERT(cur_internal_page->GetSize() >= 2, "The size of internal page must greater than one");
      cur_page_id = cur_internal_page->ValueAt(BinarySearchOfInternalPage(cur_internal_page, key));
    } else {
      return std::nullopt;
    }
  }

  return std::nullopt;
}

template class BPlusTree<GenericKey<4>, RID, GenericComparator<4>>;

template class BPlusTree<GenericKey<8>, RID, GenericComparator<8>>;

template class BPlusTree<GenericKey<16>, RID, GenericComparator<16>>;

template class BPlusTree<GenericKey<32>, RID, GenericComparator<32>>;

template class BPlusTree<GenericKey<64>, RID, GenericComparator<64>>;

}  // namespace bustub
