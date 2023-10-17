// Author: Ming Zhang
// Copyright (c) 2022

#pragma once

#include <cstring>
#include <iostream>
#include <memory>
#include <sstream>
#include <string>

#include "base/common.h"
#include "util/debug.h"

struct PageMeta {
  bool is_empty = 0;
  int pin_count_ = 0;
} Aligned8;  // Size: 560B in X86 arch.

const size_t PageMetaSize = sizeof(PageMeta);

using PageMetaPtr = std::shared_ptr<PageMeta>;