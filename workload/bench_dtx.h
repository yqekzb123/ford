#pragma once

#include <memory>

#include "dtx/structs.h"

class BenchDTX {
public:
    DTX *dtx;
    virtual bool TxReCaculate(coro_yield_t& yield) = 0;
    virtual bool StatCommit() = 0;
};
