#pragma once

#include <memory>

#include "dtx/dtx.h"

class BenchDTX {
public:
    DTX *dtx;

    virtual bool TxReCaculate(coro_yield_t& yield) = 0;
};
