#include <exception>
#include <base/common.h>
#include <iostream>
#include <string>

// 继承自std::exception
class AbortException : public std::exception {
public:
  AbortException(tx_id_t tx_id) : tx_id(tx_id) {}
  virtual const char* what() const throw() {
    return "Transaction aborted.";
  }
private:
    tx_id_t tx_id;
};
