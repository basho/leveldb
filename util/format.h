#ifndef FORMAT_H
#define FORMAT_H

#include <boost/format.hpp>

class Format
{
public:
  Format(const char* fmt): fmt_(fmt) {}

  template<class T>
  Format& operator % (const T& arg) {
      fmt_ % arg;
      return *this;
  }
  operator std::string() const {
      return fmt_.str();
  }
protected:
  boost::format fmt_;
};

#endif // FORMAT_H
