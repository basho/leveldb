#ifndef ONCEIN_H
#define ONCEIN_H

#include <chrono>

class OnceIn
{
public:
  template<class Rep, class Period = std::ratio<1> >
  OnceIn( const std::chrono::duration<Rep,Period> period ){
    period_ = std::chrono::duration_cast<decltype(period_)>( period );
  }
  bool isTime();

private:
  std::chrono::high_resolution_clock::time_point prevTime_;
  std::chrono::nanoseconds period_;
};

#endif // ONCEIN_H
