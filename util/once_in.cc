#include "once_in.h"


bool OnceIn::isTime()
{
  auto now = std::chrono::high_resolution_clock::now();
  if ( now - prevTime_ >= period_ ){
    prevTime_ = now;
    return true;
  }
  return false;
}
