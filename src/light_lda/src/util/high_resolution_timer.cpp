
#include "high_resolution_timer.hpp"

#include <limits>
#include <glog/logging.h>
//#include "wtime.h"   // for clock_gettime

namespace petuum {

HighResolutionTimer::HighResolutionTimer() {
  restart();
}

void HighResolutionTimer::restart() {
  int status = clock_gettime(CLOCK_MONOTONIC, &start_time_);
//	int status = clock_gettime(0, &start_time_);
  CHECK_NE(-1, status) << "Couldn't initialize start_time_";
}

double HighResolutionTimer::elapsed() const {
  struct timespec now;
  int status = clock_gettime(CLOCK_MONOTONIC, &now);
  //int status = clock_gettime(0, &now);
  CHECK_NE(-1, status) << "Couldn't get current time.";

  double ret_sec = double(now.tv_sec - start_time_.tv_sec);
  double ret_nsec = double(now.tv_nsec - start_time_.tv_nsec);

  while (ret_nsec < 0) {
    ret_sec -= 1.0;
    ret_nsec += 1e9;
  }
  // it returns nanoseconds
  double ret = ret_sec + ret_nsec / 1e9;
  //double ret = ret_sec + ret_nsec / 1e6;  
  return ret;
}

double HighResolutionTimer::elapsed_max() const {
  return double((std::numeric_limits<double>::max)());
}

double HighResolutionTimer::elapsed_min() const {
  return 0.0;
}

}   // namespace petuum
