// Author: Dai Wei (wdai@cs.cmu.edu)
// Date: 2013

#include "lda/context.hpp"
#include <vector>
#include <gflags/gflags.h>
#include <glog/logging.h>

namespace util {

Context& Context::get_instance()
{
  static Context instance;
  return instance;
}

Context::Context() {
  static std::vector<google::CommandLineFlagInfo> flags;
  google::GetAllFlags(&flags);
  for (size_t i = 0; i < flags.size(); i++) {
    google::CommandLineFlagInfo& flag = flags[i];
    ctx_[flag.name] = flag.is_default ? flag.default_value : flag.current_value;
  }
}

// -------------------- Getters ----------------------

int Context::get_int32(std::string key) {
  return atoi(get_string(key).c_str());
}

int64_t Context::get_int64(std::string key) {
	//return _atoi64(get_string(key).c_str());
	return strtoll(get_string(key).c_str(), NULL, 10);
}

double Context::get_double(std::string key) {
  return atof(get_string(key).c_str());
}

bool Context::get_bool(std::string key) {
  return get_string(key).compare("true") == 0;
}

std::string Context::get_string(std::string key) {
  auto it = ctx_.find(key);
  LOG_IF(FATAL, it == ctx_.end())
      << "Failed to lookup " << key << " in context.";
  return it->second;
}

// -------------------- Setters ---------------------

void Context::set(std::string key, int value) {
  ctx_[key] = std::to_string(value);
}

void Context::set(std::string key, double value) {
  ctx_[key] = std::to_string(value);
}

void Context::set(std::string key, bool value) {
  ctx_[key] = (value) ? "true" : "false";
}

void Context::set(std::string key, std::string value) {
  ctx_[key] = value;
}

}   // namespace util
