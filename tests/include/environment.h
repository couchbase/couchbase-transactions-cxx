#include <gtest/gtest.h>
#include "couchbase/client/
#define CONFIG_FILE_NAME "config.json"
class TestEnvironment : public ::testing::Environment {
  public:
    void SetUp() override {
        // read config.json

    }
  private:

