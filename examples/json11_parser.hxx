#pragma once

#include <json11.hpp>

class Json11Parser
{
  public:
    typedef json11::Json ValueType;

    static json11::Json parse(const std::string &input)
    {
        std::string errmsg;
        return json11::Json::parse(input, errmsg);
    }
};
