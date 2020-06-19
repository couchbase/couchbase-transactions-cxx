HEADER = <<HEADER

#include <stdexcept>
#include <string>
#include <vector>

#include "atr_ids.hxx"
namespace tx = couchbase::transactions;

const std::vector<std::string> ATR_IDS(
{
HEADER

FOOTER = <<FOOTER
});

const std::vector<std::string>&
tx::atr_ids::all()
{
    return ATR_IDS;
}

const std::string&
tx::atr_ids::atr_id_for_vbucket(int vbucket_id)
{
    if (vbucket_id < 0 || vbucket_id > ATR_IDS.size()) {
        throw std::invalid_argument(std::string("invalid vbucket_id: ") + std::to_string(vbucket_id));
    }
    return ATR_IDS[vbucket_id];
}

#include "../../deps/libcouchbase/src/vbucket/crc32.h"

int
tx::atr_ids::vbucket_for_key(const std::string& key)
{
    static const int num_vbuckets = 1024;
    uint32_t digest = hash_crc32(key.data(), key.size());
    return static_cast<int>(digest % num_vbuckets);
}
FOOTER
ids = []

# I'll assume you grabbed the raw atr ids from the java transactions code, and put that
# in a file named raw_atr_id.txt
# Now lets read the file raw_atr_id.txt line-by-line, into an array...
File.foreach("raw_atr_id.txt") { |line|
  next if line.start_with?('//')
  # now, make them all the same length, so it looks pretty
  # NOTE: the text is _txn:atr-XXXX-#YYYY but the XXXX and YYYY could be shorter than 4 characters
  # so lets add whitespace to make them all 25 characters long
  # (19 (max length of the atr id) + 2 (for quotes) + 1 for comma, plus a couple spaces), by prepending spaces
  # as needed
  ids.append("%-23s," % "\"#{line.chomp}\"")
}
# this array contains 20 atr ids for each of the 1024 vbuckets, however for now
# lets just make this into a vector with 1 per vbucket.  And, lets print them
# in columns of 6 elements.
File.open("atr_ids.cxx.new", "w+") { |f|
  f.write(HEADER)
  ids = ids.slice(0,1024)
  # hack off that last comma
  ids[1023] = ids[1023].slice(0,24)
  ids.each_slice(6) { |ids|
    f.write(ids.join(""))
    f.write("\n")
  }
  f.write(FOOTER)
}
