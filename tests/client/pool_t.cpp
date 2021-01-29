#include "../../src/client/pool.hxx"
#include "client_env.h"
#include <atomic>
#include <boost/optional.hpp>
#include <gtest/gtest.h>

using namespace couchbase;

static std::atomic<uint64_t> test_int{ 0 };
static std::atomic<uint64_t> last_destroyed{ 0 };

static std::shared_ptr<pool<uint64_t>>
create_pool(size_t size)
{
    return std::make_shared<pool<uint64_t>>(size, [&] { return ++test_int; }, [&](uint64_t t) { last_destroyed.store(t); });
}

TEST(PoolTests, SimpleGet)
{
    auto pool = create_pool(1);
    ASSERT_EQ(1, pool->available());
    ASSERT_EQ(0, pool->size());
    uint64_t i = pool->get();
    ASSERT_TRUE(i > 0);
    ASSERT_EQ(0, pool->available());
    ASSERT_EQ(1, pool->size());
}
TEST(PoolTests, WillCallDestroyFnInDestructor)
{
    uint64_t t1;
    {
        auto pool = create_pool(1);
        t1 = pool->get();
        pool->release(t1);
    }
    ASSERT_EQ(last_destroyed.load(), t1);
}
TEST(PoolTests, SimpleGetAndRelease)
{
    auto pool = create_pool(1);
    uint64_t i = pool->get();
    ASSERT_TRUE(i > 0);
    pool->release(i);
    auto j = pool->get();
    ASSERT_EQ(i, j);
    ASSERT_EQ(0, pool->available());
    ASSERT_EQ(1, pool->size());
    pool->release(j);
}
TEST(PoolTests, GetWillWait)
{
    auto pool = create_pool(2);
    auto i = pool->get();
    auto j = pool->get();
    std::atomic<uint64_t> thr_get(0);
    std::thread thr = std::thread([&]() { thr_get = pool->get(); });
    std::this_thread::sleep_for(std::chrono::milliseconds(100));
    ASSERT_EQ(0, thr_get.load());
    ASSERT_EQ(0, pool->available());
    // ok, now release one so the thread can run
    pool->release(i);
    if (thr.joinable()) {
        client_log->trace("joining...");
        thr.join();
    }
    ASSERT_EQ(i, thr_get.load());
    ASSERT_EQ(0, pool->available());
    pool->release(i);
    pool->release(j);
    ASSERT_EQ(2, pool->available());
    ASSERT_EQ(2, pool->size());
}
TEST(PoolTests, CanSwapAvailableTrue_WillCreate)
{
    auto pool1 = create_pool(2);
    auto pool2 = create_pool(2);
    auto t1 = pool1->get();
    auto t2 = pool2->get();
    ASSERT_EQ(1, pool1->available());
    ASSERT_EQ(1, pool1->size());
    ASSERT_EQ(1, pool2->available());
    ASSERT_EQ(1, pool2->size());
    ASSERT_TRUE(pool1->swap_available(*pool2, true));
    ASSERT_EQ(1, pool1->available());
    ASSERT_EQ(1, pool1->size());
    ASSERT_EQ(1, pool2->available());
    ASSERT_EQ(2, pool2->size());
}
TEST(PoolTests, CanSwapAvailableTrue_WillNotCreate)
{
    auto pool1 = create_pool(2);
    auto pool2 = create_pool(2);
    auto t1 = pool1->get();
    pool1->release(t1);
    auto t2 = pool2->get();
    ASSERT_EQ(2, pool1->available());
    ASSERT_EQ(1, pool1->size());
    ASSERT_EQ(1, pool2->available());
    ASSERT_EQ(1, pool2->size());
    ASSERT_TRUE(pool1->swap_available(*pool2, true));
    ASSERT_EQ(2, pool1->available());
    ASSERT_EQ(0, pool1->size());
    ASSERT_EQ(1, pool2->available());
    ASSERT_EQ(2, pool2->size());
}
TEST(PoolTests, CanSwapAvailableFalse_WillCreate)
{
    auto pool1 = create_pool(2);
    auto pool2 = create_pool(2);
    auto t1 = pool1->get();
    auto t2 = pool2->get();
    ASSERT_EQ(1, pool1->available());
    ASSERT_EQ(1, pool1->size());
    ASSERT_EQ(1, pool2->available());
    ASSERT_EQ(1, pool2->size());
    ASSERT_TRUE(pool1->swap_available(*pool2, false));
    ASSERT_EQ(1, pool1->available());
    ASSERT_EQ(1, pool1->size());
    ASSERT_EQ(0, pool2->available());
    ASSERT_EQ(2, pool2->size());
}
TEST(PoolTests, CanSwapAvailableFalse_WillNotCreate)
{
    auto pool1 = create_pool(2);
    auto pool2 = create_pool(2);
    auto t1 = pool1->get();
    pool1->release(t1);
    auto t2 = pool2->get();
    ASSERT_EQ(2, pool1->available());
    ASSERT_EQ(1, pool1->size());
    ASSERT_EQ(1, pool2->available());
    ASSERT_EQ(1, pool2->size());
    ASSERT_TRUE(pool1->swap_available(*pool2, false));
    ASSERT_EQ(2, pool1->available());
    ASSERT_EQ(0, pool1->size());
    ASSERT_EQ(0, pool2->available());
    ASSERT_EQ(2, pool2->size());
}
TEST(PoolTests, CanNotSwapIfNoneAvailable)
{
    auto pool1 = create_pool(1);
    auto pool2 = create_pool(2);
    auto t1 = pool1->get();
    auto t2 = pool2->get();
    ASSERT_EQ(0, pool1->available());
    ASSERT_EQ(1, pool1->size());
    ASSERT_EQ(1, pool2->available());
    ASSERT_EQ(1, pool2->size());
    ASSERT_FALSE(pool1->swap_available(*pool2, true));
    ASSERT_EQ(0, pool1->available());
    ASSERT_EQ(1, pool1->size());
    ASSERT_EQ(1, pool2->available());
    ASSERT_EQ(1, pool2->size());
}
TEST(PoolTests, CanNotSwapIfOtherIsFull)
{
    auto pool1 = create_pool(2);
    auto pool2 = create_pool(1);
    auto t1 = pool1->get();
    auto t2 = pool2->get();
    ASSERT_EQ(1, pool1->available());
    ASSERT_EQ(1, pool1->size());
    ASSERT_EQ(0, pool2->available());
    ASSERT_EQ(1, pool2->size());
    ASSERT_FALSE(pool1->swap_available(*pool2, true));
    ASSERT_EQ(1, pool1->available());
    ASSERT_EQ(2, pool1->size());
    ASSERT_EQ(0, pool2->available());
    ASSERT_EQ(1, pool2->size());
}
TEST(PoolTests, CanAddMakeAvailable)
{
    auto pool = create_pool(2);
    uint64_t arbitrary = 1234567891234567890ull;
    ASSERT_TRUE(pool->add(arbitrary, true));
    ASSERT_EQ(2, pool->available());
    ASSERT_EQ(1, pool->size());
    pool->release(arbitrary);
}
TEST(PoolTests, CanAddMakeUnavailable)
{
    auto pool = create_pool(2);
    uint64_t arbitrary = 1234567891234567890ull;
    ASSERT_TRUE(pool->add(arbitrary, false));
    ASSERT_EQ(1, pool->available());
    ASSERT_EQ(1, pool->size());
    ASSERT_NE(arbitrary, pool->get());
    pool->release(arbitrary);
}
TEST(PoolTests, CantAddIfFull)
{
    auto pool = create_pool(1);
    auto t1 = pool->get();
    uint64_t arbitrary = 1234567891234567890ull;
    pool->release(t1);
    ASSERT_EQ(1, pool->available());
    ASSERT_EQ(1, pool->size());
    ASSERT_FALSE(pool->add(arbitrary, true));
    ASSERT_EQ(1, pool->available());
    ASSERT_EQ(1, pool->size());
    ASSERT_EQ(t1, pool->get());
    pool->release(t1);
}
TEST(PoolTests, CantAddIfDuplicate)
{
    auto pool = create_pool(1);
    auto t1 = pool->get();
    pool->release(t1);
    ASSERT_EQ(1, pool->available());
    ASSERT_EQ(1, pool->size());
    ASSERT_FALSE(pool->add(t1, true));
    ASSERT_EQ(1, pool->available());
    ASSERT_EQ(1, pool->size());
    ASSERT_EQ(t1, pool->get());
    pool->release(t1);
}
TEST(PoolTests, CanRemoveAfterGet)
{
    auto pool = create_pool(1);
    auto t1 = pool->get();
    ASSERT_TRUE(pool->remove(t1));
    ASSERT_EQ(1, pool->available());
    ASSERT_EQ(0, pool->size());
}
TEST(PoolTests, CanRemoveBeforeGet)
{
    auto pool = create_pool(1);
    auto t1 = pool->get();
    pool->release(t1);
    ASSERT_EQ(1, pool->available());
    ASSERT_EQ(1, pool->size());
    ASSERT_TRUE(pool->remove(t1));
    ASSERT_EQ(1, pool->available());
    ASSERT_EQ(0, pool->size());
}
TEST(PoolTests, CantRemoveUnknown)
{
    auto pool = create_pool(1);
    auto t1 = pool->get();
    uint64_t arbitrary = 1234567891234567890ull;
    pool->release(t1);
    ASSERT_EQ(1, pool->available());
    ASSERT_EQ(1, pool->size());
    ASSERT_FALSE(pool->remove(arbitrary));
    ASSERT_EQ(1, pool->available());
    ASSERT_EQ(1, pool->size());
    ASSERT_EQ(t1, pool->get());
}
TEST(PoolTests, CanClone)
{
    auto pool1 = create_pool(1);
    auto pool2 = pool1->clone(2);
    ASSERT_EQ(2, pool2->max_size());
    ASSERT_TRUE(pool1->get() > 0ull);
    ASSERT_TRUE(pool2->get() > 0ull);
}
TEST(PoolTest, CanTryGet)
{
    auto pool = create_pool(1);
    auto opt_t = pool->try_get();
    ASSERT_TRUE(opt_t);
    ASSERT_TRUE(*opt_t > 0ull);
}
TEST(PoolTest, CanTryGetFail)
{
    auto pool = create_pool(1);
    auto opt_t = pool->try_get();
    ASSERT_TRUE(opt_t);
    ASSERT_TRUE(*opt_t > 0ull);
    auto opt_t2 = pool->try_get();
    ASSERT_EQ(boost::none, opt_t2);
}
TEST(PoolTest, CreateEvents)
{
    pool_event_counter<uint64_t> ev;
    auto handler = [&ev](pool_event e, const uint64_t& t) { ev.handler(e, t); };
    auto pool = create_pool(2);
    pool->set_event_handler(handler);
    auto t = pool->get();
    ASSERT_EQ(1, ev.create);
}
TEST(PoolTest, DeleteEvents)
{
    pool_event_counter<uint64_t> ev;
    auto handler = [&ev](pool_event e, const uint64_t& t) { ev.handler(e, t); };
    {
        auto pool = create_pool(2);
        pool->set_event_handler(handler);
        auto t = pool->get();
        auto t2 = pool->get();
        pool->release(t);
    }
    ASSERT_EQ(2, ev.create.load());
    ASSERT_EQ(1, ev.destroy.load());
    ASSERT_EQ(1, ev.destroy_not_available.load());
}
TEST(PoolTest, EventsWithSwap)
{
    pool_event_counter<uint64_t> ev;
    pool_event_counter<uint64_t> ev2;
    auto handler = [&ev](pool_event e, const uint64_t& t) { ev.handler(e, t); };
    {
        auto pool1 = create_pool(2);
        pool1->set_event_handler([&ev](pool_event e, const uint64_t& t) { ev.handler(e, t); });
        auto pool2 = create_pool(2);
        pool2->set_event_handler([&ev2](pool_event e, const uint64_t& t) { ev2.handler(e, t); });

        // now create one, which will be available, in 1
        pool1->release(pool1->get());
        ASSERT_EQ(1, ev.create);

        // swap available to 2
        pool1->swap_available(*pool2, true);
        ASSERT_EQ(1, ev.remove.load());
        ASSERT_EQ(1, ev2.add.load());
        ASSERT_EQ(0, ev.destroy.load());
        ASSERT_EQ(0, ev2.destroy.load());
    }
    ASSERT_EQ(0, ev.destroy.load());
    ASSERT_EQ(1, ev2.destroy.load());
}
