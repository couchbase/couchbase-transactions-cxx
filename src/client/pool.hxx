/*
 *     Copyright 2020 Couchbase, Inc.
 *
 *   Licensed under the Apache License, Version 2.0 (the "License");
 *   you may not use this file except in compliance with the License.
 *   You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 *   Unless required by applicable law or agreed to in writing, software
 *   distributed under the License is distributed on an "AS IS" BASIS,
 *   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *   See the License for the specific language governing permissions and
 *   limitations under the License.
 */
#pragma once

#include <boost/optional.hpp>
#include <spdlog/spdlog.h>
#include <couchbase/support.hxx>
#include <condition_variable>
#include <functional>
#include <list>
#include <thread>
#include <atomic>

namespace couchbase
{

enum class PoolEvent { create, remove, destroy, add, destroy_not_available };

template<typename T>
struct PoolEventCounter {
    std::atomic<uint32_t> create;
    std::atomic<uint32_t> remove;
    std::atomic<uint32_t> destroy;
    std::atomic<uint32_t> add;
    std::atomic<uint32_t> destroy_not_available;

    PoolEventCounter()
      : create(0)
      , remove(0)
      , destroy(0)
      , add(0)
      , destroy_not_available(0)
    {
    }

    void handler(PoolEvent e, const T&)
    {
        switch (e) {
            case PoolEvent::create:
                ++create;
                break;
            case PoolEvent::remove:
                ++remove;
                break;
            case PoolEvent::destroy:
                ++destroy;
                break;
            case PoolEvent::add:
                ++add;
                break;
            case PoolEvent::destroy_not_available:
                ++destroy_not_available;
                break;
        }
    }
};

template<typename T>
class Pool
{
    typedef std::pair<bool, T> pair_t;
    typedef std::function<void(PoolEvent, T&)> event_handler;

  public:
    Pool(size_t max_size, std::function<T(void)> create_fn, std::function<void(T)> destroy_fn)
      : max_size_(max_size)
      , available_(max_size)
      , create_fn_(create_fn)
      , destroy_fn_(destroy_fn)
    {
        post_create_fn_ = [](T t) { return t; };
        event_fn_ = [](PoolEvent, const T&) {};
    }

    ~Pool()
    {
        // destroy all the objects in the pool.
        std::unique_lock<std::mutex> lock(mutex_);
        for (pair_t& p : pool_) {
            if (p.first) {
                event_fn_(PoolEvent::destroy, p.second);
                destroy_fn_(p.second);
            } else {
                event_fn_(PoolEvent::destroy_not_available, p.second);
                spdlog::trace("cannot destroy {}, not available!", p.second);
            }
        }
        pool_.clear();
    }

    void set_event_handler(event_handler fn)
    {
        event_fn_ = fn;
    }

    CB_NODISCARD boost::optional<T> try_get()
    {
        if (T* t = get_internal()) {
            return *t;
        }
        return {};
    }

    CB_NODISCARD T& get()
    {
        T* found = get_internal();
        while (nullptr == found) {
            // if we get here, we have no more room, and have to wait
            {
                std::unique_lock<std::mutex> lock(mutex_);
                cv_.wait(lock, [&] { return available_.load() > 0; });
            }
            found = get_internal();
        }
        return *found;
    }

    void release(T& t)
    {
        std::unique_lock<std::mutex> lock(mutex_);
        auto it = std::find_if(pool_.begin(), pool_.end(), [&](const pair_t& p) { return t == p.second; });
        if (it == pool_.end()) {
            spdlog::error("releasing unknown {}", t);
            return;
        }
        it->first = true;
        ++available_;
        cv_.notify_one();
    }

    CB_NODISCARD bool add(T& t, bool available)
    {
        std::unique_lock<std::mutex> lock(mutex_);
        if (pool_.size() >= max_size_) {
            spdlog::warn("cannot add {}, pool full");
            return false;
        }
        auto it = std::find_if(pool_.begin(), pool_.end(), [&](const pair_t& p) { return t == p.second; });
        if (it != pool_.end()) {
            spdlog::warn("trying to add {}, which is already present", t);
            return true;
        }
        pool_.emplace_back(available, std::move(t));
        if (!available) {
            --available_;
        }
        event_fn_(PoolEvent::add, t);
        return true;
    }

    CB_NODISCARD bool remove(T& t)
    {
        std::unique_lock<std::mutex> lock(mutex_);
        auto it = std::find_if(pool_.begin(), pool_.end(), [&](const pair_t& p) { return t == p.second; });
        if (it == pool_.end()) {
            spdlog::error("trying to remove unknown {}", t);
            return false;
        }
        if (it->first) {
            spdlog::warn("trying to remove {} which is still available, probably a bug", t);
        } else {
            // if it was unavailable, increment available
            ++available_;
        }
        pool_.erase(it);
        // at this point, there is an available slot for an instance, so notify
        cv_.notify_one();
        event_fn_(PoolEvent::remove, t);
        return true;
    }

    CB_NODISCARD std::unique_ptr<Pool<T>> clone(size_t max_size = 0)
    {
        return std::unique_ptr<Pool<T>>(new Pool<T>(max_size ? max_size : max_size_, create_fn_, destroy_fn_));
    }

    bool swap_available(Pool<T>& other_pool, bool available)
    {
        // get available from this pool, insert into
        // other_pool
        T* found = get_internal();
        if (nullptr == found) {
            spdlog::trace("nothing available in this pool");
            return false;
        }
        {
            std::unique_lock<std::mutex> lock(other_pool.mutex_);
            if (other_pool.pool_.size() < other_pool.max_size_) {
                other_pool.pool_.emplace_back(available, std::move(*found));
                if (!available) {
                    --other_pool.available_;
                }
                other_pool.event_fn_(PoolEvent::add, *found);
                lock.unlock();
                // now erase from our pool
                std::unique_lock<std::mutex> lock2(mutex_);
                auto it = std::find_if(pool_.begin(), pool_.end(), [&](const pair_t& p) { return p.second == *found; });
                if (it != pool_.end()) {
                    pool_.erase(it);
                    ++available_;
                    cv_.notify_one();
                }
                event_fn_(PoolEvent::remove, *found);
                return true;
            }
        }
        spdlog::trace("other pool is full, cannot insert {}", *found);
        release(*found);
        return false;
    }

    void post_create_fn(std::function<T(T)> fn)
    {
        post_create_fn_ = fn;
        // now, we may have objects in the pool, so call this on them.
        std::unique_lock<std::mutex> lock(mutex_);
        std::for_each(pool_.begin(), pool_.end(), [this](pair_t& p) { p.second = post_create_fn_(p.second); });
    }

    CB_NODISCARD size_t available() const
    {
        return available_.load();
    }

    CB_NODISCARD size_t size() const
    {
        std::unique_lock<std::mutex> lock(mutex_);
        return pool_.size();
    }

    CB_NODISCARD size_t max_size() const
    {
        return max_size_;
    }

    template<typename R>
    R wrap_access(std::function<R(T)> fn)
    {
        auto t = get();
        try {
            auto ret = fn(t);
            release(t);
            return ret;
        } catch (...) {
            release(t);
            throw;
        }
    }

    template<typename OStream>
    friend OStream& operator<<(OStream& os, const Pool& p)
    {
        os << "Pool{";
        os << "available:" << p.available() << ",";
        os << " max: " << p.max_size() << ",";
        os << " size:" << p.size() << ",";
        os << "}";
        return os;
    }

  private:
    // mutable, so we can use it in const functions
    mutable std::mutex mutex_;
    std::condition_variable cv_;
    std::atomic<size_t> available_;
    size_t max_size_;
    std::list<pair_t> pool_;
    std::function<T(void)> create_fn_;
    std::function<void(T)> destroy_fn_;
    std::function<T(T)> post_create_fn_;
    event_handler event_fn_;
    T* get_internal()
    {
        std::unique_lock<std::mutex> lock(mutex_);
        auto it = std::find_if(pool_.begin(), pool_.end(), [&](const std::pair<bool, T>& p) { return p.first; });
        if (it != pool_.end()) {
            it->first = false;
            --available_;
            return &it->second;
        }
        if (pool_.size() < max_size_) {
            // create a new one, insert it and return.
            auto t = post_create_fn_(create_fn_());
            event_fn_(PoolEvent::create, t);
            pool_.emplace_back(false, t);
            --available_;
            return &(pool_.back().second);
        }
        return nullptr;
    }
};
}; // namespace couchbase
