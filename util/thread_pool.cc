#include "thread_pool.h"

#include <memory>
#include <assert.h>

using namespace std;

namespace leveldb {

ThreadPool::ThreadPool() : ThreadPool( std::thread::hardware_concurrency() )
{
}

ThreadPool::ThreadPool(unsigned int threads)
{
  for(size_t i = 0;i<threads;++i)
    workers.emplace_back([this]
    {
      while(true)
      {
        unique_lock<mutex> lock(queue_mutex);
        wake_up.wait(lock, [this]{ return stop || !tasks.empty(); });
        if( stop )
          return;
        packaged_task<void()> task(move(tasks.front()));
        tasks.pop();
        lock.unlock();
        task();
      }
    });
}

ThreadPool::~ThreadPool()
{
  {
    unique_lock<mutex> lock(queue_mutex);
    stop = true;
  }
  wake_up.notify_all();
  for(size_t i = 0; i < workers.size(); ++i)
    workers[i].join();
}

void ThreadPool::enqueue(function<void ()> &&f)
{
  {
    unique_lock<mutex> lock(queue_mutex);
    if(stop)
      throw runtime_error("enqueue on stopped ThreadPool");
    tasks.emplace(packaged_task<void()> (move(f)));
  }
  wake_up.notify_one();
}

}
