#include "task_queue.h"
#include <assert.h>

using namespace std;

TaskQueue::TaskQueue() : stop_(false){
  workerThread_= thread([this]{
    while(true)
    {
      std::unique_lock<std::mutex> lock(queue_mutex_);
      while(!stop_ && tasks_.empty())
        wake_up_.wait(lock);
      if(stop_)
        return;
      Task task(move(tasks_.front()));
      tasks_.pop();
      lock.unlock();
      task();
    }
  });
}

TaskQueue::~TaskQueue()
{
  if (stop_)
    return;
  stop_ = true;
  wake_up_.notify_one();
  workerThread_.join();
}

void TaskQueue::add(std::function<void()> &&f)
{
  assert(!stop_);
//  if(stop)
//    throw std::runtime_error("enqueue on stopped TaskQueue");
 {
    unique_lock<std::mutex> lock(queue_mutex_);
    tasks_.emplace(Task(move(f)));
  }
  wake_up_.notify_one();
}

std::future<void> TaskQueue::addF(std::function<void()> &&f)
{
  assert(!stop_);
  Task t(move(f));
  auto fut = t.get_future();
  {
     unique_lock<std::mutex> lock(queue_mutex_);
     tasks_.emplace(move(t));
  }
  wake_up_.notify_one();
  return fut;
}

void TaskQueue::finish()
{
  add([this] {
    stop_ = true;} );
  workerThread_.join();
}

