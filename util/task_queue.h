#ifndef TASKQUEUE_H
#define TASKQUEUE_H

#include "boost/noncopyable.hpp"
#include <thread>
#include <atomic>
#include <future>
#include <queue>

/// executes tasks in the added order
class TaskQueue : public boost::noncopyable
{
public:
  TaskQueue();
  /// joins currently ran task.
  ~TaskQueue();
  void add(std::function<void()> &&t);
  std::future<void> addF(std::function<void()> &&f);
  /// waits till all previously added tasks are done.
  /// the queue is finished after this. no add is possible.
  void finish();
private:
  typedef std::packaged_task<void()> Task;
  std::thread workerThread_;
  std::queue< std::packaged_task<void()> > tasks_;
  std::mutex queue_mutex_;
  std::condition_variable wake_up_;
  std::atomic_bool stop_;
};
#endif // TASKQUEUE_H
