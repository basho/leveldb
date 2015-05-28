#ifndef THREAD_POOL_H
#define THREAD_POOL_H

#include <thread>
#include <future>
#include <vector>
#include <queue>

namespace leveldb {

class ThreadPool {
public:
	// creates with pool with num threads == num hardware threads
	ThreadPool();
  ThreadPool(unsigned int numThreads);
  void enqueue(std::function<void()>&& f);
  template<class F, class... Args>
  auto enqueue(F&& f, Args&&... args)
				-> std::future<typename std::result_of<F(Args...)>::type>;
	// the destructor joins all threads
	~ThreadPool();
private:
	// need to keep track of threads so we can join them
	std::vector< std::thread > workers;
	// the task queue
  std::queue< std::packaged_task<void()> > tasks;

	// synchronization
	std::mutex queue_mutex;
	std::condition_variable wake_up;
  bool stop = false;
};

// add new work item to the pool
template<class F, class... Args>
auto ThreadPool::enqueue(F&& f, Args&&... args)
		-> std::future<typename std::result_of<F(Args...)>::type>
{
  typedef typename std::result_of<F(Args...)>::type return_type;

  auto task = std::packaged_task<return_type()> (
    std::bind(std::forward<F>(f), std::forward<Args>(args)...) );

  std::future<return_type> res = task.get_future();
	{
		std::unique_lock<std::mutex> lock(queue_mutex);
    if(stop)
      throw std::runtime_error("enqueue on stopped ThreadPool");
    tasks.push(std::move(task));
	}
	wake_up.notify_one();
	return res;
}

}

#endif
