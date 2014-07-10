// -------------------------------------------------------------------
//
// hot_threads.cc
//
// Copyright (c) 2011-2013 Basho Technologies, Inc. All Rights Reserved.
//
// This file is provided to you under the Apache License,
// Version 2.0 (the "License"); you may not use this file
// except in compliance with the License.  You may obtain
// a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.
//
// -------------------------------------------------------------------

// -------------------------------------------------------------------
// HotThread is a subtle variation on the eleveldb_thread_pool.  Both
//  represent a design pattern that is tested to perform better under
//  the Erlang VM than other traditional designs.
// -------------------------------------------------------------------

#include <errno.h>
#include <syslog.h>
#include <sys/fcntl.h>
#include <sys/stat.h>
#include <unistd.h>

#include "leveldb/atomics.h"
#include "util/hot_threads.h"
#include "util/thread_tasks.h"

namespace leveldb {

HotThreadPool * gImmThreads=NULL;
HotThreadPool * gWriteThreads=NULL;
HotThreadPool * gLevel0Threads=NULL;
HotThreadPool * gCompactionThreads=NULL;



void *ThreadStaticEntry(void *args)
{
    HotThread &tdata = *(HotThread *)args;

    return(tdata.ThreadRoutine());

}   // ThreadStaticEntry


/**
 * Worker threads:  worker threads have 3 states:
 *  A. doing nothing, available to be claimed: m_Available=1
 *  B. processing work passed by Erlang thread: m_Available=0, m_DirectWork=<non-null>
 *  C. processing backlog queue of work: m_Available=0, m_DirectWork=NULL
 */
void *
HotThread::ThreadRoutine()
{
    ThreadTask * submission;

    submission=NULL;

//    pthread_setname_np(m_Pool.m_PoolName.c_str());

    while(!m_Pool.m_Shutdown)
    {
        // is work assigned yet?
        //  check backlog work queue if not
        if (NULL==submission)
        {
            // test non-blocking size for hint (much faster)
            if (0!=m_Pool.m_WorkQueueAtomic)
            {
                // retest with locking
                SpinLock lock(&m_Pool.m_QueueLock);

                if (!m_Pool.m_WorkQueue.empty())
                {
                    submission=m_Pool.m_WorkQueue.front();
                    m_Pool.m_WorkQueue.pop_front();
                    dec_and_fetch(&m_Pool.m_WorkQueueAtomic);
                    m_Pool.IncWorkDequeued();
                    m_Pool.IncWorkWeighted(Env::Default()->NowMicros()
                                           - submission->m_QueueStart);
                }   // if
            }   // if
        }   // if


        // a work item identified (direct or queue), work it!
        //  then loop to test queue again
        if (NULL!=submission)
        {
            // execute the job
            (*submission)();

            submission->RefDec();

            submission=NULL;
        }   // if

        // no work found, attempt to go into wait state
        //  (but retest queue before sleep due to race condition)
        else
        {
            MutexLock lock(&m_Mutex);

            m_DirectWork=NULL; // safety

            // only wait if we are really sure no work pending
            if (0==m_Pool.m_WorkQueueAtomic)
	    {
                // yes, thread going to wait. set available now.
	        m_Available=1;
                m_Condition.Wait();
	    }    // if

            m_Available=0;    // safety
            submission=(ThreadTask *)m_DirectWork; // NULL is valid
            m_DirectWork=NULL;// safety
        }   // else
    }   // while

    return 0;

}   // HotThread::ThreadRoutine


void *QueueThreadStaticEntry(void *args)
{
    QueueThread &tdata = *(QueueThread *)args;

    return(tdata.QueueThreadRoutine());

}   // QueueThreadStaticEntry


QueueThread::QueueThread(
    class HotThreadPool & Pool)
    : m_ThreadGood(false), m_Pool(Pool), m_SemaphorePtr(NULL)
{
    int ret_val;

    m_QueueName=m_Pool.m_PoolName;
    m_QueueName.append("Semaphore");

    memset(&m_Semaphore, 0, sizeof(m_Semaphore));
    ret_val=sem_init(&m_Semaphore, 0, 0);

    if (0==ret_val)
    {
        m_SemaphorePtr=&m_Semaphore;
    }   // if

    // retry with named semaphore
    else if (0!=ret_val && ENOSYS==errno)
    {
        char pid_str[32];
        snprintf(pid_str, sizeof(pid_str), "%d", (int)getpid());
        m_QueueName.append(pid_str);

        m_SemaphorePtr=sem_open(m_QueueName.c_str(), O_CREAT | O_EXCL,
                                S_IRUSR | S_IWUSR, 0);
        // attempt delete and retry blindly
        if (SEM_FAILED==m_SemaphorePtr)
        {
            sem_unlink(m_QueueName.c_str());
            m_SemaphorePtr=sem_open(m_QueueName.c_str(), O_CREAT | O_EXCL,
                                    S_IRUSR | S_IWUSR, 0);
        }   // if

        // so ret_val will be zero on success
        ret_val=(SEM_FAILED==m_SemaphorePtr);
    }   // else if

    // only start this if we have a working semaphore
    if (0==ret_val)
    {
        ret_val=pthread_create(&m_ThreadId, NULL,  &QueueThreadStaticEntry, this);
        if (0==ret_val)
        {
            m_ThreadGood=true;
        }   // if
        else
        {
            syslog(LOG_ERR, "thread_create failed in QueueThread::QueueThread [%d, %m]",
                   errno);
            gPerfCounters->Inc(ePerfThreadError);

            if (&m_Semaphore==m_SemaphorePtr)
            {
                sem_destroy(&m_Semaphore);
            }   // if
            else
            {
                sem_close(m_SemaphorePtr);
                sem_unlink(m_QueueName.c_str());
            }   // else

            m_SemaphorePtr=NULL;
        }   // else
    }   // if
    else
    {
        m_SemaphorePtr=NULL;
        syslog(LOG_ERR, "sem_init failed in QueueThread::QueueThread [%d, %m]",
               errno);
        gPerfCounters->Inc(ePerfThreadError);
    }   // else

    return;

}   // QueueThread::QueueThread


QueueThread::~QueueThread()
{
    // only clean up resources if they were started
    if (m_ThreadGood)
    {
        // release the m_QueueThread to exit
        sem_post(m_SemaphorePtr);
        pthread_join(m_ThreadId, NULL);

        if (&m_Semaphore==m_SemaphorePtr)
        {
            sem_destroy(&m_Semaphore);
        }   // if
        else
        {
            sem_close(m_SemaphorePtr);
            sem_unlink(m_QueueName.c_str());
        }   // else
    }   // if

    return;
}   // QueueThread::~QueueThread


/**
 * Cover highly inprobable race condition with adding a queue
 *  and no hot thread sees it.
 */
void *
QueueThread::QueueThreadRoutine()
{
    ThreadTask * submission;

    submission=NULL;

//    pthread_setname_np(m_QueueName.c_str());

    while(!m_Pool.m_Shutdown)
    {
        // test non-blocking size for hint (much faster)
        if (0!=m_Pool.m_WorkQueueAtomic)
        {
            // retest with locking
            SpinLock lock(&m_Pool.m_QueueLock);

            if (!m_Pool.m_WorkQueue.empty())
            {
                submission=m_Pool.m_WorkQueue.front();
                m_Pool.m_WorkQueue.pop_front();
                dec_and_fetch(&m_Pool.m_WorkQueueAtomic);
                m_Pool.IncWorkDequeued();
                m_Pool.IncWorkWeighted(Env::Default()->NowMicros()
                                       - submission->m_QueueStart);
            }   // if
        }   // if


        // a work item identified (direct or queue), work it!
        //  then loop to test queue again
        if (NULL!=submission)
        {
            // execute the job
            (*submission)();

            submission->RefDec();

            submission=NULL;
        }   // if

        // loop until semaphore hits zero
        sem_wait(m_SemaphorePtr);

    }   // while

    return 0;

}   // QueueThread::QueueThreadRoutine


HotThreadPool::HotThreadPool(
    const size_t PoolSize,
    const char * Name,
    enum PerformanceCountersEnum Direct,
    enum PerformanceCountersEnum Queued,
    enum PerformanceCountersEnum Dequeued,
    enum PerformanceCountersEnum Weighted)
    : m_PoolName((Name?Name:"")),    // this crashes if Name is NULL ...but need it set now
      m_Shutdown(false), 
      m_WorkQueueAtomic(0), m_QueueThread(*this),
      m_DirectCounter(Direct), m_QueuedCounter(Queued),
      m_DequeuedCounter(Dequeued), m_WeightedCounter(Weighted)
{
    int ret_val;
    size_t loop;
    HotThread * hot_ptr;

    ret_val=0;
    for (loop=0; loop<PoolSize && 0==ret_val; ++loop)
    {
        hot_ptr=new HotThread(*this);

        ret_val=pthread_create(&hot_ptr->m_ThreadId, NULL,  &ThreadStaticEntry, hot_ptr);
        if (0==ret_val)
            m_Threads.push_back(hot_ptr);
        else
            delete hot_ptr;
    }   // for

    m_Shutdown=(0!=ret_val);

    return;

}   // HotThreadPool::HotThreadPool


HotThreadPool::~HotThreadPool()
{
    ThreadPool_t::iterator thread_it;
    WorkQueue_t::iterator work_it;
    // set flag
    m_Shutdown=true;

    // get all threads stopped
    for (thread_it=m_Threads.begin(); m_Threads.end()!=thread_it; ++thread_it)
    {
        {
            MutexLock lock(&(*thread_it)->m_Mutex);
            (*thread_it)->m_Condition.SignalAll();
        }   // lock

        pthread_join((*thread_it)->m_ThreadId, NULL);
        delete *thread_it;
    }   // for

    // release any objects hanging in work queue
    for (work_it=m_WorkQueue.begin(); m_WorkQueue.end()!=work_it; ++work_it)
    {
        (*work_it)->RefDec();
    }   // for

    return;

}   // HotThreadPool::~HotThreadPool


bool                           // returns true if available worker thread found and claimed
HotThreadPool::FindWaitingThread(
    ThreadTask * work, // non-NULL to pass current work directly to a thread,
                       // NULL to potentially nudge an available worker toward backlog queue
    bool OkToQueue)
{
    bool ret_flag;
    size_t start, index, pool_size;

    ret_flag=false;

    // pick "random" place in thread list.  hopefully
    //  list size is prime number.
    pool_size=m_Threads.size();
    if (OkToQueue)
        start=(size_t)pthread_self() % pool_size;
    else
        start=0;
    index=start;

    do
    {
        // perform quick test to see thread available
        if (0!=m_Threads[index]->m_Available && !shutdown_pending())
        {
            // perform expensive compare and swap to potentially
            //  claim worker thread (this is an exclusive claim to the worker)
            ret_flag = compare_and_swap(&m_Threads[index]->m_Available, 1, 0);

            // the compare/swap only succeeds if worker thread is sitting on
            //  pthread_cond_wait ... or is about to be there but is holding
            //  the mutex already
            if (ret_flag)
            {

                // man page says mutex lock optional, experience in
                //  this code says it is not.  using broadcast instead
                //  of signal to cover one other race condition
                //  that should never happen with single thread waiting.
                MutexLock lock(&m_Threads[index]->m_Mutex);
                m_Threads[index]->m_DirectWork=work;
                m_Threads[index]->m_Condition.SignalAll();
            }   // if
        }   // if

        index=(index+1)%pool_size;

    } while(index!=start && !ret_flag && OkToQueue);

    return(ret_flag);

}   // FindWaitingThread


bool
HotThreadPool::Submit(
    ThreadTask* item,
    bool OkToQueue)
{
    bool ret_flag(false);
    int ret_val;

    if (NULL!=item)
    {
        item->RefInc();

        // do nothing if shutting down
        if(shutdown_pending())
        {
            item->RefDec();
            ret_flag=false;
        }   // if

        // try to give work to a waiting thread first
        else if (FindWaitingThread(item, OkToQueue))
        {
            IncWorkDirect();
            ret_flag=true;
        }   // else if

        else if (OkToQueue)
        {
            item->m_QueueStart=Env::Default()->NowMicros();

            // no waiting threads, put on backlog queue
            {
                SpinLock lock(&m_QueueLock);
                inc_and_fetch(&m_WorkQueueAtomic);
                m_WorkQueue.push_back(item);
            }

            // to address race condition, thread might be waiting now
            FindWaitingThread(NULL, true);

            // to address second race condition, send in QueueThread
            //   (thread not likely good on OSX)
            if (m_QueueThread.m_ThreadGood)
            {
                ret_val=sem_post(m_QueueThread.m_SemaphorePtr);
                if (0!=ret_val)
                {
                    syslog(LOG_ERR, "sem_post failed in HotThreadPool::Submit [%d, %m]",
                           errno);
                    gPerfCounters->Inc(ePerfThreadError);
                }   // if
            }   // if

            IncWorkQueued();
            ret_flag=true;
        }   // else if

        // did not post to thread or queue
        else
        {
            item->RefDec();
            ret_flag=false;  // redundant, but safe
        }   // else
    }   // if

    return(ret_flag);

}   // HotThreadPool::Submit

};  // namespace leveldb
