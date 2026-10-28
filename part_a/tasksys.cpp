#include "tasksys.h"
#include <iostream>


IRunnable::~IRunnable() {}

ITaskSystem::ITaskSystem(int num_threads) {}
ITaskSystem::~ITaskSystem() {}

/*
 * ================================================================
 * Serial task system implementation
 * ================================================================
 */

const char* TaskSystemSerial::name() {
    return "Serial";
}

TaskSystemSerial::TaskSystemSerial(int num_threads): ITaskSystem(num_threads) {
}

TaskSystemSerial::~TaskSystemSerial() {}

void TaskSystemSerial::run(IRunnable* runnable, int num_total_tasks) {
    for (int i = 0; i < num_total_tasks; i++) {
        runnable->runTask(i, num_total_tasks);
    }
}

TaskID TaskSystemSerial::runAsyncWithDeps(IRunnable* runnable, int num_total_tasks,
                                          const std::vector<TaskID>& deps) {
    // You do not need to implement this method.
    return 0;
}

void TaskSystemSerial::sync() {
    // You do not need to implement this method.
    return;
}

/*
 * ================================================================
 * Parallel Task System Implementation
 * ================================================================
 */

const char* TaskSystemParallelSpawn::name() {
    return "Parallel + Always Spawn";
}

TaskSystemParallelSpawn::TaskSystemParallelSpawn(int num_threads): ITaskSystem(num_threads) {
    //
    // TODO: CS149 student implementations may decide to perform setup
    // operations (such as thread pool construction) here.
    // Implementations are free to add new class member variables
    // (requiring changes to tasksys.h).
    //
    num_threads_ = num_threads;
    threads_ = new std::thread[num_threads];
    task_index_ = 0;
}

TaskSystemParallelSpawn::~TaskSystemParallelSpawn() {
    delete [] threads_;
}

void TaskSystemParallelSpawn::runThreadDynamic(IRunnable* runnable, int num_total_tasks){
    int local_index;
    while(true){

        mutex_.lock();
        local_index = task_index_++;
        mutex_.unlock();
        
        if(local_index >= num_total_tasks)
            break;

        runnable->runTask(local_index, num_total_tasks);
    }
}

void TaskSystemParallelSpawn::runThreadStatic(int thread_id, int num_threads_needed, IRunnable* runnable, int num_total_tasks){
    for(int i = thread_id; i < num_total_tasks; i += num_threads_needed)
        runnable->runTask(i, num_total_tasks);
}

void TaskSystemParallelSpawn::run(IRunnable* runnable, int num_total_tasks) {


    //
    // TODO: CS149 students will modify the implementation of this
    // method in Part A.  The implementation provided below runs all
    // tasks sequentially on the calling thread.
    //

    /**
     * ==============================================
     * DYNAMIC ASSIGNMENT
     * ==============================================
     */
    // int num_threads_needed = std::min(num_threads_, num_total_tasks);

    // for (int i = 0; i < num_threads_needed; i++) {
    //     //runnable->runTask(i, num_total_tasks);
    //     threads_[i] = std::thread(&TaskSystemParallelSpawn::runThreadDynamic, this, runnable, num_total_tasks);
    // }

    // for (int i = 0; i < num_threads_needed; i++){
    //     threads_[i].join();
    // }

    // task_index_ = 0;

    /**
     * ==============================================
     * STATIC ASSIGNMENT
     * ==============================================
     * 
     */
    int num_threads_needed = std::min(num_threads_, num_total_tasks);
    for(int i = 0; i < num_threads_needed; i++){
        threads_[i] = std::thread(&TaskSystemParallelSpawn::runThreadStatic, this, i, num_threads_needed, runnable, num_total_tasks);
    }

    for (int i = 0; i < num_threads_needed; i++){
        threads_[i].join();
    }
    
}

TaskID TaskSystemParallelSpawn::runAsyncWithDeps(IRunnable* runnable, int num_total_tasks,
                                                 const std::vector<TaskID>& deps) {
    // You do not need to implement this method.
    return 0;
}

void TaskSystemParallelSpawn::sync() {
    // You do not need to implement this method.
    return;
}

/*
 * ================================================================
 * Parallel Thread Pool Spinning Task System Implementation
 * ================================================================
 */

const char* TaskSystemParallelThreadPoolSpinning::name() {
    return "Parallel + Thread Pool + Spin";
}

TaskSystemParallelThreadPoolSpinning::TaskSystemParallelThreadPoolSpinning(int num_threads): ITaskSystem(num_threads) {
    //
    // TODO: CS149 student implementations may decide to perform setup
    // operations (such as thread pool construction) here.
    // Implementations are free to add new class member variables
    // (requiring changes to tasksys.h).
    //
    running_ = true;
    num_threads_ = num_threads;
    threads_ = new std::thread[num_threads];
    for(int i = 0; i < num_threads_; i++)
        threads_[i] = std::thread(&TaskSystemParallelThreadPoolSpinning::runThread, this);

    task_index_ = 0;
    finished_tasks_ = 0;
    num_total_tasks_ = 0;
}

TaskSystemParallelThreadPoolSpinning::~TaskSystemParallelThreadPoolSpinning() {
    running_ = false;
    for(int i = 0; i < num_threads_; i++)
        threads_[i].join();

    delete [] threads_;
}

void TaskSystemParallelThreadPoolSpinning::runThread(){
    int local_index, total_tasks;
    while(running_){
        total_tasks_mutex_.lock();
        total_tasks = num_total_tasks_;
        total_tasks_mutex_.unlock();
        if(total_tasks){
            task_index_mutex_.lock();
            local_index = task_index_++;
            task_index_mutex_.unlock();
            
            if(local_index < total_tasks){
                runnable_->runTask(local_index, total_tasks);
                finished_tasks_mutex_.lock();
                finished_tasks_++;
                finished_tasks_mutex_.unlock();
            }
        }
    }
}

void TaskSystemParallelThreadPoolSpinning::run(IRunnable* runnable, int num_total_tasks) {
    //
    // TODO: CS149 students will modify the implementation of this
    // method in Part A.  The implementation provided below runs all
    // tasks sequentially on the calling thread.
    //
    runnable_ = runnable;

    task_index_mutex_.lock();
    task_index_ = 0;
    task_index_mutex_.unlock();

    total_tasks_mutex_.lock();
    num_total_tasks_ = num_total_tasks;
    total_tasks_mutex_.unlock();

    int current_finished_tasks = 0;
    while(true){
        finished_tasks_mutex_.lock();
        current_finished_tasks = finished_tasks_;
        finished_tasks_mutex_.unlock(); 
        
        if(current_finished_tasks == num_total_tasks)
            break;
    }
    total_tasks_mutex_.lock();
    num_total_tasks_ = 0;
    total_tasks_mutex_.unlock();

    finished_tasks_mutex_.lock();
    finished_tasks_ = 0;
    finished_tasks_mutex_.unlock();
}

TaskID TaskSystemParallelThreadPoolSpinning::runAsyncWithDeps(IRunnable* runnable, int num_total_tasks,
                                                              const std::vector<TaskID>& deps) {
    // You do not need to implement this method.
    return 0;
}

void TaskSystemParallelThreadPoolSpinning::sync() {
    // You do not need to implement this method.
    return;
}

/*
 * ================================================================
 * Parallel Thread Pool Sleeping Task System Implementation
 * ================================================================
 */

const char* TaskSystemParallelThreadPoolSleeping::name() {
    return "Parallel + Thread Pool + Sleep";
}

TaskSystemParallelThreadPoolSleeping::TaskSystemParallelThreadPoolSleeping(int num_threads): ITaskSystem(num_threads) {
    //
    // TODO: CS149 student implementations may decide to perform setup
    // operations (such as thread pool construction) here.
    // Implementations are free to add new class member variables
    // (requiring changes to tasksys.h).
    //
}

TaskSystemParallelThreadPoolSleeping::~TaskSystemParallelThreadPoolSleeping() {
    //
    // TODO: CS149 student implementations may decide to perform cleanup
    // operations (such as thread pool shutdown construction) here.
    // Implementations are free to add new class member variables
    // (requiring changes to tasksys.h).
    //
}

void TaskSystemParallelThreadPoolSleeping::run(IRunnable* runnable, int num_total_tasks) {


    //
    // TODO: CS149 students will modify the implementation of this
    // method in Parts A and B.  The implementation provided below runs all
    // tasks sequentially on the calling thread.
    //

    for (int i = 0; i < num_total_tasks; i++) {
        runnable->runTask(i, num_total_tasks);
    }
}

TaskID TaskSystemParallelThreadPoolSleeping::runAsyncWithDeps(IRunnable* runnable, int num_total_tasks,
                                                    const std::vector<TaskID>& deps) {


    //
    // TODO: CS149 students will implement this method in Part B.
    //

    return 0;
}

void TaskSystemParallelThreadPoolSleeping::sync() {

    //
    // TODO: CS149 students will modify the implementation of this method in Part B.
    //

    return;
}
