(ns dsl-crawler.threadpool
  (:import [java.util.concurrent Callable Executors ThreadPoolExecutor TimeUnit TimeoutException]
           [dsl_crawler.threadpool ConfigurableThreadFactory]))

(defn cpu-count
  "Returns the number of CPUs on this machine."
  []
  (.availableProcessors (Runtime/getRuntime)))

(defn t-pool
  "Creates and return a new thread pool. The `type` can be:

  :fixed  - Thread pool with fixed number of threads and unbounded task queue.
            This type has fixed number of threads that are never terminated
            and are executing submitted tasks. If a new task is submitted, but
            all threads are already executing other tasks, the task will be
            queued for a future execution.
            This type accepts these optional parameters:

            :size - the number of threads (defaults to the number of available
                    CPUs + 2).

  :cached - Thread pool with unbounded maximal number of threads and no task
            queue. For any new submitted task a new thread is created which
            will execute the task. The threads will be terminated if they have
            been idle for more than their keep-alive time.
            This type accepts these optional parameters:

            :keepalive - the keep-alive time in milliseconds (defaults to
                         15000ms). This is the amount of time that threads
                         may remain idle before being terminated. A value
                         of zero will cause threads to terminate immediately
                         after they ended executing a task.

  :variable - Thread pool with limited maximal number of threads and unbounded
              task queue. When a new task is submitted, this type will create
              a new thread unless the maximal allowed number of threads is
              reached. If this occurs, the task will be queued for a future
              execution. After the thread has finished executing the task, it
              will wait for the keep-alive time and then terminated.
              This type accepts these optional parameters:

              :size  - Maximal allowed number of threads (defaults to number
                       of available CPUs + 2).

              :keepalive - The same as above, but must be greater then zero
                           (defaults to 15000ms).

  All thread pool types also accept these optional parameters:

            :daemon - Specifies whether the threads in this thread pool
                      are daemon or not (default is false).
                      The JVM will shut down when all non-daemon threads
                      have terminated, so daemon threads are threads
                      whose existence has no impact on whether the JVM
                      continues to execute or shuts down.

            :prefix - Specifies the string prefix for the name of the threads.
                      This can be useful especially when debugging.

  If no `type` is specified, defaults to the :fixed type with default values
  for all available parameters.

  Examples:
            (t-pool)

            (t-pool :fixed)
            (t-pool :fixed :daemon true :prefix \"event-manager\")
            (t-pool :fixed :size 5)
            (t-pool :fixed :size 8 :daemon true)

            (t-pool :cached)
            (t-pool :cached :prefix \"server\")
            (t-pool :cached :keepalive 60000)

            (t-pool :variable :size 8)
            (t-pool :variable :size 10 :keepalive 30000 :daemon true)
  "
  ([]
    (t-pool :fixed))
  ([type & {:keys [size max keepalive daemon prefix]
            :or    {size (+ (cpu-count) 2)
                    keepalive 15000
                    daemon false
                    prefix "sweeney-backend-threadpool"}}]
  {:pre [(< 0 size)]}
    (doto
      (case type
        :fixed (Executors/newFixedThreadPool size)
        :cached (doto (Executors/newCachedThreadPool)
                   (.setKeepAliveTime keepalive TimeUnit/MILLISECONDS))
        :variable (doto (Executors/newFixedThreadPool size)
                     (assert (< 0 keepalive))
                     (.setKeepAliveTime keepalive TimeUnit/MILLISECONDS)
                     (.allowCoreThreadTimeOut true))
        (throw (RuntimeException. (str "Unsupported thread pool type: '" type "'."))))
      (.setThreadFactory (ConfigurableThreadFactory. prefix daemon)))))

(defn shutdown!
  "Executes all previously submitted tasks, shutdowns the thread `pool` and
  returns it."
  [pool]
  (.shutdown pool)
  pool)

(defn shutdown-now!
  "Shutdowns the thread `pool` immediately, stopping all executing tasks and
  returns it."
  [pool]
  (.shutdownNow pool)
  pool)

(defn shutdown?
  "Returns whether the thread `pool` has been already shut down."
  [pool]
  (.isShutdown pool))

(defn terminated?
  "Returns whether the thread `pool` has already terminated (shutdown and
  also all tasks ended."
  [pool]
  (.isTerminated pool))

(defn submitted-tasks
  "Returns the approximate total number of tasks
  that have ever been submitted for execution."
  [pool]
  (.getTaskCount pool))

(defn completed-tasks
  "Returns the approximate total number of tasks
  that have completed execution."
  [pool]
  (.getCompletedTaskCount pool))

(defn active-tasks
  "Returns the approximate number of threads
  that are actively executing tasks."
  [pool]
  (.getActiveCount pool))

(defn queued-tasks
  "Returns the number of tasks that have been submitted and
  are waiting for execution."
  [pool]
  (.size (.getQueue pool)))

(defn queue-capacity
  "Returns maximal capacity of the queue of the provided thread pool."
  [pool]
  (let [q (.getQueue pool)]
    (+ (.size q) (.remainingCapacity q))))

(defn min-size
  "Returns the core number of threads, which are the threads that aren't
  terminated even when they are idle (unless the pool is of :variable type)."
  [pool]
  (.getCorePoolSize pool))

(defn max-size
  "Returns the maximum allowed number of threads."
  [pool]
  (.getMaximumPoolSize pool))

(defn current-size
  "Returns the current number of threads in the pool."
  [pool]
  (.getPoolSize pool))

(defn peak-size
  "Returns the largest number of threads that
  have ever simultaneously been in the pool."
  [pool]
  (.getLargestPoolSize pool))

(defn core-thread-timeout?
  "Returns true if this pool allows core threads to time out and terminate
  if no tasks arrive within the keepAlive time, being replaced if needed
  when new tasks arrive."
  [pool]
  (.allowsCoreThreadTimeOut pool))

(defn keepalive-time
  "Returns the thread keep-alive time, which is the amount of time that
  threads in excess of the core pool size may remain idle before being
  terminated."
  [pool]
  (.getKeepAliveTime pool TimeUnit/MILLISECONDS))

(defn to-future
  "Takes a java.util.concurrent.Future and returns a Clojure future made
  from it."
  {:static true}
  [fut]
  (reify
    clojure.lang.IDeref
      (deref [_] (.get fut))
    clojure.lang.IBlockingDeref
      (deref [_ timeout-ms timeout-val]
        (try (.get fut timeout-ms TimeUnit/MILLISECONDS)
             (catch TimeoutException e timeout-val)))
    clojure.lang.IPending
      (isRealized [_] (.isDone fut))
    java.util.concurrent.Future
      (get [_] (.get fut))
      (get [_ timeout unit] (.get fut timeout unit))
      (isCancelled [_] (.isCancelled fut))
      (isDone [_] (.isDone fut))
      (cancel [_ interrupt?] (.cancel fut interrupt?))))

(defn to-callable
  "Takes a function of zero arguments and returns
  a java.util.concurrent.Callable made from it."
  {:static true}
  [f]
  (reify
    Callable
    (call [_] (f))))

(defn submit
  "Submits function `f` for execution by thread `pool` and returns a future
  representing the value returned by the function."
  [pool f]
  (to-future
    (.submit pool (to-callable f))))

(defmethod print-method ThreadPoolExecutor
  [p w]
  (.write w (str "#<ThreadPoolExecutor: "

                 (if (shutdown? p) "SHUTDOWN - " "")
                 (if (terminated? p) "and TERMINATED - " "")

                 "Tasks: "
                 (submitted-tasks p) " submitted, "
                 (completed-tasks p) " completed, "
                 (active-tasks p) " active, "
                 (queued-tasks p) " queued "

                 "(limit is "
                 (queue-capacity p) "); "

                 "Threads: "
                 (if (core-thread-timeout? p) "0" (min-size p)) " min, "
                 (max-size p) " max, "
                 (current-size p) " current, "
                 (peak-size p) " peak; "

                 "Timeout: "
                 (keepalive-time p) "ms"
                 ">"
            )))
