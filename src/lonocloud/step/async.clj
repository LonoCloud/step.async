(ns lonocloud.step.async
  "Implemenation of the step machine library."
  (:refer-clojure :exclude [reduce map merge into take partition partition-by])
  (:require [clojure.core.async :as async]
            [clojure.core.async.impl.protocols :as async-protocols]
            [clojure.pprint :as pprint]
            [clojure.set :as set]
            [clojure.tools.logging :refer [errorf]]
            [lonocloud.step.async.pschema :as s]
            [lonocloud.step.async.step-channel :as step-channel]
            [lonocloud.step.async.step-schedule :as step-schedule]
            [lonocloud.step.async.step-listen :as step-listen]
            [lonocloud.step.async.step-log :as step-log]
            [lonocloud.step.async.step-model :refer :all]
            [lonocloud.step.async.step-timeout :as step-timeout]
            [lonocloud.step.async.step-threads :as step-threads]
            [lonocloud.step.async.util :as util])
  (:import [java.util ArrayList]))

"TODO:
  - visualization
  - factor out repeated code
  - function to drop in a series of puts and have them run stepwise (first cut done)
  - include thread id in timeouts (tests for this)
  - use same naming conventions as core.async internals
  - cleanup machine state when go threads throw exceptions
  - use clojure.core.async.impl.protocols/ReadPort, etc to make alternate channel impl for internal use
  - support step-inputs with new external channel impl
  - keep stats on reading/writing values to messages by thread
  - name resolution for channels should also take into account timeout names
  - document the fact that a thread could indirectly block waiting for a core.async event, the step machinery does not detect this, thinks the thread is running forever, and does not step, rather it deadlocks
  - add config option for max number of steps (see 'too many steps')
  - in history views, print large data values in a truncated form
  - test for put!/take! from a function called below a go block
  - test for async/put!/take! under a step machine context
  - use a step machine 'in situ' in a larger app around some core.async code
  - alt!
  - alt!!
  - alts!!
  - puts in alts! (currently it only supports takes)
  - priority order in alts!
  - defaults on alts!
  - pass in 'arg-maker' when machine is constructed (to allow channels to be tied to the machine)
  - per core.async, put! needs to throw when called on a closed channel
  - async-function creates channels, wires them up, and produces them as the result
  - throw if >!, etc. not called in a go block
  - use thread id/name for tracking/reporting external interactions (rather than generating 'port' ids)
  - put!, >!, >!! should throw if passed a nil value
  - remove input/output port functions
  - will run out of port ids
  - switch step-wait and quiesce-wait to be promises returned from step calls
  - catch exceptions from async-f and propagate them up as result of machine
  - bug - <!! at top level of async function (outside of go block)
  - output channel of go blocks needs to be registered as step-channels
  - put! and take! inside of machine support option to run on same thread

 DONE:
  - optionally track channel history
  - plug-in algorithm to resolve non-determinism
  - cleanup garbage in channel contents maps
  - cleanup garbage in timeout names map
  - convert ids to names in printed machine (maybe just always use names, even if generated)
  - function to retrieve args to machine (use after replay history)
  - named timeouts
  - add 'last action' to state summary
  - option for recording all internal actions
  - specify channel size in input channels
  - specify names for input channels
  - add tests for vararg machine functions
  - add test for new get-channel-contents function
  - add test with non-channel args to main async function
  - sliding/dropping buffers in channels
  - take! & put! on-caller? option
  - support sliding/dropping buffers in input channels (not just for 'internal' channels)
  - support >!!, <!! in top-level of async functions
  - support put!, take! in top level functions
  - support put!, take! in go blocks
  - support >!!, <!! in go blocks
  - bug - put!/take! inside the machine don't create separate control channels (thus they collide)
  - park parent thread when a go block is started
  - set/clear 'break points'
  - use clojure.core.async.impl.protocols/ReadPort, etc to make alternate channel impl for external use
  - share single timeout on multiple threads
  - other core.async functions:
    - thread-call thread
    - map merge into take unique partition partition-by
    - map< map>
    - mapcat< mapcat>
    - pipe split
    - reduce
    - onto-chan to-chan
    - mult tap untap untap-all
    - mix admix unmix unmix-all toggle solo-mode
    - pub sub unsub unsub-all
    - filter> remove> filter< remove<
  - include operator name in channels created for channel operators
  - include 'parent' channel name in channels created for channel operators
"

(def ^:private debug false)

;;

(defprotocol StepMachine
  (step* [this])
  (step-all* [this])
  (step-inputs* [this inputs])
  (last-done-state* [this])
  (step-wait* [this])
  (quiesce-wait* [this])
  (set-breakpoint* [this f])
  (clear-breakpoint* [this f])
  (clear-all-breakpoints* [this])
  (dump-state* [this])
  (dump-thread-names* [this])
  (dump-named-blockers* [this])
  (dump-channel-history* [this])
  (dump-detailed-action-history* [this])
  (get-channel-contents* [this channel])
  (get-timeouts* [this])
  (complete-timeout* [this timeout-id])
  (external-channel* [this channel])
  (get-history* [this])
  (replay-history* [this history])
  (step-back* [this n])
  (get-args* [this])
  (get-result* [this])
  (state-trace* [this on?])
  (dump-state-trace* [this]))

(defprotocol StepMachineInternal
  (input-args [this args channel-sizes channel-unblocking channel-names])
  (add-root-thread [this thread-id])
  (add-pending-thread [this parent-thread-id thread-name thread-id f])
  (remove-thread [this thread-id])
  (make-channel [this channel-name channel-id channel channel-size take-f put-f])
  (take-channel [this thread-id channel block?])
  (multi-take-channel [this thread-id channels])
  (put-channel [this thread-id channel v block? immediate?])
  (close-channel [this thread-id channel])

  (set-result [this result])
  (register-timeout [this thread-id timeout-name ms])

  (done? [this context])

  (next-channel-id [this])
  (next-thread-id [this])
  (next-timeout-id [this])

  (print-step-machine [this])

  (replay-history** [this history])
  (mutate [this location f])
  (channel-names-f [this])
  (thread-names-f [this])

  (make-port [this channel])
  (machine-lookup-channel [this channel]))

;;

(def ^:dynamic *step-machine* nil)

(def ^:dynamic *running-thread-id* nil)

(defn- channel? [x]
  (satisfies? async-protocols/Channel x))

;; public API - core.async additions

(s/defn chan-named
  ([channel-name :- (s/maybe String)]
     (if *step-machine*
       (let [id (next-channel-id *step-machine*)
             result (async/chan)]
         (make-channel *step-machine* channel-name id result 0 nil nil)
         result)
       (async/chan)))
  ([channel-name :- (s/maybe String)
    buf-or-n]
     (if *step-machine*
       (chan-named channel-name buf-or-n nil nil)
       (async/chan buf-or-n)))
  ([channel-name :- (s/maybe String)
    buf-or-n take-f put-f]
     (if *step-machine*
       (let [id (next-channel-id *step-machine*)
             result (async/chan 10)]
         (make-channel *step-machine* channel-name id result (or buf-or-n 0) take-f put-f)
         result)
       (async/chan buf-or-n))))

(s/defn timeout-named [timeout-name :- (s/maybe String)
                       ms :- s/Int]
  (if *step-machine*
    (register-timeout *step-machine* *running-thread-id* timeout-name ms)
    (async/timeout ms)))

(defmacro go-named [thread-name & body]
  `(if *step-machine*
     (let [id# (next-thread-id *step-machine*)]
       (let [[out-chan# parent-control-channel#]
             (add-pending-thread *step-machine* *running-thread-id* ~thread-name id#
                                 #(async/go
                                   (try
                                     ~@body
                                     (catch Throwable t#
                                       (errorf t# "Uncaught exception in go block")
                                       t#))))]
         (async/<!! parent-control-channel#)
         out-chan#))
     (async/go ~@body)))

(defmacro go-loop-named [thread-name bindings & body]
  `(go-named ~thread-name (loop ~bindings ~@body)))

;; public API - core.async alternatives

(s/defn buffer [n :- s/Int]
  (if *step-machine*
    {:buffer? true
     :size n}
    (async/buffer n)))

(s/defn sliding-buffer [n :- s/Int]
  (if *step-machine*
    {:buffer? true
     :size n
     :unblocking :sliding}
    (async/sliding-buffer n)))

(s/defn dropping-buffer [n :- s/Int]
  (if *step-machine*
    {:buffer? true
     :size n
     :unblocking :dropping}
    (async/sliding-buffer n)))

(defn chan
  ([]
     (chan-named nil))
  ([x]
     (chan-named nil x)))

(s/defn timeout [ms :- s/Int]
  (timeout-named nil ms))

(defmacro go [& body]
  `(go-named nil ~@body))

(defmacro go-loop
  [bindings & body]
  `(go (loop ~bindings ~@body)))

(defmacro <!
  "This is a macro because we need it to expand in-place into a core.async call within the enclosing
  go block."
  [c]
  `(if *step-machine*
     (let [c# ~c
           [control-channel#] (take-channel *step-machine* *running-thread-id* c# true)]
       (let [[result#] (async/<! control-channel#)]
         (when (not= step-channel/close-message result#)
           result#)))
     (async/<! ~c)))

(s/defn- <!!-direct [port block?]
  (let [{:keys [machine channel port-thread-id]} port]
    (let [control-channel (first (take-channel machine port-thread-id channel block?))]
      (when debug (errorf (str "<!! control channel " control-channel)))
      (let [value (first (async/<!! control-channel))]
        (when debug (errorf (str "<!! value " value)))
        (when (not= step-channel/close-message value)
          value)))))

(s/defn <!! [port ;; :- ReadPort
             ]
  (if (and *step-machine*
           (machine-lookup-channel *step-machine* port))
    (do
      (when debug (errorf (str "<!! " port)))
      (<!!-direct (if *step-machine*
                    {:machine *step-machine*
                     :channel port
                     :port-thread-id *running-thread-id*}
                    port) false))
    (async/<!! port)))

(s/defn- take!-direct [port ;; :- ReadPort
                       immediate-fn1 :- s/Fn
                       fn1 :- s/Fn
                       on-caller? :- Boolean]
  (let [{:keys [machine channel]} port]
    (let [temp-port (make-port machine channel)
          {:keys [port-thread-id]} temp-port]
      (let [[control-channel available?] (take-channel machine port-thread-id channel false)]
        (if (and on-caller?
                 available?)
          (let [value (first (async/<!! control-channel))]
            (immediate-fn1 (when (not= step-channel/close-message value)
                             value)))
          (async/go (try
                      (let [value (first (async/<!! control-channel))]
                        (fn1 (when (not= step-channel/close-message value)
                               value)))
                      (catch Throwable t
                        (errorf t "Uncaught exception in take!"))))))
      nil)))

(s/defn take!-direct-internal [channel
                               fn1]
  (let [thread-id (next-thread-id *step-machine*)
        machine *step-machine*]
    (let [[out-chan parent-control-channel]
          (add-pending-thread *step-machine* *running-thread-id* (str "take-" thread-id) thread-id
                              #(async/go
                                (try
                                  (let [[control-channel] (take-channel machine thread-id channel false)
                                        value (first (async/<!! control-channel))]
                                    (fn1 (when (not= step-channel/close-message value)
                                           value)))
                                  (catch Throwable t
                                    (errorf t "Uncaught exception in take!-internal")
                                    t)
                                  (finally
                                    (remove-thread machine thread-id)))))]
      (async/<!! parent-control-channel)
      nil)))

(s/defn take!
  ([port ;; :- ReadPort
    fn1 :- s/Fn]
     (take! port fn1 true))
  ([port ;; :- ReadPort
    fn1 :- s/Fn
    on-caller? :- Boolean]
     (if *step-machine*
       (take!-direct-internal port fn1)
       (take!-direct port fn1 fn1 on-caller?))))

(defmacro alts! [channels]
  `(if *step-machine*
     (let [channels# ~channels]
       (let [[value# channel#] (async/<!
                                (multi-take-channel *step-machine* *running-thread-id* channels#))]
         [(when (not= step-channel/close-message value#)
            value#)
          channel#]))
     (async/alts! ~channels)))

(defmacro >! [c v]
  `(if *step-machine*
     (do (async/<! (first (put-channel *step-machine* *running-thread-id* ~c ~v true false)))
         nil)
     (async/>! ~c ~v)))

(s/defn- >!!-direct [port v block? immediate?]
  (let [{:keys [machine channel port-thread-id]} port
        [control-channel] (put-channel machine port-thread-id channel v block? immediate?)]
    (when block?
      (async/<!! control-channel))
    nil))

(s/defn >!! [port ;; :- WritePort
             v :- s/Any]
  (>!!-direct (if *step-machine*
                {:machine *step-machine*
                 :channel port
                 :port-thread-id *running-thread-id*}
                port) v true true))

(s/defn- put!-direct [port ;; :- WritePort
                      v :- s/Any
                      immediate-fn0 :- s/Fn
                      fn0 :- s/Fn
                      on-caller? :- Boolean]
  (let [{:keys [machine channel]} port]
    (let [temp-port (make-port machine channel)
          {:keys [port-thread-id]} temp-port]
      (let [[control-channel put-done?] (put-channel machine port-thread-id channel v true true)]
        (if (and on-caller?
                 put-done?)
          (do
            (async/<!! control-channel)
            (immediate-fn0))
          (async/go (try
                      (async/<! control-channel)
                      (fn0)
                      (catch Throwable t
                        (errorf t "Uncaught exception in put!"))))))))
  nil)

(s/defn- put!-direct-internal [channel
                               v
                               fn0]
  (let [thread-id (next-thread-id *step-machine*)
        machine *step-machine*]
    (let [[out-chan parent-control-channel]
          (add-pending-thread *step-machine* *running-thread-id* (str "put-" thread-id) thread-id
                              #(async/go
                                (try
                                  (let [[control-channel] (put-channel machine thread-id channel v
                                                                       true false)]
                                    (async/<!! control-channel)
                                    (fn0))
                                  (catch Throwable t
                                    (errorf t "Uncaught exception in put!-internal")
                                    t)
                                  (finally
                                    (remove-thread machine thread-id)))))]
      (async/<!! parent-control-channel)
      nil)))

(s/defn put!
  ([port ;; :- WritePort
    v :- s/Any]
     (put! port v (fn []) true))
  ([port ;;xo :- WritePort
    v :- s/Any
    fn0 :- s/Fn]
     (put! port v fn0 true))
  ([port ;; :- WritePort
    v :- s/Any
    fn0 :- s/Fn
    on-caller? :- Boolean]
     (if *step-machine*
       (put!-direct-internal port v fn0)
       (put!-direct port v fn0 fn0 on-caller?))))

(s/defn close! [c :- Channel]
  (if *step-machine*
    (close-channel *step-machine* *running-thread-id* c)
    (async/close! c)))

;; helper functions

(s/defn- notify-done [channels :- #{Channel}
                      done-message :- s/Keyword]
  (->> channels
       (clojure.core/map #(do (async/>!! % done-message)
                              (async/close! %)))
       doall))

(s/defn- check-done [location :- String
                     f :- s/Fn]
  (let [[[channels done-message] result] (f)]
    (when (and channels done-message)
      (notify-done channels done-message))
    result))

(s/defn- thread-running? [step-channels :- step-channel/StepChannelMap
                          thread-map :- step-threads/StepThreadMap]
  (not (empty? (set/difference (step-threads/running-thread-ids thread-map)
                               (step-threads/parked-thread-ids thread-map)
                               (step-channel/pending-puts step-channels)
                               (step-channel/blocked-puts step-channels)
                               (step-channel/blocked-takes step-channels)))))

(s/defn- pad [seq size pad-value]
  (clojure.core/take size (clojure.core/into (vec seq)
                                             (clojure.core/take size (repeat pad-value)))))

(s/defn- at-breakpoint? [machine listener-map]
  (->> (step-listen/breakpoints listener-map)
       (some (fn [f]
               (f (print-step-machine machine))))))

(s/defn- get-status [machine listeners step-channels thread-map]
  (let [break? (at-breakpoint? machine listeners)]
    (if (thread-running? step-channels thread-map)
      :running
      (if (or (step-threads/pending-threads-available? thread-map)
              (step-threads/parked-threads-available? thread-map)
              (step-channel/pending-puts-available? step-channels)
              (step-channel/takes-available? step-channels)
              (step-channel/puts-available? step-channels))
        (if break?
          :at-breakpoint
          :more-steps)
        (if (step-threads/all-threads-exited? thread-map)
          :all-exited
          :all-blocked)))))

(s/defn- handle-close [value]
  (if (= step-channel/close-message value)
    :step-channel/close
    value))

;; step machine type

(declare construct-step-machine)

(declare run-thread)

(declare add-waiter)

(def StepMachineOptions {:time-f s/Fn
                         :rand-seed (s/maybe s/Int)
                         :action-history? Boolean
                         :detailed-action-history? Boolean
                         :channel-history? Boolean
                         :channel-sizes [(s/maybe s/Int)]
                         :channel-unblocking [(s/maybe step-channel/UnblockingType)]
                         :channel-names [(s/maybe String)]
                         :step-timeout (s/maybe s/Int)})

(def StepMachineOptionsParts (util/all-optional-keys StepMachineOptions))

(def StepMachineConfig {:body-f s/Fn
                        :args [s/Any]
                        :arg-maker s/Fn
                        :options StepMachineOptionsParts
                        :rand-source s/Fn
                        :thread-id-source s/Atom
                        :port-thread-id-source s/Atom
                        :channel-id-source s/Atom
                        :timeout-id-source s/Atom})

(deftype StepMachineType [;; ref - all of the step channels in this machine
                          step-channels
                          ;; ref - all of the threads in this machine
                          thread-map
                          ;; ref - listeners of the machine's progress
                          listeners
                          ;; ref - pending async timeouts in the machine
                          timeout-map
                          ;; ref - history of execution of the machine
                          machine-log
                          ;; ref - the result of evaluating the machine
                          machine-result

                          config]

  Object
  (toString [this] (pr-str this))

  StepMachineInternal

  (next-channel-id [this]
    (swap! (:channel-id-source config) inc))

  (next-thread-id [this]
    (swap! (:thread-id-source config) inc))

  (next-timeout-id [this]
    (swap! (:timeout-id-source config) inc))

  (set-result [this result]
    (mutate this "set-result" #(ref-set machine-result result)))

  (done? [this context]
    (let [status (get-status this @listeners @step-channels @thread-map)]
      (when debug (errorf (str "done? " status " for " context " from " (print-step-machine this))))
      (when (not= status :running)
        (alter machine-log step-log/record-done-state status)
        (let [to-notify (if (= status :more-steps)
                          (step-listen/take-listeners @listeners :step-listeners)
                          (clojure.core/into (step-listen/take-listeners @listeners :step-listeners)
                                             (step-listen/take-listeners @listeners :quiesce-listeners)))]
          (if (= status :more-steps)
            (alter listeners step-listen/clear-listeners :step-listeners)
            (do
              (alter listeners step-listen/clear-listeners :step-listeners)
              (alter listeners step-listen/clear-listeners :quiesce-listeners)))
          (when debug (errorf (str "done? " status " for " context
                                   " running-thread-id " *running-thread-id*
                                   " listeners " to-notify)))
          [to-notify status]))))

  (input-args [this args channel-sizes channel-unblocking channel-names]
    (when (not (empty? args))
      (let [default-channel-size 0
            channels-to-add (->> (clojure.core/map (fn [a s u n]
                                                     (when (channel? a)
                                                       [a s u n]))
                                                   args
                                                   (pad channel-sizes (count args) default-channel-size)
                                                   (pad channel-unblocking (count args) nil)
                                                   (pad channel-names (count args) nil))
                                 (remove nil?))]
        (doall (clojure.core/map #(let [[channel size unblocking channel-name] %3]
                                    (let [size-to-use (or size default-channel-size)]
                                      (make-channel this (or channel-name
                                                             (str "arg-" %1))
                                                    %2 channel
                                                    (if unblocking
                                                      {:buffer? true
                                                       :size size-to-use
                                                       :unblocking unblocking}
                                                      size-to-use) nil nil)))
                                 (range)
                                 (repeatedly #(next-channel-id this))
                                 channels-to-add)))))

  (add-root-thread [this thread-id]
    (mutate this "add-thread" #(alter thread-map step-threads/register-root-thread thread-id
                                      (async/chan 10))))

  (add-pending-thread [this parent-thread-id thread-name thread-id f]
    (let [out-chan (async/chan)
          out-channel-id (next-channel-id this)
          parent-control-channel (step-threads/get-control-channel @thread-map parent-thread-id)]
      (mutate this "add-pending-thread"
              #(do (alter thread-map step-threads/register-thread thread-id thread-name f out-chan
                          (async/chan 10))
                   (alter thread-map step-threads/thread-park parent-thread-id)
                   (comment (alter step-channels step-channel/register-channel out-channel-id nil
                                   out-chan 1 (get-in config [:options :channel-history?]) nil nil))))
      [out-chan parent-control-channel]))

  (remove-thread [this thread-id]
    (mutate this "remove-thread" #(alter thread-map step-threads/thread-finished thread-id)))

  (make-channel [this channel-name channel-id channel buf-or-n take-f put-f]
    (mutate this "make-channel"
            #(alter step-channels
                    step-channel/register-channel channel-id channel-name channel
                    buf-or-n (get-in config [:options :channel-history?]) take-f put-f)))

  (take-channel [this thread-id channel block?]
    (let [value (mutate this "take-channel"
                        #(let [channel-id (step-channel/lookup-channel-id @step-channels channel)
                               value (step-channel/take-value @step-channels channel-id)]
                           (when (port-thread-id? thread-id)
                             (alter machine-log step-log/record-event [:take channel-id]))
                           (if (or block?
                                   (not value))
                             (alter step-channels step-channel/register-take channel-id thread-id)
                             (when (not= step-channel/close-message value)
                               (alter step-channels step-channel/clear-take channel-id)))
                           (when (not block?)
                             value)))]
      (let [control-channel (step-threads/get-control-channel @thread-map thread-id)]
        (when value
          (async/>!! control-channel [value channel]))
        [control-channel (not (nil? value))])))

  (multi-take-channel [this thread-id channels]
    (mutate this "multi-take-channel"
            #(let [channel-ids (->> channels
                                    (clojure.core/map (partial step-channel/lookup-channel-id @step-channels))
                                    sort)]
               (->> channel-ids
                    (clojure.core/map (fn [c-id]
                                        (alter step-channels step-channel/register-take c-id thread-id)))
                    doall)))
    (step-threads/get-control-channel @thread-map thread-id))

  (put-channel [this thread-id channel v block? immediate?]
    (let [put-done? (mutate this "put-channel"
                            #(let [channel-id (step-channel/lookup-channel-id @step-channels channel)
                                   pending-puts-already? (step-channel/pending-puts-available-for-channel-id?
                                                          @step-channels channel-id)]
                               (if (step-channel/after-closing @step-channels channel-id)
                                 true
                                 (do
                                   (alter step-channels step-channel/register-put channel-id thread-id v block?)
                                   (if (and immediate?
                                            (not pending-puts-already?)
                                            (step-channel/space-available? @step-channels channel-id))
                                     (let [thread-names (thread-names-f this)
                                           channel-names (channel-names-f this)]
                                       (when (port-thread-id? thread-id) ;; always true, right?
                                         (alter machine-log step-log/record-event
                                                [:put
                                                 channel-id
                                                 (handle-close v)]))
                                       (alter step-channels
                                              step-channel/accept-put channel-id thread-id)
                                       (alter machine-log step-log/record-action
                                              [:put
                                               (thread-names thread-id)
                                               (channel-names channel-id)
                                               (handle-close v)])
                                       (when block?
                                         (alter machine-log step-log/record-action
                                                [:un-park-put
                                                 (thread-names thread-id)]))
                                       (alter step-channels step-channel/clear-put channel-id thread-id)
                                       true)
                                     false)))))]
      (when put-done?
        (async/>!! (step-threads/get-control-channel @thread-map thread-id) :awake-from-put))
      [(step-threads/get-control-channel @thread-map thread-id) put-done?]))

  (close-channel [this thread-id channel]
    (first (put-channel this thread-id channel step-channel/close-message false false)))

  (register-timeout [this thread-id timeout-name ms]
    (let [timeout-chan (async/chan)
          timeout-id (next-timeout-id this)]
      (mutate this "register-timeout"
              #(do
                 (alter timeout-map step-timeout/add-timeout thread-id
                        (or timeout-name
                            (str "timeout-" timeout-id))
                        timeout-id ((get-in config [:options :time-f])) ms timeout-chan)
                 (make-channel this (or timeout-name
                                        (str "timeout-" timeout-id)) timeout-id timeout-chan 1
                                        nil nil)))
      timeout-chan))

  (print-step-machine [this]
    (let [thread-names (thread-names-f this)
          channel-names (channel-names-f this)
          timeout-names (step-timeout/all-timeout-names @timeout-map)]
      (-> {:threads (->> @thread-map
                         step-threads/running-thread-ids
                         (clojure.core/map thread-names)
                         sort
                         vec
                         util/no-empty)
           :parked-threads (->> @thread-map
                                step-threads/parked-thread-ids
                                (clojure.core/map thread-names)
                                sort
                                vec
                                util/no-empty)
           :channels (->> (step-channel/channel-ids @step-channels)
                          (clojure.core/map channel-names)
                          sort
                          vec
                          util/no-empty)
           :blocked-takes (-> @step-channels
                              step-channel/blocked-take-map
                              (util/update-keys thread-names)
                              (util/update-vals #(-> (clojure.core/map channel-names %)
                                                     sort
                                                     vec))
                              util/no-empty)
           :blocked-puts (-> @step-channels
                             step-channel/blocked-put-map
                             (util/update-keys thread-names)
                             (util/update-vals channel-names)
                             util/no-empty)
           :pending-puts  (-> @step-channels
                              step-channel/pending-put-map
                              (util/update-vals (fn [{:keys [channel-id value]}]
                                                  [(channel-names channel-id) value]))
                              (util/update-keys thread-names)
                              util/no-empty)
           :pending-threads (->> @thread-map
                                 step-threads/pending-thread-ids
                                 (clojure.core/map thread-names)
                                 sort
                                 vec
                                 util/no-empty)
           :channel-contents (-> (step-channel/channel-contents-map @step-channels channel-names))
           :timeouts (step-timeout/timeout-summary @timeout-map)
           :last-action (step-log/last-action @machine-log)
           :channel-history (-> (step-channel/channel-history @step-channels channel-names)
                                util/no-nil
                                util/no-empty)}
          util/no-nil)))

  (mutate [this location f]
    (check-done location #(dosync
                           (let [result (f)]
                             [(done? this location) result]))))

  (replay-history** [this inputs]
    (let [ports (atom {})]
      (loop [[[action channel-id value] & remaining-inputs] inputs]
        (when channel-id
          (let [channel (step-channel/lookup-channel @step-channels channel-id)
                port (if-let [p (get @ports channel-id)]
                       p
                       (do
                         (let [p (make-port this channel)]
                           (swap! ports assoc channel-id p)
                           p)))]
            (condp = action
              :put
              (>!!-direct port value false true)
              :take
              (<!!-direct port false))
            (step-all* this)
            (quiesce-wait* this)
            (recur remaining-inputs)))))
    this)

  (channel-names-f [this]
    (step-channel/channel-names @step-channels))

  (thread-names-f [this]
    (fn [thread-id]
      (or (get (step-threads/all-thread-names @thread-map) thread-id)
          (str "thread-" thread-id))))

  StepMachine

  (step* [this]
    (binding [*step-machine* this]
      (let [status (get-status this @listeners @step-channels @thread-map)]
        (when (#{:more-steps :at-breakpoint} status)
          (let [[next-to-run
                 pending-put-thread-id
                 [take-control-channel take-thread-id take-channel take-value]
                 put-thread-id
                 parked-id-to-run]
                (mutate this
                        "step*"
                        (fn []
                          (dosync
                           (let [choice (let [available-takes (-> @step-channels
                                                                  step-channel/takes-available-channels)
                                              channel-waiters (-> @step-channels
                                                                  step-channel/channel-waiter-map
                                                                  (select-keys available-takes))]
                                          ((if (:rand-source config)
                                             (partial step-schedule/random-scheduler
                                                      (:rand-source config))
                                             step-schedule/deterministic-scheduler)
                                           (step-threads/pending-thread-ids @thread-map)
                                           (step-threads/parked-thread-ids @thread-map)
                                           (step-channel/pending-puts-available @step-channels)
                                           available-takes
                                           channel-waiters
                                           (step-channel/blocked-put-map @step-channels)))]
                             (when choice
                               (alter machine-log step-log/record-state (print-step-machine this)))
                             (let [thread-names (thread-names-f this)
                                   channel-names (channel-names-f this)]
                               (condp = (first choice)

                                 :run-pending
                                 (let [[_ pending-id-to-run] choice
                                       pending-thread (let [{:keys [f output-channel]}
                                                            (step-threads/get-pending-step-thread
                                                             @thread-map
                                                             pending-id-to-run)]
                                                        [pending-id-to-run f output-channel])]
                                   (alter thread-map step-threads/thread-starting pending-id-to-run)
                                   (alter machine-log step-log/record-action
                                          [:run-pending (thread-names pending-id-to-run)])
                                   [pending-thread])

                                 :run-parked
                                 (let [[_ parked-id-to-run] choice]
                                   (alter thread-map step-threads/thread-unpark parked-id-to-run)
                                   (alter machine-log step-log/record-action
                                          [:run-parked (thread-names parked-id-to-run)])
                                   [nil nil nil nil parked-id-to-run])

                                 :do-pending-put
                                 (let [[_ pending-put-thread-id] choice
                                       {:keys [channel-id value block?]} (-> @step-channels
                                                                             step-channel/pending-put-map
                                                                             (get pending-put-thread-id))]
                                   (when (port-thread-id? pending-put-thread-id)
                                     (alter machine-log step-log/record-event
                                            [:put channel-id (handle-close value)]))
                                   (when debug
                                     (errorf (str "step* running action " [pending-put-thread-id
                                                                           channel-id value block?])))
                                   (alter step-channels
                                          step-channel/accept-put channel-id pending-put-thread-id)

                                   (alter machine-log step-log/record-action
                                          [:put
                                           (thread-names pending-put-thread-id)
                                           (channel-names channel-id)
                                           (handle-close value)])
                                   [nil pending-put-thread-id])

                                 :take
                                 (let [[_ take-thread-id take-channel-id] choice
                                       take-channel (step-channel/lookup-channel @step-channels
                                                                                 take-channel-id)
                                       control-channel (step-threads/get-control-channel @thread-map take-thread-id)
                                       all-channel-ids-blocked-in (-> @step-channels
                                                                      step-channel/blocked-take-map
                                                                      (get take-thread-id))
                                       value (or (step-channel/take-value @step-channels take-channel-id)
                                                 step-channel/close-message)]
                                   (alter step-channels step-channel/clear-take take-channel-id)
                                   (alter step-channels step-channel/clear-takers take-thread-id)

                                   (->> all-channel-ids-blocked-in
                                        (filter timeout-id?)
                                        (filter #(-> (step-channel/takes-available @step-channels %)
                                                     empty?))
                                        (clojure.core/map #(alter timeout-map step-timeout/remove-timeout %))
                                        doall)

                                   (alter machine-log step-log/record-action
                                          [:take
                                           (thread-names take-thread-id)
                                           (channel-names take-channel-id)
                                           (handle-close value)])
                                   [nil nil [control-channel take-thread-id take-channel value]])

                                 :put
                                 (let [[_ thread-id] choice]
                                   (alter machine-log step-log/record-action
                                          [:un-park-put
                                           (thread-names thread-id)])
                                   (let [channel-id (step-channel/lookup-channel-for-put-thread
                                                     @step-channels thread-id)]
                                     (alter step-channels step-channel/clear-put channel-id thread-id))
                                   [nil nil nil thread-id])

                                 nil))))))]
            (when debug
              (errorf (str "step* state = " (dump-state* this)))
              (errorf (str "step* choice action = "
                           [next-to-run
                            pending-put-thread-id
                            [take-control-channel take-thread-id take-channel take-value]
                            put-thread-id])))
            (if next-to-run
              (apply run-thread this next-to-run)
              (if pending-put-thread-id
                nil
                (if parked-id-to-run
                  (async/>!! (step-threads/get-control-channel @thread-map parked-id-to-run)
                             :awake-from-park)
                  (if take-control-channel
                    (async/>!! take-control-channel [take-value take-channel])
                    (if put-thread-id
                      (async/>!! (step-threads/get-control-channel @thread-map put-thread-id)
                                 :awake-from-put2))))))
            nil)))))

  (step-all* [this]
    (step* this)
    (loop [status (step-wait* this)
           counter 0]
      (when (> counter 200)
        (throw (RuntimeException. "too many steps")))
      (if (#{:all-exited :all-blocked :at-breakpoint} status)
        status
        (do
          (step* this)
          (recur (step-wait* this) (inc counter))))))

  (step-inputs* [this inputs]
    (loop [[[channel value] & remaining-inputs] inputs]
      (when channel
        (async/>!! channel value)
        (step-all* this)
        (quiesce-wait* this)
        (recur remaining-inputs))))

  (replay-history* [this inputs]
    (let [options (clojure.core/merge (:options config)
                                      {:action-history? (step-log/action-history? @machine-log)
                                       :detailed-action-history? (step-log/detailed-action-history? @machine-log)})]
      (s/validate StepMachineOptionsParts options)
      (let [new-machine (construct-step-machine
                         options
                         (:body-f config)
                         ((:arg-maker config)))]
        (step-all* new-machine)
        (quiesce-wait* new-machine)
        (replay-history** new-machine inputs))))

  (step-back* [this n]
    (when-not (step-log/action-history? @machine-log)
      (throw (RuntimeException. "set action-history? to true to step backwards")))
    (let [history (get-history* this)]
      (replay-history* this (clojure.core/take (- (count history) n) history))))

  (get-args* [this]
    (:args config))

  (last-done-state* [this]
    (step-log/last-done-state @machine-log))

  (dump-state* [this]
    {:step-channels @step-channels
     :thread-map @thread-map
     :listeners @listeners
     :timeout-map @timeout-map
     :machine-result @machine-result})

  (dump-channel-history* [this]
    (step-channel/channel-history @step-channels (channel-names-f this)))

  (dump-detailed-action-history* [this]
    (step-log/detailed-action-history @machine-log))

  (get-channel-contents* [this channel]
    (step-channel/channel-contents @step-channels channel))

  (dump-thread-names* [this]
    (step-threads/all-thread-names @thread-map))

  (dump-named-blockers* [this]
    (dosync
     (let [blocked-threads (step-channel/blocked-take-map @step-channels)
           thread-names (thread-names-f this)
           channel-names (channel-names-f this)]
       (util/update-map blocked-threads (fn [k v]
                                          [(thread-names k)
                                           (set (clojure.core/map channel-names v))])))))

  (step-wait* [this]
    (add-waiter this listeners :step-listeners (get-in config [:options :step-timeout])))

  (quiesce-wait* [this]
    (add-waiter this listeners :quiesce-listeners (get-in config [:options :step-timeout])))

  (set-breakpoint* [this f]
    (mutate this "set-breakpoint*"
            #(alter listeners step-listen/add-breakpoint f)))

  (clear-breakpoint* [this f]
    (mutate this "clear-breakpoint*"
            #(alter listeners step-listen/clear-breakpoint f)))

  (clear-all-breakpoints* [this]
    (mutate this "clear-all-breakpoints*"
            #(->> (step-listen/breakpoints @listeners)
                  (clojure.core/map (fn [breakpoint] (alter listeners step-listen/clear-breakpoint breakpoint)))
                  doall)))

  (get-timeouts* [this]
    (step-timeout/get-all-timeouts @timeout-map
                                   (step-threads/all-thread-names @thread-map)
                                   (step-timeout/all-timeout-names @timeout-map)))

  (complete-timeout* [this timeout-id]
    (when (nil? timeout-id)
      (throw (RuntimeException. "nil is not a valid timeout-id")))
    (let [timeout-chan (mutate this "complete-timeout*"
                               #(let [timeout-chan (step-timeout/get-timeout-channel
                                                    @timeout-map timeout-id)]
                                  (alter timeout-map step-timeout/remove-timeout timeout-id)
                                  timeout-chan))]
      (close-channel this (:port-thread-id (make-port this timeout-chan)) timeout-chan)))

  (make-port [this channel]
    (let [port-thread-id (swap! (:port-thread-id-source config) inc)]
      (mutate this "make-port"
              #(alter thread-map step-threads/register-external-thread port-thread-id
                      (async/chan 10)))
      {:machine this
       :channel channel
       :port-thread-id port-thread-id}))

  (machine-lookup-channel [this channel]
    (step-channel/probe-channel-id @step-channels channel))

  (external-channel* [this channel]
    (let [port (make-port this channel)]
      (reify
        clojure.core.async.impl.protocols/ReadPort
        (take! [_ fn1-handler]
          (let [result (atom [])]
            (take!-direct port
                          #(swap! result conj %)
                          (async-protocols/commit fn1-handler) true)
            (when (not (empty? @result))
              (let [[v] @result]
                (reify clojure.lang.IDeref
                  (deref [_] v))))))

        clojure.core.async.impl.protocols/WritePort
        (put! [_ val fn0-handler]
          (let [result (atom [])]
            (put!-direct port val #(swap! result conj nil)
                         (async-protocols/commit fn0-handler) true)
            (when (not (empty? @result))
              (let [[v] @result]
                (reify clojure.lang.IDeref
                  (deref [_] v))))))

        clojure.core.async.impl.protocols/Channel
        (close! [_]
          (close! channel)))))

  (get-history* [this]
    (step-log/history @machine-log))

  (get-result* [this]
    @machine-result)

  (state-trace* [this on?]
    (dosync
     (alter machine-log step-log/state-trace-switch on? (print-step-machine this))))

  (dump-state-trace* [this]
    (conj (step-log/state-trace @machine-log)
          (print-step-machine this))))

(defmethod print-method StepMachineType [x ^java.io.Writer writer]
  (print-method (print-step-machine x) writer))

(defmethod pprint/simple-dispatch StepMachineType [x]
  (pprint/pprint (print-step-machine x)))

;; more helpers

(s/defn- run-thread [this :- StepMachineType
                     thread-id :- ThreadId
                     fn0 :- s/Fn
                     out-chan :- Channel]
  (binding [*running-thread-id* thread-id]
    (let [go-out-chan (fn0)]
      (async/go
       (try
         (let [result (async/<! go-out-chan)]
           (remove-thread this *running-thread-id*)
           (if result
             (async/>! out-chan result)
             (async/close! out-chan)))
         (catch Throwable t
           (errorf t "Uncaught exception in run-thread")
           t))))))

(def ^:private default-step-timeout (* 20 1000))

(s/defn- add-waiter [machine :- StepMachineType
                     listeners :- s/Ref
                     wait-type :- s/Keyword
                     step-timeout :- (s/maybe s/Int)]
  (let [wait-channel (async/chan 10)]
    (mutate machine "add-waiter" #(alter listeners step-listen/add-listener wait-type wait-channel))
    (let [[result] (async/alts!! [wait-channel
                                  (async/timeout (or step-timeout
                                                     default-step-timeout))])]
      (if result
        result
        (throw (RuntimeException. "step-wait timed out"))))))

;; machine construction

(s/defn- make-step-machine [body-f :- s/Fn
                            args :- [s/Any]
                            options :- StepMachineOptionsParts
                            arg-maker :- s/Fn
                            rand-source :- (s/maybe s/Fn)]
  (let [thread-id-source (atom root-thread-id)
        port-thread-id-source (atom first-port-thread-id)
        channel-id-source (atom first-channel-id)
        timeout-id-source (atom first-timeout-id)]
    (StepMachineType. (ref {}) (ref {}) (ref (step-listen/init-listener-map)) (ref {})
                      (ref (step-log/init-log (:action-history? options)
                                              (:detailed-action-history? options)))
                      (ref nil)
                      {:body-f body-f
                       :args args
                       :arg-maker arg-maker
                       :options options
                       :rand-source rand-source
                       :thread-id-source thread-id-source
                       :port-thread-id-source port-thread-id-source
                       :channel-id-source channel-id-source
                       :timeout-id-source timeout-id-source})))

(defn- make-arg-maker [args]
  (fn []
    (->> args
         (clojure.core/map #(if (channel? %)
                              (async/chan)
                              %)))))

(s/defn- construct-step-machine [options :- (s/maybe StepMachineOptionsParts)
                                 f :- s/Fn
                                 args :- [s/Any]]
  (s/validate (s/maybe StepMachineOptionsParts) options)
  (let [{:keys [time-f rand-seed action-history? detailed-action-history? channel-history?
                channel-sizes channel-unblocking channel-names]} options
                machine (make-step-machine
                         f
                         args
                         (clojure.core/merge options
                                             {:time-f (or time-f
                                                          #(System/currentTimeMillis))
                                              :action-history? (if (nil? action-history?)
                                                                 false
                                                                 action-history?)
                                              :detailed-action-history? (if (nil? detailed-action-history?)
                                                                          false
                                                                          detailed-action-history?)
                                              :channel-history? (if (nil? channel-history?)
                                                                  false
                                                                  channel-history?)})
                         (make-arg-maker args)
                         (when rand-seed (step-schedule/stm-rand rand-seed)))]
    (binding [*step-machine* machine
              *running-thread-id* root-thread-id]
      (input-args machine args channel-sizes channel-unblocking channel-names)
      (add-root-thread machine root-thread-id)
      (future
        (try
          (set-result machine (apply f args))
          (catch Throwable t
            (errorf t "Uncaught exception in async function"))
          (finally
            (remove-thread machine root-thread-id))))
      machine)))

;; public construction API

(defn step-machine [& {:as options}]
  (fn [f & args]
    (construct-step-machine options f args)))

;; public step machine functions

(s/defn step [machine :- StepMachineType]
  (step* machine))

(s/defn step-all [machine :- StepMachineType]
  (step-all* machine))

(s/defn step-inputs [machine :- StepMachineType
                     inputs :- [s/Any]]
  (step-inputs* machine inputs))

(s/defn last-done-state [machine :- StepMachineType]
  (last-done-state* machine))

(s/defn step-wait [machine :- StepMachineType]
  (step-wait* machine))

(s/defn quiesce-wait [machine :- StepMachineType]
  (quiesce-wait* machine))

(s/defn set-breakpoint [machine :- StepMachineType
                        fn1 :- s/Fn]
  (set-breakpoint* machine fn1))

(s/defn clear-breakpoint [machine :- StepMachineType
                          fn1 :- s/Fn]
  (clear-breakpoint* machine fn1))

(s/defn clear-all-breakpoints [machine :- StepMachineType]
  (clear-all-breakpoints* machine))

(s/defn get-result [machine :- StepMachineType]
  (get-result* machine))

(s/defn get-timeouts [machine :- StepMachineType]
  (get-timeouts* machine))

(s/defn complete-timeout [machine :- StepMachineType
                          timeout-id :- (s/maybe TimeoutId)]
  (complete-timeout* machine timeout-id))

(s/defn external-channel [machine :- StepMachineType
                          channel :- Channel]
  (external-channel* machine channel))

(s/defn get-history [machine :- StepMachineType]
  (get-history* machine))

(s/defn replay-history [machine :- StepMachineType
                        history :- [s/Any]]
  (replay-history* machine history))

(s/defn step-back
  ([machine :- StepMachineType]
     (step-back* machine 1))
  ([machine :- StepMachineType
    n :- s/Int]
     (step-back* machine n)))

(s/defn get-args [machine :- StepMachineType]
  (get-args* machine))

(s/defn state-trace [machine :- StepMachineType
                     on? :- Boolean]
  (state-trace* machine on?))

(s/defn dump-state-trace [machine :- StepMachineType]
  (dump-state-trace* machine))

(s/defn dump-state [machine :- StepMachineType]
  (dump-state* machine))

(s/defn dump-thread-names [machine :- StepMachineType]
  (dump-thread-names* machine))

(s/defn dump-named-blockers [machine :- StepMachineType]
  (dump-named-blockers* machine))

(s/defn dump-channel-history [machine :- StepMachineType]
  (dump-channel-history* machine))

(s/defn dump-detailed-action-history [machine :- StepMachineType]
  (dump-detailed-action-history* machine))

(s/defn get-channel-contents [machine :- StepMachineType
                              channel :- Channel]
  (get-channel-contents* machine channel))

;;;;

(defn- get-channel-name [step-machine ch]
  ((channel-names-f step-machine) (machine-lookup-channel step-machine ch)))

(defn- child-chan
  ([ch suffix]
     (child-chan ch suffix nil))
  ([ch suffix buf-or-n]
     (chan-named (str (get-channel-name *step-machine* ch) "." suffix) buf-or-n)))

(defn filter> [p ch]
  (if *step-machine*
    (let [new-ch (child-chan ch "filter>")]
      (go-loop [v (<! new-ch)]
               (if (nil? v)
                 (close! ch)
                 (do
                   (when (p v)
                     (>! ch v))
                   (recur (<! new-ch)))))
      new-ch)
    (async/filter> p ch)))

(defn remove> [p ch]
  (if *step-machine*
    (let [new-ch (child-chan ch "remove>")]
      (go-loop [v (<! new-ch)]
               (if (nil? v)
                 (close! ch)
                 (do
                   (when-not (p v)
                     (>! ch v))
                   (recur (<! new-ch)))))
      new-ch)
    (async/remove> p ch)))

(defn filter< [p ch]
  (if *step-machine*
    (let [new-ch (child-chan ch "filter<")]
      (go-loop [v (<! ch)]
               (if (nil? v)
                 (close! new-ch)
                 (do
                   (when (p v)
                     (>! new-ch v))
                   (recur (<! ch)))))
      new-ch)
    (async/filter< p ch)))

(defn remove< [p ch]
  (if *step-machine*
    (let [new-ch (child-chan ch "remove<")]
      (go-loop [v (<! ch)]
               (if (nil? v)
                 (close! new-ch)
                 (do
                   (when-not (p v)
                     (>! new-ch v))
                   (recur (<! ch)))))
      new-ch)
    (async/remove< p ch)))

;;

(defn- mapcat* [f in out]
  (go-loop []
           (let [val (<! in)]
             (if (nil? val)
               (close! out)
               (let [vals (f val)]
                 (doseq [v vals]
                   (>! out v))
                 (recur))))))

(defn- mapcat<+
  "From core.async"
  [f in buf-or-n]
  (let [out (chan-named "mapcat<" buf-or-n)]
    (mapcat* f in out)
    out))

(defn mapcat<
  ([f in]
     (if *step-machine*
       (mapcat<+ f in nil)
       (async/mapcat< f in)))
  ([f in buf-or-n]
     (if *step-machine*
       (mapcat<+ f in buf-or-n)
       (async/mapcat< f in buf-or-n))))

(defn- mapcat>+
  "From core.async"
  [f out buf-or-n]
  (let [in (chan-named "mapcat>" buf-or-n)]
    (mapcat* f in out)
    in))

(defn mapcat>
  "From core.async"
  ([f out]
     (if *step-machine*
       (mapcat>+ f out nil)
       (async/mapcat> f out)))
  ([f out buf-or-n]
     (if *step-machine*
       (mapcat>+ f out buf-or-n)
       (async/mapcat> f out buf-or-n))))

(defn- pipe+
  "From core.async"
  [from to close?]
  (go-loop []
           (let [v (<! from)]
             (if (nil? v)
               (when close? (close! to))
               (do (>! to v)
                   (recur)))))
  to)

(defn pipe
  ([from to]
     (if *step-machine*
       (pipe+ from to true)
       (async/pipe from to)))
  ([from to close?]
     (if *step-machine*
       (pipe+ from to close?)
       (async/pipe from to close?))))

(defn- split+
  "From core.async"
  [p ch t-buf-or-n f-buf-or-n]
  (let [tc (child-chan ch "split1" t-buf-or-n)
        fc (child-chan ch "split2" f-buf-or-n)]
    (go-loop []
             (let [v (<! ch)]
               (if (nil? v)
                 (do (close! tc) (close! fc))
                 (do (>! (if (p v) tc fc) v)
                     (recur)))))
    [tc fc]))

(defn split
  ([p ch]
     (if *step-machine*
       (split+ p ch nil nil)
       (async/split p ch)))
  ([p ch t-buf-or-n f-buf-or-n]
     (if *step-machine*
       (split+ p ch t-buf-or-n f-buf-or-n)
       (async/split p ch t-buf-or-n f-buf-or-n))))

(defn- reduce+
  "From core.async"
  [f init ch]
  (go-loop [ret init]
           (let [v (<! ch)]
             (if (nil? v)
               ret
               (recur (f ret v))))))

(defn reduce
  [f init ch]
  (if *step-machine*
    (reduce+ f init ch)
    (async/reduce f init ch)))

(defn- bounded-count
  "From core.async"
  [n coll]
  (if (counted? coll)
    (min n (count coll))
    (loop [i 0 s (seq coll)]
      (if (and s (< i n))
        (recur (inc i) (next s))
        i))))

(defn- onto-chan+
  "From core.async"
  [ch coll close?]
  (go-loop [vs (seq coll)]
           (if vs
             (do (>! ch (first vs))
                 (recur (next vs)))
             (when close?
               (close! ch)))))

(defn onto-chan
  ([ch coll]
     (if *step-machine*
       (onto-chan+ ch coll true)
       (async/onto-chan ch coll)))
  ([ch coll close?]
     (if *step-machine*
       (onto-chan+ ch coll close?)
       (async/onto-chan ch coll close?))))

(defn- to-chan+
  "From core.async"
  [coll]
  (let [ch (chan-named "to-chan" (bounded-count 100 coll))]
    (onto-chan ch coll)
    ch))

(defn to-chan
  [coll]
  (if *step-machine*
    (to-chan+ coll)
    (async/to-chan coll)))

;;

(defprotocol Mux
  (muxch* [_]))

(defprotocol Mult
  (tap* [m ch close?])
  (untap* [m ch])
  (untap-all* [m]))

(defn- mult+
  "From core.async"
  [ch]
  (let [cs (atom {}) ;;ch->close?
        m (reify
            Mux
            (muxch* [_] ch)

            Mult
            (tap* [_ ch close?] (swap! cs assoc ch close?) nil)
            (untap* [_ ch] (swap! cs dissoc ch) nil)
            (untap-all* [_] (reset! cs {}) nil))
        dchan (child-chan ch "mult" 1)
        dctr (atom nil)
        done #(when (zero? (swap! dctr dec))
                (put! dchan true))]
    (go-loop []
             (let [val (<! ch)]
               (if (nil? val)
                 (doseq [[c close?] @cs]
                   (when close? (close! c)))
                 (let [chs (keys @cs)]
                   (reset! dctr (count chs))
                   (doseq [c chs]
                     (try
                       (put! c val done)
                       (catch Exception e
                         (swap! dctr dec)
                         (untap* m c))))
                   ;;wait for all
                   (when (seq chs)
                     (<! dchan))
                   (recur)))))
    m))

(defn mult
  [ch]
  (if *step-machine*
    (mult+ ch)
    (async/mult ch)))

(defn- tap+
  "From core.async"
  [mult ch close?]
  (tap* mult ch close?) ch)

(defn tap
  ([mult ch]
     (if *step-machine*
       (tap+ mult ch true)
       (async/tap mult ch)))
  ([mult ch close?]
     (if *step-machine*
       (tap+ mult ch close?)
       (async/tap mult ch close?))))

(defn- untap+
  "From core.async"
  [mult ch]
  (untap* mult ch))

(defn untap
  [mult ch]
  (if *step-machine*
    (untap+ mult ch)
    (async/untap mult ch)))

(defn- untap-all+
  "From core.async"
  [mult]
  (untap-all* mult))

(defn untap-all
  [mult]
  (if *step-machine*
    (untap-all+ mult)
    (async/untap-all mult)))

;;

(defprotocol Mix
  (admix* [m ch])
  (unmix* [m ch])
  (unmix-all* [m])
  (toggle* [m state-map])
  (solo-mode* [m mode]))

(defn- mix+
  "From core.async"
  [out]
  (let [cs (atom {}) ;;ch->attrs-map
        solo-modes #{:mute :pause}
        attrs (conj solo-modes :solo)
        solo-mode (atom :mute)
        change (chan-named "mix")
        changed #(put! change true)
        pick (fn [attr chs]
               (reduce-kv
                (fn [ret c v]
                  (if (attr v)
                    (conj ret c)
                    ret))
                #{} chs))
        calc-state (fn []
                     (let [chs @cs
                           mode @solo-mode
                           solos (pick :solo chs)
                           pauses (pick :pause chs)]
                       {:solos solos
                        :mutes (pick :mute chs)
                        :reads (conj
                                (if (and (= mode :pause) (not (empty? solos)))
                                  (vec solos)
                                  (vec (remove pauses (keys chs))))
                                change)}))
        m (reify
            Mux
            (muxch* [_] out)
            Mix
            (admix* [_ ch] (swap! cs assoc ch {}) (changed))
            (unmix* [_ ch] (swap! cs dissoc ch) (changed))
            (unmix-all* [_] (reset! cs {}) (changed))
            (toggle* [_ state-map] (swap! cs (partial merge-with clojure.core/merge) state-map) (changed))
            (solo-mode* [_ mode]
              (assert (solo-modes mode) (str "mode must be one of: " solo-modes))
              (reset! solo-mode mode)
              (changed)))]
    (go-loop [{:keys [solos mutes reads] :as state} (calc-state)]
             (let [[v c] (alts! reads)]
               (if (or (nil? v) (= c change))
                 (do (when (nil? v)
                       (swap! cs dissoc c))
                     (recur (calc-state)))
                 (do (when (or (solos c)
                               (and (empty? solos) (not (mutes c))))
                       (>! out v))
                     (recur state)))))
    m))

(defn mix
  [out]
  (if *step-machine*
    (mix+ out)
    (async/mix out)))

(defn- admix+
  "From core.async"
  [mix ch]
  (admix* mix ch))

(defn admix
  [mix ch]
  (if *step-machine*
    (admix+ mix ch)
    (async/admix mix ch)))

(defn- unmix+
  "From core.async"
  [mix ch]
  (unmix* mix ch))

(defn unmix
  [mix ch]
  (if *step-machine*
    (unmix+ mix ch)
    (async/unmix mix ch)))

(defn- unmix-all+
  "From core.async"
  [mix]
  (unmix-all* mix))

(defn unmix-all
  [mix]
  (if *step-machine*
    (unmix-all+ mix)
    (async/unmix-all mix)))

(defn- toggle+
  "From core.async"
  [mix state-map]
  (toggle* mix state-map))

(defn toggle
  [mix state-map]
  (if *step-machine*
    (toggle+ mix state-map)
    (async/toggle mix state-map)))

(defn- solo-mode+
  "From core.async"
  [mix mode]
  (solo-mode* mix mode))

(defn solo-mode
  [mix mode]
  (if *step-machine*
    (solo-mode+ mix mode)
    (async/solo-mode mix mode)))

;;

(defprotocol Pub
  (sub* [p v ch close?])
  (unsub* [p v ch])
  (unsub-all* [p] [p v]))

(defn- pub+
  "From core.async"
  [ch topic-fn buf-fn]
  (let [mults (atom {}) ;;topic->mult
        ensure-mult (fn [topic]
                      (or (get @mults topic)
                          (get (swap! mults
                                      #(if (% topic) % (assoc % topic (mult (child-chan
                                                                             ch "pub"
                                                                             (buf-fn topic))))))
                               topic)))
        p (reify
            Mux
            (muxch* [_] ch)

            Pub
            (sub* [p topic ch close?]
              (let [m (ensure-mult topic)]
                (tap m ch close?)))
            (unsub* [p topic ch]
              (when-let [m (get @mults topic)]
                (untap m ch)))
            (unsub-all* [_] (reset! mults {}))
            (unsub-all* [_ topic] (swap! mults dissoc topic)))]
    (go-loop []
             (let [val (<! ch)]
               (if (nil? val)
                 (doseq [m (vals @mults)]
                   (close! (muxch* m)))
                 (let [topic (topic-fn val)
                       m (get @mults topic)]
                   (when m
                     (try
                       (>! (muxch* m) val)
                       (catch Exception e
                         (swap! mults dissoc topic))))
                   (recur)))))
    p))

(defn pub
  ([ch topic-fn]
     (if *step-machine*
       (pub+ ch topic-fn (constantly nil))
       (async/pub ch topic-fn)))
  ([ch topic-fn buf-fn]
     (if *step-machine*
       (pub+ ch topic-fn buf-fn)
       (async/pub ch topic-fn buf-fn))))

(defn- sub+
  "From core.async"
  [p topic ch close?] (sub* p topic ch close?))

(defn sub
  ([p topic ch] (sub p topic ch true))
  ([p topic ch close?]
     (if *step-machine*
       (sub+ p topic ch close?)
       (async/sub p topic ch close?))))

(defn- unsub+
  "From core.async"
  [p topic ch]
  (unsub* p topic ch))

(defn unsub
  [p topic ch]
  (if *step-machine*
    (unsub+ p topic ch)
    (async/unsub p topic ch)))

(defn- unsub-all+
  "From core.async"
  ([p] (unsub-all* p
                   ))
  ([p topic]
     (unsub-all* p topic)))

(defn unsub-all
  ([p]
     (if *step-machine*
       (unsub-all+ p)
       (async/unsub-all p)))
  ([p topic]
     (if *step-machine*
       (unsub-all+ p topic)
       (async/unsub-all p topic))))
;;

(defn- map+
  "From core.async"
  [f chs buf-or-n]
  (let [chs (vec chs)
        out (chan-named "map+.out" buf-or-n)
        cnt (count chs)
        rets (object-array cnt)
        dchan (chan-named "map+.d" 1)
        dctr (atom nil)
        done (mapv (fn [i]
                     (fn [ret]
                       (aset rets i ret)
                       (when (zero? (swap! dctr dec))
                         (put! dchan (java.util.Arrays/copyOf rets cnt)))))
                   (range cnt))]
    (go-loop-named "map+" []
                   (reset! dctr cnt)
                   (dotimes [i cnt]
                     (try
                       (take! (chs i) (done i))
                       (catch Exception e
                         (swap! dctr dec))))

                   (let [rets (<! dchan)]
                     (if (some nil? rets)
                       (close! out)
                       (do (>! out (apply f rets))
                           (recur)))))
    out))

(defn map
  ([f chs]
     (if *step-machine*
       (map+ f chs nil)
       (async/map f chs)))
  ([f chs buf-or-n]
     (if *step-machine*
       (map+ f chs buf-or-n)
       (async/map f chs buf-or-n))))

(defn map< [f ch]
  (if *step-machine*
    (let [c-new (chan-named nil nil f nil)]
      (pipe ch c-new)
      c-new)
    (async/map< f ch)))

(defn map> [f ch]
  (if *step-machine*
    (let [c-new (chan-named nil nil nil f)]
      (pipe c-new ch)
      c-new)
    (async/map> f ch)))

;;

(defn- merge+
  "From core.async"
  [chs buf-or-n]
  (let [out (child-chan (first chs) "merge" buf-or-n)]
    (go-loop-named (str (get-channel-name *step-machine* (first chs)) "." "merge")
                   [cs (vec chs)]
                   (if (pos? (count cs))
                     (let [[v c] (alts! cs)]
                       (if (nil? v)
                         (recur (filterv #(not= c %) cs))
                         (do (>! out v)
                             (recur cs))))
                     (close! out)))
    out))

(defn merge
  ([chs]
     (if *step-machine*
       (merge+ chs nil)
       (async/merge chs nil)))
  ([chs buf-or-n]
     (if *step-machine*
       (merge+ chs buf-or-n)
       (async/merge chs buf-or-n))))

(defn- into+
  "From core.async"
  [coll ch]
  (reduce conj coll ch))

(defn into
  "From core.async"
  [coll ch]
  (if *step-machine*
    (into+ coll ch)
    (async/into coll ch)))

(defn- take+
  "From core.async"
  [n ch buf-or-n]
  (let [out (child-chan ch "take" buf-or-n)]
    (go (loop [x 0]
          (when (< x n)
            (let [v (<! ch)]
              (when (not (nil? v))
                (>! out v)
                (recur (inc x))))))
        (close! out))
    out))

(defn take
  ([n ch]
     (if *step-machine*
       (take+ n ch nil)
       (async/take n ch)))
  ([n ch buf-or-n]
     (if *step-machine*
       (take+ n ch buf-or-n)
       (async/take n ch buf-or-n))))

(defn- unique+
  "From core.async"
  [ch buf-or-n]
  (let [out (child-chan ch "unique" buf-or-n)]
    (go (loop [last nil]
          (let [v (<! ch)]
            (when (not (nil? v))
              (if (= v last)
                (recur last)
                (do (>! out v)
                    (recur v))))))
        (close! out))
    out))

(defn unique
  ([ch]
     (if *step-machine*
       (unique+ ch nil)
       (async/unique ch)))
  ([ch buf-or-n]
     (if *step-machine*
       (unique+ ch buf-or-n)
       (async/unique ch buf-or-n))))

(defn- partition+
  "From core.async"
  [n ch buf-or-n]
  (let [out (child-chan ch "partition" buf-or-n)]
    (go-named (str (get-channel-name *step-machine* ch) "." "partition")
              (loop [arr (make-array Object n)
                     idx 0]
                (let [v (<! ch)]
                  (if (not (nil? v))
                    (do (aset ^objects arr idx v)
                        (let [new-idx (inc idx)]
                          (if (< new-idx n)
                            (recur arr new-idx)
                            (do (>! out (vec arr))
                                (recur (make-array Object n) 0)))))
                    (do (when (> idx 0)
                          (let [narray (make-array Object idx)]
                            (System/arraycopy arr 0 narray 0 idx)
                            (>! out (vec narray))))
                        (close! out))))))
    out))

(defn partition
  ([n ch]
     (if *step-machine*
       (partition+ n ch nil)
       (async/partition n ch)))
  ([n ch buf-or-n]
     (if *step-machine*
       (partition+ n ch buf-or-n)
       (async/partition n ch buf-or-n))))

(defn- partition-by+
  "From core.async"
  [f ch buf-or-n]
  (let [out (child-chan ch "partition-by" buf-or-n)]
    (go (loop [lst (ArrayList.)
               last ::nothing]
          (let [v (<! ch)]
            (if (not (nil? v))
              (let [new-itm (f v)]
                (if (or (= new-itm last)
                        (identical? last ::nothing))
                  (do (.add ^ArrayList lst v)
                      (recur lst new-itm))
                  (do (>! out (vec lst))
                      (let [new-lst (ArrayList.)]
                        (.add ^ArrayList new-lst v)
                        (recur new-lst new-itm)))))
              (do (when (> (.size ^ArrayList lst) 0)
                    (>! out (vec lst)))
                  (close! out))))))
    out))

(defn partition-by
  ([f ch]
     (if *step-machine*
       (partition-by f ch nil)
       (async/partition-by f ch)))
  ([f ch buf-or-n]
     (if *step-machine*
       (partition-by+ f ch buf-or-n)
       (async/partition-by f ch buf-or-n))))

(defn thread-call [f]
  (if *step-machine*
    (go
     (f))
    (async/thread-call f)))

(defmacro thread [& body]
  `(let [f# (fn [] ~@body)]
     (if *step-machine*
       (thread-call f#)
       (async/thread-call f#))))
