(ns lonocloud.step.async.step-threads
  (:require [lonocloud.step.async.pschema :as s]
            [lonocloud.step.async.step-model :refer [ThreadId Channel]]
            [lonocloud.step.async.util :as util :refer [no-nil]]))

(def PendingStepThread {(s/optional-key :thread-name) String
                        :f s/Fn
                        :output-channel Channel
                        :control-channel Channel})

(def StepThread {(s/optional-key :thread-name) String
                 :control-channel Channel})

(def ExternalThread {:control-channel Channel})

(s/defn- init-pending-step-thread :- PendingStepThread
  [thread-name :- (s/maybe String)
   f :- s/Fn
   output-channel :- Channel
   control-channel :- Channel]
  (-> {:thread-name thread-name
       :f f
       :output-channel output-channel
       :control-channel control-channel}
      no-nil))

(s/defn- init-step-thread :- StepThread
  [thread-name :- (s/maybe String)
   control-channel :- Channel]
  (-> {:thread-name thread-name
       :control-channel control-channel}
      no-nil))

(s/defn- init-external-thread :- ExternalThread
  [control-channel :- Channel]
  {:control-channel control-channel})

;; maps of step threads

(def StepThreadMap {(s/optional-key :pending-step-threads) {ThreadId PendingStepThread}
                    (s/optional-key :step-threads) {ThreadId StepThread}
                    (s/optional-key :parked-threads) {ThreadId StepThread}
                    (s/optional-key :done-step-threads) {ThreadId (s/maybe String)}
                    (s/optional-key :external-threads) {ThreadId ExternalThread}})

(s/defn get-control-channel :- Channel
  [this :- StepThreadMap
   thread-id :- ThreadId]
  (or (get-in this [:step-threads thread-id :control-channel])
      (get-in this [:parked-threads thread-id :control-channel])
      (get-in this [:external-threads thread-id :control-channel])))

(s/defn all-threads-exited? :- Boolean
  [this :- StepThreadMap]
  (and (empty? (-> (:pending-step-threads this)
                   keys))
       (empty? (:step-threads this))
       (empty? (:parked-threads this))))

(s/defn running-thread-ids :- #{ThreadId}
  [this :- StepThreadMap]
  (-> (merge (:step-threads this)
             (:parked-threads this))
      keys
      set))

(s/defn pending-thread-ids :- #{ThreadId}
  [this :- StepThreadMap]
  (-> (:pending-step-threads this)
      keys
      set))

(s/defn get-pending-step-thread :- PendingStepThread
  [this :- StepThreadMap
   thread-id :- ThreadId]
  (get-in this [:pending-step-threads thread-id]))

(s/defn parked-thread-ids :- #{ThreadId}
  [this :- StepThreadMap]
  (-> (:parked-threads this)
      keys
      set))

(s/defn register-root-thread :- StepThreadMap
  [this :- StepThreadMap
   thread-id :- ThreadId
   control-channel :- Channel]
  (assoc-in this [:step-threads thread-id] (init-step-thread nil control-channel)))

(s/defn register-thread :- StepThreadMap
  [this :- StepThreadMap
   thread-id :- ThreadId
   thread-name :- (s/maybe String)
   f :- s/Fn
   output-channel :- Channel
   control-channel :- Channel]
  (assoc-in this [:pending-step-threads thread-id]
            (init-pending-step-thread thread-name f output-channel control-channel)))

(s/defn register-external-thread :- StepThreadMap
  [this :- StepThreadMap
   thread-id :- ThreadId
   control-channel :- Channel]
  (assoc-in this [:external-threads thread-id]
            (init-external-thread control-channel)))

(s/defn thread-starting :- StepThreadMap
  [this :- StepThreadMap
   thread-id :- ThreadId]
  (let [{:keys [thread-name control-channel]} (get-pending-step-thread this thread-id)]
    (-> this
        (update-in [:pending-step-threads] dissoc thread-id)
        (assoc-in [:step-threads thread-id] (init-step-thread thread-name control-channel)))))

(s/defn thread-park :- StepThreadMap
  [this :- StepThreadMap
   thread-id :- ThreadId]
  (let [thread (get-in this [:step-threads thread-id])]
    (-> this
        (update-in [:step-threads] dissoc thread-id)
        (assoc-in [:parked-threads thread-id] thread))))

(s/defn thread-unpark :- StepThreadMap
  [this :- StepThreadMap
   thread-id :- ThreadId]
  (let [thread (get-in this [:parked-threads thread-id])]
    (-> this
        (update-in [:parked-threads] dissoc thread-id)
        (assoc-in [:step-threads thread-id] thread))))

(s/defn thread-finished :- StepThreadMap
  [this :- StepThreadMap
   thread-id :- ThreadId]
  (let [thread-name (get-in this [:step-threads thread-id :thread-name])]
    (-> this
        (update-in [:step-threads] dissoc thread-id)
        (assoc-in [:done-step-threads thread-id] thread-name))))

(s/defn all-thread-names :- {ThreadId String}
  [this :- StepThreadMap]
  (-> (merge (:pending-step-threads this)
             (:step-threads this)
             (:parked-threads this))
      (util/update-vals :thread-name)
      (merge (:done-step-threads this))
      (util/update-map (fn [k v]
                         [k (or v
                                (str "thread-" k))]))))

(s/defn pending-threads-available? :- Boolean
  [this :- StepThreadMap]
  (not (empty? (-> (:pending-step-threads this)
                   keys))))

(s/defn parked-threads-available? :- Boolean
  [this :- StepThreadMap]
  (not (empty? (-> (:parked-threads this)
                   keys))))
