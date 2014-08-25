(ns lonocloud.step.async.step-timeout
  "Logic for dealing with timeouts in step machines"
  (:require [lonocloud.step.async.pschema :as s]
            [lonocloud.step.async.step-model :refer [TimeoutId ThreadId Channel]]
            [lonocloud.step.async.util :as util :refer [no-nil no-empty]]))

;; schema

(def Timeout {:thread-id ThreadId
              :start-time s/Int
              :duration s/Int
              :timeout-chan Channel
              (s/optional-key :timeout-name) String})

(def TimeoutExternal {:thread-name String
                      :timeout-name String
                      :timeout-id TimeoutId
                      :start-time s/Int
                      :duration s/Int})

(def TimeoutMap {TimeoutId Timeout})

(s/defn add-timeout :- TimeoutMap
  [this :- TimeoutMap
   thread-id :- ThreadId
   timeout-name :- String
   timeout-id :- TimeoutId
   time-now :- s/Int
   ms :- s/Int
   timeout-chan :- Channel]
  (assoc-in this [timeout-id]
            (no-nil {:thread-id thread-id
                     :start-time time-now
                     :duration ms
                     :timeout-chan timeout-chan
                     :timeout-name timeout-name})))

(s/defn get-timeout-channel :- Channel
  [this :- TimeoutMap
   timeout-id :- TimeoutId]
  (get-in this [timeout-id :timeout-chan]))

(s/defn remove-timeout :- TimeoutMap
  [this :- TimeoutMap
   timeout-id :- TimeoutId]
  (dissoc this timeout-id))

(s/defn all-timeout-names :- {TimeoutId String}
  [this :- TimeoutMap]
  (util/update-map this (fn [k v]
                          [k (or (:timeout-name v)
                                 (str "timeout-" k))])))

(s/defn timeout-summary :- (s/maybe {String TimeoutId})
  [this :- TimeoutMap]
  (-> this
      (util/update-map (fn [k v]
                         [(:timeout-name v) k]))
      no-empty))

(s/defn get-all-timeouts :- [TimeoutExternal]
  [this :- TimeoutMap
   thread-names :- {ThreadId String}
   timeout-names :- {TimeoutId String}]
  (-> (map (fn [id]
             (let [timeout (this id)]
               (-> (assoc timeout :timeout-id id)
                   (dissoc :timeout-chan :thread-id)
                   (assoc :timeout-name (timeout-names id))
                   (assoc :thread-name (thread-names (:thread-id timeout))))))
           (keys this))
      vec))

(s/defn get-timeout-thread-id :- ThreadId
  [this :- TimeoutMap
   timeout-id :- TimeoutId]
  (get-in this [timeout-id :thread-id]))
