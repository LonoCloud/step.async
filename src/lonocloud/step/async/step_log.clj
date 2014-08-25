(ns lonocloud.step.async.step-log
  "Optionally record activity in a step machine"
  (:require [lonocloud.step.async.pschema :as s]))

;; schema

(def MachineLog {:action-history? Boolean
                 :detailed-action-history? Boolean
                 (s/optional-key :last-done-state) s/Keyword
                 :history s/Any
                 :state-trace-on? Boolean
                 :state-trace [s/Map]
                 :last-action (s/maybe s/Any)
                 :detailed-action-history [s/Any]})

;;

(s/defn init-log [action-history? detailed-action-history?]
  {:action-history? action-history?
   :detailed-action-history? detailed-action-history?
   :history []
   :state-trace-on? false
   :state-trace []
   :last-action nil
   :detailed-action-history []})

(s/defn action-history? :- Boolean
  [this :- MachineLog]
  (:action-history? this))

(s/defn detailed-action-history? :- Boolean
  [this :- MachineLog]
  (:detailed-action-history? this))

(s/defn record-done-state :- MachineLog
  [this :- MachineLog
   status]
  (assoc this :last-done-state status))

(s/defn record-event :- MachineLog
  [this :- MachineLog
   item]
  (if (:action-history? this)
    (update-in this [:history] conj item)
    this))

(s/defn record-action :- MachineLog
  [this :- MachineLog
   action]
  (let [result (assoc this :last-action action)]
    (if (:detailed-action-history? this)
      (update-in result [:detailed-action-history] conj action)
      result)))

(s/defn state-trace-switch :- MachineLog
  [this :- MachineLog
   on? :- Boolean
   s]
  (let [result (assoc this :state-trace-on? on?)]
    (if on?
      (assoc result :state-trace [])
      (update-in result [:state-trace] conj s))))

(s/defn record-state :- MachineLog
  [this :- MachineLog
   s]
  (if (:state-trace-on? this)
    (update-in this [:state-trace] conj s)
    this))

(s/defn last-action
  [this :- MachineLog]
  (:last-action this))

(s/defn last-done-state
  [this :- MachineLog]
  (:last-done-state this))

(s/defn detailed-action-history
  [this :- MachineLog]
  (:detailed-action-history this))

(s/defn history
  [this :- MachineLog]
  (:history this))

(s/defn state-trace
  [this :- MachineLog]
  (:state-trace this))
