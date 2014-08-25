(ns lonocloud.step.async.step-listen
  "Deals with processes that are listening for a step machine to reach a specified state."
  (:require [clojure.tools.logging :refer [errorf]]
            [lonocloud.step.async.pschema :as s]
            [lonocloud.step.async.step-model :refer [Channel]]))

(def ListenerMap {:step-listeners #{Channel}
                  :quiesce-listeners #{Channel}
                  :breakpoints #{s/Fn}})

;; machine listeners

(s/defn init-listener-map :- ListenerMap
  []
  {:step-listeners #{}
   :quiesce-listeners #{}
   :breakpoints #{}})

(s/defn add-listener :- ListenerMap
  [this :- ListenerMap
   wait-type :- s/Keyword
   wait-channel :- Channel]
  (update-in this [wait-type] conj wait-channel))

(s/defn take-listeners :- (s/maybe #{Channel})
  [this :- ListenerMap
   wait-type :- s/Keyword]
  (wait-type this))

(s/defn clear-listeners :- ListenerMap
  [this :- ListenerMap
   wait-type :- s/Keyword]
  (assoc this wait-type #{}))

(s/defn add-breakpoint :- ListenerMap
  [this :- ListenerMap
   f :- s/Fn]
  (update-in this [:breakpoints] conj f))

(s/defn breakpoints :- #{s/Fn}
  [this :- ListenerMap]
  (:breakpoints this))

(s/defn clear-breakpoint :- ListenerMap
  [this :- ListenerMap
   f :- s/Fn]
  (update-in this [:breakpoints] disj f))
