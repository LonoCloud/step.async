(ns lonocloud.step.async.step-schedule
  "Algorithms for scheduling what to run next in a step machine"
  (:require [lonocloud.step.async.pschema :as s]
            [lonocloud.step.async.step-model :refer [ThreadId ChannelId PendingPutChoices]])
  (:import [java.util Random]))

(def ^:private max-seed 100000000)

(s/defn stm-rand [initial-seed :- s/Int]
  (let [seed (ref initial-seed)]
    (fn [n]
      (let [r (Random. @seed)
            result (.nextInt r n)
            new-seed (.nextInt r max-seed)]
        (ref-set seed new-seed)
        result))))

(s/defn- random-pick [choices
                      rand-source :- s/Fn]
  (let [n (count choices)]
    (when (pos? n)
      ((-> choices
           sort
           vec)
       (rand-source n)))))

(s/defn random-scheduler
  "Randomly select the next step to take in a step machine (if the rand-source is STM compatible,
  then the 'random' choices are actually repeatable with the same seed)."
  [rand-source :- s/Fn
   pending-threads :- #{ThreadId}
   parked-threads :- #{ThreadId}
   available-pending-puts :- PendingPutChoices
   available-takes :- #{ChannelId}
   channel-waiters :- {ChannelId #{ThreadId}}
   available-puts :- {ThreadId ChannelId}]
  (let [verb-choices (->> [(when (not (empty? pending-threads))
                             :run-pending)
                           (when (not (empty? parked-threads))
                             :run-parked)
                           (when (not (empty? available-pending-puts))
                             :do-pending-put)
                           (when (not (empty? available-takes))
                             :take)
                           (when (not (empty? available-puts))
                             :put)]
                          (remove nil?))]
    (condp = (random-pick verb-choices rand-source)
      :run-pending [:run-pending (-> pending-threads
                                     (random-pick rand-source))]
      :run-parked [:run-parked (-> parked-threads
                                   (random-pick rand-source))]
      :do-pending-put [:do-pending-put (-> (->> available-pending-puts
                                                (map first))
                                           (random-pick rand-source))]
      :take (let [channel-id (-> available-takes
                                 (random-pick rand-source))
                  thread-id (-> (channel-waiters channel-id)
                                (random-pick rand-source))]
              [:take thread-id channel-id])
      :put [:put (-> available-puts
                     keys
                     (random-pick rand-source))])))

(s/defn deterministic-scheduler
  "Deterministically select the next step to take in a step machine"
  [pending-threads :- #{ThreadId}
   parked-threads :- #{ThreadId}
   available-pending-puts :- PendingPutChoices
   available-takes :- #{ChannelId}
   channel-waiters :- {ChannelId #{ThreadId}}
   available-puts :- {ThreadId ChannelId}]
  (if (not (empty? pending-threads))
    (let [pending-id-to-run (-> pending-threads
                                sort
                                first)]
      [:run-pending pending-id-to-run])
    (if (not (empty? parked-threads))
      (let [parked-id-to-run (-> parked-threads
                                 sort
                                 first)]
        [:run-parked parked-id-to-run])
      (if (not (empty? available-pending-puts))
        (let [pending-put-to-run (->> available-pending-puts
                                      (map first)
                                      sort
                                      first)]
          [:do-pending-put pending-put-to-run])
        (if (not (empty? available-takes))
          (let [take-channel-id (->> available-takes
                                     sort
                                     first)
                take-thread-id (->> (channel-waiters take-channel-id)
                                    sort
                                    first)]
            [:take take-thread-id take-channel-id])
          (if (not (empty? available-puts))
            [:put (->> available-puts
                       keys
                       sort
                       first)]))))))
