(ns lonocloud.step.async.step-channel
  "Implementation of a step-channel, responsible for keeping track of all puts and takes on a given
  channel."
  (:require [lonocloud.step.async.pschema :as s]
            [lonocloud.step.async.step-model :refer [Channel ThreadId ChannelId PendingPutChoices]]
            [lonocloud.step.async.util :as util :refer [no-nil]]))

;; schema

(def UnblockingType (s/enum :dropping :sliding))

(def StepChannel {(s/optional-key :channel-name) String
                  :channel Channel
                  :channel-contents [s/Any]
                  (s/optional-key :unblocking) UnblockingType
                  (s/optional-key :channel-history) [s/Any]
                  :channel-size s/Int
                  :pending-puts {ThreadId {:value s/Any
                                           :block? Boolean}}
                  :blocked-puts #{ThreadId}
                  :blocked-takes #{ThreadId}
                  (s/optional-key :closed?) Boolean
                  (s/optional-key :closing?) Boolean
                  (s/optional-key :take-f) s/Fn
                  (s/optional-key :put-f) s/Fn})

(def StepChannelMap {ChannelId StepChannel})

(def close-message (Object.))

;; single channel mutators

(s/defn- channel-init :- StepChannel
  [channel-name :- (s/maybe String)
   channel :- Channel
   channel-size :- s/Int
   unblocking :- (s/maybe UnblockingType)
   channel-history? :- Boolean
   take-f :- (s/maybe s/Fn)
   put-f :- (s/maybe s/Fn)]
  (no-nil {:channel-name channel-name
           :channel channel
           :channel-size channel-size
           :unblocking unblocking
           :channel-contents []
           :channel-history (when channel-history? [])
           :pending-puts {}
           :blocked-puts #{}
           :blocked-takes #{}
           :take-f take-f
           :put-f put-f}))

(s/defn- channel-register-put [this :- StepChannel
                               thread-id :- ThreadId
                               value :- s/Any
                               block? :- Boolean]
  (let [result (assoc-in this [:pending-puts thread-id] {:value value
                                                         :block? block?})]
    (if (= close-message value)
      (assoc result :closing? true)
      result)))

(s/defn- channel-register-take [this :- StepChannel
                                thread-id]
  (update-in this [:blocked-takes] conj thread-id))

(s/defn- channel-close [this :- StepChannel]
  (-> (assoc this :closed? true)
      (dissoc :closing?)))

(s/defn- channel-after-closing :- Boolean
  [this :- StepChannel]
  (boolean (or (:closed? this)
               (:closing? this))))

(s/defn- channel-accept-put [this :- StepChannel
                             thread-id]
  (let [{:keys [channel-history unblocking channel-size channel-contents put-f]} this
        {:keys [value block?]} (get-in this [:pending-puts thread-id])]
    (let [value (if (and put-f
                         (not= close-message value))
                  (put-f value)
                  value)
          result (update-in this [:pending-puts] dissoc thread-id)
          result (if (= close-message value)
                   (channel-close result)
                   (if (:closed? this)
                     result
                     (if (and (pos? channel-size)
                              (= (count channel-contents) channel-size))
                       (condp = unblocking
                         :dropping result
                         :sliding (-> (update-in result [:channel-contents] conj value)
                                      (update-in [:channel-contents] subvec 1)))
                       (update-in result [:channel-contents] conj value))))
          result (if block?
                   (update-in result [:blocked-puts] conj thread-id)
                   result)]
      (if (and channel-history
               (not= close-message value))
        (update-in result [:channel-history] conj value)
        result))))

(s/defn- channel-clear-put [this :- StepChannel
                            thread-id]
  (update-in this [:blocked-puts] disj thread-id))

(s/defn- channel-take-value [this :- StepChannel]
  (or (if-let [v (first (get this :channel-contents))]
        (if-let [take-f (:take-f this)]
          (take-f v)
          v))
      (when (:closed? this)
        close-message)))

(s/defn- channel-clear-take
  [this :- StepChannel]
  (if (empty? (:channel-contents this))
    this
    (update-in this [:channel-contents] subvec 1)))

(s/defn- channel-clear-taker
  [this :- StepChannel
   thread-id]
  (update-in this [:blocked-takes] disj thread-id))

;; single channel readers

(s/defn- channel-space-available? :- Boolean
  [this :- StepChannel]
  (let [{:keys [channel-contents channel-size closed? blocked-takes blocked-puts unblocking]}
        this]
    (or (< (count channel-contents)
           channel-size)
        (boolean unblocking)
        closed?
        (and (= 0 channel-size)
             (not (empty? blocked-takes))
             (empty? blocked-puts))
        (and (= 1 (count (:pending-puts this)))
             (channel-after-closing this)))))

(s/defn- channel-pending-puts-available [this :- StepChannel]
  (keys (:pending-puts this)))

(s/defn- channel-pending-puts-available? [this :- StepChannel]
  (and (not (empty? (:pending-puts this)))
       (channel-space-available? this)))

(s/defn- channel-puts-available :- #{ThreadId}
  [this :- StepChannel]
  (:blocked-puts this))

(s/defn- channel-puts-available? :- (s/maybe Boolean)
  [this :- StepChannel]
  (not (empty? (:blocked-puts this))))

(s/defn- channel-takes-available :- #{ThreadId}
  [this :- StepChannel]
  (:blocked-takes this))

(s/defn- channel-takes-available? :- (s/maybe Boolean)
  [this :- StepChannel]
  (and (not (empty? (:blocked-takes this)))
       (or (not (empty? (:channel-contents this)))
           (:closed? this))))

;; public API, step-channel maps

(s/defn pending-puts :- #{ThreadId}
  [this :- StepChannelMap]
  (->> this
       vals
       (map (comp keys :pending-puts))
       (reduce into #{})))

(s/defn pending-put-map :- {ThreadId {:channel-id ChannelId
                                      :value s/Any
                                      :block? Boolean}}
  [this :- StepChannelMap]
  (->> this
       (mapcat (fn [[k v]]
                 (for [[thread-id {:keys [value block?]}] (:pending-puts v)]
                   [thread-id {:channel-id k
                               :value value
                               :block? block?}])))
       (mapcat identity)
       (apply hash-map)))

(s/defn blocked-puts :- #{ThreadId}
  [this :- StepChannelMap]
  (->> this
       vals
       (map :blocked-puts)
       (reduce into #{})))

(s/defn blocked-put-map :- {ThreadId ChannelId}
  [this :- StepChannelMap]
  (->> this
       (mapcat (fn [[k v]]
                 (for [thread-id (:blocked-puts v)]
                   [thread-id k])))
       (mapcat identity)
       (apply hash-map)))

(s/defn blocked-takes :- #{ThreadId}
  [this :- StepChannelMap]
  (->> this
       vals
       (map :blocked-takes)
       (reduce into #{})))

(s/defn blocked-take-map :- (s/maybe {ThreadId #{ChannelId}})
  [this :- StepChannelMap]
  (let [ms (->> this
                (mapcat (fn [[k v]]
                          (for [thread-id (:blocked-takes v)]
                            {thread-id #{k}}))))]
    (apply merge-with into ms)))

(s/defn channel-waiter-map :- (s/maybe {ChannelId #{ThreadId}})
  [this :- StepChannelMap]
  (-> this
      (util/update-vals (comp util/no-empty :blocked-takes))
      util/no-nil))

(s/defn takes-available? :- (s/maybe Boolean)
  [this :- StepChannelMap]
  (->> this
       vals
       (map channel-takes-available?)
       (some true?)))

(s/defn takes-available :- #{ThreadId}
  [this :- StepChannelMap
   channel-id :- ChannelId]
  (channel-takes-available (get this channel-id)))

(s/defn puts-available? :- (s/maybe Boolean)
  [this :- StepChannelMap]
  (->> this
       vals
       (map channel-puts-available?)
       (some true?)))

(s/defn pending-puts-available :- PendingPutChoices
  [this :- StepChannelMap]
  (->> this
       (mapcat (fn [[k v]]
                 (when (channel-pending-puts-available? v)
                   (for [[thread-id pending-put] (:pending-puts v)]
                     [thread-id (assoc pending-put :channel-id k)]))))))

(s/defn pending-puts-available?
  [this :- StepChannelMap]
  (not (empty? (pending-puts-available this))))

(s/defn lookup-channel-for-put-thread :- ChannelId
  [this :- StepChannelMap
   thread-id :- ThreadId]
  (some (fn [[k v]]
          (when (contains? (channel-puts-available v) thread-id)
            k))
        this))

(s/defn takes-available-channels :- #{ChannelId}
  [this :- StepChannelMap]
  (->> this
       (map (fn [[k v]]
              (when (channel-takes-available? v)
                k)))
       (remove nil?)
       set))

(s/defn probe-channel-id :- (s/maybe ChannelId)
  [this :- StepChannelMap
   channel :- Channel]
  (->> this
       (some (fn [[k v]]
               (when (= channel (:channel v))
                 k)))))

(s/defn lookup-channel-id :- ChannelId
  [this :- StepChannelMap
   channel :- Channel]
  (probe-channel-id this channel))

(s/defn lookup-channel :- Channel
  [this :- StepChannelMap
   channel-id :- ChannelId]
  (get-in this [channel-id :channel]))

(s/defn clear-takers :- StepChannelMap
  [this :- StepChannelMap
   thread-id :- ThreadId]
  (reduce (fn [r [k v]]
            (if (contains? (:blocked-takes v) thread-id)
              (update-in r [k] channel-clear-taker thread-id)
              r))
          this this))

(s/defn register-channel :- StepChannelMap
  [this :- StepChannelMap
   channel-id :- ChannelId
   channel-name :- (s/maybe String)
   channel :- Channel
   buf-or-n :- (s/either s/Int {:buffer? (s/eq true)
                                :size s/Int
                                (s/optional-key :unblocking) UnblockingType})
   channel-history? :- Boolean
   take-f :- (s/maybe s/Fn)
   put-f :- (s/maybe s/Fn)]
  (let [[channel-size unblocking] (if (number? buf-or-n)
                                    [buf-or-n nil]
                                    [(:size buf-or-n)
                                     (:unblocking buf-or-n)])]
    (assoc this channel-id
           (channel-init channel-name channel channel-size unblocking channel-history?
                         take-f put-f))))

(s/defn take-value :- s/Any
  [this :- StepChannelMap
   channel-id :- ChannelId]
  (channel-take-value (get this channel-id)))

(s/defn register-take :- StepChannelMap
  [this :- StepChannelMap
   channel-id :- ChannelId
   thread-id :- ThreadId]
  (update-in this [channel-id] channel-register-take thread-id))

(s/defn after-closing :- Boolean
  [this :- StepChannelMap
   channel-id :- ChannelId]
  (channel-after-closing (get this channel-id)))

(s/defn register-put :- StepChannelMap
  [this :- StepChannelMap
   channel-id :- ChannelId
   thread-id :- ThreadId
   value :- s/Any
   block? :- Boolean]
  (update-in this [channel-id] channel-register-put thread-id value block?))

(s/defn clear-take :- StepChannelMap
  [this :- StepChannelMap
   channel-id :- ChannelId]
  (update-in this [channel-id] channel-clear-take))

(s/defn accept-put :- StepChannelMap
  [this :- StepChannelMap
   channel-id :- ChannelId
   thread-id :- ThreadId]
  (update-in this [channel-id] channel-accept-put thread-id))

(s/defn clear-put :- StepChannelMap
  [this :- StepChannelMap
   channel-id :- ChannelId
   thread-id :- ThreadId]
  (update-in this [channel-id] channel-clear-put thread-id))

(s/defn space-available? :- Boolean
  [this :- StepChannelMap
   channel-id :- ChannelId]
  (channel-space-available? (get this channel-id)))

(s/defn pending-puts-available-for-channel-id? :- Boolean
  [this :- StepChannelMap
   channel-id :- ChannelId]
  (channel-pending-puts-available? (get this channel-id)))

(s/defn channel-contents :- [s/Any]
  [this :- StepChannelMap
   channel :- Channel]
  (get-in this [(lookup-channel-id this channel) :channel-contents]))

(s/defn channel-ids :- #{ChannelId}
  [this :- StepChannelMap]
  (-> this
      keys
      set))

(s/defn channel-names :- {ChannelId String}
  [this :- StepChannelMap]
  (-> this
      (util/update-map (fn [k v]
                         [k (str (or (:channel-name v)
                                     "channel")
                                 "-" k)]))))

(s/defn channel-contents-map :- (s/maybe {String [s/Any]})
  [this :- StepChannelMap
   channel-names :- {ChannelId String}]
  (-> this
      (util/update-keys channel-names)
      (util/update-vals #(:channel-contents %))
      util/no-nil
      util/no-empty
      (dissoc nil)))

(s/defn channel-history :- {String [s/Any]}
  [this :- StepChannelMap
   channel-names :- {ChannelId String}]
  (-> this
      (util/update-keys channel-names)
      (util/update-vals #(:channel-history %))))

(comment
  (let [c (-> (init-step-channel "my-chan" (clojure.core.async/chan 10) 10 true)
              (register-put 1001 :a true)
              (register-put 1003 :b true)
              (register-take 1002)
              (accept-put 1001)
              (accept-put 1003)
              (clear-put 1001))]
    (clear-take c 1002))
  )
