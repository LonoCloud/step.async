(ns lonocloud.step.async.step-model
  "Schema definitions used in step implementation"
  (:require [clojure.core.async.impl.protocols :as async-protocols]
            [lonocloud.step.async.pschema :as s]))

(def root-thread-id 0)

(def first-port-thread-id 10000)

(def first-channel-id 20000)

(def first-timeout-id 30000)

;; schema

(deftype ChannelType []
  s/Schema
  (check [this x]
    (when (not (satisfies? async-protocols/Channel x))
      [x 'not "satisfies async Channel protocol"]))

  (explain [this]
    "satisfies async Channel protocol"))

(def Channel (ChannelType.))

(deftype ReadPortType []
  s/Schema
  (check [this x]
    (when (not (satisfies? async-protocols/ReadPort x))
      [x 'not "satisfies async ReadPort protocol"]))

  (explain [this]
    "satisfies async ReadPort protocol"))

(def ReadPort (ReadPortType.))

(deftype WritePortType []
  s/Schema
  (check [this x]
    (when (not (satisfies? async-protocols/WritePort x))
      [x 'not "satisfies async WritePort protocol"]))

  (explain [this]
    "satisfies async WritePort protocol"))

(def WritePort (WritePortType.))

(deftype ThreadIdType []
  s/Schema
  (check [this x]
    (when (not (and (integer? x)
                    (< x first-channel-id)))
      [x 'not "go thread-id"]))

  (explain [this]
    "go thread-id"))

(def ThreadId (ThreadIdType.))

(deftype ChannelIdType []
  s/Schema
  (check [this x]
    (when (not (and (integer? x)
                    (>= x first-channel-id)))
      [x 'not "channel-id"]))

  (explain [this]
    "channel-id"))

(def ChannelId (ChannelIdType.))

(defn timeout-id? [x]
  (and (integer? x)
       (>= x first-timeout-id)))

(defn port-thread-id? [x]
  (and (integer? x)
       (>= x first-port-thread-id)
       (< x first-channel-id)))

(deftype TimeoutIdType []
  s/Schema
  (check [this x]
    (when (not (timeout-id? x))
      [x 'not "timeout-id"]))

  (explain [this]
    "timeout-id"))

(def TimeoutId (TimeoutIdType.))

(def PendingThread [(s/one ThreadId "thread-id")
                    (s/one s/Fn "function")
                    (s/one Channel "channel")])

(def PendingPutChoices [[(s/one ThreadId "thread-id")
                         (s/one {:channel-id ChannelId
                                 :value s/Any
                                 :block? Boolean} "pending-put")]])
