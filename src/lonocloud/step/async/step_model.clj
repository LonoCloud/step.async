(ns lonocloud.step.async.step-model
  "Schema definitions used in step implementation"
  (:require [clojure.core.async.impl.protocols :as async-protocols]
            [lonocloud.step.async.pschema :as s]))

(def root-thread-id 0)

(def first-port-thread-id 10000)

(def first-channel-id 20000)

(def first-timeout-id 30000)

;; schema

(s/satisfies-protocol ChannelType async-protocols/Channel)

(def Channel (ChannelType.))

(s/satisfies-protocol ReadPortType async-protocols/ReadPort)

(def ReadPort (ReadPortType.))

(s/satisfies-protocol WritePortType async-protocols/WritePort)

(def WritePort (WritePortType.))

(deftype ThreadIdType []
  s/Schema
  (walker [this]
    (fn [x]
      (if (and (integer? x)
               (< x first-channel-id))
        x
        (s/error x))))

  (explain [this]
    "go thread-id"))

(def ThreadId (ThreadIdType.))

(deftype ChannelIdType []
  s/Schema
  (walker [this]
    (fn [x]
      (if (and (integer? x)
               (>= x first-channel-id))
        x
        (s/error x))))

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
  (walker [this]
    (fn [x]
      (if (timeout-id? x)
        x
        (s/error x))))

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
