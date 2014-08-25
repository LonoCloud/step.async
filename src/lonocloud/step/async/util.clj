(ns lonocloud.step.async.util
  "General utility functions."
  (:require [clojure.algo.generic.functor :refer [fmap]]
            [lonocloud.step.async.pschema :as s]))

(defn update-keys
  "Applies f to the keys of the map. The key is the first arg, optionally followed by remaining args."
  [m f & args]
  (when m
    (zipmap (map #(apply f (into [%] args)) (keys m))
            (vals m))))

(defn update-vals
  "Applies f to the values of the map. The value is the first arg, optionally followed by remaining
  args."
  [m f & args]
  (when m
    (fmap #(apply f (into [%] args)) m)))

(defn update-map
  "Replace all of the map entries with the resulting of applying f to each key-value pair."
  [m f]
  (into {} (for [[k v] m] (f k v))))

(defn remove-vals-from-map
  "Remove entries from the map if applying f to the value produced false."
  [m f]
  (->> m
       (remove (comp f second))
       (mapcat identity)
       (apply hash-map)))

(defn no-nil
  "Remove all nil values from the map"
  [m]
  (remove-vals-from-map m nil?))

(defn no-empty
  "Convert empty collection to nil"
  [coll]
  (when-not (empty? coll)
    coll))

(defn all-optional-keys
  "Provided a Prismatic schema describing a map, produces a Prismatic schema describing the same map
  except all of the keys will be flagged as optional."
  [map-schema]
  (letfn [(make-optional [k] (if (s/optional-key? k)
                               k
                               (s/optional-key k)))]
    (update-keys map-schema make-optional)))
