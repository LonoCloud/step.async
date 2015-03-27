(ns lonocloud.step.async.pschema
  (:refer-clojure :exclude [defn defn- fn])
  (:require [clojure.pprint :as pprint :refer [pprint]]
            [schema.core :as s]
            [schema.utils :as schema-utils])
  (:import [schema.core OptionalKey]))

(def eq s/eq)

(def enum s/enum)

(def Int s/Int)

(ns-unmap *ns* 'Number)

(def Num s/Num)

(def maybe s/maybe)

(def Any s/Any)

(def Map {s/Any s/Any})

(def Keyword s/Keyword)

(def Fn (s/pred fn?))

(def Ref clojure.lang.Ref)

(def Atom clojure.lang.Atom)

(def either s/either)

(def Schema s/Schema)

(def one s/one)

(def required-key s/required-key)

(def optional-key s/optional-key)

(def optional-key? s/optional-key?)

(def Inst s/Inst)

(def check s/check)

(def explain s/explain)

(def validate s/validate)

(def optional s/optional)

;;

(def error
  "helper when writing custom type checkers"
  schema-utils/error)

(defmacro satisfies-protocol [t p]
  `(deftype ~t []
     s/Schema
     (walker [this#]
       (clojure.core/fn [x#]
         (if (satisfies? ~p x#)
           x#
           (schema-utils/error [x# 'not (str (:on ~p))]))))

     (explain [this#]
       (str (:on ~p)))))

(clojure.core/defn analyze-defn-params
  "Produce [type comment params body]"
  [[a1 a2 a3 a4 :as params]]
  (if (= :- a1)
    (if (string? a3)
      (let [[_ _ _ _ & body] params]
        [a2 a3 a4 body])
      (let [[_ _ _ & body] params]
        [a2 nil a3 body]))
    (if (string? a1)
      (let [[_ _ & body] params]
        [nil a1 a2 body])
      (let [[_ & body] params]
        [nil nil a1 body]))))

(defmacro defn [name & more]
  (let [[type comment-str params body] (analyze-defn-params more)]
    (if type
      (if comment-str
        `(s/defn ~(with-meta name (assoc (meta name) :always-validate true)) :- ~type
           ~comment-str
           ~params
           ~@body)
        `(s/defn ~(with-meta name (assoc (meta name) :always-validate true)) :- ~type
           ~params
           ~@body))
      (if comment-str
        `(s/defn ~(with-meta name (assoc (meta name) :always-validate true))
           ~comment-str
           ~params
           ~@body)
        `(s/defn ~(with-meta name (assoc (meta name) :always-validate true))
           ~params
           ~@body)))))

(defmacro defn- [name & more]
  (let [[type comment-str params body] (analyze-defn-params more)]
    (if type
      (if comment-str
        `(s/defn ~(with-meta name (assoc (meta name) :private true :always-validate true)) :- ~type
           ~comment-str
           ~params
           ~@body)
        `(s/defn ~(with-meta name (assoc (meta name) :private true :always-validate true)) :- ~type
           ~params
           ~@body))
      (if comment-str
        `(s/defn ~(with-meta name (assoc (meta name) :private true :always-validate true))
           ~comment-str
           ~params
           ~@body)
        `(s/defn ~(with-meta name (assoc (meta name) :private true :always-validate true))
           ~params
           ~@body)))))

(defmulti to-key class)

(defmethod to-key clojure.lang.Keyword ;; NOTE: Keyword is shadowed above, so must fully qualify here
  [k]
  k)

(defmethod to-key OptionalKey
  [k]
  (:k k))

(defn get-type-keys [m]
  (map to-key (keys m)))

(comment
  (pprint (defnx foo :- String
            "this is "
            [a :- String]
            (+ 1 2)
            {:a 10}))

  (pprint (defnx foo :- String
            [a :- String]
            (+ 1 2)
            {:a 10}))

  (pprint (defnx foo
            "this is "
            [a :- String]
            (+ 1 2)
            {:a 10}))

  (pprint (defnx foo
            [a :- String]
            (+ 1 2)
            {:a 10}))

  (pprint (defnx foo
            [a]
            (+ 1 2)
            {:a 10}))
  )

;; typed functions

(defn simplify-schema
  "Convert a function schema to a simpler representation containing only what must be checked"
  [schema]
  (let [{:keys [input-schemas output-schema]} schema]
    {:output-schema (when output-schema (s/explain output-schema))
     :input-schemas (vec (map (comp vec s/explain-input-schema) input-schemas))}))

(defn- get-schema-of
  "Lookup the schema type of a function."
  [f]
  (simplify-schema (schema-utils/class-schema (class f))))

(defn- vararg-type
  "Check a single input schema and if it contains a varargs, return the element type of the vararg"
  [schema]
  (let [n (count schema)
        start-n (max (- n 2) 0)
        [l1 l2] (drop start-n schema)]
    (when (= '& l1)
      (first l2))))

(defn- expand-var-arg-to
  "'Unroll' part of a vararg (if present) in the source to make the source input schema the same
  length as the target schema. Note: the schemas in this case are individual arities."
  [source target]
  (let [target-count (count target)
        n (count source)
        start-n (max (- n 2) 0)
        start (take start-n source)]
    (if-let [source-vararg-type (vararg-type source)]
      (if-let [target-vararg-type (vararg-type target)]
        (if (and (= source-vararg-type target-vararg-type)
                 (< n target-count))
          (reduce into (vec start)
                  [(repeat (max (- target-count start-n 2) 0) source-vararg-type)
                   ['& [source-vararg-type]]])
          source)
        (into (vec start) (repeat (max (- target-count start-n) 0) source-vararg-type)))
      source)))

(defn- arg-matches [schema-field target-field]
  (or (= schema-field target-field)
      (if (or (= schema-field '&)
              (= target-field '&))
        false
        (and (= target-field {'Any 'Any})
             (map? schema-field)))))

(defn- matches? [schema target]
  (and (= (count schema) (count target))
       (->> (map arg-matches schema target)
            (every? identity))))

(defn- is-matching-schema?
  "Determine if there are any schemas in the choices that matches the target schema."
  [choices target]
  (->> choices
       (map #(expand-var-arg-to % target))
       (some #(matches? % target))))

(defn is-schema-a?
  "Determine if the child function schema is a sub-type of the parent function schema."
  [child parent]
  (let [parent-out (:output-schema parent)
        parent-ins (:input-schemas parent)
        child-out (:output-schema child)
        child-ins (:input-schemas child)]
    (or (and (nil? child-out)
             (empty? child-ins))
        (and (= child-out
                parent-out)
             (->> (map #(is-matching-schema? child-ins %) parent-ins)
                  (every? identity))))))

(defprotocol TypedFunctionProtocol
  (get-schema [this])
  (print-typed-function [this]))

(deftype TypedFunctionType [schema]
  TypedFunctionProtocol
  (get-schema [this]
    schema)

  (print-typed-function [this]
    schema)
  s/Schema
  (walker [this]
    (clojure.core/fn [x]
      (if (is-schema-a? (if (= identity x)
                          (let [out-schema (:output-schema schema)]
                            {:output-schema out-schema
                             :input-schemas [[out-schema]]})
                          (get-schema-of x))
                        schema)
        x
        (schema-utils/error [x 'not (str "typed function " schema)]))))

  (explain [this]
    schema))

(defmethod print-method TypedFunctionType [x ^java.io.Writer writer]
  (print-method (print-typed-function x) writer))

(defmethod pprint/simple-dispatch TypedFunctionType [x]
  (pprint/pprint (print-typed-function x)))

(defmacro fn [& args]
  `(TypedFunctionType. (simplify-schema (s/=>* ~@args))))
