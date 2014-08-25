(ns lonocloud.step.async.pschema
  (:require [clojure.pprint :refer [pprint]]
            [schema.core :as s]))

(def eq s/eq)

(def enum s/enum)

(def Int s/Int)

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

(defn analyze-defn-params
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
