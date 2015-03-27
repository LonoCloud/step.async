(defproject lonocloud/step.async "0.0.2"
  :description "Library for running core.async code in a deterministic, transparent fashion"
  :url "http://github.com/lonocloud/step.async/"
  :license {:name "Eclipse Public License - v 1.0"
            :url "http://www.eclipse.org/legal/epl-v10.html"
            :distribution :repo
            :comments "same as Clojure"}
  :min-lein-version "2.0.0"
  :dependencies [[org.clojure/clojure "1.6.0"]
                 [org.clojure/core.async "0.1.267.0-0d7780-alpha"]
                 [org.clojure/tools.logging "0.2.6"]
                 [org.clojure/algo.generic "0.1.1"]
                 [cc.artifice/vijual "0.2.5"]
                 [prismatic/schema "0.4.0"]])
