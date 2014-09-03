(ns lonocloud.step.test-step
  (:require [clojure.core.async :as async]
            [clojure.string :as str]
            [clojure.test :refer :all]
            [lonocloud.step.async :as step :refer [>! <! chan go step-machine step-wait step step-all
                                                   quiesce-wait get-result chan-named go-named
                                                   timeout-named state-trace complete-timeout
                                                   dump-state-trace go-loop-named external-channel
                                                   step-back get-args dump-channel-history
                                                   dump-detailed-action-history dump-thread-names
                                                   dump-named-blockers get-channel-contents timeout
                                                   get-timeouts close! go-loop step-inputs
                                                   replay-history get-history dump-state buffer
                                                   sliding-buffer dropping-buffer >!! <!! put! take!
                                                   set-breakpoint clear-breakpoint
                                                   clear-all-breakpoints alts! print-step-machine]]
            [lonocloud.step.async.util :as util]
            [vijual :as v]))

;;;; demo script

;; place async code in functions
(defn foo []
  (let [c (chan)]
    (go
     (>! c 100))
    (go
     (<! c))))

(deftest demo-10
  ;; invoke the function the "normal" way and the async step machine is not used
  (is (= 100
         (async/<!! (foo)))))

(deftest demo-20
  ;; alternatively invoke the function via a step-machine, this produces a machine that can be stepped
  ;; through
  (let [machine ((step-machine) foo)]
    machine))

(deftest demo-30
  ;; step through the machine, and print the machine state after each step
  (let [machine ((step-machine) foo)]
    [(step-wait machine)
     ;;(clojure.pprint/pprint machine)

     ;; stepping perform a single "action" (e.g. putting/taking a value from a channel) in the machine
     ;; then stops
     (step machine)
     ;; stepping is asynchronous from this thread, so wait for the step to complete
     (step-wait machine)
     ;;(clojure.pprint/pprint machine)

     (step machine)
     (step-wait machine)
     ;;(clojure.pprint/pprint machine)

     (step machine)
     (step-wait machine)
     ;;(clojure.pprint/pprint machine)

     (step machine)
     (step-wait machine)
     ;;(clojure.pprint/pprint machine)

     (step machine)
     (step-wait machine)
     ;;(clojure.pprint/pprint machine)

     (step machine)
     (step-wait machine)
     ;;(clojure.pprint/pprint machine)

     (step machine)
     (step-wait machine)
     ;;(clojure.pprint/pprint machine)
     ]

    ;; waiting returns an indication of why the machine is stopped, in this case it is because all
    ;; of the go threads exited
    (is (= :all-exited (step-wait machine)))))

(deftest demo-40
  (let [machine ((step-machine) foo)]
    ;; use step all to repeatedly step the machine until no further steps are possible (without more
    ;; input)
    (step-all machine)
    ;; wait for the machine to reach the state in which no more steps are possible
    (quiesce-wait machine)
    ;; retrieve the result value from the function, in this case the result channel from the final go
    ;; block
    (is (= 100 (async/<!! (get-result machine))))))

;; previous example used just core.async constructs in the async function, the machines state is
;; easier to understand if the threads and channels are named, use <x>-named to name the parts
(defn foo-named []
  (let [c (chan-named "my-chan")]
    (go-named "writer"
              (>! c 100))
    (go-named "timer"
              (<! (timeout-named "my-timeout"
                                 20)))
    (go-named "reader"
              (<! c))))

(deftest demo-50
  ;; the function can still be run in the "normal" fashion
  (is (= 100 (async/<!! (foo-named)))))

(deftest demo-60
  ;; but now the state output uses the names we provided
  (let [machine ((step-machine) foo-named)]
    ;; turn on state tracing so the step machine will automatically record each state it goes through
    (state-trace machine true)

    (step-all machine)
    (quiesce-wait machine)

    ;; manually fire the timeout
    (complete-timeout machine 30001)
    (step-all machine)
    (quiesce-wait machine)

    ;; turn off tracing
    (state-trace machine false)

    ;; view the trace
    (dump-state-trace machine)))

;; create an async function that loops forever
(defn doubler [in out]
  (go-loop-named "doubler-main" [v (<! in)]
                 (>! out (* 2 v))
                 (recur (<! in))))

;; compose the doubler function in another async function
(defn blah []
  (let [c1 (chan-named "in")
        c2 (chan-named "out")]
    ;; wire the doubler into this functions channels
    (doubler c1 c2)
    (go-named "writer"
              (>! c1 100))
    (go-named "reader"
              (<! c2))))

(deftest demo-70
  ;; collect a trace of the composite machine execution
  (let [machine ((step-machine) blah)]
    (state-trace machine true)

    (step-all machine)
    (quiesce-wait machine)

    (state-trace machine false)
    (dump-state-trace machine)))

(comment
  (defn find-edges-from-trace-frame [f]
    (reduce into #{} [(map (fn [k] [k k]) (:channels f))
                      (map (fn [k] [k k]) (into (:threads f) (:pending-threads f)))
                      (let [pp (:pending-puts f)]
                        (map (fn [k]
                               [k (first (pp k))]) (keys pp)))
                      (let [bt (:blocked-takes f)]
                        (mapcat (fn [k]
                                  (map (fn [x] [x k]) (bt k))) (keys bt)))
                      (let [bp (:blocked-puts f)]
                        (map (fn [k]
                               [k (bp k)]) (keys bp)))]))

  (defn trim-channel-names [s]
    (if (and (string? s)
             (not (= "thread-0" s)))
      (.replaceAll s "-[0-9]+" "")
      s))

  (defn node-labels [f]
    (reduce merge {} (reduce into [] [(let [pp (:pending-puts f)]
                                        (map (fn [k]
                                               {k (str " >" (trim-channel-names (first (pp k))))})
                                             (keys pp)))
                                      (let [bt (:blocked-takes f)]
                                        (map (fn [k]
                                               {k (str " <" (str/join
                                                             ","
                                                             (map trim-channel-names (bt k))))})
                                             (keys bt)))
                                      (let [bp (:blocked-puts f)]
                                        (map (fn [k]
                                               {k (str " >" (trim-channel-names (bp k)))}) (keys bp)))
                                      (let [ch (:channel-history f)
                                            cc (:channel-contents f)]
                                        (map (fn [k]
                                               {k (str (ch k) " " (cc k))}) (keys cc)))])))

  (defn render [i frames]
    (let [frames (take i frames)]
      (let [new-edges (reduce #(into %1 (find-edges-from-trace-frame %2)) #{} frames)
            nodes (set (mapcat identity new-edges))
            node-map (->> nodes
                          (map (fn [k]
                                 {k (keyword (gensym))}))
                          (reduce merge {}))
            labels (node-labels (last frames))
            node-map-2 (util/update-keys node-map #(str (trim-channel-names %) (get labels %)))]
        [new-edges node-map node-map-2])))

  (defn render-img [i frames]
    (let [[new-edges node-map node-map-2] (render i frames)]
      (v/draw-directed-graph-image (map #(map node-map %) new-edges)
                                   (->> node-map-2
                                        (map (fn [[k v]]
                                               {v k}))
                                        (reduce merge {})))))

  (defn show-trace [frames]
    (let [frames (vec frames)
          imgs (->> (range 1 (inc (count frames)))
                    (map #(render-img % frames))
                    vec)]
      (let [f (javax.swing.JFrame.)
            bb (javax.swing.JButton. "<")
            bf (javax.swing.JButton. ">")
            index (atom 0)
            label (javax.swing.JLabel. (javax.swing.ImageIcon. (imgs @index)))
            counter-label (javax.swing.JLabel.)
            action-label (javax.swing.JLabel.)
            navigate (fn [forward?]
                       (do (when (and (or (> @index 0) forward?)
                                      (or (< @index (dec (count imgs))) (not forward?)))
                             (swap! index (if forward? inc dec)))
                           (.setIcon label (javax.swing.ImageIcon. (imgs @index)))
                           (.setText counter-label (str @index " / " (dec (count imgs))))
                           (.setText action-label (str (->> (frames @index)
                                                            :last-action
                                                            (map trim-channel-names)
                                                            vec)))
                           (.pack f)))
            key-listener (proxy [java.awt.event.KeyListener] []
                           (keyReleased [e]
                             (let [keycode (.getKeyCode e)]
                               (cond
                                (= keycode java.awt.event.KeyEvent/VK_N) (navigate true)
                                (= keycode java.awt.event.KeyEvent/VK_P) (navigate false)))))]
        (.setText counter-label (str @index " / " (dec (count imgs))))
        (.addActionListener bf
                            (proxy [java.awt.event.ActionListener] []
                              (actionPerformed [e]
                                (navigate true))))
        (.addKeyListener bf key-listener)
        (.addActionListener bb
                            (proxy [java.awt.event.ActionListener] []
                              (actionPerformed [e]
                                (navigate false))))
        (.addKeyListener bb key-listener)

        (.setLayout (.getContentPane f) (java.awt.FlowLayout.))
        (.add (.getContentPane f) bb)
        (.add (.getContentPane f) label)
        (.add (.getContentPane f) bf)
        (.add (.getContentPane f) counter-label)
        (.add (.getContentPane f) action-label)
        (.pack f)
        (.setVisible f true))))

  ;; in vijual: set image-wrap-threshold to a much larger value, also add a (long...) around the args to the failing abs call on line 901

  ;; increase image-wrap-threshold

  (show-trace t0)

  (def i0 (v/draw-directed-graph-image [[:G__355862 :G__355861]
                                        [:G__355861 :G__355861]
                                        [:G__355859 :G__355857]
                                        [:G__355859 :G__355859]
                                        [:G__355857 :G__355857]
                                        [:G__355862 :G__355862]
                                        [:G__355858 :G__355858]
                                        [:G__355857 :G__355858]
                                        [:G__355861 :G__355859]
                                        [:G__355860 :G__355860]]
                                       {:G__355862 "writer",
                                        :G__355861 "in-20001",
                                        :G__355860 "thread-0    ddd   efg h i jklke mnopq"
                                        :G__355859 "doubler-main in-2001"
                                        :G__355858 "reader",
                                        :G__355857 "out-20002"})))

;; create an async function that maintains state
(defn accumulator [in out]
  (go-named "accumulator"
            (loop [x 0]
              (let [v (<! in)
                    sum (+ x v)]
                (>! out sum)
                (recur sum)))))

(deftest demo-80
  ;; invoke this via plain old core.async
  (let [in (async/chan)
        out (async/chan)]
    (accumulator in out)

    (is (= "1
3
6
"
           (with-out-str
             (async/>!! in 1)
             (println (async/<!! out))

             (async/>!! in 2)
             (println (async/<!! out))

             (async/>!! in 3)
             (println (async/<!! out)))))))

(deftest demo-90
  ;; invoke the same function as a step machine
  (let [in (async/chan 10)
        out (async/chan 10)
        machine ((step-machine :channel-sizes [10 10]) accumulator in out)
        in-port (external-channel machine in)
        out-port (external-channel machine out)]
    (is (= "1
3
6
"
           (with-out-str
             (async/>!! in-port 1)
             (step-all machine)
             (quiesce-wait machine)
             (println (async/<!! out-port))

             (async/>!! in-port 2)
             (step-all machine)
             (quiesce-wait machine)
             (println (async/<!! out-port))

             (async/>!! in-port 3)
             (step-all machine)
             (quiesce-wait machine)
             (println (async/<!! out-port)))))))

(deftest demo-100
  ;; run the accumulator again, step-by-step
  (let [in (async/chan 10)
        out (async/chan 10)
        ;; configure the step-machine to record a history of all external actions
        machine ((step-machine :action-history? true
                               :channel-sizes [10 10]) accumulator in out)
        in-port (external-channel machine in)
        out-port (external-channel machine out)]
    (is (= "1
3
6
7
"
           (with-out-str
             (async/>!! in-port 1)
             (step-all machine)
             (quiesce-wait machine)
             (println (async/<!! out-port))

             (async/>!! in-port 2)
             (step-all machine)
             (quiesce-wait machine)
             (println (async/<!! out-port))

             (async/>!! in-port 3)
             (step-all machine)
             (quiesce-wait machine)
             (println (async/<!! out-port))

             ;; step backwards two steps to reverse the last <!! and >!!
             (let [machine (step-back machine 2)
                   ;; currently this returns a new machine, so it is necessary to "rebind" to the channels
                   [in out] (get-args machine)
                   in-port (external-channel machine in)
                   out-port (external-channel machine out)]

               ;; wait for the step back to complete
               (quiesce-wait machine)

               ;; proceed forward with an alternate input value
               (async/>!! in-port 4)
               (step-all machine)
               (quiesce-wait machine)
               (println (async/<!! out-port))
               (dump-channel-history machine)))))))

(deftest demo-110
  ;; do that again, this time with channel-history? turned on
  (let [in (async/chan 10)
        out (async/chan 10)
        machine ((step-machine :action-history? true
                               :detailed-action-history? true
                               :channel-history? true
                               :channel-sizes [10 10]
                               :channel-names ["in" "out"]) accumulator in out)
        in-port (external-channel machine in)
        out-port (external-channel machine out)]
    (async/>!! in-port 1)
    (step-all machine)
    (quiesce-wait machine)
    (async/<!! out-port)

    (async/>!! in-port 2)
    (step-all machine)
    (quiesce-wait machine)
    (async/<!! out-port)

    (async/>!! in-port 3)
    (step-all machine)
    (quiesce-wait machine)
    (async/<!! out-port)

    (let [machine (step-back machine 2)
          [in out] (get-args machine)
          in-port (external-channel machine in)
          out-port (external-channel machine out)]

      (quiesce-wait machine)

      (async/>!! in-port 4)
      (step-all machine)
      (quiesce-wait machine)
      (async/<!! out-port)

      (is (= [[:run-pending "accumulator"]
              [:run-parked "thread-0"]
              [:put "thread-10001" "in-20001" 1]
              [:take "accumulator" "in-20001" 1]
              [:put "accumulator" "out-20002" 1]
              [:un-park-put "accumulator"]
              [:put "thread-10001" "in-20001" 2]
              [:take "accumulator" "in-20001" 2]
              [:put "accumulator" "out-20002" 3]
              [:un-park-put "accumulator"]
              [:put "thread-10005" "in-20001" 4]
              [:un-park-put "thread-10005"]
              [:take "accumulator" "in-20001" 4]
              [:put "accumulator" "out-20002" 7]
              [:un-park-put "accumulator"]]
             (dump-detailed-action-history machine)))
      (is (= {"in-20001" [1 2 4], "out-20002" [1 3 7]} (dump-channel-history machine))))))

;; create a function which returns a channel
(defn doubler2 [in]
  (let [out (chan-named "doubler-out")]
    (go-loop-named "doubler-main" [v (<! in)]
                   (>! out (* 2 v))
                   (recur (<! in)))
    out))

;; call the function from another
(defn blah2 []
  (let [c1 (chan-named "in")
        c2 (doubler2 c1)]
    (go-named "writer"
              (>! c1 100))
    (go-named "reader"
              (<! c2))))

(deftest demo-120
  ;; run it with plain core.async
  (is (= 200
         (async/<!! (blah2)))))

(deftest demo-130
  ;; run it through the step machine
  (let [machine ((step-machine :channel-history? true) blah2)]
    (state-trace machine true)
    (step-all machine)
    (quiesce-wait machine)
    (state-trace machine false)
    (dump-state-trace machine)
    (dump-channel-history machine)
    (is (= 200 (async/<!! (get-result machine))))))

;;;; end of demo script

(defn chan-builder []
  (chan-named "channel-2" 10))

(deftest test-simple
  (let [async-f (fn []
                  (go 1))
        machine ((step-machine) async-f)]
    (step-all machine)
    (is (= :all-exited
           (quiesce-wait machine)))))

(deftest test-simple-named
  (let [async-f (fn []
                  (go-named "foo"
                            1))
        machine ((step-machine) async-f)]
    (step-all machine)
    (is (= :all-exited
           (quiesce-wait machine)))
    (is (= {0 "thread-0"
            1 "foo"}
           (dump-thread-names machine)))))

(deftest test-simple-result
  (let [async-f (fn []
                  (go 1))
        machine ((step-machine) async-f)]
    (step-all machine)
    (quiesce-wait machine)
    (is (= 1
           (first (async/alts!! [(get-result machine)
                                 (async/timeout 1000)]))))))

(deftest test-basic
  (let [async-f (fn []
                  (let [c (chan 10)
                        c2 (chan-builder)
                        reader (go
                                (<! c))
                        writer (do
                                 (go (>! c 100))
                                 (go (>! c2 200)))
                        reader2 (go
                                 (<! c2))]
                    reader))
        machine ((step-machine) async-f)]
    (step-all machine)
    (is (= :all-exited
           (quiesce-wait machine))))

  (let [async-f (fn []
                  (let [c (chan 10)
                        c2 (chan-builder)
                        reader (go
                                (<! c))
                        writer (go (>! c 100))
                        reader2 (go
                                 (<! c2))]
                    reader))
        machine ((step-machine) async-f)]
    (step-all machine)
    (is (= :all-blocked
           (quiesce-wait machine)))))

(deftest test-basic-named
  (let [async-f (fn []
                  (let [c (chan-named "channel-1" 10)
                        c2 (chan-builder)
                        reader (go-named "reader"
                                         (<! c))]
                    (go-named "writer"
                              (>! c 100))
                    (go-named "reader-2"
                              (<! c2))
                    reader))
        machine ((step-machine) async-f)]
    (step-all machine)
    (is (= :all-blocked
           (quiesce-wait machine)))
    (is (= {"reader-2" #{"channel-2-20002"}}
           (dump-named-blockers machine)))))

(deftest test-external-input-0
  (is (= :all-blocked
         (let [async-f (fn [c]
                         (go
                          (<! c)))
               machine ((step-machine) async-f (async/chan))]
           (step-all machine)
           (quiesce-wait machine)))))

(deftest test-external-input
  (let [async-f (fn [c]
                  (let [c2 (chan-builder)
                        reader (go
                                (<! c))
                        writer (go (>! c2 200))
                        reader2 (go
                                 (<! c2))]
                    reader))
        c-in (async/chan)
        machine ((step-machine) async-f c-in)
        in (external-channel machine c-in)]
    (step-all machine)
    (is (= :all-blocked
           (quiesce-wait machine)))
    (is (= :all-exited
           (do (async/>!! in 100)
               (step-all machine)
               (quiesce-wait machine))))))

(deftest test-external-input-via-go
  (let [async-f (fn [c]
                  (let [c2 (chan-builder)
                        reader (go
                                (<! c))
                        writer (go (>! c2 200))
                        reader2 (go
                                 (<! c2))]
                    reader))
        c-in (async/chan)
        machine ((step-machine) async-f c-in)
        in (external-channel machine c-in)]
    (step-all machine)
    (is (= :all-blocked
           (quiesce-wait machine)))
    (is (= :all-exited
           (do (async/<!! (async/go
                           (async/>! in 100)))
               (step-all machine)
               (quiesce-wait machine))))))

(deftest test-external-output
  (let [async-f (fn [in-c out-c]
                  (go (loop [x (<! in-c)]
                        (when x
                          (>! out-c (* 2 x))
                          (recur (<! in-c))))))]

    ;; run as "plain old core async"
    (let [in-c (async/chan)
          out-c (async/chan)]
      (async-f in-c out-c)
      (async/put! in-c 4)
      (async/put! in-c 7)
      (is (= [8 14]
             [(async/<!! out-c)
              (async/<!! out-c)])))

    ;; use in a step-wise fashion
    (let [in-c (async/chan 10)
          out-c (async/chan 10)
          machine ((step-machine :channel-sizes [10 10]) async-f in-c out-c)
          in (external-channel machine in-c)
          out (external-channel machine out-c)]
      (step-all machine)
      (is (= :all-blocked
             (quiesce-wait machine)))
      (async/>!! in 5)
      (step-all machine)
      (is (= :all-blocked
             (quiesce-wait machine)))
      (is (= 10 (async/<!! out)))
      (async/>!! in 6)
      (step-all machine)
      (is (= :all-blocked
             (quiesce-wait machine)))
      (is (= 12 (async/<!! out))))))

(deftest test-external-output-via-go
  (let [async-f (fn [in-c out-c]
                  (go (loop [x (<! in-c)]
                        (when x
                          (>! out-c (* 2 x))
                          (recur (<! in-c))))))]
    (let [in-c (async/chan 10)
          out-c (async/chan 10)
          machine ((step-machine :channel-sizes [10 10]) async-f in-c out-c)
          in (external-channel machine in-c)
          out (external-channel machine out-c)]
      (step-all machine)
      (is (= :all-blocked
             (quiesce-wait machine)))
      (async/>!! in 5)
      (step-all machine)
      (is (= :all-blocked
             (quiesce-wait machine)))
      (is (= 10 (async/<!! (async/go (async/<! out)))))
      (async/>!! in 6)
      (step-all machine)
      (is (= :all-blocked
             (quiesce-wait machine)))
      (is (= 12 (async/<!! (async/go (async/<! out))))))))

(deftest test-external-output-via-get-channel-contents
  (let [async-f (fn [in-c out-c]
                  (go (loop [x (<! in-c)]
                        (when x
                          (>! out-c (* 2 x))
                          (recur (<! in-c))))))]

    ;; use in a step-wise fashion
    (let [in-c (async/chan 10)
          out-c (async/chan 10)
          machine ((step-machine :channel-sizes [10 10]) async-f in-c out-c)
          in (external-channel machine in-c)]
      (async/put! in 4)
      (async/put! in 5)
      (async/put! in 6)

      (step-all machine)
      (quiesce-wait machine)

      (is (= [8 10 12]
             (get-channel-contents machine out-c))))))

(deftest test-deterministic-go-blocks
  (let [async-f (fn [out-c]
                  (let [c (chan 10)]
                    (go (>! out-c (<! c))
                        (>! out-c (<! c)))
                    (go (Thread/sleep 100)
                        (>! c 100))
                    (go (>! c 200))))]
    ;; run as plain core async
    (let [out-c (async/chan 1)]
      (async-f out-c)
      (is (= [200 100]
             [(async/<!! out-c)
              (async/<!! out-c)])))

    ;; run as step-machine
    (let [out-c (async/chan 1)
          machine ((step-machine :channel-history? true
                                 :channel-sizes [1]) async-f out-c)
          out (external-channel machine out-c)]
      (step-all machine)
      (is (= :all-blocked
             (quiesce-wait machine)))
      (is (= 100 (async/<!! out)))
      (step-all machine)
      (is (= :all-exited
             (quiesce-wait machine)))
      (is (= 200 (async/<!! out))))))

(deftest test-deadlock-go-blocks
  (let [async-f (fn []
                  (let [c1 (chan-named "c1" 1)
                        c2 (chan-named "c2" 1)]
                    (go-named "w3"
                              (Thread/sleep 1000)
                              (>! c1 3))

                    (go-named "w2"
                              (<! c1)
                              (>! c2 2)
                              (<! c1))
                    (go-named "w1"
                              (>! c1 1)
                              (<! c2))))]
    ;; run as plain core async
    (async-f)

    (let [machine ((step-machine :channel-history? true) async-f)]
      (state-trace machine true)

      (step-all machine)
      (quiesce-wait machine)
      (dump-state-trace machine))))

(deftest test-nested-go-blocks
  (let [async-f (fn []
                  (go (go 1)))
        machine ((step-machine) async-f)]
    (step-all machine)
    (is (= :all-exited
           (quiesce-wait machine))))

  (let [async-f (fn []
                  (go (go (go 1))))
        machine ((step-machine) async-f)]
    (step-all machine)
    (is (= :all-exited
           (quiesce-wait machine))))

  (let [async-f (fn []
                  (go (go 1)
                      (go (go (go 2))
                          (go 3))))
        machine ((step-machine) async-f)]
    (step-all machine)
    (is (= :all-exited
           (quiesce-wait machine)))))

(deftest test-nested-go-results
  (let [async-f (fn []
                  (go (go 1)))
        machine ((step-machine) async-f)]
    (step-all machine)
    (is (= :all-exited
           (quiesce-wait machine)))

    (is (= 1
           (-> (get-result machine)
               async/<!!
               async/<!!)))))

(deftest test-alts
  (let [async-f (fn [c1 c2]
                  (go (alts! [c1 c2])))
        c1 (async/chan)
        c2 (async/chan)
        machine ((step-machine) async-f c1 c2)
        c1-port (external-channel machine c1)]
    (step-all machine)
    (is (= :all-blocked
           (quiesce-wait machine)))
    (is (= :all-exited
           (do (async/>!! c1-port 100)
               (step-all machine)
               (quiesce-wait machine))))
    (is (= [100 c1]
           (async/<!! (get-result machine))))))

(deftest test-alts-no-step
  (let [async-f (fn [c1 c2]
                  (go (alts! [c1 c2])))
        c1 (async/chan)
        c2 (async/chan)
        out (async-f c1 c2)]
    (async/>!! c1 100)

    (is (= [100 c1]
           (async/<!! out)))))

(deftest test-alts-deliver-to-second
  (let [async-f (fn [c1 c2]
                  (go (alts! [c1 c2])))
        c1 (async/chan)
        c2 (async/chan)
        machine ((step-machine) async-f c1 c2)
        c2-port (external-channel machine c2)]
    (step-all machine)
    (is (= :all-blocked
           (quiesce-wait machine)))
    (is (= :all-exited
           (do (async/>!! c2-port 100)
               (step-all machine)
               (quiesce-wait machine))))
    (is (= [100 c2]
           (async/<!! (get-result machine))))))

(deftest test-alts-deliver-from-outside-before-receive
  (let [c1 (async/chan 10)
        c2 (async/chan 10)
        c3 (async/chan 10)
        c4 (async/chan 10)]
    (let [async-f (fn [c1 c2 c3 c4]
                    (go (alts! [c1 c2 c3 c4])))
          machine ((step-machine :channel-sizes [10 10 10 10]) async-f c1 c2 c3 c4)
          c2-port (external-channel machine c2)
          c4-port (external-channel machine c4)]
      (async/>!! c2-port 200)
      (async/>!! c4-port 400)
      (step-all machine)
      (is (= :all-exited
             (quiesce-wait machine)))
      (is (= :all-exited
             (do (async/>!! c2-port 100)
                 (step-all machine)
                 (quiesce-wait machine))))
      (is (= 200
             (-> (async/alts!! [(get-result machine)
                                (async/timeout 500)])
                 ffirst))))))

(deftest test-alts-deliver-before-receive
  (let [c1 (async/chan 10)
        c2 (async/chan 10)
        c3 (async/chan 10)
        c4 (async/chan 10)]
    (let [async-f (fn [c1 c2 c3 c4]
                    (go
                     ;; if multiple messages are delivered "simultaneously" from inside
                     ;; machine then these are delivered deterministicly
                     (>! c2 200)
                     (>! c4 400))
                    (go (alts! [c1 c2 c3 c4])))
          machine ((step-machine :channel-sizes [10 10 10 10]) async-f c1 c2 c3 c4)]
      (step-all machine)
      (is (= :all-exited
             (quiesce-wait machine)))

      (is (= [200 c2]
             (async/<!! (get-result machine)))))))

(deftest test-alts-deliver-to-nth
  (let [async-f (fn [c1 c2 c3 c4 c5 c6]
                  (go (alts! [c1 c2 c3 c4 c5 c6])))
        c1 (async/chan)
        c2 (async/chan)
        c3 (async/chan)
        c4 (async/chan)
        c5 (async/chan)
        c6 (async/chan)
        machine ((step-machine) async-f c1 c2 c3 c4 c5 c6)
        c4-port (external-channel machine c4)]
    (step-all machine)
    (is (= :all-blocked
           (quiesce-wait machine)))
    (is (= :all-exited
           (do (async/>!! c4-port 100)
               (step-all machine)
               (quiesce-wait machine))))
    (is (= [100 c4]
           (async/<!! (get-result machine))))))

(deftest test-timeout
  (let [time (atom 0)
        async-f (fn []
                  (go (<! (timeout 10))))
        machine ((step-machine :time-f #(deref time)) async-f)]
    (step-all machine)
    (is (= :all-blocked
           (quiesce-wait machine)))
    (is (= [{:thread-name "thread-1"
             :timeout-name "timeout-30001"
             :timeout-id 30001
             :start-time 0
             :duration 10}]
           (get-timeouts machine)))
    (let [timeout-id (-> (get-timeouts machine)
                         first
                         :timeout-id)]
      (complete-timeout machine timeout-id))
    (step-all machine)
    (is (= :all-exited
           (quiesce-wait machine)))))

(deftest test-shared-timeout
  (let [time (atom 0)
        async-f (fn []
                  (let [t (timeout 10)]
                    (go (<! t))
                    (go (<! t))))
        machine ((step-machine :time-f #(deref time)) async-f)]
    (step-all machine)
    (is (= :all-blocked
           (quiesce-wait machine)))
    (is (= [{:thread-name "thread-0"
             :timeout-name "timeout-30001"
             :timeout-id 30001
             :start-time 0
             :duration 10}]
           (get-timeouts machine)))
    (let [timeout-id (-> (get-timeouts machine)
                         first
                         :timeout-id)]
      (complete-timeout machine timeout-id))
    (step-all machine)
    (is (= :all-exited
           (quiesce-wait machine)))))

(deftest test-timeout-named
  (let [time (atom 0)
        async-f (fn []
                  (go-named "main"
                            (<! (timeout-named "my timeout" 10))))
        machine ((step-machine :time-f #(deref time)) async-f)]
    (step-all machine)
    (is (= :all-blocked
           (quiesce-wait machine)))
    (is (= {"main" #{"my timeout-30001"}}
           (dump-named-blockers machine)))))

(deftest test-timeout-with-alts-timeout-fires-first
  (let [time (atom 0)
        async-f (fn [c]
                  (go (alts! [c
                              (timeout 10)])))
        c (async/chan)
        machine ((step-machine :time-f #(deref time)) async-f c)]
    (step-all machine)
    (is (= :all-blocked
           (quiesce-wait machine)))
    (let [timeout-id (-> (get-timeouts machine)
                         first
                         :timeout-id)]
      (complete-timeout machine timeout-id))
    (step-all machine)
    (is (= :all-exited
           (quiesce-wait machine)))
    (is (nil?
         (first (async/<!! (get-result machine)))))))

(deftest test-timeout-with-alts-data-arrives-first
  (let [time (atom 0)
        async-f (fn [c]
                  (go (alts! [c
                              (timeout 10)])))
        c (async/chan)
        machine ((step-machine :time-f #(deref time)) async-f c)
        c-port (external-channel machine c)]
    (step-all machine)
    (is (= :all-blocked
           (quiesce-wait machine)))
    (async/>!! c-port 100)
    (step-all machine)
    ;; timeout should be gone now that data has arrived to the alts!
    (is (empty?
         (get-timeouts machine)))
    (is (= :all-exited
           (quiesce-wait machine)))
    (is (= 100
           (first (async/<!! (get-result machine)))))))

(deftest test-timeout-with-alts-timeout-fires-after-data
  (let [time (atom 0)
        async-f (fn [c]
                  (go (alts! [c
                              (timeout 10)])))
        c (async/chan)
        machine ((step-machine :time-f #(deref time)) async-f c)
        c-port (external-channel machine c)]
    (step-all machine)
    (is (= :all-blocked
           (quiesce-wait machine)))
    (async/>!! c-port 100)
    (is (not (empty? (get-timeouts machine))))
    ;; need to step so the data will be processed
    (step machine)
    (step-wait machine)
    (step machine)
    (step-wait machine)
    ;; and now the time-out is gone
    (is (empty? (get-timeouts machine)))

    (step-all machine)
    (is (= :all-exited
           (quiesce-wait machine)))
    (is (= 100
           (first (async/<!! (get-result machine)))))))

(deftest test-close
  (let [async-f (fn []
                  (let [c (chan 10)
                        reader (go
                                [(<! c)])
                        writer (go (close! c))]
                    reader))
        machine ((step-machine) async-f)]
    (step-all machine)
    (quiesce-wait machine)
    (is (= [nil]
           (async/<!! (get-result machine))))))

(deftest test-put-after-close
  (let [async-f (fn []
                  (let [c (chan 10)
                        reader (go [(<! c)
                                    (<! (timeout 200))
                                    (<! c)])
                        writer (go (<! (timeout 100))
                                   (>! c 100))
                        closer (go (close! c))]
                    [reader writer]))
        machine ((step-machine) async-f)]
    (step-all machine)
    (quiesce-wait machine)
    (let [timeout-ids (->> (get-timeouts machine)
                           (map :timeout-id)
                           sort)]
      (complete-timeout machine (first timeout-ids))
      (step-all machine)
      (quiesce-wait machine)
      (complete-timeout machine (second timeout-ids))
      (step-all machine)
      (quiesce-wait machine))
    (let [[reader writer] (get-result machine)]
      (is (= [nil nil nil]
             (async/<!! reader)))
      (is (= nil
             (async/<!! writer))))))

(deftest test-close-broadcast
  (let [async-f (fn []
                  (let [c (chan 10)
                        reader-1 (go
                                  [(<! c)])
                        reader-2 (go
                                  [(<! c)])
                        writer (go (close! c))]
                    [reader-1 reader-2]))
        machine ((step-machine) async-f)]
    (step-all machine)
    (quiesce-wait machine)
    (let [result (get-result machine)]
      (is (= [nil]
             (async/<!! (first result))))
      (is (= [nil]
             (async/<!! (second result)))))))

(deftest test-go-loop
  (let [async-f (fn [c-in c-out]
                  (go-loop [x (<! c-in)]
                           (>! c-out (* 2 x))
                           (recur (<! c-in))))]

    ;; use via plain core.async
    (let [c-in (async/chan)
          c-out (async/chan)]
      (async-f c-in c-out)
      (async/go
       (async/>! c-in 3)
       (async/>! c-in 7))
      (is (= [6 14]
             [(async/<!! c-out)
              (async/<!! c-out)])))

    ;; use in step mode
    (let [c-in (async/chan 10)
          c-out (async/chan 10)
          machine ((step-machine :channel-sizes [10 10]) async-f c-in c-out)
          in (external-channel machine c-in)
          out (external-channel machine c-out)]
      (async/>!! in 4)
      (step-all machine)
      (is (= :all-blocked
             (quiesce-wait machine)))
      (async/>!! in 8)
      (step-all machine)
      (is (= :all-blocked
             (quiesce-wait machine)))
      (is (= [8 16]
             [(async/<!! out)
              (async/<!! out)])))))

(deftest test-go-loop-named
  (let [async-f (fn [c-in c-out]
                  (go-loop-named "test-loop" [x (<! c-in)]
                                 (>! c-out (* 2 x))
                                 (recur (<! c-in))))]

    (let [c-in (async/chan 10)
          c-out (async/chan 10)
          machine ((step-machine :channel-sizes [10 10]) async-f c-in c-out)
          in (external-channel machine c-in)
          out (external-channel machine c-out)]
      (async/>!! in 4)
      (step-all machine)
      (quiesce-wait machine)
      (is (= 8
             (async/<!! out))))))

(deftest test-multi-blockers
  (let [async-f (fn [c-in]
                  (let [c2 (chan 10)]
                    (go (go (>! c2 [10 (<! c-in)]))
                        (go (>! c2 [20 (<! c-in)])
                            (>! c2 [21 (<! c-in)]))
                        [(<! c2) (<! c2)])))
        c-in (async/chan 10)
        machine ((step-machine :channel-sizes [10]) async-f c-in)
        in (external-channel machine c-in)]
    (step-all machine)
    (quiesce-wait machine)
    (async/>!! in 100)
    (step-all machine)
    (quiesce-wait machine)
    (step-all machine)
    (async/>!! in 200)
    (step-all machine)
    (quiesce-wait machine)
    (is (= [[10 100] [20 200]]
           (async/<!! (get-result machine))))))

(deftest test-multi-blockers-unfair
  (let [async-f (fn [c-in]
                  (let [c2 (chan 10)]
                    (go (go
                         ;; this thread is trying to read from input channel repeatedly
                         (let [x (<! c-in)
                               y (<! c-in)
                               z (<! c-in)]
                           (>! c2 [10 x])
                           (>! c2 [11 y])
                           (>! c2 [12 z])))

                        (go
                         ;; this thread is trying to read from the same input channel
                         (>! c2 [20 (<! c-in)])
                         (>! c2 [21 (<! c-in)]))

                        [(<! c2) (<! c2) (<! c2) (<! c2)])))
        c-in (async/chan 10)
        machine ((step-machine :channel-sizes [10]) async-f c-in)
        in (external-channel machine c-in)]
    (step-all machine)
    (quiesce-wait machine)

    (async/>!! in 100)
    (step-all machine)
    (quiesce-wait machine)

    (async/>!! in 200)
    (step-all machine)
    (quiesce-wait machine)

    (async/>!! in 300)
    (step-all machine)
    (quiesce-wait machine)

    (async/>!! in 400)
    (step-all machine)
    (quiesce-wait machine)

    ;; the first go block receives all of the input messages to the channel as long as it is taking
    ;; this is "unfair", but shows the system is now deterministic
    (is (= [[10 100] [11 200] [12 300] [20 400]]
           (first (async/alts!! [(get-result machine)
                                 (async/timeout 1000)]))))))

(deftest test-multi-blockers-with-random-choices
  (let [async-f (fn [c-in]
                  (let [c2 (chan 10)]
                    (go (go
                         ;; this thread is trying to read from input channel repeatedly
                         (>! c2 [10 (<! c-in)])
                         (>! c2 [11 (<! c-in)])
                         (>! c2 [12 (<! c-in)]))

                        (go
                         ;; this thread is trying to read from the same input channel
                         (>! c2 [20 (<! c-in)])
                         (>! c2 [21 (<! c-in)]))

                        [(<! c2) (<! c2) (<! c2) (<! c2)])))
        c-in (async/chan 10)
        machine ((step-machine :rand-seed 123456789 :channel-sizes [10]) async-f c-in)
        in (external-channel machine c-in)]
    (step-all machine)
    (quiesce-wait machine)

    (async/>!! in 100)
    (step-all machine)
    (quiesce-wait machine)

    (async/>!! in 200)
    (step-all machine)
    (quiesce-wait machine)

    (async/>!! in 300)
    (step-all machine)
    (quiesce-wait machine)

    (async/>!! in 400)
    (step-all machine)
    (quiesce-wait machine)

    ;; with the random choices, the output is mixed results from both threads
    (is (= [[20 100] [21 200] [10 300] [11 400]]
           (first (async/alts!! [(get-result machine)
                                 (async/timeout 1000)]))))))

(deftest test-multi-blockers-unfair-2
  (let [async-f (fn [c-in]
                  (let [c2 (chan 10)]
                    (go (go
                         ;; same as test above, but intersperse reads with writes
                         (>! c2 [10 (<! c-in)])
                         (>! c2 [11 (<! c-in)])
                         (>! c2 [12 (<! c-in)]))

                        (go
                         (>! c2 [20 (<! c-in)])
                         (>! c2 [21 (<! c-in)]))

                        [(<! c2) (<! c2) (<! c2) (<! c2)])))
        c-in (async/chan 10)
        machine ((step-machine :channel-sizes [10]) async-f c-in)
        in (external-channel machine c-in)]
    (step-all machine)
    (quiesce-wait machine)

    (async/>!! in 100)
    (step-all machine)
    (quiesce-wait machine)

    (async/>!! in 200)
    (step-all machine)
    (quiesce-wait machine)

    (async/>!! in 300)
    (step-all machine)
    (quiesce-wait machine)

    (async/>!! in 400)
    (step-all machine)
    (quiesce-wait machine)

    (is (= [[10 100] [11 200] [12 300] [20 400]]
           (first (async/alts!! [(get-result machine)
                                 (async/timeout 1000)]))))))

(deftest test-<!!
  (let [async-f (fn [c]
                  (go
                   (>! c 100)))
        c (async/chan 10)
        machine ((step-machine :channel-sizes [10]) async-f c)
        out (external-channel machine c)]
    (step-all machine)
    (quiesce-wait machine)
    (is (= 100
           (async/<!! out)))))

(deftest test-<!!-close
  (let [async-f (fn [c]
                  (go
                   (close! c)))
        c (async/chan 10)
        machine ((step-machine :channel-sizes [10]) async-f c)
        out (external-channel machine c)]
    (step-all machine)
    (quiesce-wait machine)

    (let [result (async/<!! out)]
      (step-all machine)
      (quiesce-wait machine)
      (is (nil?
           result)))))

(deftest test-take!
  (let [async-f (fn [c]
                  (go
                   (>! c 100)))
        c (async/chan 10)
        machine ((step-machine :channel-sizes [10]) async-f c)
        out (external-channel machine c)]
    (step-all machine)
    (quiesce-wait machine)
    (let [result (promise)]
      (async/take! out (fn [x]
                         (deliver result x)))
      (is (= 100
             @result)))))

(deftest test-take!-on-same-thread-by-default
  (let [async-f (fn [c]
                  (go
                   (>! c 100)))
        c (async/chan 10)
        machine ((step-machine :channel-sizes [10]) async-f c)
        out (external-channel machine c)]
    (step-all machine)
    (quiesce-wait machine)
    (let [result (promise)]
      (async/take! out (fn [x]
                         (deliver result [x (.getName (Thread/currentThread))])))
      (is (= [100 (.getName (Thread/currentThread))]
             @result)))))

(deftest test-take!-different-thread-if-not-available
  (let [async-f (fn [c]
                  (go
                   (>! c 100)))
        c (async/chan 10)
        machine ((step-machine :channel-sizes [10]) async-f c)
        out (external-channel machine c)]
    (let [result (promise)]
      (async/take! out (fn [x]
                         (deliver result [x (.getName (Thread/currentThread))])))
      (step-all machine)
      (quiesce-wait machine)
      (is (= 100
             (first @result)))
      (is (not= (.getName (Thread/currentThread))
                (second @result))))))

(deftest test-take!-on-same-thread-explicit
  (let [async-f (fn [c]
                  (go
                   (>! c 100)))
        c (async/chan 10)
        machine ((step-machine :channel-sizes [10]) async-f c)
        out (external-channel machine c)]
    (step-all machine)
    (quiesce-wait machine)
    (let [result (promise)]
      (async/take! out (fn [x]
                         (deliver result [x (.getName (Thread/currentThread))]))
                   true)
      (is (= [100 (.getName (Thread/currentThread))]
             @result)))))

(deftest test-take!-on-different-thread
  (let [async-f (fn [c]
                  (go
                   (>! c 100)))
        c (async/chan 10)
        machine ((step-machine :channel-sizes [10]) async-f c)
        out (external-channel machine c)]
    (step-all machine)
    (quiesce-wait machine)
    (let [result (promise)]
      (async/take! out (fn [x]
                         (deliver result [x (.getName (Thread/currentThread))]))
                   false)
      (is (= 100
             (first @result)))
      (is (not= (.getName (Thread/currentThread))
                (second @result))))))

(deftest test-take!-close
  (let [async-f (fn [c]
                  (go
                   (close! c)))
        c (async/chan 10)
        machine ((step-machine :channel-sizes [10]) async-f c)
        out (external-channel machine c)]
    (step-all machine)
    (quiesce-wait machine)
    (let [result (promise)]
      (async/take! out (fn [x]
                         (deliver result x)))
      (is (nil?
           (deref result 500 :failed))))))

(deftest test-put!
  (let [async-f (fn [c d]
                  (go
                   (>! d (<! c))))
        c (async/chan)
        d (async/chan 10)
        machine ((step-machine :channel-history? true
                               :channel-sizes [nil 10]) async-f c d)
        in (external-channel machine c)]
    (async/put! in 100)
    (step-all machine)
    (quiesce-wait machine)

    (is (= [100] (get-channel-contents machine d)))))

(deftest test-put!-with-fn
  (let [async-f (fn [c d]
                  (go
                   (>! d (<! c))))
        c (async/chan)
        d (async/chan 10)
        machine ((step-machine :channel-sizes [nil 10]) async-f c d)
        in (external-channel machine c)
        out (external-channel machine d)]
    (step-all machine)
    (quiesce-wait machine)

    (let [result (promise)]
      (async/put! in 100 #(deliver result :done))
      (step-all machine)
      (quiesce-wait machine)

      (is (= [100] (get-channel-contents machine d)))

      (is (= :done
             (deref result 1000 nil))))))

(deftest test-put!-with-fn-same-thread-by-default
  (let [async-f (fn [c d]
                  (go
                   (>! d (<! c))))
        c (async/chan)
        d (async/chan 10)
        machine ((step-machine :channel-sizes [nil 10]) async-f c d)
        in (external-channel machine c)
        out (external-channel machine d)]
    (step-all machine)
    (quiesce-wait machine)

    (let [result (promise)]
      (async/put! in 100 #(deliver result [:done (.getName (Thread/currentThread))]))
      (step-all machine)
      (quiesce-wait machine)

      (is (= [100]
             (get-channel-contents machine d)))

      (is (= [:done (.getName (Thread/currentThread))]
             (deref result 1000 nil))))))

(deftest test-put!-with-fn-different-thread-if-no-available
  (let [async-f (fn [c d]
                  (go
                   (>! d (<! c))))
        c (async/chan)
        d (async/chan 10)
        machine ((step-machine :channel-sizes [nil 10]) async-f c d)
        in (external-channel machine c)
        out (external-channel machine d)]
    (let [result (promise)]
      (async/put! in 100 #(deliver result [:done (.getName (Thread/currentThread))]))
      (step-all machine)
      (quiesce-wait machine)

      (is (= [100]
             (get-channel-contents machine d)))

      (is (= :done
             (first (deref result 1000 nil))))
      (is (not= (.getName (Thread/currentThread))
                (second (deref result 1000 nil)))))))

(deftest test-put!-with-fn-same-thread-explicit
  (let [async-f (fn [c d]
                  (go
                   (>! d (<! c))))
        c (async/chan)
        d (async/chan 10)
        machine ((step-machine :channel-sizes [nil 10]) async-f c d)
        in (external-channel machine c)
        out (external-channel machine d)]
    (step-all machine)
    (quiesce-wait machine)

    (let [result (promise)]
      (async/put! in 100
                  #(deliver result [:done (.getName (Thread/currentThread))])
                  true)
      (step-all machine)
      (quiesce-wait machine)

      (is (= [100]
             (get-channel-contents machine d)))

      (is (= [:done (.getName (Thread/currentThread))]
             (deref result 1000 nil))))))

(deftest test-put!-with-fn-different-thread-explicit
  (let [async-f (fn [c d]
                  (go
                   (>! d (<! c))))
        c (async/chan)
        d (async/chan 10)
        machine ((step-machine :channel-sizes [nil 10]) async-f c d)
        in (external-channel machine c)
        out (external-channel machine d)]
    (step-all machine)
    (quiesce-wait machine)

    (let [result (promise)]
      (async/put! in 100
                  #(deliver result [:done (.getName (Thread/currentThread))])
                  false)
      (step-all machine)
      (quiesce-wait machine)

      (is (= [100]
             (get-channel-contents machine d)))
      (is (= :done
             (first (deref result 1000 nil))))
      (is (not= (.getName (Thread/currentThread))
                (second (deref result 1000 nil)))))))

(deftest test-put!-on-caller-false
  ;; not implemented yet
  )

(deftest test-step-inputs
  (let [async-f (fn [c1 c2]
                  (go-loop [v (<! c1)]
                           (>! c2 (* 10 v))
                           (recur (<! c1))))
        c1 (async/chan)
        c2 (async/chan 10)
        machine ((step-machine :channel-sizes [1 10]) async-f c1 c2)
        in (external-channel machine c1)
        out (external-channel machine c2)]
    (step-all machine)
    (is (= :all-blocked
           (quiesce-wait machine)))
    (step-inputs machine [[in 1]
                          [in 2]
                          [in 3]])
    (is (= [10 20 30]
           [(async/<!! out)
            (async/<!! out)
            (async/<!! out)]))))

(deftest test-step-input-history
  (let [async-f (fn [c1 c2]
                  (go-loop [v (<! c1)]
                           (>! c2 (* 10 v))
                           (recur (<! c1))))
        c1 (async/chan)
        c2 (async/chan)
        machine ((step-machine :channel-sizes [1 10]
                               :action-history? true) async-f c1 c2)
        in (external-channel machine c1)]
    (step-all machine)
    (quiesce-wait machine)
    (step-inputs machine [[in 1]
                          [in 2]
                          [in 3]])

    (let [new-machine (replay-history machine (drop-last (get-history machine)))]
      (dump-state new-machine))

    (let [new-machine (step-back machine)
          new-machine-2 (step-back new-machine)
          new-machine-3 (step-back new-machine-2)]
      [machine
       new-machine
       new-machine-2
       new-machine-3])))

(deftest test-named-inputs-detailed-action-history
  (let [async-f (fn [c1 c2]
                  (go-named "forwarder"
                            (>! c2 (<! c1)))
                  (go-named "source"
                            (>! c1 100)
                            (>! c1 200)))
        machine ((step-machine :channel-names ["c1" "c2"]
                               :detailed-action-history? true) async-f (async/chan) (async/chan))]
    (step-all machine)
    (quiesce-wait machine)
    (is (= [[:run-pending "forwarder"]
            [:run-parked "thread-0"]
            [:run-pending "source"]
            [:run-parked "thread-0"]
            [:put "source" "c1-20001" 100]
            [:take "forwarder" "c1-20001" 100]
            [:un-park-put "source"]]
           (dump-detailed-action-history machine)))))

(deftest test-var-arg-fn
  (let [async-f (fn [& channels]
                  (doseq [[i c] (map vector (range) channels)]
                    (go-named (str "go-reader-" i)
                              (<! c))
                    (go-named (str "go-writer-" i)
                              (>! c 100))))
        machine ((step-machine :channel-names ["c1" "c2" "c3" "c4"]
                               :detailed-action-history? true) async-f
                               (async/chan) (async/chan) (async/chan) (async/chan))]
    (step-all machine)
    (quiesce-wait machine)
    (is (= [[:run-pending "go-reader-0"]
            [:run-parked "thread-0"]
            [:run-pending "go-writer-0"]
            [:run-parked "thread-0"]
            [:run-pending "go-reader-1"]
            [:run-parked "thread-0"]
            [:run-pending "go-writer-1"]
            [:run-parked "thread-0"]
            [:run-pending "go-reader-2"]
            [:run-parked "thread-0"]
            [:run-pending "go-writer-2"]
            [:run-parked "thread-0"]
            [:run-pending "go-reader-3"]
            [:run-parked "thread-0"]
            [:run-pending "go-writer-3"]
            [:run-parked "thread-0"]
            [:put "go-writer-0" "c1-20001" 100]
            [:put "go-writer-1" "c2-20002" 100]
            [:put "go-writer-2" "c3-20003" 100]
            [:put "go-writer-3" "c4-20004" 100]
            [:take "go-reader-0" "c1-20001" 100]
            [:take "go-reader-1" "c2-20002" 100]
            [:take "go-reader-2" "c3-20003" 100]
            [:take "go-reader-3" "c4-20004" 100]
            [:un-park-put "go-writer-0"]
            [:un-park-put "go-writer-1"]
            [:un-park-put "go-writer-2"]
            [:un-park-put "go-writer-3"]]
           (dump-detailed-action-history machine)))))

(deftest test-non-channel-args
  (let [async-f (fn [x c y]
                  (go
                   (>! c (+ x y))))
        c (async/chan 10)
        machine ((step-machine :channel-sizes [nil 10 nil]
                               :channel-names [nil "c" nil]) async-f 2 c 3)]
    (step-all machine)
    (quiesce-wait machine)
    (is (= "{:channels [\"c-20001\"], :channel-contents {\"c-20001\" [5]}, :last-action [:un-park-put \"thread-1\"]}"
           (str machine)))
    (is (= [5]
           (get-channel-contents machine c)))))

(deftest test-race
  (let [test-f #(let [async-f (fn [a c1 c2 c3 c4 c5]
                                (go-loop-named "loop-1" [x (<! c1)]
                                               (>! c5 x)

                                               (recur (<! c1)))
                                (go-loop-named "loop-2" [x (<! c2)]
                                               (>! c5 (* 2 x))
                                               (recur (<! c2)))
                                (go-named "loop-3"
                                          (while true
                                            (alts! [c3 c4])
                                            (>! c5 300))))
                      c1 (async/chan)
                      c2 (async/chan)
                      c3 (async/chan)
                      c4 (async/chan)
                      c5 (async/chan 10)
                      machine ((step-machine :channel-sizes [nil 10 10 10 10 10]
                                             :channel-names [nil "c1" "c2" "c3" "c4" "c5"]
                                             :action-history? true
                                             :detailed-action-history? true
                                             :channel-history? true) async-f 9 c1 c2 c3 c4 c5)]
                  (step-all machine)
                  (quiesce-wait machine)

                  (let [in (external-channel machine ([c1 c2 c3 c4] (rand-int 4)))]
                    (async/put! in 100)
                    (step-all machine)
                    (quiesce-wait machine)

                    (is (#{[100] [200] [300]} (get-channel-contents machine c5)))))]
    (doall (repeatedly 1 test-f))))

(deftest test-step-timeout
  (let [async-f (fn []
                  (go
                   ;; async fn takes a long time
                   (Thread/sleep 1000)))
        ;; set the step timeout very low
        machine ((step-machine :step-timeout 1) async-f)]
    (is (thrown-with-msg? RuntimeException #"step-wait timed out"
                          (step-all machine)))))

(deftest test-explicit-buffer-object
  (let [async-f (fn []
                  (let [c (chan (buffer 4))]
                    (go (>! c 10)
                        (>! c 20)
                        (>! c 30)
                        (>! c 40)
                        (let [x (<! c)
                              y (<! c)]
                          [x y]))))
        machine ((step-machine) async-f)]
    (step-all machine)
    (quiesce-wait machine)
    (is (= [10 20]
           (first (async/alts!! [(get-result machine)
                                 (async/timeout 1000)]))))
    (is (= [30 40]
           (get-in (dump-state machine) [:step-channels 20001 :channel-contents])))))

(deftest test-sliding-buffer
  (let [async-f (fn []
                  (let [c (chan (sliding-buffer 2))]
                    (go (>! c 10)
                        (>! c 20)
                        (>! c 30)
                        (>! c 40)
                        (let [x (<! c)
                              y (<! c)]
                          [x y]))))
        machine ((step-machine) async-f)]
    (step-all machine)
    (quiesce-wait machine)
    (is (= [30 40]
           (first (async/alts!! [(get-result machine)
                                 (async/timeout 1000)]))))
    (is (= []
           (get-in (dump-state machine) [:step-channels 20001 :channel-contents])))))

(deftest test-dropping-buffer
  (let [async-f (fn []
                  (let [c (chan (dropping-buffer 2))]
                    (go (>! c 10)
                        (>! c 20)
                        (>! c 30)
                        (>! c 40)
                        (let [x (<! c)
                              y (<! c)]
                          [x y]))))
        machine ((step-machine) async-f)]
    (step-all machine)
    (quiesce-wait machine)
    (is (= [10 20]
           (first (async/alts!! [(get-result machine)
                                 (async/timeout 1000)]))))
    (is (= []
           (get-in (dump-state machine) [:step-channels 20001 :channel-contents])))))

(deftest test-dropping-input-buffer
  (let [async-f (fn [c]
                  (let []
                    (go (let [x (<! c)
                              y (<! c)]
                          [x y]))))
        c (async/chan (async/dropping-buffer 2))
        machine ((step-machine :channel-sizes [2]
                               :channel-unblocking [:dropping]) async-f c)
        in (external-channel machine c)]
    (async/>!! in 10)
    (async/>!! in 20)
    (async/>!! in 30)
    (async/>!! in 40)
    (step-all machine)
    (quiesce-wait machine)
    (is (= [10 20]
           (first (async/alts!! [(get-result machine)
                                 (async/timeout 1000)]))))
    (is (= []
           (get-in (dump-state machine) [:step-channels 20001
                                         :channel-contents])))))

(deftest test-sliding-input-buffer
  (let [async-f (fn [c]
                  (let []
                    (go (let [x (<! c)
                              y (<! c)]
                          [x y]))))
        c (async/chan (async/sliding-buffer 2))
        machine ((step-machine :channel-sizes [2]
                               :channel-unblocking [:sliding]) async-f c)
        in (external-channel machine c)]
    (async/>!! in 10)
    (async/>!! in 20)
    (async/>!! in 30)
    (async/>!! in 40)
    (step-all machine)
    (quiesce-wait machine)
    (is (= [30 40]
           (first (async/alts!! [(get-result machine)
                                 (async/timeout 1000)]))))
    (is (= []
           (get-in (dump-state machine) [:step-channels 20001
                                         :channel-contents])))))

(deftest test->!!-at-top
  (let [async-f (fn [d]
                  (let [c (chan 10)]
                    (go (>! d (inc (<! c))))

                    (>!! c 100)))
        d (chan 10)
        machine ((step-machine :channel-sizes [10]) async-f d)
        out (external-channel machine d)]
    (step-all machine)
    (quiesce-wait machine)
    (is (= 101
           (async/<!! out)))))

(deftest test-<!!-at-top
  (let [async-f (fn []
                  (let [c (chan 10)]
                    (go (>! c 100))

                    (<!! c)))
        machine ((step-machine :channel-sizes [10]) async-f)]
    (step-all machine)
    (quiesce-wait machine)
    (is (= 100
           (get-result machine)))))

(deftest test-put!-at-top
  (let [async-f (fn [d]
                  (let [c (chan 10)]
                    (go (>! d (inc (<! c))))

                    (put! c 100)))
        d (chan 10)
        machine ((step-machine :channel-sizes [10]) async-f d)
        out (external-channel machine d)]
    (step-all machine)
    (quiesce-wait machine)
    (is (= 101
           (async/<!! out)))))

(deftest test-take!-at-top
  (let [async-f (fn []
                  (let [c (chan 10)]
                    (go (>! c 100))

                    (let [result (promise)]
                      (take! c #(deliver result %))
                      ;; NOTE: de-refing the promise here would result in a hung machine that
                      ;; cannot be stepped, because it would look like this thread was running
                      ;; indefinitely and the machine would wait for it to "stop" before taking
                      ;; another step, so the machine would be dead-locked
                      result)))
        machine ((step-machine :channel-sizes [10]) async-f)]
    (step-all machine)
    (quiesce-wait machine)
    (is (= 100
           @(get-result machine)))))

(deftest test->!!-in-go
  (let [async-f (fn [d]
                  (let [c (chan 10)]
                    (go (>! d (inc (<! c))))

                    (go
                     (>!! c 100))))
        d (chan 10)
        machine ((step-machine :channel-sizes [10]) async-f d)
        out (external-channel machine d)]
    (step-all machine)
    (quiesce-wait machine)
    (is (= 101
           (async/<!! out)))))

(deftest test-<!!-in-go
  (let [async-f (fn []
                  (let [c (chan 10)]
                    (go (>! c 100))
                    (go
                     (<!! c))))
        machine ((step-machine :channel-sizes [10]) async-f)]
    (step-all machine)
    (quiesce-wait machine)
    (is (= 100
           (async/<!! (get-result machine))))))

(deftest test-put!-in-go
  (let [async-f (fn [d]
                  (let [c (chan 10)]
                    (go (>! d (inc (<! c))))

                    (go
                     (put! c 100))))
        d (chan 10)
        machine ((step-machine :channel-sizes [10]) async-f d)
        out (external-channel machine d)]
    (step-all machine)
    (quiesce-wait machine)
    (is (= 101
           (async/<!! out)))))

(deftest test-take!-in-go
  (let [async-f (fn []
                  (let [c (chan 10)]
                    (go (>! c 100))

                    (go
                     (let [result (promise)]
                       (take! c #(deliver result %))
                       result))))
        machine ((step-machine :channel-sizes [10]) async-f)]
    (step-all machine)
    (quiesce-wait machine)
    (is (= 100
           @(async/<!! (get-result machine))))))

(deftest test-breakpoint
  (let [async-f (fn []
                  (let [c (chan 10)]
                    (go (>! c 100))

                    (go
                     (<! c))))
        machine ((step-machine :channel-sizes [10]) async-f)]
    (set-breakpoint machine (constantly true))
    (step-all machine)
    (is (= :at-breakpoint (quiesce-wait machine)))
    (step-all machine)
    (is (= :at-breakpoint (quiesce-wait machine)))
    (step-all machine)
    (quiesce-wait machine)
    (step-all machine)
    (quiesce-wait machine)
    (step-all machine)
    (quiesce-wait machine)
    (step-all machine)
    (quiesce-wait machine)
    (step-all machine)
    (quiesce-wait machine)
    (step-all machine)
    (quiesce-wait machine)
    (step-all machine)
    (quiesce-wait machine)
    (step-all machine)
    (is (= :all-exited (quiesce-wait machine)))))

(deftest test-real-breakpoint
  (let [async-f (fn []
                  (let [c (chan 10)]
                    (go (>! c 100))

                    (go
                     (<! c))))
        machine ((step-machine :channel-sizes [10]) async-f)]
    (set-breakpoint machine (fn [state]
                              (not (empty? (:blocked-takes state)))))
    (is (= {}
           (dump-named-blockers machine)))
    (step-all machine)
    (quiesce-wait machine)
    (is (= {"thread-2" #{"channel-20001"}}
           (dump-named-blockers machine)))
    (step-all machine)
    (quiesce-wait machine)
    (is (= {"thread-2" #{"channel-20001"}}
           (dump-named-blockers machine)))))

(deftest test-clear-breakpoint
  (let [async-f (fn []
                  (let [c (chan 10)]
                    (go (>! c 100))

                    (go
                     (<! c))))
        machine ((step-machine :channel-sizes [10]) async-f)
        breakpoint-f (fn [state]
                       (not (empty? (:blocked-takes state))))]
    (set-breakpoint machine breakpoint-f)
    (is (= {}
           (dump-named-blockers machine)))
    (step-all machine)
    (quiesce-wait machine)
    (is (= {"thread-2" #{"channel-20001"}}
           (dump-named-blockers machine)))
    (clear-breakpoint machine breakpoint-f)
    (step-all machine)
    (quiesce-wait machine)
    (is (= {}
           (dump-named-blockers machine)))))

(deftest test-clear-all-breakpoint
  (let [async-f (fn []
                  (let [c (chan 10)]
                    (go (>! c 100))

                    (go
                     (<! c))))
        machine ((step-machine :channel-sizes [10]) async-f)
        breakpoint-f (fn [state]
                       (not (empty? (:blocked-takes state))))]
    (set-breakpoint machine breakpoint-f)
    (is (= {}
           (dump-named-blockers machine)))
    (step-all machine)
    (quiesce-wait machine)
    (is (= {"thread-2" #{"channel-20001"}}
           (dump-named-blockers machine)))
    (set-breakpoint machine (constantly true))
    (clear-all-breakpoints machine)
    (step-all machine)
    (quiesce-wait machine)
    (is (= {}
           (dump-named-blockers machine)))))

(deftest test-multiple-breakpoints
  (let [async-f (fn []
                  (let [c (chan 10)]
                    (go (>! c 100))

                    (go
                     (<! c))))
        machine ((step-machine :channel-sizes [10]) async-f)]
    (set-breakpoint machine  (fn [state]
                               (not (empty? (:blocked-takes state)))))
    (set-breakpoint machine  (fn [state]
                               (not (empty? (:pending-puts state)))))
    (is (nil?
         (:pending-puts (print-step-machine machine))))
    (step-all machine)
    (quiesce-wait machine)
    (is (= {"thread-1" ["channel-20001" 100]}
           (:pending-puts (print-step-machine machine))))
    (step-all machine)
    (quiesce-wait machine)
    (is (= {"thread-1" ["channel-20001" 100]}
           (:pending-puts (print-step-machine machine))))
    (is (= {}
           (dump-named-blockers machine)))
    (step-all machine)
    (quiesce-wait machine)
    (step-all machine)
    (quiesce-wait machine)
    (step-all machine)
    (quiesce-wait machine)
    (is (nil?
         (:pending-puts (print-step-machine machine))))
    (is (= {"thread-2" #{"channel-20001"}}
           (dump-named-blockers machine)))))

(deftest test-map
  (let [async-f (fn []
                  (let [c1 (chan-named "c1" 10)
                        c2 (chan-named "c2" 10)
                        c3 (step/map #(+ (* 10 %1) %2) [c1 c2])]
                    (go-named "worker"
                              (>! c1 1)
                              (>! c2 2)
                              (>! c1 3)
                              (>! c2 4)
                              (close! c1)
                              (close! c2))
                    [(<!! c3)
                     (<!! c3)
                     (<!! c3)]))
        machine ((step-machine) async-f)]
    (step-all machine)
    (is (= :all-exited (quiesce-wait machine)))

    (is (= [12 34 nil]
           (get-result machine)
           (async-f)))))

(deftest test-map<
  (let [async-f (fn []
                  (let [c1 (chan-named "c1" 10)
                        c2 (step/map< inc c1)]
                    (go-named "worker"
                              ;; write to original channel
                              (>! c1 10)
                              (>! c1 20)
                              ;; write to new channel
                              (>! c2 30)
                              ;; close original channel
                              (close! c1))
                    [(<!! c2)
                     (<!! c2)
                     (<!! c2)
                     (<!! c2)]))
        machine ((step-machine) async-f)]
    (step-all machine)
    (is (= :all-exited (quiesce-wait machine)))

    (is (= [11 21 31 nil]
           (get-result machine)
           (async-f)))))

(deftest test-map>
  (let [async-f (fn []
                  (let [c1 (chan-named "c1" 10)
                        c2 (step/map> dec c1)]
                    (go-named "worker"
                              ;; write to original channel
                              (>! c1 10)
                              (>! c1 20)
                              ;; write to new channel
                              (>! c2 30)
                              ;; close new channel
                              (close! c2))
                    [(<!! c1)
                     (<!! c1)
                     (<!! c1)
                     (<!! c1)]))
        machine ((step-machine) async-f)]
    (step-all machine)
    (is (= :all-exited (quiesce-wait machine)))
    (is (= [10 20 29 nil]
           (get-result machine)
           (async-f)))))

(deftest test-merge
  (let [async-f (fn []
                  (let [c1 (chan-named "c1" 10)
                        c2 (chan-named "c2" 10)
                        c3 (chan-named "c3" 10)]
                    (go-named "worker" (>! c1 1)
                              (>! c2 2)
                              (>! c3 3)
                              (close! c1)
                              (close! c2)
                              (close! c3))

                    (let [c3 (step/merge [c1 c2 c3] 10)]
                      (set [(<!! c3)
                            (<!! c3)
                            (<!! c3)
                            (<!! c3)]))))
        machine ((step-machine :channel-history? true) async-f)]
    (state-trace machine true)
    (step-all machine)
    (is (= :all-exited (quiesce-wait machine)))
    (is (= (set [1 2 3 nil])
           (get-result machine)
           (async-f)))
    (dump-state-trace machine)))

(deftest test-merge-2
  (let [async-f (fn []
                  (let [c1 (chan-named "c1" 10)
                        c2 (chan-named "c2" 10)
                        c3 (chan-named "c3" 10)]
                    (go-named "w1"
                              (>! c1 1)
                              (close! c1))
                    (go-named "w2"
                              (>! c2 2)
                              (close! c2))
                    (go-named "w3"
                              (>! c3 3)
                              (close! c3))

                    (let [c3 (step/merge [c1 c2 c3] 10)]
                      (set [(<!! c3)
                            (<!! c3)
                            (<!! c3)
                            (<!! c3)]))))
        machine ((step-machine :channel-history? true) async-f)]
    (state-trace machine true)
    (step-all machine)
    (is (= :all-exited (quiesce-wait machine)))
    (is (= (set [1 2 3 nil])
           (get-result machine)
           (async-f)))
    (dump-state-trace machine)))

(deftest test-into
  (let [async-f (fn []
                  (let [c1 (chan 10)]
                    (go (>! c1 1)
                        (>! c1 2)
                        (>! c1 3)
                        (close! c1))

                    (step/into [99 100] c1)))
        machine ((step-machine) async-f)]
    (step-all machine)
    (is (= :all-exited (quiesce-wait machine)))
    (is (= [99 100 1 2 3]
           (async/<!! (get-result machine))
           (async/<!! (async-f))))))

(deftest test-take
  (let [async-f (fn []
                  (let [c1 (chan 10)
                        c2 (step/take 2 c1)]
                    (go (>! c1 1)
                        (>! c1 2)
                        (>! c1 3)
                        (close! c1))

                    [(<!! c2)
                     (<!! c2)
                     (<!! c2)]))
        machine ((step-machine) async-f)]
    (step-all machine)
    (is (= :all-exited (quiesce-wait machine)))
    (is (= [1 2 nil]
           (get-result machine)
           (async-f)))))

(deftest test-unique
  (let [async-f (fn []
                  (let [c1 (chan 10)
                        c2 (step/unique c1)]
                    (go (>! c1 1)
                        (>! c1 2)
                        (>! c1 2)
                        (>! c1 3)
                        (>! c1 3)
                        (>! c1 3)
                        (close! c1))

                    [(<!! c2)
                     (<!! c2)
                     (<!! c2)
                     (<!! c2)]))
        machine ((step-machine) async-f)]
    (step-all machine)
    (is (= :all-exited (quiesce-wait machine)))
    (is (= [1 2 3 nil]
           (get-result machine)
           (async-f)))))

(deftest test-partition
  (let [async-f (fn []
                  (let [c1 (chan-named "c1" 10)
                        c2 (step/partition 2 c1)]
                    (go-named "w" (>! c1 1)
                              (>! c1 2)
                              (>! c1 3)
                              (>! c1 -9)
                              (>! c1 -10)
                              (>! c1 4)
                              (close! c1))

                    [(<!! c2)
                     (<!! c2)
                     (<!! c2)
                     (<!! c2)]))
        machine ((step-machine :channel-history? true) async-f)]
    (state-trace machine true)
    (step-all machine)
    (is (= :all-exited (quiesce-wait machine)))
    (is (= [[1 2] [3 -9] [-10 4] nil]
           (get-result machine)
           (async-f)))
    (dump-state-trace machine)
    (dump-channel-history machine)))

(deftest test-partition-by
  (let [async-f (fn []
                  (let [c1 (chan 10)
                        c2 (step/partition-by pos? c1)]
                    (go (>! c1 1)
                        (>! c1 2)
                        (>! c1 3)
                        (>! c1 -9)
                        (>! c1 -10)
                        (>! c1 4)
                        (close! c1))

                    [(<!! c2)
                     (<!! c2)
                     (<!! c2)
                     (<!! c2)]))
        machine ((step-machine) async-f)]
    (step-all machine)
    (is (= :all-exited (quiesce-wait machine)))
    (is (= [[1 2 3] [-9 -10] [4] nil]
           (get-result machine)))))

(deftest test-onto-chan
  (let [async-f (fn []
                  (let [x [1 2 3]
                        c (chan 10)]
                    (step/onto-chan c x)
                    (go
                     [(<! c)
                      (<! c)
                      (<! c)
                      (<! c)])))
        machine ((step-machine) async-f)]
    (step-all machine)
    (is (= :all-exited (quiesce-wait machine)))
    (is (= [1 2 3 nil]
           (async/<!! (get-result machine))
           (async/<!! (async-f))))))

(deftest test-to-chan
  (let [async-f (fn []
                  (let [x [1 2 3]
                        c (step/to-chan x)]
                    (go
                     [(<! c)
                      (<! c)
                      (<! c)
                      (<! c)])))
        machine ((step-machine) async-f)]
    (step-all machine)
    (is (= :all-exited (quiesce-wait machine)))
    (is (= [1 2 3 nil]
           (async/<!! (get-result machine))
           (async/<!! (async-f))))))

(deftest test-pipe
  (let [async-f (fn []
                  (let [c1 (chan 10)
                        c2 (chan 10)]
                    (step/pipe c1 c2)
                    (go (>! c1 10)
                        (>! c1 20)
                        (close! c1))
                    (go [(<!! c2)
                         (<!! c2)
                         (<!! c2)
                         (<!! c1)])))
        machine ((step-machine) async-f)]
    (step-all machine)
    (is (= :all-exited (quiesce-wait machine)))
    (is (= [10 20 nil nil]
           (async/<!! (get-result machine))
           (async/<!! (async-f))))))

(deftest test-split
  (let [async-f (fn []
                  (let [c1 (chan 10)
                        [c2 c3] (step/split pos? c1)]
                    (go (>! c1 10)
                        (>! c1 -20)
                        (>! c1 -30)
                        (>! c1 40)
                        (close! c1))
                    (go [(<!! c2)
                         (<!! c3)
                         (<!! c3)
                         (<!! c2)
                         (<!! c2)
                         (<!! c3)])))
        machine ((step-machine) async-f)]
    (step-all machine)
    (is (= :all-exited (quiesce-wait machine)))
    (is (= [10 -20 -30 40 nil nil]
           (async/<!! (get-result machine))
           (async/<!! (async-f))))))

(deftest test-thread-call
  (let [async-f (fn []
                  (let [c1 (step/thread-call (fn [] 99))
                        c2 (step/thread-call (fn [] 100))]
                    [(<!! c1)
                     (<!! c2)]))
        machine ((step-machine) async-f)]
    (step-all machine)
    (is (= :all-exited (quiesce-wait machine)))
    (is (= [99 100]
           (get-result machine)
           (async-f)))))

(deftest test-thread-call
  (let [async-f (fn []
                  (let [c1 (step/thread 99)
                        c2 (step/thread 100)]
                    [(<!! c1)
                     (<!! c2)]))
        machine ((step-machine) async-f)]
    (step-all machine)
    (is (= :all-exited (quiesce-wait machine)))
    (is (= [99 100]
           (get-result machine)
           (async-f)))))

(deftest test-tap
  (let [async-f (fn []
                  (let [c1 (chan)
                        m1 (step/mult c1)
                        c2 (chan)
                        c3 (chan)
                        out (chan)]
                    (step/tap m1 c2)
                    (step/tap m1 c3)
                    (go (>! c1 10)
                        (>! c1 20)
                        (close! c1))
                    (go (>! out [(<! c2)
                                 (<! c2)
                                 (<! c2)]))
                    (go (>! out [(<! c3)
                                 (<! c3)
                                 (<! c3)]))
                    (go [(<! out)
                         (<! out)])))
        machine ((step-machine) async-f)]

    (step-all machine)
    (is (= :all-exited (quiesce-wait machine)))
    (is (= [[10 20 nil] [10 20 nil]]
           (async/<!! (get-result machine))
           (async/<!! (async-f))))))

(deftest test-untap
  (let [async-f (fn []
                  (let [c1 (chan 10)
                        m1 (step/mult c1)
                        c2 (chan 10)
                        c3 (chan 10)
                        out (chan 10)
                        sig (chan 10)]
                    (step/tap m1 c2)
                    (step/tap m1 c3)
                    (go
                     (>! c1 10)
                     (<! sig)
                     (step/untap m1 c3)
                     (>! c1 20)
                     (close! c1)
                     (>! c3 30))
                    (go
                     (>! out [(<! c2)
                              (<! c2)
                              (<! c2)]))
                    (go
                     (>! out [(<! c3)
                              (>! sig :done)
                              (<! c3)]))
                    (go
                     (set [(<! out)
                           (<! out)]))))
        machine ((step-machine) async-f)]
    (step-all machine)
    (is (= :all-exited (quiesce-wait machine)))
    (is (= #{[10 nil 30]
             [10 20 nil]}
           (async/<!! (get-result machine))
           (async/<!! (async-f))))))

(deftest test-untap-all
  (let [async-f (fn []
                  (let [c1 (chan 10)
                        m1 (step/mult c1)
                        c2 (chan 10)
                        c3 (chan 10)
                        sig (chan 10)
                        out (chan 10)]
                    (step/tap m1 c2)
                    (step/tap m1 c3)
                    (go
                     (>! c1 10)
                     (<! sig)
                     (<! sig)
                     (step/untap-all m1)
                     (>! c1 20)
                     (close! c1)
                     (>! c3 30)
                     (>! c2 40))
                    (go
                     (>! out [(<! c2)
                              (>! sig :done)
                              (<! c2)]))
                    (go
                     (>! out [(<! c3)
                              (>! sig :done)
                              (<! c3)]))
                    (go (set [(<! out)
                              (<! out)]))))
        machine ((step-machine) async-f)]
    (step-all machine)
    (is (= :all-exited (quiesce-wait machine)))
    (is (= #{[10 nil 30]
             [10 nil 40]}
           (async/<!! (get-result machine))
           (async/<!! (async-f))))))

(deftest test-mix
  (let [async-f (fn []
                  (let [c1 (chan-named "c1" 10)
                        mix1 (step/mix c1)
                        in1 (chan-named "in1" 10)
                        in2 (chan-named "in2" 10)]
                    (step/admix mix1 in1)
                    (step/admix mix1 in2)
                    (go-named "writer1"
                              (>! in1 10)
                              (>! in1 20))

                    (go-named "writer2"
                              (>! in2 100)
                              (>! in2 200))

                    (go-named "reader"
                              (set [(<!! c1)
                                    (<!! c1)
                                    (<!! c1)
                                    (<!! c1)]))))
        machine ((step-machine) async-f)]
    (step-all machine)
    (is (= :all-blocked (quiesce-wait machine)))

    (is (= #{100 200 10 20}
           (async/<!! (get-result machine))
           (async/<!! (async-f))))))

(deftest test-mix-unmix
  (let [async-f (fn []
                  (let [c1 (chan-named "c1" 10)
                        mix1 (step/mix c1)
                        in1 (chan-named "in1" 10)
                        in2 (chan-named "in2" 10)
                        sig (chan-named "sig" 10)]
                    (step/admix mix1 in1)
                    (step/admix mix1 in2)
                    (go-named "writer1"
                              (>! in1 10)
                              (<! sig)
                              (step/unmix mix1 in1)
                              (>! in1 20)
                              (>! c1 30))

                    (go-named "writer2"
                              (>! in2 100)
                              (<! sig)
                              (>! in2 200))

                    (go-named "reader"
                              (set [(<! c1)
                                    (<! c1)
                                    (>! sig :done)
                                    (>! sig :done)
                                    (<! c1)
                                    (<! c1)]))))
        machine ((step-machine) async-f)]
    (step-all machine)
    (is (= :all-blocked (quiesce-wait machine)))

    (is (= #{nil 100 200 10 30}
           (async/<!! (get-result machine))
           (async/<!! (async-f))))))

(deftest test-mix-unmix-all
  (let [async-f (fn []
                  (let [c1 (chan-named "c1" 10)
                        mix1 (step/mix c1)
                        in1 (chan-named "in1" 10)
                        in2 (chan-named "in2" 10)
                        sig (chan-named "sig" 10)]
                    (step/admix mix1 in1)
                    (step/admix mix1 in2)
                    (go-named "writer1"
                              (>! in1 10)
                              (<! sig)
                              (step/unmix mix1 in1)
                              (>! in1 20)
                              (>! c1 30))

                    (go-named "writer2"
                              (>! in2 100)
                              (<! sig)
                              (step/unmix mix1 in2)
                              (>! in2 200)
                              (>! c1 300))

                    (go-named "reader"
                              (set [(<! c1)
                                    (<! c1)
                                    (>! sig :done)
                                    (>! sig :done)
                                    (<! c1)
                                    (<! c1)]))))
        machine ((step-machine) async-f)]
    (step-all machine)
    (is (= :all-blocked (quiesce-wait machine)))

    (comment
      ;; TODO: investigate this test result
      (is (= #{nil 100 300 10 30}
             (async/<!! (get-result machine))
             (async/<!! (async-f)))))))

(deftest test-mix-toggle
  (let [async-f (fn []
                  (let [c1 (chan-named "c1" 10)
                        mix1 (step/mix c1)
                        in1 (chan-named "in1" 10)
                        in2 (chan-named "in2" 10)
                        sig (chan-named "sig" 10)]
                    (step/admix mix1 in1)
                    (step/admix mix1 in2)
                    (go-named "writer1"
                              (>! in1 10)
                              (<! sig)
                              (>! in1 20))

                    (go-named "writer2"
                              (>! in2 100)
                              (<! sig)
                              (step/toggle mix1 {in2 {:mute true}})
                              (>! in2 200)
                              (>! c1 300))

                    (go-named "reader"
                              (set [(<! c1)
                                    (<! c1)
                                    (>! sig :done)
                                    (>! sig :done)
                                    (<! c1)
                                    (<! c1)]))))
        machine ((step-machine) async-f)]
    (step-all machine)
    (is (= :all-blocked (quiesce-wait machine)))

    (is (= #{nil 100 300 10 20}
           (async/<!! (get-result machine))
           (async/<!! (async-f))))))

(deftest test-mix-solo-mode
  (let [async-f (fn []
                  (let [c1 (chan-named "c1" 10)
                        mix1 (step/mix c1)
                        in1 (chan-named "in1" 10)
                        in2 (chan-named "in2" 10)
                        sig (chan-named "sig" 10)
                        sig2 (chan-named "sig2" 10)]
                    (step/admix mix1 in1)
                    (step/admix mix1 in2)
                    (step/solo-mode mix1 :mute)
                    (go-named "writer1"
                              (>! in1 10)
                              (<! sig2)
                              (>! in1 20))

                    (go-named "writer2"
                              (>! in2 100)
                              (<! sig)
                              (step/toggle mix1 {in2 {:solo true}})
                              (>! sig2 :done)
                              (>! in2 200)
                              (>! c1 300))

                    (go-named "reader"
                              (set [(<! c1)
                                    (<! c1)
                                    (>! sig :done)
                                    (<! c1)
                                    (<! c1)]))))
        machine ((step-machine) async-f)]
    (step-all machine)
    (is (= :all-blocked (quiesce-wait machine)))

    (is (= #{nil 100 200 10 300}
           (async/<!! (get-result machine))
           (async/<!! (async-f))))))

(deftest test-mapcat>
  (let [async-f (fn []
                  (let [c1 (chan 10)
                        c2 (step/mapcat> (fn [n]
                                           (vec (map #(* 10 %) (range n))))
                                         c1)]
                    (go (>! c2 3)
                        (>! c2 2)
                        (close! c2))
                    (go [(<! c1)
                         (<! c1)
                         (<! c1)
                         (<! c1)
                         (<! c1)
                         (<! c1)])))
        machine ((step-machine) async-f)]
    (step-all machine)
    (is (= :all-exited (quiesce-wait machine)))

    (is (= [0 10 20 0 10 nil]
           (async/<!! (get-result machine))
           (async/<!! (async-f))))))

(deftest test-mapcat<
  (let [async-f (fn []
                  (let [c1 (chan 10)
                        c2 (step/mapcat< (fn [n]
                                           (vec (map #(* 10 %) (range n))))
                                         c1)]
                    (go (>! c1 3)
                        (>! c1 2)
                        (close! c1))
                    (go [(<! c2)
                         (<! c2)
                         (<! c2)
                         (<! c2)
                         (<! c2)
                         (<! c2)])))
        machine ((step-machine) async-f)]
    (step-all machine)
    (is (= :all-exited (quiesce-wait machine)))
    (is (= [0 10 20 0 10 nil]
           (async/<!! (get-result machine))
           (async/<!! (async-f))))))

(deftest test-pub-sub
  (let [async-f (fn []
                  (let [c1 (chan 10)
                        p1 (step/pub c1 :name)
                        c-j (chan 10)
                        c-b (chan 10)]
                    (step/sub p1 :joe c-j)
                    (step/sub p1 :bob c-b)
                    (go
                     (>! c1 {:name :joe
                             :id 10})
                     (>! c1 {:name :joe
                             :id 20})
                     (>! c1 {:name :bob
                             :id 30})
                     (close! c1))
                    (go [(<! c-j)
                         (<! c-j)
                         (<! c-b)
                         (<! c-j)
                         (<! c-b)])))
        machine ((step-machine) async-f)]
    (step-all machine)
    (is (= :all-exited (quiesce-wait machine)))
    (is (= [{:name :joe, :id 10}
            {:name :joe, :id 20}
            {:name :bob, :id 30}
            nil
            nil]
           (async/<!! (get-result machine))
           (async/<!! (async-f))))))

(deftest test-unsub
  (let [async-f (fn []
                  (let [c1 (chan-named "c1" 10)
                        p1 (step/pub c1 :name)
                        c-j (chan-named "c-j" 10)
                        c-b (chan-named "c-b" 10)
                        sig (chan-named "sig" 10)]
                    (step/sub p1 :joe c-j)
                    (step/sub p1 :bob c-b)
                    (go-named "writer"
                              (>! c1 {:name :joe
                                      :id 10})
                              (<! sig)
                              (step/unsub p1 :joe c-j)
                              (>! c1 {:name :joe
                                      :id 20})
                              (>! c1 {:name :bob
                                      :id 30})
                              (close! c1)
                              (>! c-j 40))
                    (go-named "reader"
                              [(<! c-j)
                               (>! sig :done)
                               (<! c-b)
                               (<! c-b)
                               (<! c-j)])))
        machine ((step-machine) async-f)]
    (step-all machine)
    (is (= :all-exited (quiesce-wait machine)))
    (is (= [{:name :joe, :id 10} nil {:name :bob, :id 30} nil 40]
           (async/<!! (get-result machine))
           (async/<!! (async-f))))))

(deftest test-unsub-all
  (let [async-f (fn []
                  (let [c1 (chan-named "c1" 10)
                        p1 (step/pub c1 :name)
                        c-j (chan-named "c-j" 10)
                        c-b (chan-named "c-b" 10)
                        sig (chan-named "sig" 10)]
                    (step/sub p1 :joe c-j)
                    (step/sub p1 :bob c-b)
                    (go-named "writer"
                              (>! c1 {:name :joe
                                      :id 10})
                              (<! sig)
                              (step/unsub-all p1)
                              (>! c1 {:name :joe
                                      :id 20})
                              (>! c1 {:name :bob
                                      :id 30})
                              (close! c1)
                              (>! c-j 40)
                              (>! c-b 50))
                    (go-named "reader"
                              [(<! c-j)
                               (>! sig :done)
                               (<! c-j)
                               (<! c-b)])))
        machine ((step-machine) async-f)]
    (step-all machine)
    (is (= :all-blocked (quiesce-wait machine)))
    (is (= [{:name :joe, :id 10} nil 40 50]
           (async/<!! (get-result machine))
           (async/<!! (async-f))))))

(deftest test-filter>
  (let [async-f (fn []
                  (let [c1 (chan 10)
                        c2 (step/filter> pos? c1)]
                    (go (>! c2 1)
                        (>! c2 -2)
                        (>! c2 3)
                        (close! c2))
                    (go [(<! c1)
                         (<! c1)
                         (<! c1)])))
        machine ((step-machine) async-f)]
    (step-all machine)
    (is (= :all-exited (quiesce-wait machine)))
    (is (= [1 3 nil]
           (async/<!! (get-result machine))
           (async/<!! (async-f))))))

(deftest test-remove>
  (let [async-f (fn []
                  (let [c1 (chan 10)
                        c2 (step/remove> pos? c1)]
                    (go (>! c2 -1)
                        (>! c2 2)
                        (>! c2 -3)
                        (close! c2))
                    (go [(<! c1)
                         (<! c1)
                         (<! c1)])))
        machine ((step-machine) async-f)]
    (step-all machine)
    (is (= :all-exited (quiesce-wait machine)))
    (is (= [-1 -3 nil]
           (async/<!! (get-result machine))
           (async/<!! (async-f))))))

(deftest test-filter<
  (let [async-f (fn []
                  (let [c1 (chan-named "c1" 10)
                        c2 (step/filter< pos? c1)]
                    (go-named "writer"
                              (>! c1 1)
                              (>! c1 -2)
                              (>! c1 3)
                              (close! c1))
                    (go-named "reader"
                              [(<! c2)
                               (<! c2)
                               (<! c2)])))
        machine ((step-machine :detailed-action-history? true
                               :channel-history? true) async-f)]
    (step-all machine)
    (is (= :all-exited (quiesce-wait machine)))
    (is (= [1 3 nil]
           (async/<!! (get-result machine))
           (async/<!! (async-f))))))

(deftest test-remove<
  (let [async-f (fn []
                  (let [c1 (chan-named "c1" 10)
                        c2 (step/remove< pos? c1)]
                    (go-named "writer"
                              (>! c1 -1)
                              (>! c1 2)
                              (>! c1 -3)
                              (close! c1))
                    (go-named "reader"
                              [(<! c2)
                               (<! c2)
                               (<! c2)])))
        machine ((step-machine :detailed-action-history? true
                               :channel-history? true) async-f)]
    (step-all machine)
    (is (= :all-exited (quiesce-wait machine)))
    (is (= [-1 -3 nil]
           (async/<!! (get-result machine))
           (async/<!! (async-f))))))

(comment
  {:step-channels {1 (-> (init-step-channel "a" (async/chan) 10 false)
                         (register-put 1001 100 true)
                         (register-put 2001 200 false))
                   2 (-> (init-step-channel "a" (async/chan) 10 false)
                         (register-put 3001 300 true))}}

  (pad '(10 20) 5 99)
  ;; => (10 20 99 99 99)

  )

;; (time (run-tests))
