(ns exponential-backoff-example.core)

(def ^:const default-backoff-millis 4)
(def ^:const default-backoff-max-attempts 8)
(def ^:const default-backoff-jitter-millis 32)

(defn request
  ([explicit-status-code]
   ;; 200 or 429
   {:status explicit-status-code})
  ([] (request 429)))

(defn- exponential-seq [init]
  (iterate (fn [n] (*' n init)) init))

(defn exponential-backoff
  [req-fn & {:keys [init-backoff-millis max-attempts jitter-millis
                    verbose]
             :or   {init-backoff-millis default-backoff-millis
                    max-attempts        default-backoff-max-attempts
                    jitter-millis       default-backoff-jitter-millis
                    verbose             false}}]
  (when verbose
    (prn :init-backoff-millis init-backoff-millis
         :max-attempts max-attempts
         :jitter-millis jitter-millis))
  (let [backoffs (exponential-seq init-backoff-millis)]
    (loop [i 0]
      (let [{:keys [status] :as resp} (req-fn)]
        (case status
          429 (let [;; This will loop indefinitely (until non-429), but limits the max sleep time.
                    n            (min i max-attempts)
                    backoff      (nth backoffs n)
                    jitter       (rand-int jitter-millis)
                    sleep-millis (+' backoff jitter)]
                (do (printf "got 429, sleeping for %d milliseconds\n" sleep-millis)
                    (when verbose (prn :i i, :n n, :backoff backoff, :jitter jitter))
                    (flush))
                (Thread/sleep (nth backoffs i))
                (recur (inc i)))
          ;; else
          resp)))))

(comment

  (defn faux-request-fn [max-attempts]
    (let [attempt (atom 0)]
      (fn []
        (if (< @attempt max-attempts)
          (do
            (swap! attempt inc)
            {:status 429})
          {:status 200}))))
  (def f (faux-request-fn 2))
  (f) ;; => {:status 429}
  (f) ;; => {:status 429}
  (f) ;; => {:status 200}

  (exponential-backoff (faux-request-fn 5))
  ;; got 429, sleeping for 5 milliseconds
  ;; got 429, sleeping for 17 milliseconds
  ;; got 429, sleeping for 77 milliseconds
  ;; got 429, sleeping for 271 milliseconds
  ;; got 429, sleeping for 1049 milliseconds
  ;; => {:status 200}

  (exponential-backoff (faux-request-fn 10)
                       {:max-attempts 4
                        :init-backoff-millis 2
                        :verbose true})
  ;; :init-backoff-millis 2 :max-attempts 4 :jitter-millis 32
  ;; got 429, sleeping for 5 milliseconds
  ;; :i 0 :n 0 :backoff 2 :jitter 3
  ;; got 429, sleeping for 7 milliseconds
  ;; :i 1 :n 1 :backoff 4 :jitter 3
  ;; got 429, sleeping for 26 milliseconds
  ;; :i 2 :n 2 :backoff 8 :jitter 18
  ;; got 429, sleeping for 37 milliseconds
  ;; :i 3 :n 3 :backoff 16 :jitter 21
  ;; got 429, sleeping for 44 milliseconds
  ;; :i 4 :n 4 :backoff 32 :jitter 12
  ;; got 429, sleeping for 35 milliseconds
  ;; :i 5 :n 4 :backoff 32 :jitter 3
  ;; got 429, sleeping for 40 milliseconds
  ;; :i 6 :n 4 :backoff 32 :jitter 8
  ;; got 429, sleeping for 61 milliseconds
  ;; :i 7 :n 4 :backoff 32 :jitter 29
  ;; got 429, sleeping for 33 milliseconds
  ;; :i 8 :n 4 :backoff 32 :jitter 1
  ;; got 429, sleeping for 35 milliseconds
  ;; :i 9 :n 4 :backoff 32 :jitter 3
  ;; => {:status 200}

  ,)
