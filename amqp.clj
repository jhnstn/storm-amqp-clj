(ns promojam.storm.spouts.amqp
  (:gen-class)
  (:use [backtype.storm clojure log config])
  (:require [langohr.core      :as rmq]
            [langohr.channel   :as lch]
            [langohr.queue     :as lq]
            [langohr.consumers :as lc]
            [langohr.basic     :as lb]))

(defn- connection
  "Set up a connection to amqp"
 [params queue]
  (let [conn  (rmq/connect params)
        ch    (lch/open conn)]
    (lq/declare ch (queue :name)
                :exclusive   (queue :exclusive)
                :auto-delete (queue :auto-delete)
                :persistent  (queue :persistent)
                :durable     (queue :durable)
                :arguments   (queue :arguements))
    [conn ch]))

(defspout amqp-spout ["message"] {:params [params queue]}
  [conf context collector]
   (let [[conn ch] (connection params queue)
         queue-name (queue :name)]
     (if (queue :topic-name)
       (lq/bind ch queue-name (queue :exchange) :routing-key (queue :topic-name)))
     (spout
       (nextTuple []
                  (lc/subscribe ch queue-name
                                (fn [ch {:keys [delivery-tag]} ^bytes payload]
                                    (emit-spout! collector  [(String. payload "UTF-8")] :id delivery-tag))))
       (ack [delivery-id]
            (lb/ack ch delivery-id)))))
