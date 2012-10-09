(ns cascalog-tutorial.core
    (:require [clojure.string :as str])
    (:require [cascalog.conf :as conf])
    (:use [cascalog.checkpoint])
    (:use cascalog.api
         [jackknife.seq :only (find-first)])
    (:require (cascalog [workflow :as w]
                        [ops :as c]
                        [vars :as v]))
    (:gen-class)
    )


;;;;
;;;1 -- first steps
;;;;

(def age
  [
   ;; [person age]
   ["alice" 28]
   ["bob" 33]
   ["chris" 40]
   ["david" 25]
   ["emily" 25]
   ["george" 31]
   ["gary" 28]
   ["kumar" 27]
   ["luanne" 36]
   ])

(comment

  (?<- (stdout) [?person] (age ?person 25))

  (def tuple1a (??<- [?person ?age] (age ?person ?age)))
  (def tuple1b (??<- [?person ?age] (age ?person ?age) (= ?age 25)))
  (def tuple1c (??<- [?person] (age ?person 25))) ; "constant substitution" example
  (def tuple1d (??<- [?person ?age] (age ?person ?age) (< ?age 30)))

  )




;;;;
;;;2 -- a bit more complicated
;;;;

(def follows
  [
   ;; [person-follower person-followed]
   ["alice" "david"]
   ["alice" "bob"]
   ["alice" "emily"]
   ["bob" "david"]
   ["bob" "george"]
   ["bob" "luanne"]
   ["david" "alice"]
   ["david" "luanne"]
   ["emily" "alice"]
   ["emily" "bob"]
   ["emily" "george"]
   ["emily" "gary"]
   ["george" "gary"]
   ["harold" "bob"]
   ["luanne" "harold"]
   ["luanne" "gary"]
   ])

(def gender
  [
   ;; [person gender]
   ["alice" "f"]
   ["bob" "m"]
   ["chris" "m"]
   ["david" "m"]
   ["emily" "f"]
   ["george" "m"]
   ["gary" "m"]
   ["harold" "m"]
   ["luanne" "f"]
   ])

(comment

  ; returns all the persons that emily follows, along with their gender.
  (def tuple2a (??<- [?person ?gender] (follows "emily" ?person) (gender ?person ?gender)))

  ; returns all the males that emily follows.
  (def tuple2b (??<- [?person] (follows "emily" ?person) (gender ?person "m")))

  ; complicate things a little. What do you think this does?
  (def tuple2c (??<- [?person ?a2] (age :> ?person ?age) (< ?age 30) (* 2 :< ?age :> ?a2)))
  (def tuple2d (??<- [?person ?a2] (age ?person ?age) (< ?age 30) (* 2 ?age :> ?a2)))

  ; different types of predicates:
  ; generators
  ; operations (either create new variables as functions of old variables, or act as filter)
  ; aggregators (we'll get there next)
  ;
  ; each predicate takes a number of input variables (following :<) 
  ; and generates a number of output variables (following :>).

  ; note: "constant substitution" feature = using a constant in 
  ; place of an output variable to act as a filter. 
  ; Already used in a previous example.
  ; (* 4 ?v2 :> 100) is another example, doing the exact same thing as (= ?v2 25)


  ; another example illustrating the power of what we've seen so far:
  (def tuple2e (??<- [?person1 ?person2] 
		     (age ?person1 ?age1) (follows ?person1 ?person2)
		     (age ?person2 ?age2) (< ?age2 ?age1)))
  ; What does this query do?


  (def tuple2f (??<- [?person1 ?person2 ?delta] 
		     (age ?person1 ?age1) (follows ?person1 ?person2)
		     (age ?person2 ?age2) (- ?age2 ?age1 :> ?delta)
		     (< ?delta 0)))


  ; oh by the way let me introduce a little notation, 
  ; useful when there's a variable in your tap that you're not 
  ; interested in and can't be bothered to choose a name for:
  (def tuple2g (??<- [?person1] (age ?person1 _)))



  )



;;;;
;;;3 -- Let's aggregate
;;;;

(comment 

  ; this counts all people younger than 30:
  (def tuple3a (??<- [?count] (age _ ?a) (< ?a 30) (c/count ?count)))


  ; this counts followees of each individual:
  (def tuple3b (??<- [?person ?count] (follows ?person _)
		     (c/count ?count)))

  ; you can have several aggregators in a query, 
  ; as long as the "group by" variables are the same:
  (def tuple3c (?<- [?country ?avg] 
		    (location ?person ?country _ _) (age ?person ?age)
		    (c/count ?count) (c/sum ?age :> ?sum)
		    (div ?sum ?count :> ?avg)))

  )

;;;;
;;;4 -- Custom operations
;;;;


(def sentence
  [
["In the Darwinian perspective, order is not immanent in reality, but it is a"]
["self-affirming aspect of reality in so far as it is experienced by situated"]
["subjects. However, it is not so much reality that is self-affirming, but the"]
["creative order structuring reality which manifests itself to us. Being-whole,"]
["as opposed to being-one, underwrites our fundamental sense of locatedness and"]
["particularity in the universe. The valuation of order qua meaningful order,"]
["rather than order-in-itself, has been thoroughly objectified in the Darwinian"]
["worldview. This process of de-contextualization and reification of meaning has"]
["ultimately led to the establishment of ‘dis-order’ rather than ‘this-order’. As"]
["a result, Darwinian materialism confronts us with an eradication of meaning"]
["from the phenomenological experience of reality. Negative theology however"]
["suggests a revaluation of disorder as a necessary precondition of order, as"]
["that without which order could not be thought of in an orderly fashion. In that"]
["sense, dis-order dissolves into the manifestations of order transcending the"]
["materialist realm. Indeed, order becomes only transparent qua order in so far"]
["as it is situated against a background of chaos and meaninglessness. This"]
["binary opposition between order and dis-order, or between order and that which"]
["disrupts order, embodies a central paradox of Darwinian thinking. As Whitehead"]
["suggests, reality is not composed of disordered material substances, but as"]
["serially-ordered events that are experienced in a subjectively meaningful way."]
["The question is not what structures order, but what structure is imposed on our"]
["transcendent conception of order. By narrowly focusing on the disorderly state"]
["of present-being, or the “incoherence of a primordial multiplicity”, as John"]
["Haught put it, Darwinian materialists lose sense of the ultimate order"]
["unfolding in the not-yet-being. Contrary to what Dawkins asserts, if we reframe"]
["our sense of locatedness of existence within a the space of radical contingency"]
["of spiritual destiny, then absolute order reemerges as an ontological"]
["possibility. The discourse of dis-order always already incorporates a creative"]
["moment that allows the self to transcend the context in which it finds itself,"]
["but also to find solace and responsiveness in an absolute Order which both"]
["engenders and withholds meaning. Creation is the condition of possibility of"]
["discourse which, in turn, evokes itself as presenting creation itself."]
["Darwinian discourse is therefore just an emanation of the absolute discourse of"]
["dis-order, and not the other way around, as crude materialists such as Dawkins"]
["suggest."]
   ])


; defmapcatop defines a map operation (operating on every record)

; the following defines an operation that splits a sentence into a tuple of words:
(defmapcatop split [sentence]
       (seq (.split sentence "\\s+")))

(comment

; easy to then return a word count:
(def tuple4a (??<- [?word ?count] (sentence ?s)
              (split ?s :> ?word) (c/count ?count)))

)


; regular clojure functions can also be used as maps:

(defn lowercase [w] (.toLowerCase w))


(comment

(def tuple4b (??<- [?word ?count] 
        (sentence ?s) (split ?s :> ?word1)
        (lowercase ?word1 :> ?word) (c/count ?count)))

)


; to define aggregation operators, several options that I won't get into here:
; defbufferop
; defaggregateop
; defparallelagg
; Examples using these and illustrating their differences are easy to find online.




;;;;
;;;5 -- nullable v non-nullable variables
;;;;


; so far we have only been using non-nullable variables.
; They are prefixed by "?". When they take a null value, the record is ignored.
; You can instead manipulate a nullable variable by using the "!" prefix.


(def location
  [
   ;; [person country state city]
   ["alice" "usa" "california" nil]
   ["bob" "canada" nil nil]
   ["chris" "usa" "pennsylvania" "philadelphia"]
   ["david" "usa" "california" "san francisco"]
   ["emily" "france" nil nil]
   ["gary" "france" nil "paris"]
   ["luanne" "italy" nil nil]
   ])

(comment

  (def tuple5a (??<- [?person ?city] (location ?person _ _ ?city)))

  (def tuple5b (??<- [?person !city] (location ?person _ _ !city)))

  )



;;;;
;;;6 -- queries, subqueries
;;;;

; <- defines a query, but does not execute it. 
; It can then be used as a tap in another query.
; Or, it can be run as is, using the ??- operator 
; (or ?-, which is to ??- what ?<- is to ??<-)

(comment 

  (def tuple6a 
       (let [
	     many-follows (<- [?person] (follows ?person _) (c/count ?c) (> ?c 2))
	     active-follows (<- [?p1 ?p2] (many-follows ?p1) (many-follows ?p2) (follows ?p1 ?p2))
	     ]
	 (??- many-follows active-follows)))

  )





;;;;
;;;7 -- workflow
;;;;

; Not obvious how to gracefully handly a cascalog workflow composed of a succession of many queries.
; I recommend looking into the 'checkpoint' contributed extension.
; The goal is to save intermediate results, allowing for more gracious failure and faster development.
; https://github.com/nathanmarz/cascalog-contrib/tree/master/cascalog.checkpoint
;
;
; In the author's words:
;
; The workflow macro in the checkpoint module allows you to break complicated
; workflows out into small, checkpointed steps. If one of these steps causes a
; job to fail and you restart the job, the workflow macro will skip every step
; up to the previous point of failure. Fault-tolerant MapReduce topologies ftw!



; That's it! Questions?
