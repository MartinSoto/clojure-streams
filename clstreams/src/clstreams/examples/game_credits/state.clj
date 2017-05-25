(ns clstreams.examples.game-credits.state)

(defn update-credits
  [{balance :balance :as state} {type :type credits :credits}]
  (case type
    ::create-account-requested
    {:type ::account-created
     :balance 0}

    ::add-credits-requested
    {:type ::credits-added
     :credits credits
     :balance (+ balance credits)}

    ::use-credits-requested
    (if (> balance credits)
      {:type ::credits-used
       :credits credits
       :balance (- balance credits)}
      {:type ::insufficient-credits-error
       :errors {:credits "Insufficient credits in account"}})))
