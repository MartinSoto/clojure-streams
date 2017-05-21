(ns clstreams.examples.game-credits.state)

(defn update-credits
  [{credits :credits :as state} {type :type req-credits :credits}]
  (case type
    ::create-account-requested
    {:state {:credits 0}}

    ::add-credits-requested
    {:state (assoc state :credits (+ credits req-credits))}

    ::use-credits-requested
    (if (> credits req-credits)
      {:state (assoc state :credits (- credits req-credits))}
      {:state state
       :errors {:credits "Insufficient credits in account"}})))
