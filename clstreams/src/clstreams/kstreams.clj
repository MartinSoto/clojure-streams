(ns clstreams.kstreams
  (:refer-clojure :only [])
  (:require [clstreams.kstreams.impl :refer [kstream-multimethod-defs define-multimethods]]))

(define-multimethods kstream-multimethod-defs)

