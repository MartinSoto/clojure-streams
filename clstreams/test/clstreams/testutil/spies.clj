(ns clstreams.testutil.spies)

(defn spy
  ([]
   (spy nil))
  ([retval]
   (let [calls-atom (atom [])]
     [(fn [& args] (swap! calls-atom (fn [c] (conj c (vec args)))) retval)
      calls-atom])))

(defn calls [spy]
  @spy)

(defmacro with-spy-redefs [bindings & body-forms]
  (let [binding-pairs (partition 2 bindings)
        func-syms (for [_ binding-pairs] (gensym "func"))
        let-bindings (mapcat (fn [func-sym [[redef-var-sym spy-sym] spy-expr]]
                               [[func-sym spy-sym] spy-expr])
                             func-syms binding-pairs)
        redef-bindings (mapcat (fn [func-sym [[redef-var-sym spy-sym] spy-expr]]
                                 [redef-var-sym func-sym])
                               func-syms binding-pairs)]
    `(let [~@let-bindings]
       (with-redefs [~@redef-bindings]
         ~@body-forms))))
