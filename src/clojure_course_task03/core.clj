(ns clojure-course-task03.core
  (:require [clojure.set]
            [clojure.string :refer [lower-case]])
  (:gen-class))

(defn join* [table-name conds]
  (let [op (first conds)
        f1 (name (nth conds 1))
        f2 (name (nth conds 2))]
    (str table-name " ON " f1 " " op " " f2)))

(defn where* [data]
  (let [ks (keys data)
        res (reduce str (doall (map #(let [src (get data %)
                                           v (if (string? src)
                                               (str "'" src "'")
                                               src)]
                                       (str (name %) " = " v ",")) ks)))]
    (reduce str (butlast res))))

(defn order* [column ord]
  (str (name column)
       (if-not (nil? ord) (str " " (name ord)))))

(defn limit* [v] v)

(defn offset* [v] v)

(defn -fields** [data]
  (reduce str (butlast (reduce str (doall (map #(str (name %) ",") data))))))

(defn fields* [flds allowed]
  (let [v1 (apply hash-set flds)
        v2 (apply hash-set allowed)
        v (clojure.set/intersection v1 v2)]
    (cond
     (and (= (first flds) :all) (= (first allowed) :all)) "*"
     (and (= (first flds) :all) (not= (first allowed) :all)) (-fields** allowed)
     (= :all (first allowed)) (-fields** flds)
     :else (-fields** (filter v flds)))))

(defn select* [table-name {:keys [fields where join order limit offset]}]
  (-> (str "SELECT " fields " FROM " table-name " ")
      (str (if-not (nil? where) (str " WHERE " where)))
      (str (if-not (nil? join) (str " JOIN " join)))
      (str (if-not (nil? order) (str " ORDER BY " order)))
      (str (if-not (nil? limit) (str " LIMIT " limit)))
      (str (if-not (nil? offset) (str " OFFSET " offset)))))


(defmacro select [table-name & data]
  (let [;; Var containing allowed fields
        fields-var# (symbol (str table-name "-fields-var"))

        ;; The function takes one of the definitions like (where ...) or (join ...)
        ;; and returns a map item [:where (where* ...)] or [:join (join* ...)].
        transf (fn [elem]
                 (let [v (first elem)
                       v2 (second elem)
                       v3 (if (> (count elem) 2) (nth elem 2) nil)
                       val (case v
                               fields (list 'fields* (vec (next elem)) fields-var#)
                               offset (list 'offset* v2)
                               limit (list 'limit* v2)
                               order (list 'order* v2 v3)
                               join (list 'join* (list 'quote v2) (list 'quote v3))
                               where (list 'where* v2))]
                   [(keyword v) val]))

        ;; Takes a list of definitions like '((where ...) (join ...) ...) and returns
        ;; a vector [[:where (where* ...)] [:join (join* ...)] ...].
        env* (loop [d data
                    v (first d)
                    res []]
               (if (empty? d)
                 res
                 (recur (next d) (second d) (conj res (transf v)))))

        ;; Accepts vector [[:where (where* ...)] [:join (join* ...)] ...],
        ;; returns map {:where (where* ...), :join (join* ...), ...}
        env# (apply hash-map (apply concat env*))]
    
    `(select* ~(str table-name)  ~env#)))


;; Examples:
;; -------------------------------------

(let [proposal-fields-var [:person, :phone, :address, :price]]
  (select proposal
          (fields :person, :phone, :id)
          (where {:price 11})
          (join agents (= agents.proposal_id proposal.id))
          (order :f3)
          (limit 5)
          (offset 5)))

(let [proposal-fields-var [:person, :phone, :address, :price]]
  (select proposal
          (fields :all)
          (where {:price 11})
          (join agents (= agents.proposal_id proposal.id))
          (order :f3)
          (limit 5)
          (offset 5)))

(let [proposal-fields-var [:all]]
  (select proposal
          (fields :all)
          (where {:price 11})
          (join agents (= agents.proposal_id proposal.id))
          (order :f3)
          (limit 5)
          (offset 5)))

(comment
  ;; Описание и примеры использования DSL
  ;; ------------------------------------
  ;; Предметная область -- разграничение прав доступа на таблицы в реелтерском агенстве
  ;;
  ;; Работают три типа сотрудников: директор (имеет доступ ко всему), операторы ПК (принимают заказы, отвечают на тел. звонки,
  ;; передают агенту инфу о клиентах), агенты (люди, которые лично встречаются с клиентами).
  ;;
  ;; Таблицы:
  ;; proposal -> [id, person, phone, address, region, comments, price]
  ;; clients -> [id, person, phone, region, comments, price_from, price_to]
  ;; agents -> [proposal_id, agent, done]

  ;; Определяем группы пользователей и
  ;; их права на таблицы и колонки
  (group Agent
         proposal -> [person, phone, address, price]
         agents -> [clients_id, proposal_id, agent])

  ;; Предыдущий макрос создает эти функции
  (select-agent-proposal) ;; select person, phone, address, price from proposal;
  (select-agent-agents)  ;; select clients_id, proposal_id, agent from agents;




  (group Operator
         proposal -> [:all]
         clients -> [:all])

  ;; Предыдущий макрос создает эти функции
  (select-operator-proposal) ;; select * proposal;
  (select-operator-clients)  ;; select * from clients;



  (group Director
         proposal -> [:all]
         clients -> [:all]
         agents -> [:all])

  ;; Предыдущий макрос создает эти функции
  (select-director-proposal) ;; select * proposal;
  (select-director-clients)  ;; select * from clients;
  (select-director-agents)  ;; select * from agents;
  

  ;; Определяем пользователей и их группы

  ;; Макрос user должен сохранять разрешенные пользователю таблицы и поля в атоме *user-tables-vars*.
  
  (user Ivanov
        (belongs-to Agent))

  (user Sidorov
        (belongs-to Agent))

  (user Petrov
        (belongs-to Operator))

  (user Directorov
        (belongs-to Operator,
                    Agent,
                    Director))


  ;; Оператор select использует внутри себя переменную <table-name>-fields-var.
  ;; Для указанного юзера макрос with-user должен определять переменную <table-name>-fields-var
  ;; для каждой таблицы, которая должна содержать список допустимых полей этой таблицы
  ;; для этого пользователя.

  ;; Агенту можно видеть свои "предложения"
  (with-user Ivanov
    (select proposal
            (fields :person, :phone, :address, :price)
            (join agents (= agents.proposal_id proposal.id))))

  ;; Агенту не доступны клиенты
  (with-user Ivanov
    (select clients
            (fields :all)))  ;; Empty set

  ;; Директор может видеть состояние задач агентов
  (with-user Directorov
    (select agents
            (fields :done)
            (where {:agent "Ivanov"})
            (order :done :ASC)))
  
  )

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(defn table-fields-var-sym [table]
  (symbol (str (name table) "-fields-var")))

(defmacro group [group-sym & body]
  (defonce groups-tables-columns (atom {}))
  (let [tables-columns (into {} (map (fn [[table arrow columns]]
                                       (assert (= '-> arrow))
                                       [(keyword table) (mapv keyword columns)])
                                     (partition-all 3 body)))
        group-select-fns (map (fn [[table-kw columns]]
                                (let [table-name (name table-kw)
                                      fn-sym (symbol (str "select-" (lower-case (name group-sym)) "-" table-name))
                                      fields-var-sym (table-fields-var-sym table-name)
                                      table-sym (symbol table-name)]
                                  `(defn ~fn-sym []
                                     (let [~fields-var-sym ~columns]
                                       (select ~table-sym
                                               (~(symbol "fields") ~@columns))))))
                              tables-columns)]
    (swap! groups-tables-columns assoc (keyword group-sym) tables-columns) ; saving this for further use by user macro
    `(do ~@group-select-fns)))

(defn merge-columns [cols1 cols2]
  (let [columns-set (into #{} (concat cols1 cols2))]
    (if (contains? columns-set :all)
      [:all]
      (vec columns-set))))

(defmacro user [user-sym [belongs-to-sym & group-syms]]
  (assert (= 'belongs-to belongs-to-sym))
  (defonce users-tables-columns (atom {}))
  (let [tables-columns-seq (vals (select-keys @groups-tables-columns (map keyword group-syms)))
        merged-tables-columns (apply merge-with merge-columns tables-columns-seq)]
    (swap! users-tables-columns assoc (keyword user-sym) merged-tables-columns)))

(defmacro with-user [user-sym & body]
  (let [tables-columns (get @users-tables-columns (keyword user-sym))
        table-syms-with-columns (for [[table-kw columns] tables-columns]
                                  [(table-fields-var-sym table-kw) columns])]
    `(let [~@(apply concat table-syms-with-columns)]
       ~@body)))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(group G1
       T1 -> [:c1 :c2 :c3])
(user U1
      (belongs-to G1))

;;; dumping this to check they have value at compile time only - not runtime
(println "namespace level: groups-tables-columns value is" groups-tables-columns)
(println "namespace level: users-tables-columns value is" users-tables-columns)

(defn -main []
  ;; group/user defined at namespace level
  (let [query (with-user U1
                (select T1
                        (fields :c2 :c4)))]
    (println "T1 query with U1 is" query))
  
  ;; try with definition inside function also
  (group G2
         T2 -> [:c5 :c6 :c7])
  (user U2
        (belongs-to G2))
  (let [query (with-user U2
                (select T2
                        (fields :c6 :c8)))]
    (println "T2 query with U2 is" query))
  
  ;; also verify no difference here for defintions inside function
  (println "function level: groups-tables-columns value is" groups-tables-columns)
  (println "function level: users-tables-columns value is" users-tables-columns))
