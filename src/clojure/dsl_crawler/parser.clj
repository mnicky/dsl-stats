(ns dsl-crawler.parser
  (:require [clojure.string :as string]
            [pl.danieljanus.tagsoup :as tagsoup :refer [tag attributes children parse]]
            [lambic.core :as lambic]
            [dsl-crawler.threadpool :as threadpool]
            [dsl-crawler.article :as article]
            [dsl-crawler.user :as user]))

;=== helper functions ================

(defn content
  "Return the content of the element (without html elements)."
  [e]
  (string/join
    (filter string? (rest (rest e)))))

(defn tag?
  "Return whether the specified html is (just one) element."
  [html]
  (keyword? (first html)))

(defn get-all
  "Returns seq of all elements satisfying the specified predicate."
  [html pred]
  (if (keyword? (first html))
    (get-all (seq (vector html)) pred)
    (when (seq html)
      (concat (filter pred html)
              (get-all (mapcat #(rest (rest %)) (remove string? html)) pred)))))

(defn get-classes
  "Returns seq of all elements having the specified class."
  [html c]
  (get-all html #(= c (:class (second %)))))

(defn get-ids
  "Returns seq of all elements having the specified id."
  [html i]
  (get-all html #(= i (:id (second %)))))

(defn get-elements
  "Returns seq of all elements having the specified element
  (must be keyword - e.g. :div, :table, ...)."
  [html e]
  (get-all html #(= e (first %))))

(defn get-attributes
  "Returns seq of all elements having the attribute with the specified value
  (attr must be keyword)."
  ([html attr]
    (get-all html #(not (nil? (attr (second %))))))
  ([html attr value]
    (get-all html #(= value (attr (second %))))))

;=== extracting functions ================

(defn article-title
  "Returns the string containing the title of the dsl.sk article."
  [html]
  (-> html (get-classes "page_title") first content))

(defn to-timestamp
  "Converts java.util.Date to java.sql.Timestamp."
  [^java.sql.Date date]
  (java.sql.Timestamp. (.getTime date)))

(defn article-date
  "Returns the date of the dsl.sk article as java.util.Date."
  [html]
  (let [perex (get-classes html "article_perex")
        string (nth (first perex) 2)
        date-str (re-find (re-pattern "[\\d]{1,2}.[\\d]{1,2}.[\\d]{4}") string)]
    (to-timestamp (. (java.text.SimpleDateFormat. "dd.MM.yyyy") (parse date-str)))))

(defn article-num
  "Returns the containing dsl.sk article num."
  [html]
  (let [url (-> html (get-attributes :property "og:url") first second :content)
        ^String num-str (re-find (re-pattern "[\\d]+") url)]
    (Integer. num-str)))

(defn comments
  "Returns seq of comments in parsed form."
  [html]
  (-> html (get-ids "body")
           (get-attributes :cellspacing "10")
           (get-attributes :bgcolor "#ffffff")))

(defn comment-info
  "Returns string containing comment info from the comment in parsed form."
  [comment-html]
  (-> comment-html (get-elements :div) (get-elements :font) first content))

(defn remove-newlines
  "Returns the string with all newlines removed"
  [s]
  (string/replace s "\n" ""))

(defn comment-user
  "Returns the string containing the username of the comment author."
  [comment-html]
  (let [matches (rest (re-find (re-pattern " {8}Od: (.*) {17}| {8}Od reg.: (.*) {17}")
                  (remove-newlines (comment-info comment-html))))]
    (or (first matches) (second matches))))

(defn user-registered?
  "Returns whether the author of this comment was registered."
  [comment-html]
  (let [matches (rest (re-find (re-pattern " {8}Od: (.*) {9}| {8}Od reg.: (.*) {9}")
                  (comment-info comment-html)))]
    (boolean (second matches))))

(defn comment-time
  "Returns string containing comment time from the comment in parsed form."
  [comment-html]
  (let [date-str (re-find (re-pattern "[\\d]{1,2}.[\\d]{1,2}.[\\d]{4} [\\d]{1,2}:[\\d]{1,2}")
                   (comment-info comment-html))]
    (to-timestamp (. (java.text.SimpleDateFormat. "dd.MM.yyyy HH:mm") (parse date-str)))))

(defn has-rank?
  "Returns whether this comment has rank."
  [comment-html]
  (== 2 (count (-> comment-html (get-elements :span) (get-elements :font
  )))))

(defn comment-rank
  "Returns the comment rank (a double) or nil when none."
  [comment-html]
  (when (has-rank? comment-html)
    (let [rank-string (-> comment-html (get-elements :span) (get-elements :font) first content)
          ^String rank-str (re-find (re-pattern "-?[\\d]{1,2}.?[\\d]?") rank-string)]
      (Double. rank-str))))

(defn comment-text
  "Returns the text of comment as a string."
  [comment-html]
  (string/trim (content comment-html)))

(defn parse-comments
  "Returns map of information extracted from dsl.sk comments in parsed html form."
  [comment-html]
  (map-indexed #(assoc %2 :first (== 0 %1))
    (map #(zipmap [:user :registered :time :rank :text] %)
            (map (juxt comment-user user-registered? comment-time comment-rank comment-text) comment-html))))

(defn parse-article
  "Returns map of information extracted from dsl.sk article in parsed html form."
  [html]
  (let [comments (parse-comments (comments html))]
    (-> (zipmap [:num :title :date] ((juxt article-num article-title article-date) html))
        (assoc :comment_count (count comments))
        (assoc :first_comment_date (:time (first (sort-by :time comments))))
        (assoc :last_comment_date (:time (last (sort-by :time comments))))
        (assoc :total_comment_length (count (remove-newlines (string/join (map :text comments)))))
        (assoc :comments comments))))

(defn is-article?
  "Returns whether the page in parsed html form is a dsl.sk article."
  [html]
  (empty? (get-classes html "box_title")))

(defn last-article-num
  "Returns the number of the last article on dsl.sk"
  []
  (let [rss (parse "http://www.dsl.sk/export/rss_articles.php")
        ^String num-str (re-find (re-pattern "[\\d]+")
                  (-> rss (get-elements :item) first content string/trim))]
    (Integer. num-str)))

;=== processing functions ================

(defn process-comments
  "Parses the comments in the form of the map sequence and uses them
  to update the information in the database. Returns the 'id'
  of the created/updated user."
  [comments article-num]
  (doseq [grouped (group-by #(vector (:user %) (:registered %)) comments)]
    (let [name (first (key grouped))
          registered (second (key grouped))
          {:keys [id
                  commented_article_count
                  comment_count
                  first_comment_time
                  ranked_comment_count
                  last_comment_time
                  first_comment_count
                  total_comment_rank
                  total_comment_length
                  most_commented_article_comment_count
                  most_commented_article_num]} (user/find-record {:name name :registered registered})
          comm (val grouped)]
      (if id
        (:id (user/update {:id id
                           :commented_article_count (inc commented_article_count)
                           :comment_count (+ comment_count (count comm))
                           :ranked_comment_count (+ ranked_comment_count (count (keep :rank comm)))
	                         :first_comment_time (let [^java.sql.Timestamp new (:time (first (sort-by :time comm)))
                                                     ^java.sql.Timestamp before first_comment_time]
                                                 (if (< (.getTime new) (.getTime before))
                                                   new
                                                   before))
	                         :last_comment_time (let [^java.sql.Timestamp new (:time (last (sort-by :time comm)))
                                                    ^java.sql.Timestamp before last_comment_time]
                                                 (if (> (.getTime new) (.getTime before))
                                                   new
                                                   before))
	                         :first_comment_count (+ first_comment_count (count (filter true? (map :first comm))))
	                         :total_comment_rank (+ total_comment_rank (reduce + (keep :rank comm)))
	                         :total_comment_length (+ total_comment_length (count (remove-newlines (string/join (keep :text comm)))))
	                         :most_commented_article_comment_count (if (> (count comm) most_commented_article_comment_count)
                                                                   (count comm)
                                                                   most_commented_article_comment_count)
	                         :most_commented_article_num (if (> (count comm) most_commented_article_comment_count)
                                                         article-num
                                                         most_commented_article_num)}))
        (:id (user/create {:name name
                           :registered registered
	                         :commented_article_count 1
                           :comment_count (count comm)
                           :ranked_comment_count (count (keep :rank comm))
	                         :first_comment_time (:time (first (sort-by :time comm)))
	                         :last_comment_time (:time (last (sort-by :time comm)))
	                         :first_comment_count (count (filter true? (map :first comm)))
	                         :total_comment_rank (reduce + (keep :rank comm))
	                         :total_comment_length (count (remove-newlines (string/join (keep :text comm))))
	                         :most_commented_article_comment_count (count comm)
	                         :most_commented_article_num article-num}))))))

(defn process-article
  "Parses an dsl.sk article with the given url and adds it to the database.
  Returns the 'id' of the created/updated article or nil if the url is not
  a dsl.sk article."
  [url]
  (let [html (parse url)]
    (when (is-article? html)
      (let [parsed (parse-article html)
            id (:id (article/find-record {:num (:num parsed)}))]
        (let [id (if id
                   (:id (article/update (assoc (select-keys parsed [:comment_count :first_comment_date :last_comment_date :total_comment_length]) :id id)))
                   (:id (article/create (select-keys parsed [:num :title :date :comment_count :first_comment_date :last_comment_date :total_comment_length]))))]
          (process-comments (:comments parsed) (:num parsed))
          id)))))

(defn process-dsl
  "Processes all dsl.sk articles with numbers in given range
  with process-article function. If no argument is given,
  processes all the articles."
  ([]
    (process-dsl 1))
  ([from]
    (process-dsl from (last-article-num)))
  ([from to]
    (let [url "http://www.dsl.sk/article.php?article="]
      (doseq [num (range from (inc to))]
        (process-article (str url num))))))

(def pool (threadpool/t-pool :variable :size 8 :keepalive 500 :daemon true :prefix "dsl-crawler"))

(defn process-dsl-par
  "Processes dsl.sk articles with numbers in given range
  with process-article function. If no argument is given,
  processes all the articles."
  ([]
    (process-dsl-par 1))
  ([from]
    (process-dsl-par from (last-article-num)))
  ([from to]
    (let [url "http://www.dsl.sk/article.php?article="
          nums (range from (inc to))
          urls (map #(string/join [url %]) nums)]
      (doall (map (fn [u] (threadpool/submit pool #(process-article u))) urls)))))

(defn ended?
  "Returnw whether the specified threadpool has ended working."
  [p]
  (and (zero? (threadpool/active-tasks p))
       (== (threadpool/submitted-tasks p) (threadpool/completed-tasks p))))

(defn -main
  "Main method that will start the benchmark."
  [& args]
  ;(println "waiting 5sec...")
  ;(Thread/sleep 5000)
  (println "running...")
  (let [res (map deref (process-dsl-par 11925 12974))]
    (while (not (ended? pool))
      (Thread/sleep 2000)
      (println pool))
    (println "done.\nresults:" res)))
