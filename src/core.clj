(ns core
  (:require [cheshire.core :as json]
            [clojure.string :as s]
            [clojure.java.shell :as shell]
            [java-time :as t])
  (:gen-class))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; Custom variables

(def csv-output-dir "covid19-orientation-csv")
(def zip-output-dir "covid19-orientation-zip")

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; Main core

;; There is no data before Apr. 8th
(def start-day (t/local-date-time 2020 4 8))

;; Get the home directory
(def home-dir (System/getenv "HOME")) 

;; Set the absolute formatting string for csv file names
(def csv-fname-fstring
  (str home-dir "/" csv-output-dir "/" "SIG_%s_Anonymous.csv"))

;; Set the formatting string for raw csv file names
(def csv-fname-nodir-fstring "SIG_%s_Anonymous.csv")

;; Set the zip file name
(def zip-fname (format "%s/%s/SIG-orientation-covid19-%s.zip"
                       home-dir zip-output-dir (t/local-date)))

;; A string with the csv header
(def csv-header
  (str (s/join "," ["algo_version" "form_version" "date" "duration" "postal_code" "age_range" "sore_throat_aches" "agueusia_anosmia" "breathlessness" "cough" "diarrhea" "tiredness" "tiredness_details" "imc" "breathing_disease" "cancer" "diabetes" "feeding_day" "kidney_disease" "liver_disease" "pregnant" "temperature_cat" "fever" "fever_algo" "heart_disease" "heart_disease_algo" "immunosuppressant_disease" "immunosuppressant_disease_algo" "immunosuppressant_drug" "immunosuppressant_drug_algo"]) "\n"))

;; Convert a timestamp ts to Europe/Paris time
(defn iso-utc-to-europe-paris [ts]
  (t/instant
   (t/with-zone (t/zoned-date-time ts)
     "Europe/Paris")))

;; Convert a json line into a csv line
(defn json-line-to-csv [json-line]
  (let [data        (:data json-line)
        {:keys [respondent metadata symptoms
                calculations risk_factors]}
        data
        {:keys [date duration algo_version
                form_version orientation]}
        metadata
        {:keys [imc age_range postal_code]}
        respondent
        {:keys [cough fever diarrhea tiredness
                tiredness_details sore_throat_aches
                feeding_day temperature_cat
                breathlessness agueusia_anosmia]}
        symptoms
        {:keys [pregnant cancer diabetes
                liver_disease kidney_disease
                heart_disease breathing_disease
                immunosuppressant_drug
                immunosuppressant_disease]}
        risk_factors
        {:keys [fever_algo heart_disease_algo
                immunosuppressant_drug_algo
                immunosuppressant_disease_algo
                ]}
        calculations
        postal_code (try (subs postal_code 0 2)
                         (catch Exception _ "XX"))]
    (str (s/join "," [algo_version form_version
                      date duration postal_code age_range
                      sore_throat_aches agueusia_anosmia
                      breathlessness cough diarrhea
                      tiredness tiredness_details
                      imc breathing_disease cancer
                      diabetes feeding_day
                      kidney_disease liver_disease
                      pregnant temperature_cat fever
                      fever_algo heart_disease
                      heart_disease_algo
                      immunosuppressant_disease
                      immunosuppressant_disease_algo
                      immunosuppressant_drug
                      immunosuppressant_drug_algo])
         "\n")))

;; Get the last week days csv file names for later zipping
(def last-week-days
  (for [n (range 7)]
    (format csv-fname-nodir-fstring
            (t/minus (t/truncate-to (t/local-date-time) :days)
                     (t/days (inc n))))))

;; Generate the csv files between now and start-day
(defn generate-csv-files []
  (dotimes [n (t/time-between start-day (t/local-date-time) :days)]
    (spit (format csv-fname-fstring
                  (t/plus start-day (t/days n)))
          csv-header)))

;; Dispatch the json line from the input file into csv files
(defn dispatch [f]
  (with-open [rdr (clojure.java.io/reader f)]
    (doseq [line (line-seq rdr)]
      (let [jline (json/parse-string line true)
            date  (str (-> jline :data :metadata :date
                           iso-utc-to-europe-paris))
            fname (format csv-fname-fstring
                          (str (last (re-matches
                                      #".*(\d{4}-\d{2}-\d{2}).*" date))
                               "T00:00"))]
        (spit fname (json-line-to-csv jline) :append true)))))

;; Put it all together:
;; - create the csv files
;; - dispatch json data into csv files
;; - put last week csv files into a zip file
(defn -main [& [input-file]]
  (generate-csv-files)
  (dispatch input-file)
  (shell/with-sh-dir (str home-dir "/" csv-output-dir)
    (apply shell/sh (flatten ["zip" "-r" zip-fname last-week-days])))
  (System/exit 0))
