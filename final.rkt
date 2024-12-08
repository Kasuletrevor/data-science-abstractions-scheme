#lang racket
(require data-science-master
         plot
         srfi/19)
(require (only-in srfi/19 string->date))

;; ----------------------------------------
;; Data Loading Abstractions
;; ----------------------------------------

;; Preserve quotes in strings
(define (convert-row-elements-to-strings row)
  (map (lambda (element)
         (if (string? element) (string-trim element) (symbol->string element)))
       row))

;; Load CSV tweets and preprocess into a desired format
(define (load-tweets-from-csv file-path)
  (let* ([csv-data (read-csv file-path #:header? #t #:->number? #f)]
         [tweet-rows 
          (map (lambda (row)
                 (list 
                  (first row)    ; Tweet text
                  (second row)   ; Country
                  (third row)))  ; Date
               (cdr csv-data))])
    tweet-rows))

;; ----------------------------------------
;; Filtering Abstractions
;; ----------------------------------------

;; Filter tweets by country and date range
(define (filter-by-country-and-date tweets country start-date end-date)
  (define start-julian (date->julian-day (string->date start-date "~Y-~m-~d")))
  (define end-julian (date->julian-day (string->date end-date "~Y-~m-~d")))
  (filter (lambda (tweet)
            (and (list? tweet)
                 (= (length tweet) 3)
                 (string? (list-ref tweet 1))
                 (string? (list-ref tweet 2))
                 (string=? (list-ref tweet 1) country)
                 (let ([tweet-date (string->date (list-ref tweet 2) "~Y-~m-~d")])
                   (and tweet-date
                        (<= (date->julian-day tweet-date) end-julian)
                        (>= (date->julian-day tweet-date) start-julian)))))
          tweets))

;; ----------------------------------------
;; Analysis Abstractions
;; ----------------------------------------

;; Combine all tweet texts into a single document
(define (combine-tweet-texts tweets)
  (string-join (map (λ (tweet) (list-ref tweet 0)) tweets) " "))

;; Tokenize combined text and calculate word frequencies
(define (tokenize-and-count-with-doc tweets)
  (document->tokens (combine-tweet-texts tweets) #:sort? #t))

;; Perform sentiment analysis on tokenized data
(define (analyze-sentiments tokens)
  (filter
   (λ (entry) 
     (and (list? entry)
          (= (length entry) 3)
          (number? (list-ref entry 2)))) ; Ensure frequency is numeric
   (list->sentiment tokens #:lexicon 'nrc)))

;; Aggregate sentiments by type
(define (aggregate-sentiments sentiments)
  (let ([aggregated (make-hash)])
    (for-each
     (λ (sentiment)
       (let* ([sentiment-type (list-ref sentiment 1)]
              [freq (list-ref sentiment 2)]
              [existing (hash-ref aggregated sentiment-type 0)])
         (hash-set! aggregated sentiment-type (+ existing freq))))
     sentiments)
    aggregated))

;; ----------------------------------------
;; Visualization Abstractions
;; ----------------------------------------

;; Visualize sentiments for a specific month
(define (visualize-sentiments-for-month sentiments-by-month selected-month)
  (define month-sentiments (hash-ref sentiments-by-month selected-month (hash)))
  (define data (hash->list month-sentiments))
  (define formatted-data (map (λ (pair) (list (car pair) (cdr pair))) data))
  (parameterize ([plot-width 800]
                 [plot-height 500]
                 [plot-x-label "Sentiment"]
                 [plot-y-label "Frequency"]
                 [plot-title (string-append "Sentiment Analysis for " selected-month)])
    (plot
     (discrete-histogram
      formatted-data
      #:color "Blue"
      #:line-color "Black"))))

;; Visualize sentiments across all months
(define (visualize-sentiments sentiments-by-month)
  ;; Generate months in the format "2024-01" to "2024-12"
  (define all-months
    (for/list ([n (in-range 1 13)])
      (string-append "2024-" (if (< n 10) (string-append "0" (number->string n)) (number->string n)))))

  ;; Convert `sentiments-by-month` to a mutable hash table
  (define mutable-sentiments (make-hash (hash->list sentiments-by-month)))

  ;; Ensure every month exists in the mutable hash table with default empty data
  (for ([month all-months])
    (unless (hash-has-key? mutable-sentiments month)
      (hash-set! mutable-sentiments month #hash())))

  ;; Visualization
  (parameterize ([plot-width 1000]
                 [plot-height 600]
                 [plot-x-label "Sentiment"]
                 [plot-y-label "Frequency"]
                 [plot-title "Sentiment Analysis Across All Months"])
    (plot
     (for/list ([month all-months])
       (let* ([month-sentiments (hash-ref mutable-sentiments month #hash())]
              [data (hash->list month-sentiments)] ; Convert hash to list
              [formatted-data (map (λ (pair) (list (car pair) (cdr pair))) data)]) ; Format data
         (discrete-histogram
          formatted-data
          #:color (cond
                    [(string=? month "2024-01") "Blue"]
                    [(string=? month "2024-02") "Green"]
                    [(string=? month "2024-03") "Red"]
                    [(string=? month "2024-04") "Yellow"]
                    [(string=? month "2024-05") "Purple"]
                    [(string=? month "2024-06") "Cyan"]
                    [(string=? month "2024-07") "Orange"]
                    [(string=? month "2024-08") "Magenta"]
                    [(string=? month "2024-09") "Pink"]
                    [(string=? month "2024-10") "Brown"]
                    [(string=? month "2024-11") "Teal"]
                    [(string=? month "2024-12") "Gray"]
                    [else "Black"])
          #:line-color "Black"
          #:label month))))))

;; ----------------------------------------
;; Main Pipeline
;; ----------------------------------------

(define (main file-path country plot-param)
  ;; Step 1: Load tweets
  (define tweets (load-tweets-from-csv file-path))
  
  ;; Step 2: Filter tweets for the specified country and date range
  (define start-date "2024-01-01")
  (define end-date "2024-12-31")
  (define filtered-tweets (filter-by-country-and-date tweets country start-date end-date))
  
  ;; Step 3: Group tweets by month
  (define grouped-tweets
    (let ([grouped (make-hash)])
      (for-each
       (λ (tweet)
         (let* ([date (string->date (list-ref tweet 2) "~Y-~m-~d")]
                [month (date->string date "~Y-~m")]
                [existing (hash-ref grouped month '())])
           (hash-set! grouped month (cons tweet existing))))
       filtered-tweets)
      grouped))
  
  ;; Step 4: Analyze sentiments for each month
  (define sentiments-by-month
    (for/hash ([month (hash-keys grouped-tweets)])
      (values month 
              (aggregate-sentiments
               (analyze-sentiments
                (tokenize-and-count-with-doc (hash-ref grouped-tweets month)))))))
  
  ;; Step 5: Plot sentiments based on the parameter
  (cond
    [(string=? plot-param "all") (visualize-sentiments sentiments-by-month)]
    [else (visualize-sentiments-for-month sentiments-by-month plot-param)]))

;; ----------------------------------------
;; Run Main
;; ----------------------------------------

;(main "tweets_with_quotes.csv" "Uganda" "all")
(main "tweets_with_quotes.csv" "Uganda" "2024-07")
