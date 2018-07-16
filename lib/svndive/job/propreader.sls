(library (svndive job propreader)
         (export make-propreader
                 propreader-result
                 propreader-result-ht
                 propreader-enter!
                 propreader-gen-mapping!
                 propreader-map-result!
                 )
         (import (yuni scheme)
                 (yuni hashtables)
                 (svndive util-propfile)
                 (svndive propfile))


(define (make-propreader) ;; => ((ht . result) . enter!)
  (let* ((result* '())
         (ht (make-string-hashtable))
         (compress! (make-propfile-compress ht)))
    (define (enter! rev bv)
      (let ((p (bv->propfile bv)))
       (xform-propfile! p compress!)
       (set! result* (cons (cons rev p) result*))))
    (define (result) result*)
    (cons (cons ht result) enter!)))

(define (propreader-enter! rdr rev bv)
  ((cdr rdr) rev bv))

(define (propreader-result rdr)
  ((cdar rdr)))
(define (propreader-result-ht rdr)
  (caar rdr))

(define (propreader-gen-mapping! master-ht master-max result result-ht) 
  ;; => max map-ht
  (let ((mymax master-max)
        (mapping-ht (make-integer-hashtable)))
    (hashtable-for-each
      (lambda (k v)
        (let ((m (hashtable-ref master-ht k #f)))
         (cond
           (m (hashtable-set! mapping-ht v m))
           (else
             (let ((x mymax))
              (set! mymax (+ x 1))
              (hashtable-set! mapping-ht v x)
              (hashtable-set! master-ht k x))))))
      result-ht)
    (values mymax mapping-ht)))

(define (propreader-map-result! mapping-ht result)
  (define (ref i)
    (let ((f (hashtable-ref mapping-ht i #f)))
     (unless (integer? f)
       (error "Invalid mapping" i f))
     f))
  (define (xform! p)
    (let ((a (car p))
          (b (cdr p)))
      (when (integer? a)
        (set-car! p (ref a)))
      (when (integer? b)
        (set-cdr! p (ref b)))))
  (xform-propfile! result xform!))

)
