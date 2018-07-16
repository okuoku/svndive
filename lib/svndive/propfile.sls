(library (svndive propfile)
         (export bv->propfile)
         (import (yuni scheme))

(define CR 13)
(define LF 10)
(define COLON 58)
(define SPACE 32)
(define K 75)
(define V 86)
(define P 80)
(define PROPS-END (string->utf8 "PROPS-END"))
(define (ascii->string bv start end)
  (let loop ((idx start)
             (cur '()))
   (if (= idx end)
     (list->string (map (lambda (e) (integer->char e)) (reverse cur)))
     (loop (+ 1 idx)
           (cons (bytevector-u8-ref bv idx) cur)))))

(define (bv-range-is-PROPS-END? bv start)
  (let loop ((cur 0))
   (if (= (bytevector-u8-ref PROPS-END cur)
          (bytevector-u8-ref bv (+ start cur)))
     (if (= cur 8)
       #t
       (loop (+ cur 1)))
     #f)))

(define (parse-kv-header bv start) ;; => end K/V size
  (let ((type #f)
        (end #f))
    (let ((c (bytevector-u8-ref bv start)))
     (cond
       ((= c K) (set! type 'K))
       ((= c V) (set! type 'V))
       ((= c P)
        ;; Check for "PROPS-END"
        (unless (bv-range-is-PROPS-END? bv start)
          (error "Invalid PROPS-END tag" bv start))
        (set! type 'PROPS-END))
       (else (error "Invalid K/V symbol" start (integer->char c))))
     (cond
       ((eq? type 'PROPS-END)
        ;; Special handling for PROPS-END
        (values (+ start 10) type 0))
       (else
         (unless (= SPACE (bytevector-u8-ref bv (+ 1 start)))
           (error "Invalid K/V sep" (+ 1 start)))
         (let loop ((cur (+ 2 start)))
          (let ((c (bytevector-u8-ref bv cur)))
           (cond
             ((= c LF)
              (set! end cur))
             (else (loop (+ cur 1))))))
         (let ((str (ascii->string bv (+ start 2) end)))
          (values end type (string->number str))))))))

(define (parse-mimelike-header bv start) ;; => end key value / end #f #f
  (let ((key #f)
        (value-start #f))
    (let loop ((cur start))
     (let ((c (bytevector-u8-ref bv cur)))
      (cond
        ((= c LF)
         (cond
           ((and (= start cur) (not key))
            (values (+ start 1) #f #f))
           (key
             (let ((str (ascii->string bv value-start cur)))
              (values (+ cur 1) key str)))
           (else
             (error "Invalid MIME header sequence" start))))
        ((and (not key) (= c COLON))
         (let ((sym (string->symbol (ascii->string bv start cur))))
          (set! key sym)
          (set! value-start (+ cur 2))
          (loop (+ cur 2))))
        (else
          (loop (+ cur 1))))))))

(define (bv-range->propkv* bv start)
  ;; => ((key . "value") ...)
  (let loop ((pos start)
             (cur '())
             (key #f))
   (let-values (((next type num) (parse-kv-header bv pos)))
               (case type
                 ((PROPS-END) 
                  (when key
                    (error "Stray key in property" start))
                  (reverse cur))
                 ((K) 
                  (when key
                    (error "Invalid KV sequence" start))
                  (let* ((keystart (+ next 1)) ;; Consume \n
                         (keyend (+ next num 1)) ;; Consume \n
                         (sym (string->symbol (ascii->string bv keystart keyend))))
                    (loop (+ keyend 1) cur sym)))
                 ((V)
                  (unless key
                    (error "Invalid VV sequence" start))
                  (let* ((valuestart (+ next 1)) ;; Consume \n
                         (valueend (+ next num 1)) ;; Consume \n
                         (str (ascii->string bv valuestart valueend)))
                    (loop (+ valueend 1) (cons (cons key str) cur) #f)))
                 (else (error "???"))))))

(define (bv->flat-propfile/reverse bv)
  (let ((end (bytevector-length bv))
        (prop-size #f))
    (let loop ((pos 0)
               (cur '()))
      (cond
        ((= pos end) cur)
        (else
          (let-values (((next key value) (parse-mimelike-header bv pos)))
                      (case key
                        ((Node-path)  ;; Begin node processing
                         (set! prop-size #f)
                         (loop next (cons (cons key value) cur)))
                        ((Prop-content-length) 
                         (set! prop-size (string->number value))
                         (loop next (cons (cons key value) cur)))
                        ((#f) ;; Begin property section
                         (cond
                           (prop-size
                             (let ((loopnext (+ next prop-size)))
                              (set! prop-size #f)
                              (loop loopnext 
                                    (cons (bv-range->propkv* bv next)
                                          cur))))
                           (else
                             ;; Ignore empty line
                             (loop next cur))))
                        (else
                          (loop next (cons (cons key value) cur))))))))))

(define (bv->propfile bv)
  (let ((l (bv->flat-propfile/reverse bv)))
   ;; Collect "Node-path" header
   (let nodeloop ((nodes '())
                  (queue l))
     (let loop ((cur '())
                (q queue))
       (cond
         ((null? q)
          (unless (null? cur)
            (error "Extra headers" cur))
          nodes)
         (else
           (let ((e (car q)))
            (cond
              ((and (pair? e) (symbol? (car e)))
               (case (car e)
                 ((Node-path)
                  (nodeloop (cons (cons e cur) nodes) (cdr q)))
                 (else
                   (loop (cons e cur) (cdr q)))))
              (else
                (loop (cons e cur) (cdr q))))))))))) 
)
