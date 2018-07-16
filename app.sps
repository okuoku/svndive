(import (yuni scheme)
        (yuni hashtables)
        (yuni util files)
        (yuni serialize)
        (only (chezscheme)
              fork-thread
              make-condition
              condition-wait
              condition-signal
              make-mutex
              with-mutex)
        (svndive job propreader)
        (svndive util-propfile)
        (svndive pathgen)
        (svndive propfile))

;(define pg (make-pathgen "/mnt/c/cygwin64/home/oku/repos/mksvnmeta/wrk" ".prop.txt"))
(define pg (make-pathgen "c:/cygwin64/home/oku/repos/mksvnmeta/wrk" ".prop.txt"))

(define OUTFILE "out.bin")
(define END 262492)
;(define END 50000)

(define THRCOUNT 20)

(define (make-work-queue)
  (let loop ((cur '())
             (idx 1))
    (let ((next (+ idx 100)))
     (cond
       ((< next END) (loop (cons (cons idx next) cur)
                           (+ 1 next)))
       (else (cons (cons idx END) cur))))))

(let ((v (make-vector (+ END 1) #f))
      (out (make-vector (+ END 1) #f))
      (mtx (make-mutex))
      (wrkq (make-work-queue))
      (donemtx (make-mutex))
      (donecnd (make-condition))
      (master-ht (make-string-hashtable))
      (master-count 0)
      (thrresult (make-vector THRCOUNT #f))
      (thrdone 0))
 (define (thread-run rdr ident)

   (let ((my-work (with-mutex mtx
                              (and (pair? wrkq)
                                   (let ((mine (car wrkq)))
                                    (set! wrkq (cdr wrkq))
                                    mine)))))

     (cond (my-work
             (let ((start (car my-work))
                   (end (cdr my-work)))
               (let loop ((rev start))
                (let ((bv (file->bytevector (pg rev))))
                 (when (= 0 (modulo rev 1000))
                   (display "Pass2: rev.")
                   (display rev)
                   (newline))
                 (propreader-enter! rdr rev bv))
                (unless (= end rev)
                  (loop (+ rev 1)))))
             (thread-run rdr ident))
           (else
             (with-mutex donemtx
                         (set! thrdone (+ 1 thrdone))
                         (vector-set! thrresult 
                                      ident
                                      (cons
                                        (propreader-result rdr)
                                        (propreader-result-ht rdr)))
                         (display "Pass2: Done ")
                         (display thrdone)
                         (display " ")
                         (display ident)
                         (newline)
                         (condition-signal donecnd))))))

 (let ((arg* (let loop ((i 0)
                        (arg* '()))
               (if (= THRCOUNT i) 
                 arg* 
                 (loop (+ i 1) 
                       (cons (cons i (make-propreader)) arg*))))))
   (for-each (lambda (e) 
               (fork-thread (lambda () (thread-run (cdr e) (car e))))) 
             arg*)

   ;; Wait for completion
   (let loop ()
    (let ((do-loop #t))
     (with-mutex donemtx
                 (cond
                   ((= THRCOUNT thrdone)
                    (set! do-loop #f))
                   (else
                     (condition-wait donecnd donemtx))))
     (when do-loop (loop)))))

 ;; Merge
 (let loop ((idx 0))
  (unless (= idx THRCOUNT)
    (let ((e (vector-ref thrresult idx)))
     (when e
       (let ((result (car e))
             (ht (cdr e)))
         (call-with-values
           (lambda ()
             (display "Start mapping..\n")
             (propreader-gen-mapping! master-ht master-count result ht))
           (lambda (next mapping-ht)
             (display "Remap..\n")
             (display "From = ")
             (display master-count)
             (newline)
             (propreader-map-result! mapping-ht result)
             (set! master-count next)
             (display "To = ")
             (display master-count)
             (newline))))))
    (loop (+ idx 1))))

 (let loop ((idx 0))
   (unless (= idx THRCOUNT)
     (display (list "Distrib..." idx))
     (newline)
     (let ((e (vector-ref thrresult idx)))
      (when e
        (let ((result (car e)))
         (for-each (lambda (r)
                     (let ((rev (car r))
                           (res (cdr r)))
                       (vector-set! out rev res)))
                   result))))
     (loop (+ idx 1))))

 ;; Output
 (when (file-exists? OUTFILE)
   (delete-file OUTFILE))
 (let ((p (open-binary-output-file OUTFILE)))
  (let block ((v (make-vector 1000 #f))
              (blockstart 1))
    (let loop ((idx blockstart)
               (off 0))
      (vector-set! v off (vector-ref out idx))
      (vector-set! out idx #f)
      (cond ((or (= idx END) (= off 999))
             (display (list 'WRITE: blockstart))
             (newline)
             (put-object/serialize p v)
             (let ((next (+ blockstart (vector-length v))))
              (unless (< END next)
                (block (make-vector 1000 #f) next))))
            (else
              (loop (+ idx 1) (+ off 1))))))
  (close-port p)))

