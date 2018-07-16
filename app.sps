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
        (svndive pathgen)
        (svndive propfile))

;(define pg (make-pathgen "/mnt/c/cygwin64/home/oku/repos/mksvnmeta/wrk" ".prop.txt"))
(define pg (make-pathgen "c:/cygwin64/home/oku/repos/mksvnmeta/wrk" ".prop.txt"))

(define OUTFILE "out.bin")
(define END 262492)
;(define END 50000)

(define (make-work-queue)
  (let loop ((cur '())
             (idx 1))
    (let ((next (+ idx 100)))
     (cond
       ((< next END) (loop (cons (cons idx next) cur)
                           (+ 1 next)))
       (else (cons (cons idx END) cur))))))

(define (xform-propfile! prop replace!)
  (when (pair? prop)
    (let nodeloop ((q prop))
     (let ((node (car q))
           (next (cdr q)))
       (let proploop ((l node))
        (when (pair? l)
          (cond
            ((and (pair? (car l)) (or (string? (caar l)) (symbol? (caar l))))
             (replace! (car l)))
            ((and (pair? (car l)) (pair? (caar l)))
             (proploop (car l))))
          (proploop (cdr l))))
       (when (pair? next)
         (nodeloop next))))))

(let ((v (make-vector (+ END 1) #f))
      (out (make-vector (+ END 1) #f))
      (htv (make-vector (+ END 1) #f))
      (mtx (make-mutex))
      (wrkq (make-work-queue))
      (donemtx (make-mutex))
      (donecnd (make-condition))
      (thrdone 0)
      (thrcount 20))
 (define (thread-run ht ident)

   (let ((cnt 0)
         (my-work (with-mutex mtx
                              (and (pair? wrkq)
                                   (let ((mine (car wrkq)))
                                    (set! wrkq (cdr wrkq))
                                    mine)))))
     (define (compress! p)
       (define (gen-slash-loc-list str)
         (let ((len (string-length str)))
          (let loop ((idx 0)
                     (cur '()))
            (if (= idx len)
              cur
              (let ((c (string-ref str idx)))
               (loop (+ idx 1)
                     (if (char=? #\/ c)
                       (cons idx cur)
                       cur)))))))
       (define (split-path e)
         (let ((loc* (gen-slash-loc-list e)))
          (let loop ((q loc*)
                     (pos (string-length e))
                     (cur '()))
            (if (pair? q)
              (let ((start (car q))
                    (next (cdr q)))
                (loop next start (cons (substring e start pos) cur)))
              (cons (substring e 0 pos) cur)))))
       (define (xform-path e) ;; FIXME: Unused. Inefficient.
         (let ((p* (map xform (split-path e))))
          (list->vector p*)))
       (define (xform e)
         (cond
           ((string? e)
            (let ((i (hashtable-ref ht e #f)))
             (cond
               (i i)
               (else 
                 (let ((mycnt cnt))
                  (hashtable-set! ht e mycnt)
                  (set! cnt (+ mycnt 1))
                  mycnt)))))
           ((symbol? e) #f)
           (else (error "Invalid pair" p))))
       (let ((a (car p))
             (b (cdr p)))
         (cond ((and (symbol? a) 
                     (or (eq? 'Text-content-length a)
                         (eq? 'Prop-content-length a)
                         (eq? 'Content-length a)))
                (unless (string? (cdr p))
                  (error "Invalid pair??" p))
                ;; We no longer need any length informations
                (set-cdr! p #f))
               (else
                 (let ((ea (xform a))
                       (eb (xform b)))
                   (when ea (set-car! p ea))
                   (when eb (set-cdr! p eb)))))))

     (cond (my-work
             (let ((start (car my-work))
                   (end (cdr my-work)))
               (let loop ((rev start))
                (vector-set! htv rev ht)
                (let ((bv (file->bytevector (pg rev))))
                 (when (= 0 (modulo rev 1000))
                   (display "Pass2: rev.")
                   (display rev)
                   (newline))
                 (let ((p (bv->propfile bv)))
                  (xform-propfile! p compress!)
                  (vector-set! out rev p)))
                (unless (= end rev)
                  (loop (+ rev 1)))))
             (thread-run ht ident))
           (else
             (with-mutex donemtx
                         (set! thrdone (+ 1 thrdone))
                         (display "Pass2: Done ")
                         (display thrdone)
                         (display " ")
                         (display ident)
                         (newline)
                         (condition-signal donecnd))))))

 (let ((arg* (let loop ((i 0)
                        (arg* '()))
               (if (= thrcount i) 
                 arg* 
                 (loop (+ i 1) 
                       (cons (cons i (make-string-hashtable)) arg*))))))
   (for-each (lambda (e) 
               (fork-thread (lambda () (thread-run (cdr e) (car e))))) 
             arg*)

   ;; Wait for completion
   (let loop ()
    (let ((do-loop #t))
     (with-mutex donemtx
                 (cond
                   ((= thrcount thrdone)
                    (set! do-loop #f))
                   (else
                     (condition-wait donecnd donemtx))))
     (when do-loop (loop)))))


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

