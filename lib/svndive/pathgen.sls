(library (svndive pathgen)
         (export make-pathgen)
         (import (yuni scheme))

         
(define (pathgen rev)
  ;; Gen 4/3 decimal path
  (define (split str)
    (let ((len (string-length str)))
     (string-append
       (substring str 0 (- len 3))
       "/"
       (substring str (- len 3) len))))
  (split (let* ((numstr (number->string rev))
         (len (string-length numstr)))
    (if (> 7 len)
      (string-append (make-string (- 7 len) #\0) numstr)
      numstr))))

(define (make-pathgen basepath ext) ;; => ^(rev)
  (lambda (rev)
    (string-append basepath "/" (pathgen rev) ext)))         
)
