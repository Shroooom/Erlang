# Erlang

~HW1 for ProgLang~

Haskell representation of lambda expression 
data Lexp = Atom String | Lambda String Lexp | Apply Lexp  Lexp 

Given a filename and function for reducing lambda expressions, reduce all valid lambda expressions in the file with alpha, beta, and eta reductions and output results.
