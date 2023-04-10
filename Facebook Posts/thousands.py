import re

n = input ('Coloque um número grande sff:')

pattern = "(\d)(?=(\d{3})+(?!\d))"
repl = r"\1,"
string = str(n)

print(re.sub(pattern, repl, string))
