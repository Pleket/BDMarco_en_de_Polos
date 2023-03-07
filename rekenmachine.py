lst = [63, 52, 31, 58, 35, 48, 48, 46, 44, 64, 31, 57, 62, 58, 22, 58, 40, 63, 45, 41]

quadratic = 0
mu = 0
for value in lst:
    quadratic += value**2
    mu += value / len(lst)

print( quadratic / len(lst) - mu**2)


