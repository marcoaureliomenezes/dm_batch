import builtins
a = 1
print(type(a))


b = getattr(builtins, 'str')(a)

print(type(b))

