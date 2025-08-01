operator_dunder_fns = [
    "lt",
    "le",
    "eq",
    "ne",
    "gt",
    "ge",
    "add",
    "sub",
    "mul",
    "truediv",
    "floordiv",
    "mod",
    "divmod",
    "pow",
    "radd",
    "rsub",
    "rmul",
    "rtruediv",
    "rfloordiv",
    "rmod",
    "rdivmod",
    "rpow",
    "and",
    "or",
    "xor",
    "lshift",
    "rshift",
    "rand",
    "ror",
    "rxor",
    "rlshift",
    "rrshift",
    "neg",
    "pos",
    "abs",
    "invert",
]


def build_capture_fn(name):
    def f(self, *args):
        return self.__class__(self._xcls_steps + [lambda v: getattr(v, name)(*args)])

    return f


class XMeta(type):
    # Ideally we could implement these in __getattr__ or by using __setattr__ in
    # __init__, but something about the implementation of operators makes it so it only
    # counts if the method is actually defined on the class, and this is the only way
    # to do that without typing everything a bunch of times.
    def __new__(mcs, name, bases, namespace, **kwargs):
        for x in operator_dunder_fns:
            namespace[f"__{x}__"] = build_capture_fn(f"__{x}__")
        return super().__new__(mcs, name, bases, namespace)


class XCls(metaclass=XMeta):
    def __init__(self, _xcls_steps=None):
        self._xcls_steps = _xcls_steps or []

    def __call__(self, *args, **kwargs):
        return XCls(self._xcls_steps + [lambda v: v(*args, **kwargs)])

    def __getattr__(self, name):
        return XCls(self._xcls_steps + [lambda v: getattr(v, name)])

    @property
    def l(self):
        def f(x):
            value = x
            for step in self._xcls_steps:
                value = step(value)
            return value

        return f


X = XCls()
