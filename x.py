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
    "radd",  # eg 3 + X
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
    "len",  # Doesn't handle len(X) like you'd expect, unfortunately. But X.__len__ works.
]


def _build_operator_capture_fn(name):
    def f(self, *args):
        # X + 1 means capture the .__add__ attr on X, then capture the call on it with arg 1.
        return self._xcls_capture_attr(name)._xcls_capture_call(args, {})

    return f


class _XClsMeta(type):
    # Ideally we could implement these in __getattr__ or by using __setattr__ in
    # __init__, but something about the implementation of operators makes it so it only
    # counts if the method is actually defined on the class, and metaclass is the only way
    # to do that without typing everything a bunch of times.
    def __new__(cls, name, bases, dct):
        for x in operator_dunder_fns:
            dct[f"__{x}__"] = _build_operator_capture_fn(f"__{x}__")
        return super().__new__(cls, name, bases, dct)


class _XCls(metaclass=_XClsMeta):
    def __init__(self, n_args, steps):
        self._xcls_n_args = n_args
        self._xcls_steps = steps

    def __call__(self, *args, **kwargs):
        return self._xcls_capture_call(args, kwargs)

    def __getattr__(self, name):
        return self._xcls_capture_attr(name)

    def l(self, *args):
        assert len(args) == self._xcls_n_args
        return self._xcls_resolve(
            args[0], args[1] if len(args) > 1 else None, args[2] if len(args) > 2 else None
        )

    def _xcls_clone(self, step):
        return _XCls(self._xcls_n_args, self._xcls_steps + [step])

    def _xcls_capture_call(self, args, kwargs):
        # Do this up here instead of in the lambda to minimize work we're doing per call
        arg_indexes_to_sub = set()
        kwarg_keys_to_sub = set()
        for i, a in enumerate(args):
            if isinstance(a, self.__class__):
                assert a._xcls_n_args == self._xcls_n_args
                arg_indexes_to_sub.add(i)
        for k, v in kwargs.items():
            if isinstance(v, self.__class__):
                assert v._xcls_n_args == self._xcls_n_args
                kwarg_keys_to_sub.add(k)

        return self._xcls_clone(
            # eg X2.fn(Y2.abc) needs Y2.abc to resolve before calling .fn
            lambda x, y, z, v: v(
                *[
                    (a._xcls_resolve(x, y, z) if i in arg_indexes_to_sub else a)
                    for i, a in enumerate(args)
                ],
                **{
                    k: (a._xcls_resolve(x, y, z) if k in kwarg_keys_to_sub else a)
                    for k, a in kwargs.items()
                },
            )
        )

    def _xcls_capture_attr(self, name):
        return self._xcls_clone(lambda x, y, z, v: getattr(v, name))

    def _xcls_resolve(self, x, y, z):
        value = None
        for step in self._xcls_steps:
            value = step(x, y, z, value)
        return value


X = _XCls(1, [lambda x, y, z, v: x])
X2 = _XCls(2, [lambda x, y, z, v: x])
Y2 = _XCls(2, [lambda x, y, z, v: y])
X3 = _XCls(3, [lambda x, y, z, v: x])
Y3 = _XCls(3, [lambda x, y, z, v: y])
Z3 = _XCls(3, [lambda x, y, z, v: z])
