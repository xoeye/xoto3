from xoto3.utils.lazy import Lazy


def test_lazy():
    cont = dict(done=False)

    def sentinel():
        assert not cont["done"], "This got called twice!!"
        cont["done"] = True
        return 3

    obj = Lazy(sentinel)
    assert not cont["done"]
    assert obj() == 3
    assert cont["done"]

    assert obj() == 3
