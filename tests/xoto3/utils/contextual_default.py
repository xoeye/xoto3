from xoto3.utils.contextual_default import ContextualDefault

IntDefault = ContextualDefault("i", 1)


def test_that_the_name_is_used_and_everything_works():
    @IntDefault.apply
    def f(a: str, i: int = 2):
        return i

    assert f("a") == 1
    with IntDefault.set_default(4):
        assert f("b") == 4
        with IntDefault.set_default(7):
            assert f("c") == 7
            assert f("c", 8) == 8
        assert f("d") == 4
    assert f("e") == 1
    assert f("f", i=3) == 3
