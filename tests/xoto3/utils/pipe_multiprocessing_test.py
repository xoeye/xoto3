import xoto3.utils.pipe_multiprocessing as xpm


def ident(x):
    return x


def test_pool():
    pool = xpm.PipedProcessPool()

    results = set(pool.map(ident, range(123), 9))

    assert results == set(range(123))

    result2 = sorted(list(pool.map(ident, ["{}".format(i) for i in range(9)], 2)))
    assert result2 == ["0", "1", "2", "3", "4", "5", "6", "7", "8"]
