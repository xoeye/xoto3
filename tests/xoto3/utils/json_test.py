from datetime import datetime
import json

import pytest

from xoto3.utils.jsn import pre_json_dump


def test_pre_json_dump():
    d = dict(rang=range(10))
    pre = pre_json_dump(d)  # this won't fail, but it also won't transform the range
    with pytest.raises(TypeError):
        json.dumps(pre)

    d = dict(t=(1, 2, 4), lst=[1, 2, dict(st={1, 2, 2})])  # type: ignore
    pre = pre_json_dump(d)
    assert '{"t": [1, 2, 4], "lst": [1, 2, {"st": [1, 2]}]}' == json.dumps(pre)

    d2 = dict(dt=datetime(2019, 6, 27, 15, 34, 22), s="string", Set={1, 2, 3})
    j = pre_json_dump(d2)
    assert j["dt"] == "2019-06-27T15:34:22.000000Z"
    assert j["s"] == "string"
    assert j["Set"] == [1, 2, 3]
