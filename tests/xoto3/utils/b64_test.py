from xoto3.utils.b64 import obj_to_base64, base64_to_obj


def test_decoding():
    obj = dict(birthday=[2014, 12, 22, 6, 42, 11], age="Steve")
    assert base64_to_obj(obj_to_base64(obj)) == obj
    assert base64_to_obj(obj_to_base64(dict())) == dict()
