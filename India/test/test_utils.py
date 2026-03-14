from src.utils.cf_email import decode_cfemail


def _encode_cfemail(email: str, key: int = 0x12) -> str:
    encoded = [f"{key:02x}"]
    for ch in email:
        encoded.append(f"{(ord(ch) ^ key):02x}")
    return "".join(encoded)


def test_decode_cfemail():
    email = "test@example.com"
    encoded = _encode_cfemail(email)
    assert decode_cfemail(encoded) == email
