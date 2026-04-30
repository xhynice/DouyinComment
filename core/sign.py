"""
纯 Python 签名实现

将 douyin.js 的 SM3 + RC4 + 自定义Base64 签名逻辑完整翻译为 Python，
彻底消除 Node.js 子进程依赖。

算法流程:
1. SM3 哈希 (国密算法，类似 SHA-256)
2. RC4 加密
3. 自定义 Base64 编码 (非标准替换表)
4. a_bogus 签名组装
"""

import struct
import time
import random
import math
from typing import List


# ============================================================
# RC4 加密
# ============================================================

def rc4_encrypt(plaintext: str, key: str) -> str:
    s = list(range(256))
    j = 0
    for i in range(256):
        j = (j + s[i] + ord(key[i % len(key)])) % 256
        s[i], s[j] = s[j], s[i]

    i = j = 0
    cipher = []
    for k in range(len(plaintext)):
        i = (i + 1) % 256
        j = (j + s[i]) % 256
        s[i], s[j] = s[j], s[i]
        t = (s[i] + s[j]) % 256
        cipher.append(chr(s[t] ^ ord(plaintext[k])))
    return ''.join(cipher)


# ============================================================
# SM3 哈希 (国密算法)
# ============================================================

def _sm3_rotl(x: int, n: int) -> int:
    n = n % 32
    return ((x << n) | (x >> (32 - n))) & 0xFFFFFFFF


def _sm3_const_tj(j: int) -> int:
    if 0 <= j < 16:
        return 0x79CC4519
    elif 16 <= j < 64:
        return 0x7A879D8A
    return 0


def _sm3_ff_j(j: int, x: int, y: int, z: int) -> int:
    if 0 <= j < 16:
        return (x ^ y ^ z) & 0xFFFFFFFF
    elif 16 <= j < 64:
        return ((x & y) | (x & z) | (y & z)) & 0xFFFFFFFF
    return 0


def _sm3_gg_j(j: int, x: int, y: int, z: int) -> int:
    if 0 <= j < 16:
        return (x ^ y ^ z) & 0xFFFFFFFF
    elif 16 <= j < 64:
        return ((x & y) | ((~x) & z)) & 0xFFFFFFFF
    return 0


def _sm3_p0(x: int) -> int:
    return (x ^ _sm3_rotl(x, 9) ^ _sm3_rotl(x, 17)) & 0xFFFFFFFF


def _sm3_p1(x: int) -> int:
    return (x ^ _sm3_rotl(x, 15) ^ _sm3_rotl(x, 23)) & 0xFFFFFFFF


def _sm3_compress(v: List[int], block: bytes) -> List[int]:
    w = []
    for i in range(16):
        w.append(struct.unpack('>I', block[i*4:(i+1)*4])[0])

    for i in range(16, 68):
        a = w[i-16] ^ w[i-9] ^ _sm3_rotl(w[i-3], 15)
        a = _sm3_p1(a)
        w.append((a ^ _sm3_rotl(w[i-13], 7) ^ w[i-6]) & 0xFFFFFFFF)

    w1 = [(w[j] ^ w[j+4]) & 0xFFFFFFFF for j in range(64)]

    a, b, c, d, e, f, g, h = v

    for j in range(64):
        ss1 = _sm3_rotl(((_sm3_rotl(a, 12) + e + _sm3_rotl(_sm3_const_tj(j), j)) & 0xFFFFFFFF), 7)
        ss2 = (ss1 ^ _sm3_rotl(a, 12)) & 0xFFFFFFFF
        tt1 = (_sm3_ff_j(j, a, b, c) + d + ss2 + w1[j]) & 0xFFFFFFFF
        tt2 = (_sm3_gg_j(j, e, f, g) + h + ss1 + w[j]) & 0xFFFFFFFF
        d = c
        c = _sm3_rotl(b, 9)
        b = a
        a = tt1
        h = g
        g = _sm3_rotl(f, 19)
        f = e
        e = _sm3_p0(tt2)

    return [
        (v[0] ^ a) & 0xFFFFFFFF,
        (v[1] ^ b) & 0xFFFFFFFF,
        (v[2] ^ c) & 0xFFFFFFFF,
        (v[3] ^ d) & 0xFFFFFFFF,
        (v[4] ^ e) & 0xFFFFFFFF,
        (v[5] ^ f) & 0xFFFFFFFF,
        (v[6] ^ g) & 0xFFFFFFFF,
        (v[7] ^ h) & 0xFFFFFFFF,
    ]


def sm3_hash(data: bytes) -> List[int]:
    """SM3 哈希，返回 32 字节的整数列表"""
    iv = [
        0x7380166F, 0x4914B2B9, 0x172442D7, 0xDA8A0600,
        0xA96F30BC, 0x163138AA, 0xE38DEE4D, 0xB0FB0E4E,
    ]

    # 填充
    msg_len = len(data)
    data = bytearray(data)
    data.append(0x80)
    while len(data) % 64 != 56:
        data.append(0x00)
    bit_len = msg_len * 8
    data.extend(struct.pack('>Q', bit_len))

    # 压缩
    v = iv[:]
    for i in range(0, len(data), 64):
        v = _sm3_compress(v, data[i:i+64])

    # 输出整数列表 (32 bytes)
    result = []
    for val in v:
        result.append((val >> 24) & 0xFF)
        result.append((val >> 16) & 0xFF)
        result.append((val >> 8) & 0xFF)
        result.append(val & 0xFF)
    return result


def sm3_sum_hex(data: bytes) -> str:
    """SM3 哈希，返回 hex 字符串"""
    h = sm3_hash(data)
    return ''.join(f'{b:02x}' for b in h)


# ============================================================
# 自定义 Base64 编码
# ============================================================

BASE64_TABLES = {
    "s0": "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/=",
    "s1": "Dkdpgh4ZKsQB80/Mfvw36XI1R25+WUAlEi7NLboqYTOPuzmFjJnryx9HVGcaStCe=",
    "s2": "Dkdpgh4ZKsQB80/Mfvw36XI1R25-WUAlEi7NLboqYTOPuzmFjJnryx9HVGcaStCe=",
    "s3": "ckdp1h4ZKsUB80/Mfvw36XIgR25+WQAlEi7NLboqYTOPuzmFjJnryx9HVGDaStCe",
    "s4": "Dkdpgh2ZmsQB80/MfvV36XI1R45-WUAlEixNLwoqYTOPuzKFjJnry79HbGcaStCe",
}

BASE64_CONSTANTS = {
    "0": 16515072,
    "1": 258048,
    "2": 4032,
}


def result_encrypt(long_str: str, num: str = None) -> str:
    if num is None:
        # 标准 base64
        import base64
        return base64.b64encode(long_str.encode('latin-1')).decode()

    table = BASE64_TABLES[num]
    c = BASE64_CONSTANTS

    result = []
    lound = 0
    for i in range(len(long_str) // 3 * 4):
        if i // 4 != lound:
            lound += 1

        long_int = (ord(long_str[lound * 3]) << 16) | \
                   (ord(long_str[lound * 3 + 1]) << 8) | \
                   ord(long_str[lound * 3 + 2])

        key = i % 4
        if key == 0:
            temp_int = (long_int & c["0"]) >> 18
        elif key == 1:
            temp_int = (long_int & c["1"]) >> 12
        elif key == 2:
            temp_int = (long_int & c["2"]) >> 6
        else:
            temp_int = long_int & 63
        result.append(table[temp_int])

    return ''.join(result)


# ============================================================
# 签名生成
# ============================================================

def gener_random(r: int, option: List[int]) -> List[int]:
    r = int(r)
    return [
        (r & 255 & 170) | option[0] & 85,
        (r & 255 & 85) | option[0] & 170,
        (r >> 8 & 255 & 170) | option[1] & 85,
        (r >> 8 & 255 & 85) | option[1] & 170,
    ]


def generate_random_str() -> str:
    result = []
    result.extend(gener_random(random.randint(0, 9999), [3, 45]))
    result.extend(gener_random(random.randint(0, 9999), [1, 0]))
    result.extend(gener_random(random.randint(0, 9999), [1, 5]))
    return ''.join(chr(x) for x in result)


def generate_rc4_bb_str(url_search_params: str, user_agent: str,
                         window_env_str: str, suffix: str = "cus",
                         arguments: List[int] = None) -> str:
    if arguments is None:
        arguments = [0, 1, 14]

    # 三次 SM3 加密
    # 1: url_search_params 两次 SM3
    url_params_bytes = (url_search_params + suffix).encode('utf-8')
    sm3_once = bytes(sm3_hash(url_params_bytes))
    url_search_params_list = sm3_hash(sm3_once)

    # 2: 对后缀两次 SM3
    cus_bytes = suffix.encode('utf-8')
    cus_once = bytes(sm3_hash(cus_bytes))
    cus = sm3_hash(cus_once)

    # 3: 对 UA 处理之后的结果
    rc4_key = ''.join(chr(int(x)) for x in [0.00390625, 1, arguments[2]])
    ua_encrypted = rc4_encrypt(user_agent, rc4_key)
    ua_encoded = result_encrypt(ua_encrypted, "s3")
    ua = sm3_hash(ua_encoded.encode('utf-8'))

    # 时间戳
    start_time = int(time.time() * 1000)
    end_time = start_time  # Python 很快，同一毫秒

    # b 对象
    b = {
        8: 3,
        10: end_time,
        15: {
            "aid": 6383, "pageId": 6241, "boe": False, "ddrt": 7,
            "paths": {"include": [{} for _ in range(7)], "exclude": []},
            "track": {"mode": 0, "delay": 300, "paths": []},
            "dump": True, "rpU": "",
        },
        16: start_time,
        18: 44,
        19: [1, 0, 1, 5],
    }

    # start_time 字节分解
    b[20] = (b[16] >> 24) & 255
    b[21] = (b[16] >> 16) & 255
    b[22] = (b[16] >> 8) & 255
    b[23] = b[16] & 255
    b[24] = int(b[16] / 256 / 256 / 256 / 256)
    b[25] = int(b[16] / 256 / 256 / 256 / 256 / 256)

    # Arguments 字节分解
    b[26] = (arguments[0] >> 24) & 255
    b[27] = (arguments[0] >> 16) & 255
    b[28] = (arguments[0] >> 8) & 255
    b[29] = arguments[0] & 255

    b[30] = int(arguments[1] / 256) & 255
    b[31] = arguments[1] % 256 & 255
    b[32] = (arguments[1] >> 24) & 255
    b[33] = (arguments[1] >> 16) & 255

    b[34] = (arguments[2] >> 24) & 255
    b[35] = (arguments[2] >> 16) & 255
    b[36] = (arguments[2] >> 8) & 255
    b[37] = arguments[2] & 255

    # SM3 结果取特定字节
    b[38] = url_search_params_list[21]
    b[39] = url_search_params_list[22]
    b[40] = cus[21]
    b[41] = cus[22]
    b[42] = ua[23]
    b[43] = ua[24]

    # end_time 字节分解
    b[44] = (b[10] >> 24) & 255
    b[45] = (b[10] >> 16) & 255
    b[46] = (b[10] >> 8) & 255
    b[47] = b[10] & 255
    b[48] = b[8]
    b[49] = int(b[10] / 256 / 256 / 256 / 256)
    b[50] = int(b[10] / 256 / 256 / 256 / 256 / 256)

    # 配置项
    b[51] = b[15]['pageId']
    b[52] = (b[15]['pageId'] >> 24) & 255
    b[53] = (b[15]['pageId'] >> 16) & 255
    b[54] = (b[15]['pageId'] >> 8) & 255
    b[55] = b[15]['pageId'] & 255

    b[56] = b[15]['aid']
    b[57] = b[15]['aid'] & 255
    b[58] = (b[15]['aid'] >> 8) & 255
    b[59] = (b[15]['aid'] >> 16) & 255
    b[60] = (b[15]['aid'] >> 24) & 255

    # window_env_str
    window_env_list = [ord(c) for c in window_env_str]
    b[64] = len(window_env_list)
    b[65] = b[64] & 255
    b[66] = (b[64] >> 8) & 255

    b[69] = 0  # [].length
    b[70] = b[69] & 255
    b[71] = (b[69] >> 8) & 255

    # XOR 校验
    b[72] = (
        b[18] ^ b[20] ^ b[26] ^ b[30] ^ b[38] ^ b[40] ^ b[42] ^
        b[21] ^ b[27] ^ b[31] ^ b[35] ^ b[39] ^ b[41] ^ b[43] ^
        b[22] ^ b[28] ^ b[32] ^ b[36] ^ b[23] ^ b[29] ^ b[33] ^
        b[37] ^ b[44] ^ b[45] ^ b[46] ^ b[47] ^ b[48] ^ b[49] ^
        b[50] ^ b[24] ^ b[25] ^ b[52] ^ b[53] ^ b[54] ^ b[55] ^
        b[57] ^ b[58] ^ b[59] ^ b[60] ^ b[65] ^ b[66] ^ b[70] ^ b[71]
    )

    bb = [
        b[18], b[20], b[52], b[26], b[30], b[34], b[58], b[38],
        b[40], b[53], b[42], b[21], b[27], b[54], b[55], b[31],
        b[35], b[57], b[39], b[41], b[43], b[22], b[28], b[32],
        b[60], b[36], b[23], b[29], b[33], b[37], b[44], b[45],
        b[59], b[46], b[47], b[48], b[49], b[50], b[24], b[25],
        b[65], b[66], b[70], b[71],
    ]
    bb.extend(window_env_list)
    bb.append(b[72])

    return rc4_encrypt(''.join(chr(x) for x in bb), chr(121))


WINDOW_ENV = "1536|747|1536|834|0|30|0|0|1536|834|1536|864|1525|747|24|24|Win32"


def sign(url_search_params: str, user_agent: str, arguments: List[int]) -> str:
    result_str = generate_random_str() + generate_rc4_bb_str(
        url_search_params, user_agent, WINDOW_ENV, "cus", arguments
    )
    return result_encrypt(result_str, "s4") + "="


def sign_datail(params: str, user_agent: str) -> str:
    return sign(params, user_agent, [0, 1, 14])


def sign_reply(params: str, user_agent: str) -> str:
    return sign(params, user_agent, [0, 1, 8])


# ============================================================
# 公开接口: 替代 Node.js subprocess
# ============================================================

def sign_request(params: str, user_agent: str, method: str = 'sign_datail') -> str:
    """生成 a_bogus 签名。替代 Node.js subprocess 调用。"""
    fn = sign_reply if method == 'sign_reply' else sign_datail
    return fn(params, user_agent)


# ============================================================
# 兼容性测试: 与 Node.js 输出对比
# ============================================================

if __name__ == '__main__':
    import time

    test_params = 'device_platform=webapp&aid=6383&channel=channel_pc_web'
    test_ua = 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/132.0.0.0 Safari/537.36'

    print("=== 签名测速 ===")
    times = []
    for i in range(10):
        t0 = time.time()
        r = sign_request(test_params, test_ua, 'sign_datail')
        elapsed = (time.time() - t0) * 1000
        times.append(elapsed)
    print(f"  sign_datail: avg={sum(times)/len(times):.2f}ms  min={min(times):.2f}ms  max={max(times):.2f}ms")

    times = []
    for i in range(10):
        t0 = time.time()
        r = sign_request(test_params, test_ua, 'sign_reply')
        elapsed = (time.time() - t0) * 1000
        times.append(elapsed)
    print(f"  sign_reply:  avg={sum(times)/len(times):.2f}ms  min={min(times):.2f}ms  max={max(times):.2f}ms")
