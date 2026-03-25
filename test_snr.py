import time, hmac, hashlib, urllib.request, json

SECRET = "9b1c4b6904911424a9585d4c77747d60bb70c9959b176925a41e534e237e987a"

ts  = str(int(time.time()))
msg = ts + "POST" + "/api/snr-test" + ""
sig = hmac.new(SECRET.encode(), msg.encode(), hashlib.sha256).hexdigest()
auth = f"HMAC {ts}.{sig}"

req = urllib.request.Request(
    "http://localhost:5000/api/snr-test",
    data=b"",
    method="POST",
    headers={"Authorization": auth, "Content-Type": "application/json"}
)

try:
    with urllib.request.urlopen(req, timeout=5) as r:
        result = json.loads(r.read())
        print("SUCCESS:", result)
except Exception as e:
    print("FAILED:", e)
