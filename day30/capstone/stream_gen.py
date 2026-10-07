import random, time, datetime, os
os.makedirs("stream_in", exist_ok=True)
for b in range(12):
    rows = []
    for i in range(20):
        now = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        rows.append(f"S{b}_{i},C{random.randint(1,200)},P{random.randint(1,12)},{random.randint(1,5)},{round(random.uniform(200,3000),2)},{now}")
    tmp = f"stream_in/.tmp_{b}"
    with open(tmp, "w") as f: f.write("\n".join(rows) + "\n")
    os.rename(tmp, f"stream_in/batch_{b}.csv")   # atomic: Spark ignores hidden files
    time.sleep(5)
