# PySpark fan-out and skew lab

Four join problems from production lakehouses (duplicate keys, a hot key, a window over a weak key, and legitimate fan-out) rebuilt on synthetic data. Each runs three ways: as first written, with the fix people try first, and with the fix that addresses the cause. The benchmark runs in GitHub Actions; the results are the workflow artifact.

```bash
pip install -r requirements.txt   # needs Java 17
python -m lab.bench generate --rows 2000000 --data /tmp/lab-data
python -m lab.bench run --scenario duplicate_key --variant right --data /tmp/lab-data --out results.jsonl
python -m lab.summary results.jsonl
```
