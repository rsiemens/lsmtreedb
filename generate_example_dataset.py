import json
import sys
import uuid

from faker import Faker

records = []
if __name__ == "__main__":
    fake = Faker()
    n = int(sys.argv[1])
    with open("example_transactions.jl", "wb") as f:
        for i in range(n):
            uid = str(uuid.uuid4())
            record = {
                "id": uid,
                "name": fake.name(),
                "address": fake.address(),
                "country": fake.country(),
                "job": fake.job(),
            }
            records.append(record)
            data = json.dumps(record) + "\n"
            f.write(data.encode("utf8"))

        # generate a bunch of updates to records that should be compacted by the compactor
        for i, record in enumerate(records):
            if i % 2 == 0:
                record["updated"] = True
                data = json.dumps(record) + "\n"
                f.write(data.encode("utf8"))

    with open("expected_state.jl", "wb") as f:
        for record in records:
            data = json.dumps(record) + "\n"
            f.write(data.encode("utf8"))
