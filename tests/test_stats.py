from fastapi.testclient import TestClient

from main import app

client = TestClient(app)


def _upload_dataset() -> str:
    csv_data = "region,revenue\nNA,10\nEU,20\nNA,30\n"
    files = {"file": ("sample.csv", csv_data, "text/csv")}
    response = client.post("/datasets/upload", files=files)
    assert response.status_code == 200
    return response.json()["dataset_id"]


def test_describe_endpoint() -> None:
    dataset_id = _upload_dataset()

    response = client.post(f"/datasets/{dataset_id}/describe", json={"columns": ["revenue"]})
    assert response.status_code == 200
    body = response.json()
    assert body["columns"]
    assert body["row_count"] > 0

    client.delete(f"/datasets/{dataset_id}")


def test_value_counts_endpoint() -> None:
    dataset_id = _upload_dataset()

    response = client.get(f"/datasets/{dataset_id}/value-counts", params={"column": "region"})
    assert response.status_code == 200

    body = response.json()
    assert body["columns"] == ["region", "count"]
    assert body["rows"][0][1] >= body["rows"][1][1]

    client.delete(f"/datasets/{dataset_id}")