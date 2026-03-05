from fastapi.testclient import TestClient

from main import app

client = TestClient(app)


def _upload_dataset() -> str:
    csv_data = "region,revenue,units\nNA,10,1\nEU,20,2\nNA,30,3\n"
    files = {"file": ("sample.csv", csv_data, "text/csv")}
    response = client.post("/datasets/upload", files=files)
    assert response.status_code == 200
    return response.json()["dataset_id"]


def test_aggregate_groupby_sum() -> None:
    dataset_id = _upload_dataset()

    payload = {
        "group_by": ["region"],
        "aggregations": [
            {"column": "revenue", "function": "sum", "alias": "revenue_sum"},
            {"column": "units", "function": "count", "alias": "row_count"},
        ],
        "order_by": "region",
    }

    response = client.post(f"/datasets/{dataset_id}/aggregate", json=payload)
    assert response.status_code == 200

    body = response.json()
    assert body["columns"] == ["region", "revenue_sum", "row_count"]
    assert body["rows"] == [["EU", 20, 1], ["NA", 40, 2]]

    client.delete(f"/datasets/{dataset_id}")


def test_aggregate_unknown_column_returns_400() -> None:
    dataset_id = _upload_dataset()

    payload = {
        "group_by": ["missing"],
        "aggregations": [{"column": "revenue", "function": "sum"}],
    }

    response = client.post(f"/datasets/{dataset_id}/aggregate", json=payload)
    assert response.status_code == 400

    client.delete(f"/datasets/{dataset_id}")