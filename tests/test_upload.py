from fastapi.testclient import TestClient

from main import app

client = TestClient(app)


def test_health() -> None:
    response = client.get("/health")
    assert response.status_code == 200
    assert response.json() == {"status": "ok"}


def test_upload_and_delete_dataset() -> None:
    csv_data = "region,revenue\nNA,10\nEU,20\n"
    files = {"file": ("sample.csv", csv_data, "text/csv")}

    response = client.post("/datasets/upload", files=files)
    assert response.status_code == 200

    body = response.json()
    assert "dataset_id" in body
    assert body["row_count"] == 2
    assert set(body["columns"]) == {"region", "revenue"}

    delete_response = client.delete(f"/datasets/{body['dataset_id']}")
    assert delete_response.status_code == 200
    assert delete_response.json()["deleted"] is True