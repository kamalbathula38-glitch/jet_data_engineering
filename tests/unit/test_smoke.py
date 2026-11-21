from ingestion.data_validator import CustomDataValidator


def test_empty_expectations_pass():
    assert CustomDataValidator().validate([], []) == {
        "total_expectations": 0,
        "passed": 0,
        "failed": 0,
        "results": [],
    }
