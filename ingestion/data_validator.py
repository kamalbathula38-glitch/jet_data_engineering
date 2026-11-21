from ingestion.exceptions import DataQualityError


class CustomDataValidator:
    """Runs configurable business/data-quality expectations after schema validation."""

    def validate(self, records: list[dict], expectations: list[dict]) -> dict:
        results = []
        for expectation in expectations:
            try:
                column = expectation["column"]
                rule = expectation["rule"]
                if any(column not in record for record in records):
                    raise ValueError(f"Column '{column}' is absent from one or more records")
                values = [record[column] for record in records]

                if rule == "not_null":
                    failed = sum(value is None for value in values)
                elif rule == "unique":
                    non_null = [value for value in values if value is not None]
                    failed = len(non_null) - len(set(non_null))
                elif rule == "accepted_values":
                    accepted = expectation["values"]
                    failed = sum(value not in accepted for value in values)
                elif rule == "min":
                    threshold = expectation["value"]
                    failed = sum(value is None or value < threshold for value in values)
                elif rule == "max":
                    threshold = expectation["value"]
                    failed = sum(value is None or value > threshold for value in values)
                else:
                    raise ValueError(f"Unsupported expectation rule: {rule}")

                results.append({
                    "column": column,
                    "rule": rule,
                    "failed_records": failed,
                    "status": "PASS" if failed == 0 else "FAIL",
                })
            except (KeyError, TypeError) as exc:
                raise DataQualityError(f"Invalid expectation {expectation}: {exc}") from exc
            except ValueError as exc:
                raise DataQualityError(str(exc)) from exc

        failed = [result for result in results if result["status"] == "FAIL"]
        summary = {
            "total_expectations": len(results),
            "passed": len(results) - len(failed),
            "failed": len(failed),
            "results": results,
        }
        if failed:
            raise DataQualityError(f"Data quality validation failed: {summary}")
        return summary
