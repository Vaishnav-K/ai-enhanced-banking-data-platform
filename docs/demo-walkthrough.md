# Demo Walkthrough

## Objective

Demonstrate a synthetic banking data platform that combines ETL, data quality, anomaly detection, lineage, operational diagnostics, and dashboard reporting.

## 1. Generate or refresh the dashboard

From the repository root, run:

```powershell
.\.venv312\Scripts\python.exe src\platform_dashboard.py
```

The default output is:

`reports/platform_health_dashboard.html`

Open that HTML file in a browser to present the executive view.

## 2. Walk through the dashboard

1. **Pipeline KPIs:** explain the recorded run count, success rate, and available duration metrics.
2. **Data quality:** show which scorecard dimensions have actual recorded evidence and which remain unavailable.
3. **Regression detection:** explain how comparable observations are used to identify potential quality deterioration.
4. **Recovery and lineage:** show how failed stages and artifact discrepancies inform investigation recommendations.
5. **Prioritized actions:** explain how the summary directs attention to the issues supported by recorded evidence.
6. **Raw JSON:** expand the evidence payload to demonstrate that the displayed metrics are traceable to a structured report.

## 3. Explain the engineering decisions

- Synthetic data avoids dependence on real banking customer information.
- SQLite-backed metadata supports diagnostic and historical reporting.
- Incremental processing avoids treating every run as a full reload.
- Lineage reconciliation compares declared expectations with observed artifacts.
- Quality regressions require comparable historical observations.
- Missing metrics are reported as unavailable rather than silently converted to passing scores.
- Diagnostic recommendations are separate from job execution; the dashboard does not restart or remediate jobs.

## 4. Be transparent about limitations

Distinguish local demonstration results from production deployment. Explain which stages are instrumented, whether sufficient historical reports exist for trend analysis, and which integrations have been exercised end to end.

Do not claim production-scale performance, automatic recovery, or complete observability unless supported by additional implementation and test evidence.

## Suggested interview narrative

"I built a synthetic banking data platform with ETL and incremental processing, data quality validation, anomaly detection, and lineage tracking. I then extended it with runtime reconciliation, recovery diagnostics, performance analytics, and a consolidated dashboard. The focus is not just processing data: it is making failures explainable, measuring quality using available evidence, and identifying the next investigation step safely."
