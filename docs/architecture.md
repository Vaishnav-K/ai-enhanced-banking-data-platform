\# Architecture



```text

Synthetic Banking Data

&#x20;         |

&#x20;         v

&#x20;    Raw CSV Files

&#x20;         |

&#x20;         v

&#x20;  DuckDB Ingestion

&#x20;         |

&#x20;         v

&#x20;Data Quality Validation

&#x20;         |

&#x20;         v

&#x20;  Deduplication

&#x20;         |

&#x20;         v

&#x20; +-------+--------+

&#x20; |                |

&#x20; v                v

Valid Records   Quarantined Records

&#x20; |                |

&#x20; v                v

Parquet Output  Rejection Reasons

&#x20; |

&#x20; v

SQL Analytics

&#x20; |

&#x20; v

Automated Unit Tests





Design Principles

\- Use synthetic data only.

\- Separate valid and rejected records.

\- Preserve rejection reasons for investigation.

\- Store processed datasets in Parquet format.

\- Validate outputs with automated tests.

\- Keep the pipeline runnable locally without paid cloud services.

