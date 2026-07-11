# Framework Flowchart Documentation

## 1. HIGH-LEVEL EXECUTION FLOW

```mermaid
graph TD
    Start([User/Scheduler Triggers Extraction]) --> LoadPipeline[Load Pipeline Configuration from PostgreSQL]
    LoadPipeline --> ValidateConfig{Configuration Valid?}
    ValidateConfig -->|No| ErrorValidation[Return Configuration Error]
    ValidateConfig -->|Yes| CreateConnector[Create Data Connector via Factory]
    
    CreateConnector --> ConnectSource[Connect to Source System]
    ConnectSource --> TestConnection{Connection OK?}
    TestConnection -->|No| Retry1{Retry < 5?}
    Retry1 -->|Yes| Wait1[Wait 1-16s Exponential Backoff]
    Wait1 --> ConnectSource
    Retry1 -->|No| CircuitBreaker[Open Circuit Breaker]
    CircuitBreaker --> ErrorConnection[Return Connection Error]
    
    TestConnection -->|Yes| ExtractData[Extract Data from Source]
    ExtractData --> ValidateQuality[Apply Data Quality Rules]
    ValidateQuality --> QualityCheck{Quality OK?}
    QualityCheck -->|REJECT| QuarantineData[Move Bad Records to Dead Letter]
    QualityCheck -->|QUARANTINE| ContinueExtract
    QualityCheck -->|PASS| ContinueExtract[Continue Processing]
    
    ContinueExtract --> TransformData[Transform Data per Metadata Rules]
    TransformData --> StoreS3[Store to S3 Raw Layer]
    StoreS3 --> UpdateMetadata[Update Metadata & Checkpoint]
    UpdateMetadata --> LogSuccess[Log Execution Success]
    LogSuccess --> Return[Return ExecutionResponse]
    Return --> End([Execution Complete])
    
    ErrorValidation --> End
    ErrorConnection --> End
    
    style Start fill:#90EE90
    style End fill:#FFB6C6
    style ValidateConfig fill:#87CEEB
    style TestConnection fill:#87CEEB
    style QualityCheck fill:#FFD700
```

---

## 2. DETAILED PIPELINE CONTROLLER FLOW

```mermaid
graph TD
    A[Pipeline Controller Invoked] --> B[Load Pipeline Metadata]
    B --> C[Create Execution Log Entry]
    C --> D[Validate Configuration]
    
    D --> E{Config Valid?}
    E -->|No| E1[Log Error]
    E -->|Yes| F[Create Connector]
    
    F --> G[Connect to Source]
    G --> H{Connected?}
    H -->|No| H1[Retry with Backoff]
    H1 --> H2{Max Retries?}
    H2 -->|No| G
    H2 -->|Yes| H3[Fail Execution]
    
    H -->|Yes| I[Execute Extraction Query]
    I --> J[Stream Data in Batches]
    J --> K[Apply Quality Validations]
    K --> L[Transform Data]
    L --> M[Upload to S3]
    M --> N[Update Checkpoints]
    N --> O[Log Success Metrics]
    
    O --> P[Disconnect Resources]
    P --> Q[Return Response]
    
    E1 --> Q
    H3 --> Q
    
    Q --> R[End]
    
    style A fill:#FFB6C1
    style R fill:#FFB6C1
    style I fill:#87CEEB
    style K fill:#FFD700
    style L fill:#DDA0DD
    style M fill:#98FB98
```

---

## 3. DATA EXTRACTION FLOW (By Source Type)

```mermaid
graph TD
    A[Connector.execute_query] --> B{Source Type?}
    
    B -->|SQL Server/Oracle| C[SQL Extraction]
    C --> C1[Execute Parameterized Query]
    C1 --> C2[Enable Row Number Pagination]
    C2 --> C3[Stream in 100K Chunks]
    C3 --> C4[Apply Type Conversions]
    C4 --> C5[Return DataFrame]
    
    B -->|MongoDB| D[MongoDB Extraction]
    D --> D1[Query Collection]
    D1 --> D2[Apply Filter from Metadata]
    D2 --> D3[Stream Documents]
    D3 --> D4[Flatten Nested Structures]
    D4 --> D5[Convert ObjectID to String]
    D5 --> D6[Return DataFrame]
    
    B -->|CSV/Excel| E[File Extraction]
    E --> E1[Locate File]
    E1 --> E2[Read in Chunks]
    E2 --> E3[Apply Column Mapping]
    E3 --> E4[Type Convert]
    E4 --> E5[Return DataFrame]
    
    B -->|REST API| F[API Extraction]
    F --> F1[Build Request URL]
    F1 --> F2[Add Authentication]
    F2 --> F3[Enable Pagination]
    F3 --> F4{Next Page?}
    F4 -->|Yes| F5[Make HTTP Request]
    F5 --> F6[Handle Rate Limiting]
    F6 --> F7[Parse Response]
    F7 --> F4
    F4 -->|No| F8[Combine All Pages]
    F8 --> F9[Return DataFrame]
    
    C5 --> G[Return to Controller]
    D6 --> G
    E5 --> G
    F9 --> G
    
    style C fill:#87CEEB
    style D fill:#87CEEB
    style E fill:#87CEEB
    style F fill:#87CEEB
```

---

## 4. DATA TRANSFORMATION FLOW

```mermaid
graph TD
    A[Input: Raw DataFrame] --> B[Step 1: Rename Columns]
    B --> C[Step 2: Type Conversions]
    C --> D[Step 3: Standardization]
    D --> E[Step 4: Enrichment/Joins]
    E --> F[Step 5: Apply Business Rules]
    F --> G[Step 6: PII Masking]
    G --> H[Step 7: Deduplication]
    H --> I[Output: Clean DataFrame]
    
    B --> B1["RENAME: customer_id → cust_id"]
    C --> C1["CONVERT: '2024-01-15' → DATE"]
    D --> D1["UPPERCASE, TRIM, FORMAT"]
    E --> E1["JOIN with dimensions"]
    F --> F1["FILTER: Keep valid rows"]
    G --> G1["HASH phone, REDACT ssn"]
    H --> H1["GROUP BY key, KEEP LATEST"]
    
    style A fill:#FFB6C1
    style I fill:#90EE90
    style B fill:#DDA0DD
    style C fill:#DDA0DD
    style D fill:#DDA0DD
    style E fill:#DDA0DD
    style F fill:#DDA0DD
    style G fill:#FFD700
    style H fill:#FFD700
```

---

## 5. DATA QUALITY VALIDATION FLOW

```mermaid
graph TD
    A[Input: Extracted Data] --> B[For Each Validation Rule]
    B --> C{Validation Type}
    
    C -->|NULL_CHECK| C1["Check for NULLs in field"]
    C1 --> C2{"Violation % > Threshold?"}
    
    C -->|RANGE_CHECK| D["Check: Min <= value <= Max"]
    D --> D2{"Violation %?"}
    
    C -->|REGEX| E["Apply regex pattern"]
    E --> E2{"Match fails %?"}
    
    C -->|UNIQUENESS| F["Count distinct values"]
    F --> F2{"Duplicates > threshold?"}
    
    C2 -->|Yes| G{Action}
    D2 -->|Yes| G
    E2 -->|Yes| G
    F2 -->|Yes| G
    
    C2 -->|No| H["Pass ✓"]
    D2 -->|No| H
    E2 -->|No| H
    F2 -->|No| H
    
    G -->|REJECT| G1["Reject all records"]
    G -->|QUARANTINE| G2["Save bad records"]
    G -->|FLAG| G3["Mark records, continue"]
    G -->|WARN| G4["Log warning, continue"]
    
    G1 --> I["Execution FAILED"]
    G2 --> J["Record quality issue"]
    G3 --> J
    G4 --> J
    H --> J
    
    J --> K["Continue to transformation"]
    I --> L["Stop execution"]
    
    style A fill:#FFB6C1
    style C fill:#87CEEB
    style H fill:#90EE90
    style I fill:#FFB6C6
```

---

## 6. S3 STORAGE FLOW

```mermaid
graph TD
    A[Transformed DataFrame] --> B[Determine Storage Parameters]
    B --> C["Partition columns: [date, source_system]"]
    C --> D["File format: PARQUET"]
    D --> E["Compression: SNAPPY"]
    
    E --> F[Build S3 Path]
    F --> F1["s3://data-lake/raw/sqlserver_prod/customers/2024-01-15/exec_12345/"]
    
    F1 --> G[Write Partitioned Dataset]
    G --> G1["If partitions: Create directory structure"]
    G1 --> G2["Write PARQUET files ~256MB each"]
    G2 --> G3["Apply compression"]
    G3 --> G4["Write metadata row groups"]
    G4 --> G5["Create _SUCCESS marker"]
    
    G5 --> H[Add Manifest File]
    H --> H1["JSON: source, timestamp, record_count"]
    H1 --> H2["Add checksums for integrity"]
    
    H2 --> I[Update Metadata Database]
    I --> I1["Insert into data_assets table"]
    I1 --> I2["Record s3_location, file_count"]
    I2 --> I3["Store lifecycle_stage: ACTIVE"]
    
    I3 --> J[Return Storage Result]
    J --> K[End]
    
    style A fill:#FFB6C1
    style K fill:#90EE90
    style G2 fill:#98FB98
    style H1 fill:#DDA0DD
```

---

## 7. ERROR HANDLING & RETRY FLOW

```mermaid
graph TD
    A[Error Occurs] --> B{Error Type}
    
    B -->|CONNECTION| C["Transient Error?"]
    B -->|TIMEOUT| C
    B -->|QUERY| D["Permanent Error?"]
    B -->|VALIDATION| E["Configuration Error"]
    
    C -->|Yes| C1[Retrieve Retry Count]
    C1 --> C2{Retry < Max?}
    C2 -->|Yes| C3[Calculate Backoff]
    C3 --> C4["Delay: 1s, 2s, 4s, 8s, 16s"]
    C4 --> C5[Retry Operation]
    C5 --> C6{Success?}
    C6 -->|Yes| C7["Continue ✓"]
    C6 -->|No| C2
    C2 -->|No| C8["Fail after retries"]
    
    D -->|Yes| D1[Log Error]
    D1 --> D2[Fail Immediately]
    
    E --> E1[Fail Immediately]
    E1 --> E2[No retry attempt]
    
    C8 --> F[Check Circuit Breaker]
    D2 --> F
    E2 --> F
    
    F --> F1{Failures > Threshold?}
    F1 -->|Yes| F2[Open Circuit Breaker]
    F2 --> F3["Future calls fail fast"]
    F1 -->|No| F4["Circuit CLOSED"]
    
    F3 --> G[Insert into error_log]
    F4 --> G
    C7 --> G
    
    G --> H[Return Error Response]
    H --> I[End]
    
    style A fill:#FFB6C6
    style C7 fill:#90EE90
    style C8 fill:#FFB6C6
    style F2 fill:#FF6B6B
```

---

## 8. AUDIT LOGGING FLOW

```mermaid
graph TD
    A[Pipeline Execution Starts] --> B[Create execution_log Entry]
    B --> B1["execution_id, pipeline_id, status=RUNNING"]
    
    A --> C[For Each Execution Step]
    C --> C1["Record step_name, step_status, duration"]
    C1 --> D["INSERT into execution_details"]
    
    A --> E[If Error Occurs]
    E --> E1["Record error_type, error_message"]
    E1 --> E2["INSERT into error_log"]
    
    A --> F[Data Quality Check]
    F --> F1["Record validation_results"]
    F1 --> F2["INSERT into data_quality_log"]
    
    A --> G[Data Lineage]
    G --> G1["Record source → target mapping"]
    G1 --> G2["INSERT into data_lineage"]
    
    A --> H[Pipeline Completion]
    H --> H1["Record end_time, status=SUCCESS/FAILED"]
    H1 --> H2["Calculate duration, quality_score"]
    H2 --> H3["UPDATE execution_log"]
    
    H3 --> I[Performance Metrics]
    I --> I1["connection_time, query_time, transform_time, upload_time"]
    I1 --> I2["INSERT into performance_metrics"]
    
    I2 --> J[Send Notification]
    J --> J1["Email, Slack, Teams based on config"]
    
    J1 --> K[Log Audit Trail Complete]
    K --> L[End]
    
    style A fill:#FFB6C1
    style D fill:#87CEEB
    style E2 fill:#FFD700
    style F2 fill:#FFD700
    style G2 fill:#DDA0DD
    style I2 fill:#98FB98
```

---

## 9. COMPLETE END-TO-END FLOW (Timeline)

```
Timeline: SQL Server Extraction Pipeline
═════════════════════════════════════════

T+00s  ├─ Request arrives: extract_pipeline(pipeline_id=1)
       │
T+01s  ├─ Load metadata from PostgreSQL
       │  └─ Pipeline configuration
       │  └─ Connection details
       │  └─ Field mappings
       │  └─ Transformations
       │  └─ Validations
       │
T+02s  ├─ Create execution log entry
       │  └─ execution_id=12345
       │  └─ status=RUNNING
       │
T+03s  ├─ Validate configuration
       │  └─ Check SQL syntax
       │  └─ Check S3 path format
       │  └─ Check retention policy
       │  └─ Result: VALID ✓
       │
T+05s  ├─ Create SQL Server connector
       │  └─ Retrieve encrypted credentials
       │  └─ Initialize PyODBC
       │  └─ Connector ready
       │
T+07s  ├─ Connect to SQL Server
       │  ├─ Connection attempt #1: SUCCESS
       │  └─ Connection pool size: 10
       │
T+08s  ├─ Validate connection
       │  ├─ Execute: SELECT 1
       │  └─ Result: Connection OK ✓
       │
T+10s  ├─ Execute extraction query
       │  ├─ Query: SELECT * FROM customers WHERE modified_date > '2024-01-14'
       │  ├─ Enable pagination: 100K rows per batch
       │  └─ Batch 1/100: Processing...
       │
T+04m30s ├─ Extraction complete
        │  ├─ Total records: 10,000,000
        │  ├─ Batches processed: 100
        │  └─ Duration: 270 seconds
        │
T+04m45s ├─ Data quality validation
        │  ├─ Validation 1 (NULL checks): PASS
        │  ├─ Validation 2 (Email regex): PASS (5 quarantined)
        │  ├─ Validation 3 (Uniqueness): PASS
        │  ├─ Overall quality score: 99.95%
        │  └─ Action: CONTINUE ✓
        │
T+05m15s ├─ Data transformations
        │  ├─ Standardize dates: 30s
        │  ├─ Standardize names: 45s
        │  ├─ Mask PII (phone, ssn): 20s
        │  ├─ Join with dimensions: 15s
        │  ├─ Deduplication: 10s
        │  └─ Total: 120 seconds
        │
T+06m35s ├─ S3 upload
        │  ├─ Path: s3://data-lake/raw/sqlserver_prod/customers/2024-01-15/exec_12345/
        │  ├─ Files: 40 Parquet files
        │  ├─ Compression: Snappy
        │  ├─ Size: ~10GB
        │  └─ Duration: 80 seconds
        │
T+06m55s ├─ Update metadata
        │  ├─ data_assets table: INSERT
        │  ├─ checkpoint table: UPDATE (for next incremental)
        │  └─ Duration: 20 seconds
        │
T+07m05s ├─ Audit logging
        │  ├─ execution_log: status=SUCCESS
        │  ├─ execution_details: All steps recorded
        │  ├─ performance_metrics: Recorded
        │  ├─ data_lineage: Recorded
        │  └─ Duration: 10 seconds
        │
T+07m10s ├─ Send notification
        │  ├─ Success notification: Sent to data-team@company.com
        │  └─ Details: 10M records extracted in 9m 50s
        │
T+07m15s └─ EXECUTION COMPLETE ✓
          └─ Return ExecutionResponse
             ├─ execution_id: 12345
             ├─ status: SUCCESS
             ├─ total_records: 10,000,000
             ├─ quality_score: 99.95%
             └─ s3_location: s3://data-lake/raw/...
```

---

## 10. COMPARISON: Direct Access vs Framework

```
DIRECT DATABASE ACCESS:
┌──────────────┐
│ Application  │
└──────┬───────┘
       │ SQL Query
       │ Direct Connection
       │ No Abstraction
       ▼
    [DATABASE] ← VULNERABLE!
    - Multiple concurrent connections
    - No audit trail
    - SQL injection risk
    - Connection strings in code
    - Difficult to scale


FRAMEWORK-BASED ACCESS:
┌──────────────────────────┐
│ User/Scheduler Request   │
└──────────┬───────────────┘
           │
    ┌──────▼──────────┐
    │ Controller      │
    │ (Gate Keeper)   │
    └──────┬──────────┘
           │
    ┌──────▼──────────────────────────┐
    │ Service Layer (Authorization)   │
    │ • Load metadata                 │
    │ • Verify permissions            │
    │ • Load credentials (encrypted)  │
    │ • Create connector              │
    └──────┬───────────────────────────┘
           │
    ┌──────▼──────────────────────┐
    │ Single Connector            │
    │ • Connection pooling        │
    │ • Parameterized queries     │
    │ • Automatic retry           │
    │ • Resource cleanup          │
    └──────┬────────────────────┬─────┐
           │                    │     │
     ┌─────▼──┐        ┌──────▼──┐  ┌▼──────┐
     │[SQL]   │        │[ORACLE] │  │[MONGO]│
     └────────┘        └─────────┘  └───────┘
           │                    │     │
           └──────────────┬─────┴─────┘
                          │
                  ┌───────▼────────────┐
                  │ Audit Service      │
                  │ • Log all access   │
                  │ • Record lineage   │
                  │ • Track metrics    │
                  └────────────────────┘

PROTECTED!
- Single point of control
- Complete audit trail
- No SQL injection possible
- Credentials never exposed
- Easy to scale and monitor
```

---

## 11. KEY DECISION POINTS

```
Pipeline Execution Decision Tree

START
  │
  ├─ Configuration Valid?
  │  ├─ NO  → ERROR: Return config error
  │  └─ YES → Continue
  │
  ├─ Can Connect to Source?
  │  ├─ NO (transient)   → RETRY (exponential backoff)
  │  ├─ NO (persistent)  → ERROR: Return connection error
  │  └─ YES              → Continue
  │
  ├─ Data Quality OK?
  │  ├─ NO (REJECT)      → ERROR: Reject entire dataset
  │  ├─ NO (QUARANTINE)  → Continue (bad records isolated)
  │  ├─ NO (FLAG)        → Continue (records marked)
  │  └─ YES              → Continue
  │
  ├─ Transformation Successful?
  │  ├─ NO  → ERROR: Log and fail
  │  └─ YES → Continue
  │
  ├─ S3 Upload Successful?
  │  ├─ NO  → ERROR: Retry, then fail
  │  └─ YES → Continue
  │
  ├─ Metadata Update Successful?
  │  ├─ NO  → WARNING: Data loaded but metadata not updated
  │  └─ YES → Continue
  │
  └─ SUCCESS: Execution complete
     └─ Return ExecutionResponse with metrics
```

---

**Flowchart Version**: 1.0  
**Status**: Production Ready  
**Updated**: January 2024
