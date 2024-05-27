## Datata Quality Checks
Ensuring data quality involves validating data against several dimensions to ensure it meets the necessary standard for accuracy, completeness, reliability and relevance
Ensuring data quality is crucial for reliable ETL processes. Here are best practices for data quality testing using PySpark, including examples with `pytest` where appropriate:

### Best Practices for Data Quality Testing

1. **Schema Validation**
2. **Null Value Checks**
3. **Unique Constraints**
4. **Range and Value Checks**
5. **Pattern Matching**
6. **Referential Integrity**
7. **Consistency Checks**
8. **Timeliness**
9. **Data Profiling**

### Examples

#### 1. Schema Validation
Ensure that the data conforms to the expected schema.

#### 2. Null Value Checks
Ensure that critical columns do not contain null values.

#### 3. Unique Constraints
Ensure that specific columns have unique values.

#### 4. Range and Value Checks
Ensure that numerical columns fall within a specified range.

#### 5. Pattern Matching
Ensure that string columns follow a specific pattern (e.g., email format).

#### 6. Referential Integrity
Ensure that foreign keys match primary keys in another table.

#### 7. Consistency Checks
Ensure that related columns are consistent (e.g., start date is before end date).

#### 8. Timeliness
Ensure that data is processed within expected time frames.

#### 9. Data Profiling
Generate summary statistics to understand the data distribution.
