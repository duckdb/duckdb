-- Create test data for anonymization benchmarks
CREATE TABLE anonymization_test AS 
SELECT 
    i as id,
    'TestData' || i::VARCHAR as short_string,
    'This is a longer test string with more characters for testing performance ' || i::VARCHAR as long_string,
    'user' || i::VARCHAR || '@company.com' as email,
    'Sensitive' || i::VARCHAR || 'Data' || (i*123)::VARCHAR as sensitive_data,
    'MotörHead' || i::VARCHAR || '🦆🐱' as unicode_string
FROM range(1, 100001) as t(i);