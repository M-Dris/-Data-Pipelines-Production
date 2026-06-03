INSERT INTO dead_letter_events (payload, error_type, original_topic)
VALUES ('{"user_id": null, "track_id": "invalid"}', 'missing_fields', 'listening_events');

SELECT status, COUNT(*) FROM dead_letter_events GROUP BY status;