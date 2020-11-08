SELECT Switch.id AS sw
FROM Switch
LEFT JOIN monitoredBy ON monitoredBy.TrackElement_id = Switch.id
WHERE monitoredBy.TrackElement_id IS NULL;
