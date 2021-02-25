

CREATE TABLE executions(id BIGINT PRIMARY KEY, time_start TIMESTAMP DEFAULT(current_timestamp()), time_end TIMESTAMP, magic BIGINT NOT NULL DEFAULT(0));



