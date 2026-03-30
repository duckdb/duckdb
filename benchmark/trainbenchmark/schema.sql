CREATE TABLE Route (
  id int NOT NULL,
  active int,
  entry int,
  exit int,
  PRIMARY KEY (id)
);

CREATE TABLE Region (
  id int NOT NULL,
  PRIMARY KEY (id)
);

CREATE TABLE Segment (
  id int NOT NULL,
  length int NOT NULL DEFAULT 1,
  PRIMARY KEY (id)
);

CREATE TABLE Sensor (
  id int NOT NULL,
  region int NOT NULL, -- inverse of the sensors edge
  PRIMARY KEY (id)
);

CREATE TABLE Semaphore (
  id int NOT NULL,
  segment int NOT NULL, -- inverse of the semaphores edge
  signal int NOT NULL,
  PRIMARY KEY (id)
);

CREATE TABLE Switch (
  id int NOT NULL,
  currentPosition int NOT NULL,
  PRIMARY KEY (id)
);

CREATE TABLE SwitchPosition (
  id int NOT NULL,
  route int, -- inverse of the follows edge
  target int,
  position int NOT NULL,
  PRIMARY KEY (id)
);

CREATE TABLE TrackElement (
  id int NOT NULL,
  region int NOT NULL, -- inverse of the elements edge
  PRIMARY KEY (id)
);

--
-- Edges
--

CREATE TABLE connectsTo (
  TrackElement1_id int NOT NULL,
  TrackElement2_id int NOT NULL,
  PRIMARY KEY (TrackElement1_id, TrackElement2_id)
);

CREATE TABLE monitoredBy (
  TrackElement_id int NOT NULL,
  Sensor_id int NOT NULL,
  PRIMARY KEY (TrackElement_id, Sensor_id)
);

CREATE TABLE requires (
  Route_id int NOT NULL,
  Sensor_id int NOT NULL,
  PRIMARY KEY (Route_id, Sensor_id)
);
