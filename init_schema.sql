use kafka;
create table event (
  event_id INT NOT NULL,
  event_timestamp VARCHAR(255) NOT NULL,
  service_code VARCHAR(255),
  event_context VARCHAR(255),
  PRIMARY KEY (event_id)
);