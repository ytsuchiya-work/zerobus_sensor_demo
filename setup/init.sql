-- Zerobus Sensor Data Demo: Table Setup
CREATE SCHEMA IF NOT EXISTS classic_stable_ytcy_catalog.zerobus;

CREATE TABLE IF NOT EXISTS classic_stable_ytcy_catalog.zerobus.sensor_data (
  id INT,
  device STRING,
  payload VARIANT
);
