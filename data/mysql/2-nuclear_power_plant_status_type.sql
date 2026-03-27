SET NAMES utf8mb4;
SET FOREIGN_KEY_CHECKS = 0;

DROP TABLE IF EXISTS `nuclear_power_plant_status_type`;
CREATE TABLE `nuclear_power_plant_status_type` (
  `id` tinyint UNSIGNED NOT NULL,
  `type` varchar(64) NOT NULL,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

INSERT INTO `nuclear_power_plant_status_type` VALUES (0, 'Unknown');
INSERT INTO `nuclear_power_plant_status_type` VALUES (1, 'Planned');
INSERT INTO `nuclear_power_plant_status_type` VALUES (2, 'Under Construction');
INSERT INTO `nuclear_power_plant_status_type` VALUES (3, 'Operational');
INSERT INTO `nuclear_power_plant_status_type` VALUES (4, 'Suspended Operation');
INSERT INTO `nuclear_power_plant_status_type` VALUES (5, 'Shutdown');
INSERT INTO `nuclear_power_plant_status_type` VALUES (6, 'Unfinished');
INSERT INTO `nuclear_power_plant_status_type` VALUES (7, 'Never Built');
INSERT INTO `nuclear_power_plant_status_type` VALUES (8, 'Suspended Construction');
INSERT INTO `nuclear_power_plant_status_type` VALUES (9, 'Cancelled Construction');
INSERT INTO `nuclear_power_plant_status_type` VALUES (10, 'Never Commissioned');
INSERT INTO `nuclear_power_plant_status_type` VALUES (11, 'Decommissioning Completed');

SET FOREIGN_KEY_CHECKS = 1;
