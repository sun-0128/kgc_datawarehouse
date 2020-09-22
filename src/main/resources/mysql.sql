DROP DATABASE IF EXISTS exp;
CREATE DATABASE IF NOT EXISTS exp;
USE exp;
/*
Navicat MySQL Data Transfer

Source Server         : Mysql
Source Server Version : 50520
Source Host           : localhost:3306
Source Database       : exp

Target Server Type    : MYSQL
Target Server Version : 50520
File Encoding         : 65001

Date: 2020-03-04 22:16:01
*/

SET FOREIGN_KEY_CHECKS=0;

-- ----------------------------
-- Table structure for forcast_volume
-- ----------------------------
DROP TABLE IF EXISTS `forcast_volume`;
CREATE TABLE `forcast_volume` (
  `waybill_number` varchar(255) DEFAULT NULL,
  `old_bill_time` varchar(255) DEFAULT NULL,
  `car_number` varchar(255) DEFAULT NULL,
  `bill_time` varchar(255) DEFAULT NULL,
  `goods_weight` varchar(255) DEFAULT NULL,
  `goods_volume` varchar(255) DEFAULT NULL,
  `goods_qty` varchar(255) DEFAULT NULL,
  `bill_weight` varchar(255) DEFAULT NULL,
  `bill_volume` varchar(255) DEFAULT NULL,
  `bill_qty` varchar(255) DEFAULT NULL,
  `leave_department_code` varchar(255) DEFAULT NULL,
  `arrive_department_code` varchar(255) DEFAULT NULL,
  `customer_pickup_department_code` varchar(255) DEFAULT NULL,
  `start_time` varchar(255) DEFAULT NULL,
  `end_time` varchar(255) DEFAULT NULL,
  `line_type` varchar(255) DEFAULT NULL,
  `waybill_type` varchar(255) DEFAULT NULL,
  `waybill_status` varchar(255) DEFAULT NULL,
  `next_day_arrive` varchar(255) DEFAULT NULL,
  `pre_org_code` varchar(255) DEFAULT NULL,
  `pre_org_name` varchar(255) DEFAULT NULL,
  `path_no` varchar(255) DEFAULT NULL,
  `depart_org_transfer` varchar(255) DEFAULT NULL,
  `arrive_org_transfer` varchar(255) DEFAULT NULL,
  `refresh_time` varchar(255) DEFAULT NULL,
  `create_time` varchar(255) DEFAULT NULL
) ENGINE=MyISAM DEFAULT CHARSET=utf8;

-- ----------------------------
-- Table structure for loadwarning_waybill_result
-- ----------------------------
DROP TABLE IF EXISTS `loadwarning_waybill_result`;
CREATE TABLE `loadwarning_waybill_result` (
  `waybill_no` varchar(255) DEFAULT NULL,
  `unload_org_code` varchar(255) DEFAULT NULL,
  `unload_time` varchar(255) NULL DEFAULT NULL,
  `tray_time` varchar(255) DEFAULT NULL,
  `sort_org_code` varchar(255) DEFAULT NULL,
  `sort_time` varchar(255) DEFAULT NULL,
  `expect_place` varchar(255) DEFAULT NULL,
  `refresh_time` varchar(255) DEFAULT NULL
) ENGINE=MyISAM DEFAULT CHARSET=utf8;

