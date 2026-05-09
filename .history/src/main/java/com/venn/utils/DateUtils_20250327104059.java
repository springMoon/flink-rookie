package com.venn.utils;

import java.time.*;
import java.time.format.DateTimeFormatter;
import java.time.temporal.ChronoUnit;
import java.time.temporal.TemporalAdjusters;
import java.util.Date;

/**
 * 日期工具类
 */
public class DateUtils {
    
    /**
     * 默认日期格式
     */
    public static final String DEFAULT_DATE_FORMAT = "yyyy-MM-dd";
    
    /**
     * 默认时间格式
     */
    public static final String DEFAULT_TIME_FORMAT = "HH:mm:ss";
    
    /**
     * 默认日期时间格式
     */
    public static final String DEFAULT_DATETIME_FORMAT = "yyyy-MM-dd HH:mm:ss";

    /**
     * 将日期字符串转换为LocalDate
     *
     * @param dateStr 日期字符串
     * @param pattern 日期格式
     * @return LocalDate
     */
    public static LocalDate parseDate(String dateStr, String pattern) {
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern(pattern);
        return LocalDate.parse(dateStr, formatter);
    }

    /**
     * 将日期字符串转换为LocalDateTime
     *
     * @param dateTimeStr 日期时间字符串
     * @param pattern 日期格式
     * @return LocalDateTime
     */
    public static LocalDateTime parseDateTime(String dateTimeStr, String pattern) {
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern(pattern);
        return LocalDateTime.parse(dateTimeStr, formatter);
    }

    /**
     * 将LocalDate格式化为字符串
     *
     * @param date LocalDate
     * @param pattern 日期格式
     * @return 格式化后的日期字符串
     */
    public static String formatDate(LocalDate date, String pattern) {
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern(pattern);
        return date.format(formatter);
    }

    /**
     * 将LocalDateTime格式化为字符串
     *
     * @param dateTime LocalDateTime
     * @param pattern 日期格式
     * @return 格式化后的日期时间字符串
     */
    public static String formatDateTime(LocalDateTime dateTime, String pattern) {
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern(pattern);
        return dateTime.format(formatter);
    }

    /**
     * 获取两个日期之间的天数
     *
     * @param startDate 开始日期
     * @param endDate 结束日期
     * @return 天数
     */
    public static long getDaysBetween(LocalDate startDate, LocalDate endDate) {
        return ChronoUnit.DAYS.between(startDate, endDate);
    }

    /**
     * 获取两个日期时间之间的分钟数
     *
     * @param startDateTime 开始日期时间
     * @param endDateTime 结束日期时间
     * @return 分钟数
     */
    public static long getMinutesBetween(LocalDateTime startDateTime, LocalDateTime endDateTime) {
        return ChronoUnit.MINUTES.between(startDateTime, endDateTime);
    }

    /**
     * 获取指定日期的开始时间（00:00:00）
     *
     * @param date 日期
     * @return LocalDateTime
     */
    public static LocalDateTime getStartOfDay(LocalDate date) {
        return date.atStartOfDay();
    }

    /**
     * 获取指定日期的结束时间（23:59:59）
     *
     * @param date 日期
     * @return LocalDateTime
     */
    public static LocalDateTime getEndOfDay(LocalDate date) {
        return date.atTime(LocalTime.MAX);
    }

    /**
     * 获取指定日期所在月份的第一天
     *
     * @param date 日期
     * @return LocalDate
     */
    public static LocalDate getFirstDayOfMonth(LocalDate date) {
        return date.with(TemporalAdjusters.firstDayOfMonth());
    }

    /**
     * 获取指定日期所在月份的最后一天
     *
     * @param date 日期
     * @return LocalDate
     */
    public static LocalDate getLastDayOfMonth(LocalDate date) {
        return date.with(TemporalAdjusters.lastDayOfMonth());
    }

    /**
     * 判断日期是否在指定范围内
     *
     * @param date 待判断的日期
     * @param startDate 开始日期
     * @param endDate 结束日期
     * @return 是否在范围内
     */
    public static boolean isDateInRange(LocalDate date, LocalDate startDate, LocalDate endDate) {
        return !date.isBefore(startDate) && !date.isAfter(endDate);
    }

    /**
     * 将Date转换为LocalDateTime
     *
     * @param date Date对象
     * @return LocalDateTime
     */
    public static LocalDateTime dateToLocalDateTime(Date date) {
        return LocalDateTime.ofInstant(date.toInstant(), ZoneId.systemDefault());
    }

    /**
     * 将LocalDateTime转换为Date
     *
     * @param dateTime LocalDateTime对象
     * @return Date
     */
    public static Date localDateTimeToDate(LocalDateTime dateTime) {
        return Date.from(dateTime.atZone(ZoneId.systemDefault()).toInstant());
    }

    /**
     * 获取当前时间戳（毫秒）
     *
     * @return 时间戳
     */
    public static long getCurrentTimestamp() {
        return System.currentTimeMillis();
    }

    /**
     * 获取指定时区的当前时间
     *
     * @param zoneId 时区ID
     * @return LocalDateTime
     */
    public static LocalDateTime getCurrentDateTime(ZoneId zoneId) {
        return LocalDateTime.now(zoneId);
    }
} 