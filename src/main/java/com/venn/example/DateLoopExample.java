//package com.venn.example;
//
//import com.venn.utils.DateUtils;
//import java.time.LocalDate;
//
///**
// * 日期循环示例
// */
//public class DateLoopExample {
//
//    public static void main(String[] args) {
//        // 定义开始和结束日期
//        LocalDate startDate = LocalDate.of(2025, 3, 1);
//        LocalDate endDate = LocalDate.of(2025, 4, 18);
//
//        // 方法1：使用 while 循环
//        System.out.println("方法1：使用 while 循环");
//        LocalDate currentDate = startDate;
//        while (!currentDate.isAfter(endDate)) {
//            System.out.println("当前日期：" + DateUtils.formatDate(currentDate, DateUtils.DEFAULT_DATE_FORMAT));
//            currentDate = currentDate.plusDays(1);
//        }
//
//        // 方法2：使用 for 循环
//        System.out.println("\n方法2：使用 for 循环");
//        for (LocalDate date = startDate; !date.isAfter(endDate); date = date.plusDays(1)) {
//            System.out.println("当前日期：" + DateUtils.formatDate(date, DateUtils.DEFAULT_DATE_FORMAT));
//        }
//
//        // 方法3：使用 Stream API
//        System.out.println("\n方法3：使用 Stream API");
//        startDate.datesUntil(endDate.plusDays(1))
//                .forEach(date -> System.out.println("当前日期：" + DateUtils.formatDate(date, DateUtils.DEFAULT_DATE_FORMAT)));
//    }
//}