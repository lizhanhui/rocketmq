package org.apache.rocketmq.example.simple;

public class Test {
    public static void main(String[] args) {
        String s = "[a-z]{1,6}_?[0-9]{0,4}@hackrank\\.com$";
        System.out.println("julia@hackrank.com".matches(s));
    }
}
