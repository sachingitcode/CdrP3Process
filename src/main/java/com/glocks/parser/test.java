package com.glocks.parser;

public class test {
    public static void main(String[] args) {
        twoSum(new int[]{4, 2, 5, 6, 7, 5, 0,4, 2, 6, 3, 2}, 5);
    }

    public static void twoSum(int[] nums, int target) {
        for (int i = 0; i < nums.length - 1; i++) {
            if (nums[i] <= target && target == nums[i] + nums[i + 1]) {
                System.out.println("ITS " + nums[i] + " : : " + nums[i + 1]);
            }
        }
    }
}
