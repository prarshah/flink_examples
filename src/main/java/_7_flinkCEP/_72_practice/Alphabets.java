package _7_flinkCEP._72_practice;

import java.util.Random;

public class Alphabets {

    private char alphabet;
    private int ps = 31*(alphabet)*(new Random(10)).hashCode();

    Alphabets(String alp)
    {
        alphabet =alp.charAt(0);
    }

    public void setAlphabet(char alphabet) {
        this.alphabet = alphabet;
    }

    public char getAlphabet() {
        return alphabet;
    }

    @Override
    public int hashCode() {
        return ps;
    }

    @Override
    public String toString() {
        return ""+alphabet;
    }

    @Override
    public boolean equals(Object o) {
        return (o.hashCode() ==  ps);
    }
}
